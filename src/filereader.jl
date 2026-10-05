# Parquet file reader

struct ParquetFile
    data::Vector{UInt8}   # Mmap.mmap'd file contents
    path::String
    metadata::FileMetaData
end

function read_footer(data::Vector{UInt8})::FileMetaData
    file_size = length(data)
    file_size < 12 && error("File too small")

    data[end-3:end] == PARQUET_MAGIC || error("Missing trailing magic")

    footer_len = ltoh(reinterpret(UInt32, data[end-7:end-4])[1])
    footer_len > file_size - 12 && error("Invalid footer length")

    data[1:4] == PARQUET_MAGIC || error("Missing leading magic")

    parse_file_metadata(data[end-7-footer_len:end-8])
end

function open_parquet(path::String)::ParquetFile
    # Checked first: Mmap.mmap creates an empty file at a path that does not exist
    isfile(path) || throw(SystemError("opening file $(repr(path))", Libc.ENOENT))
    data = Mmap.mmap(path)
    ParquetFile(data, path, read_footer(data))
end

Base.close(::ParquetFile) = nothing

num_rows(pf::ParquetFile) = pf.metadata.num_rows
num_row_groups(pf::ParquetFile) = length(pf.metadata.row_groups)
schema(pf::ParquetFile) = pf.metadata.schema
metadata(pf::ParquetFile) = pf.metadata

"""
    column_names(pf::ParquetFile) -> Vector{String}

The file's columns as `read_parquet` returns them: the top-level fields of the schema.
"""
column_names(pf::ParquetFile)::Vector{String} =
    [child.element.name for child in build_schema_tree(pf.metadata.schema).children]

function build_schema_tree(schema::Vector{SchemaElement})::SchemaNode
    isempty(schema) && error("Empty schema")

    function build(idx, def, rep)
        elem = schema[idx]

        # Calculate this node's contribution to def/rep levels. The root is not a field:
        # whatever repetition a writer gives it does not count.
        adds_def = idx > 1 && elem.repetition_type in (OPTIONAL, REPEATED) ? 1 : 0
        adds_rep = idx > 1 && elem.repetition_type == REPEATED ? 1 : 0

        new_def = def + adds_def
        new_rep = rep + adds_rep

        children = SchemaNode[]
        next = idx + 1

        if elem.num_children !== nothing && elem.num_children > 0
            for _ in 1:elem.num_children
                child, next = build(next, new_def, new_rep)
                push!(children, child)
            end
        end

        node = SchemaNode(
            element = elem,
            children = children,
            max_def_level = new_def,
            max_rep_level = new_rep,
            own_def_level = adds_def > 0 ? new_def : 0,
            own_rep_level = adds_rep > 0 ? new_rep : 0
        )
        (node, next)
    end

    first(build(1, 0, 0))
end

function get_leaf_columns(root::SchemaNode)
    result = Tuple{Vector{String}, SchemaNode}[]

    function traverse(node, path)
        current = vcat(path, [node.element.name])
        if isempty(node.children)
            push!(result, (current, node))
        else
            for child in node.children
                traverse(child, current)
            end
        end
    end

    # Start from root's children, skipping root schema name
    for child in root.children
        traverse(child, String[])
    end
    result
end

"""
    schema_string(pf::ParquetFile) -> String

Get a human-readable schema representation.
"""
function schema_string(pf::ParquetFile)::String
    lines = String[]

    function format_element(elem::SchemaElement, indent::Int)
        parts = String[]
        elem.repetition_type !== nothing && push!(parts, string(elem.repetition_type))
        elem.type !== nothing && push!(parts, string(elem.type))
        (elem.num_children !== nothing && elem.num_children > 0) && push!(parts, "group")
        push!(parts, elem.name)
        elem.converted_type !== nothing && push!(parts, "($(elem.converted_type))")
        "  "^indent * join(parts, " ")
    end

    function traverse(schema, idx, indent)
        idx > length(schema) && return idx
        elem = schema[idx]
        push!(lines, format_element(elem, indent))

        next_idx = idx + 1
        if elem.num_children !== nothing
            for _ in 1:elem.num_children
                next_idx = traverse(schema, next_idx, indent + 1)
            end
        end
        next_idx
    end

    traverse(pf.metadata.schema, 1, 0)
    join(lines, "\n")
end
