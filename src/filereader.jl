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
    data = Mmap.mmap(path)
    ParquetFile(data, path, read_footer(data))
end

Base.close(::ParquetFile) = nothing

num_rows(pf::ParquetFile) = pf.metadata.num_rows
num_row_groups(pf::ParquetFile) = length(pf.metadata.row_groups)
schema(pf::ParquetFile) = pf.metadata.schema
metadata(pf::ParquetFile) = pf.metadata

function column_names(pf::ParquetFile)::Vector{String}
    tree = build_schema_tree(pf.metadata.schema)
    seen = Set{String}()
    names = String[]
    for (path, node) in get_leaf_columns(tree)
        name = node.max_rep_level > 0 ? path[1] : join(path, ".")
        name in seen && continue
        push!(seen, name)
        push!(names, name)
    end
    names
end

function build_schema_tree(schema::Vector{SchemaElement})::SchemaNode
    isempty(schema) && error("Empty schema")

    function build(idx, def, rep)
        elem = schema[idx]

        # Calculate this node's contribution to def/rep levels
        adds_def = elem.repetition_type in (OPTIONAL, REPEATED) ? 1 : 0
        adds_rep = elem.repetition_type == REPEATED ? 1 : 0

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

function find_column(root::SchemaNode, path::Vector{String})
    node = root
    for name in path
        found = findfirst(c -> c.element.name == name, node.children)
        found === nothing && return nothing
        node = node.children[found]
    end
    node
end

"""
    compute_def_thresholds(root::SchemaNode, path::Vector{String}) -> Vector{Int}

Compute the definition level threshold for each repetition level.
`thresholds[i]` is the minimum def_level at which rep_level `i` has a defined element.
Used by nested column assembly to distinguish "empty inner list" from "null leaf value".
"""
function compute_def_thresholds(root::SchemaNode, path::Vector{String})
    thresholds = Int[]
    cum_def = 0
    node = root
    for name in path
        idx = findfirst(c -> c.element.name == name, node.children)
        idx === nothing && break
        child = node.children[idx]
        rt = child.element.repetition_type
        cum_def += (rt == OPTIONAL || rt == REPEATED) ? 1 : 0
        rt == REPEATED && push!(thresholds, cum_def)
        node = child
    end
    thresholds
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
