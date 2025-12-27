# Parquet file reader

"""
    ParquetFile

Represents an open Parquet file with parsed metadata.
"""
struct ParquetFile
    io::IO
    path::String
    metadata::FileMetaData
end

"""
    read_footer(io::IO) -> FileMetaData

Read and parse the Parquet file footer (metadata).
"""
function read_footer(io::IO)::FileMetaData
    # Parquet file structure:
    # - Magic number "PAR1" (4 bytes)
    # - Row groups and pages (variable)
    # - Footer (Thrift-encoded FileMetaData)
    # - Footer length (4 bytes, little-endian)
    # - Magic number "PAR1" (4 bytes)

    # Read file size
    seekend(io)
    file_size = position(io)

    if file_size < 12
        error("File too small to be a valid Parquet file")
    end

    # Read trailing magic number
    seek(io, file_size - 4)
    magic = read(io, 4)
    if magic != PARQUET_MAGIC
        error("Invalid Parquet file: missing trailing magic number")
    end

    # Read footer length
    seek(io, file_size - 8)
    footer_length = ltoh(read(io, UInt32))

    if footer_length > file_size - 12
        error("Invalid footer length")
    end

    # Read leading magic number
    seek(io, 0)
    magic = read(io, 4)
    if magic != PARQUET_MAGIC
        error("Invalid Parquet file: missing leading magic number")
    end

    # Read footer data
    footer_start = file_size - 8 - footer_length
    seek(io, footer_start)
    footer_data = read(io, footer_length)

    # Parse the Thrift-encoded metadata
    decoder = ThriftDecoder(footer_data)
    parse_file_metadata(decoder)
end

"""
    open_parquet(path::String) -> ParquetFile

Open a Parquet file and parse its metadata.
"""
function open_parquet(path::String)::ParquetFile
    io = open(path, "r")
    try
        metadata = read_footer(io)
        ParquetFile(io, path, metadata)
    catch e
        close(io)
        rethrow(e)
    end
end

"""
    close(pf::ParquetFile)

Close a Parquet file.
"""
function Base.close(pf::ParquetFile)
    close(pf.io)
end

"""Get the number of rows in the file."""
num_rows(pf::ParquetFile) = pf.metadata.num_rows

"""Get the number of row groups in the file."""
num_row_groups(pf::ParquetFile) = length(pf.metadata.row_groups)

"""Get the schema from the file."""
schema(pf::ParquetFile) = pf.metadata.schema

"""Get column names from the schema."""
function column_names(pf::ParquetFile)::Vector{String}
    # First element is the root, skip it
    # Collect leaf columns (those without children)
    names = String[]
    schema_elems = pf.metadata.schema
    i = 2  # Skip root
    while i <= length(schema_elems)
        elem = schema_elems[i]
        if elem.num_children === nothing || elem.num_children == 0
            push!(names, elem.name)
        end
        i += 1
    end
    names
end

"""
    SchemaNode

Represents a node in the schema tree for handling nested structures.
"""
struct SchemaNode
    element::SchemaElement
    children::Vector{SchemaNode}
    max_def_level::Int
    max_rep_level::Int
end

"""Build schema tree from flat schema list."""
function build_schema_tree(schema::Vector{SchemaElement})::SchemaNode
    isempty(schema) && error("Empty schema")

    # Helper to recursively build tree
    function build_node(idx::Int, def_level::Int, rep_level::Int)::Tuple{SchemaNode, Int}
        elem = schema[idx]

        # Update levels based on repetition type
        new_def = def_level
        new_rep = rep_level
        if elem.repetition_type !== nothing
            if elem.repetition_type == OPTIONAL || elem.repetition_type == REPEATED
                new_def = def_level + 1
            end
            if elem.repetition_type == REPEATED
                new_rep = rep_level + 1
            end
        end

        children = SchemaNode[]
        next_idx = idx + 1

        if elem.num_children !== nothing && elem.num_children > 0
            for _ in 1:elem.num_children
                child, next_idx = build_node(next_idx, new_def, new_rep)
                push!(children, child)
            end
        end

        (SchemaNode(elem, children, new_def, new_rep), next_idx)
    end

    node, _ = build_node(1, 0, 0)
    node
end

"""Find a leaf column node by path."""
function find_column(root::SchemaNode, path::Vector{String})::Union{SchemaNode, Nothing}
    current = root
    for name in path
        found = false
        for child in current.children
            if child.element.name == name
                current = child
                found = true
                break
            end
        end
        !found && return nothing
    end
    current
end

"""Get all leaf columns with their paths."""
function get_leaf_columns(root::SchemaNode)::Vector{Tuple{Vector{String}, SchemaNode}}
    result = Tuple{Vector{String}, SchemaNode}[]

    function traverse(node::SchemaNode, path::Vector{String})
        current_path = isempty(path) ? String[] : vcat(path, [node.element.name])

        if isempty(node.children)
            # Leaf node
            if !isempty(current_path)
                push!(result, (current_path, node))
            end
        else
            for child in node.children
                traverse(child, current_path)
            end
        end
    end

    traverse(root, String[])
    result
end
