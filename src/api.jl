# High-level API for reading Parquet files -> Arrow Tables

using Arrow: Arrow, Table

"""
    read_parquet(path::String; columns=nothing) -> Arrow.Table

Read a Parquet file and return an Arrow Table.
"""
function read_parquet(path::String; columns::Union{Vector{String}, Nothing}=nothing)
    pf = open_parquet(path)
    try
        return read_parquet(pf; columns=columns)
    finally
        close(pf)
    end
end

function read_parquet(pf::ParquetFile; columns::Union{Vector{String}, Nothing}=nothing)
    schema_tree = build_schema_tree(pf.metadata.schema)
    leaf_columns = get_leaf_columns(schema_tree)

    # Filter columns if specified
    if columns !== nothing
        leaf_columns = filter(leaf_columns) do (path, node)
            col_name = join(path, ".")
            col_name in columns || path[end] in columns
        end
    end

    # Build column data
    col_names = Symbol[]
    col_vectors = []

    for (path, node) in leaf_columns
        col_name = join(path, ".")

        try
            values, nulls = read_column_data(pf, path, node)
            converted = convert_to_julia_type(values, nulls, node.element)
            push!(col_names, Symbol(col_name))
            push!(col_vectors, converted)
        catch e
            @warn "Failed to read column $col_name" exception=(e, catch_backtrace())
        end
    end

    # Create Arrow Table
    Arrow.Table(NamedTuple{Tuple(col_names)}(Tuple(col_vectors)))
end

"""Read column data from all row groups."""
function read_column_data(pf::ParquetFile, column_path::Vector{String}, node::SchemaNode)
    type_length = something(node.element.type_length, 0)

    all_pages = DecodedPage[]

    for rg in pf.metadata.row_groups
        chunk = nothing
        for col in rg.columns
            if col.meta_data !== nothing && col.meta_data.path_in_schema == column_path
                chunk = col
                break
            end
        end

        chunk === nothing && error("Column chunk not found: $(join(column_path, "."))")

        reader = ColumnReader(pf.io, chunk.meta_data, node, type_length)
        append!(all_pages, read_all_pages(reader))
    end

    isempty(all_pages) && return ([], falses(0))
    assemble_column(all_pages, node.max_def_level)
end

"""Convert Parquet values to appropriate Julia types with nulls as missing."""
function convert_to_julia_type(values, nulls::BitVector, elem::SchemaElement)
    ptype = elem.type
    ctype = elem.converted_type

    # Convert raw values based on type
    converted = if ctype == CT_UTF8 && ptype == BYTE_ARRAY
        [String(copy(v)) for v in values]
    elseif ctype == CT_DATE && ptype == INT32
        [Date(1970, 1, 1) + Day(v) for v in values]
    elseif ctype == CT_TIMESTAMP_MILLIS && ptype == INT64
        [DateTime(1970, 1, 1) + Millisecond(v) for v in values]
    elseif ctype == CT_TIMESTAMP_MICROS && ptype == INT64
        [DateTime(1970, 1, 1) + Microsecond(v) for v in values]
    elseif ptype == BYTE_ARRAY
        [copy(v) for v in values]  # Keep as Vector{UInt8}
    else
        values
    end

    # Handle nulls - create vector with missing values
    if any(nulls)
        result = Vector{Union{Missing, eltype(converted)}}(undef, length(nulls))
        val_idx = 1
        for i in eachindex(nulls)
            if nulls[i]
                result[i] = missing
            else
                result[i] = converted[val_idx]
                val_idx += 1
            end
        end
        return result
    else
        return converted
    end
end

"""
    metadata(pf::ParquetFile) -> FileMetaData

Get the file metadata.
"""
metadata(pf::ParquetFile) = pf.metadata

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
