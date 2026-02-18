# High-level API for reading Parquet files

"""
    read_parquet(path::String; columns=nothing) -> Arrow.Table

Read a Parquet file and return an Arrow.Table (Tables.jl-compatible).
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
    fsl_info = parse_arrow_schema(pf.metadata.key_value_metadata)

    if columns !== nothing
        leaf_columns = filter(leaf_columns) do (path, node)
            col_name = join(path, ".")
            col_name in columns || path[1] in columns || path[end] in columns
        end
    end

    col_names = Symbol[]
    col_vectors = AbstractVector[]

    for (path, node) in leaf_columns
        top_name = path[1]
        col_name = join(path, ".")

        try
            is_nested = node.max_rep_level > 0
            ptype = node.element.type

            name, converted = if !is_nested && ptype == BOOLEAN
                pages = _read_pages(pf, path, node)
                col_name, (isempty(pages) ? Arrow.BoolVector{Bool}(UInt8[], 1, Arrow.ValidityBitmap(UInt8[], 1, 0, 0), Int64(0), nothing) :
                    _to_arrow_bool(pages, node.max_def_level))
            elseif !is_nested && ptype == BYTE_ARRAY
                pages = _read_pages(pf, path, node)
                col_name, (isempty(pages) ? Arrow.List{String, Int32, Vector{UInt8}}(UInt8[], Arrow.ValidityBitmap(UInt8[], 1, 0, 0), Arrow.Offsets(UInt8[], Int32[0]), UInt8[], 0, nothing) :
                    _to_arrow_bytes(pages, node.max_def_level, node.element.converted_type == CT_UTF8))
            else
                values, nulls = read_column_data(pf, path, node, schema_tree)
                if is_nested && haskey(fsl_info, top_name)
                    top_name, convert_fixed_size_list(values, nulls, node.element, fsl_info[top_name])
                elseif is_nested
                    top_name, convert_nested_type(values, nulls, node.max_rep_level)
                else
                    col_name, _to_arrow(values, nulls, node.element)
                end
            end
            push!(col_names, Symbol(name))
            push!(col_vectors, converted)
        catch e
            @warn "Failed to read column $col_name" exception=(e, catch_backtrace())
        end
    end

    col_types = Type[eltype(v) for v in col_vectors]
    lookup = Dict{Symbol,AbstractVector}(zip(col_names, col_vectors))
    meta = _parse_kv_metadata(pf.metadata.key_value_metadata)
    Arrow.Table(col_names, col_types, col_vectors, lookup,
        Ref{Arrow.Meta.Schema}(),
        Ref{Union{Nothing,Base.ImmutableDict{String,String}}}(meta))
end

"""Collect decoded pages from all row groups for a column."""
function _read_pages(pf::ParquetFile, column_path::Vector{String}, node::SchemaNode)
    type_length = Int(something(node.element.type_length, 0))
    all_pages = DecodedPage[]
    for rg in pf.metadata.row_groups
        idx = findfirst(c -> c.meta_data !== nothing && c.meta_data.path_in_schema == column_path, rg.columns)
        idx === nothing && error("Column chunk not found: $(join(column_path, "."))")
        reader = ColumnReader(pf.io, rg.columns[idx].meta_data, node, type_length)
        append!(all_pages, read_all_pages(reader))
    end
    all_pages
end

"""Read column data from all row groups."""
function read_column_data(pf::ParquetFile, column_path::Vector{String}, node::SchemaNode, schema_tree::SchemaNode)
    all_pages = _read_pages(pf, column_path, node)
    isempty(all_pages) && return ([], falses(0))
    def_thresholds = compute_def_thresholds(schema_tree, column_path)
    max_def = node.max_def_level
    max_rep = node.max_rep_level

    # Flat columns: assemble directly
    max_rep == 0 && return assemble_column(all_pages, max_def, max_rep, def_thresholds)

    # Nested columns: pre-convert values before assembly so the structure
    # contains final Julia types (String, Date, etc.) instead of raw Parquet primitives
    all_rep, all_def, raw_values = collect_page_data(all_pages, max_def)
    isempty(all_rep) && return ([], falses(0))
    converted = convert_primitive_values(raw_values, node.element.type, node.element.converted_type)
    T = eltype(converted)
    assemble_nested(all_rep, all_def, converted, max_def, max_rep, T, def_thresholds)
end

"""Build Arrow.BoolVector directly from decoded pages, preserving bit-packing."""
function _to_arrow_bool(pages::Vector{<:DecodedPage}, max_def::Int)
    total = sum(p.num_values for p in pages)
    bytes = zeros(UInt8, cld(total, 8))
    nulls = falses(total)
    out = 0
    for page in pages
        src = page.values::BitVector
        val = 0
        for i in 1:page.num_values
            out += 1
            if page.def_levels !== nothing && page.def_levels[i] < max_def
                nulls[out] = true
            else
                val += 1
                # Copy bit directly from source BitVector chunk to output byte
                src_bit = (src.chunks[((val-1) >> 6) + 1] >> ((val-1) & 63)) & UInt64(1)
                bytes[((out-1) >> 3) + 1] |= UInt8(src_bit) << ((out-1) & 7)
            end
        end
    end
    v = _validity(nulls)
    ET = v.nc > 0 ? Union{Missing,Bool} : Bool
    Arrow.BoolVector{ET}(bytes, 1, v, Int64(total), nothing)
end

"""Build Arrow.List directly from decoded pages — one pass, no intermediate Vector{SubArray}."""
function _to_arrow_bytes(pages::Vector{<:DecodedPage}, max_def::Int, is_utf8::Bool)
    total = sum(p.num_values for p in pages)
    flat = UInt8[]
    offsets = Int32[0]
    nulls = falses(total)
    out = 0
    for page in pages
        val = 0
        for i in 1:page.num_values
            out += 1
            if page.def_levels !== nothing && page.def_levels[i] < max_def
                nulls[out] = true
            else
                val += 1
                append!(flat, page.values[val])
            end
            push!(offsets, Int32(length(flat)))
        end
    end
    v = _validity(nulls)
    T = is_utf8 ? String : Vector{UInt8}
    ET = v.nc > 0 ? Union{Missing,T} : T
    Arrow.List{ET, Int32, Vector{UInt8}}(UInt8[], v, Arrow.Offsets(UInt8[], offsets), flat, total, nothing)
end

"""Wrap flat numeric column (values, nulls) from assemble_flat_column into Arrow.Primitive."""
function _to_arrow(values, nulls::BitVector, elem::SchemaElement)
    v = _validity(nulls)
    converted = convert_primitive_values(values, elem.type, elem.converted_type)
    T = eltype(converted)
    Arrow.Primitive(v.nc > 0 ? Union{Missing,T} : T, UInt8[], v, converted, length(nulls), nothing)
end

"""Invert a Parquet nulls BitVector into an Arrow ValidityBitmap."""
function _validity(nulls::BitVector)
    nc = count(nulls)
    nc == 0 && return Arrow.ValidityBitmap(UInt8[], 1, length(nulls), 0)
    bytes = Vector{UInt8}(reinterpret(UInt8, .~nulls.chunks))
    Arrow.ValidityBitmap(bytes, 1, length(nulls), nc)
end

"""Parse Parquet key-value metadata into Arrow-compatible ImmutableDict."""
function _parse_kv_metadata(kv::Union{Vector{KeyValue}, Nothing})
    kv === nothing && return nothing
    isempty(kv) && return nothing
    d = Base.ImmutableDict(kv[1].key => something(kv[1].value, ""))
    for i in 2:length(kv)
        d = Base.ImmutableDict(d, kv[i].key => something(kv[i].value, ""))
    end
    d
end

"""Convert primitive values based on Parquet and converted types."""
function convert_primitive_values(values, ptype, ctype)
    if ctype == CT_UTF8 && ptype == BYTE_ARRAY
        [String(copy(v)) for v in values]
    elseif ctype == CT_DATE && ptype == INT32
        [Date(1970, 1, 1) + Day(v) for v in values]
    elseif ctype == CT_TIMESTAMP_MILLIS && ptype == INT64
        [DateTime(1970, 1, 1) + Millisecond(v) for v in values]
    elseif ctype == CT_TIMESTAMP_MICROS && ptype == INT64
        [DateTime(1970, 1, 1) + Microsecond(v) for v in values]
    elseif ptype == BYTE_ARRAY
        [copy(v) for v in values]
    elseif (T = _converted_int_type(ptype, ctype)) !== nothing
        T.(values)
    else
        T = element_julia_type(ptype, ctype)
        T === Any ? values : T.(values)
    end
end

"""Map ConvertedType integer annotations to Julia types. Returns nothing if no conversion needed."""
function _converted_int_type(ptype, ctype)
    ctype === nothing && return nothing
    ctype == CT_INT_8   && return Int8
    ctype == CT_INT_16  && return Int16
    ctype == CT_INT_32  && return Int32
    ctype == CT_INT_64  && return Int64
    ctype == CT_UINT_8  && return UInt8
    ctype == CT_UINT_16 && return UInt16
    ctype == CT_UINT_32 && return UInt32
    ctype == CT_UINT_64 && return UInt64
    nothing
end

"""Convert a FixedSizeList column to an ArrayOfSimilarArrays (backed by list_size × nrows Matrix).
Values are already converted; only missing needs replacing with zero."""
function convert_fixed_size_list(values, nulls::BitVector, elem::SchemaElement, list_size::Int)
    T = element_julia_type(elem.type, elem.converted_type)
    nrows = length(values)

    mat = Matrix{T}(undef, list_size, nrows)
    for row in 1:nrows
        inner = values[row]
        for col in 1:list_size
            v = inner[col]
            mat[col, row] = v === missing ? zero(T) : v
        end
    end
    nestedview(mat)
end

"""Type nested containers and apply top-level nulls.
Values are already converted; only intermediate container types need fixing."""
function convert_nested_type(values, nulls::BitVector, max_rep::Int)
    typed = _type_containers(values, max_rep)
    any(nulls) || return typed

    result = Vector{Union{Missing, eltype(typed)}}(undef, length(nulls))
    for i in eachindex(nulls)
        result[i] = nulls[i] ? missing : typed[i]
    end
    result
end

"""Recursively narrow container types from Any[] to concrete vectors.
Leaf lists (depth 0) are already correctly typed from assembly."""
_type_containers(list, depth) =
    depth <= 0 ? list :
    [x === missing ? missing : _type_containers(x, depth - 1) for x in list]

"""Get Julia type for a Parquet primitive type."""
function element_julia_type(ptype, ctype)
    if ctype == CT_UTF8 && ptype == BYTE_ARRAY
        String
    elseif ctype == CT_DATE && ptype == INT32
        Date
    elseif ctype in (CT_TIMESTAMP_MILLIS, CT_TIMESTAMP_MICROS) && ptype == INT64
        DateTime
    elseif (T = _converted_int_type(ptype, ctype)) !== nothing
        T
    elseif ptype == INT96
        Int96
    elseif ptype == BOOLEAN
        Bool
    elseif ptype == INT32
        Int32
    elseif ptype == INT64
        Int64
    elseif ptype == FLOAT
        Float32
    elseif ptype == DOUBLE
        Float64
    elseif ptype == BYTE_ARRAY
        Vector{UInt8}
    else
        Any
    end
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
