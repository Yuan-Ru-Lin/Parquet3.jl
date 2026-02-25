# High-level API for reading Parquet files

# ── FixedSizeList types ──────────────────────────────────────────────────────

"""Lightweight zero-copy view into a flat array, carrying list size N in the type."""
struct FixedSizeView{N, T} <: AbstractVector{T}
    parent::Vector{T}
    offset::Int  # 0-based; element j is at parent[offset + j]
end

Base.size(::FixedSizeView{N}) where N = (N,)
Base.IndexStyle(::Type{<:FixedSizeView}) = Base.IndexLinear()
@Base.propagate_inbounds function Base.getindex(v::FixedSizeView{N,T}, i::Int) where {N,T}
    @boundscheck checkbounds(v, i)
    @inbounds v.parent[v.offset + i]
end

# Tell Arrow.write this is a FixedSizeList element
ArrowTypes.ArrowKind(::Type{FixedSizeView{N,T}}) where {N,T} = ArrowTypes.FixedSizeListKind{N,T}()

"""Fixed-size list column: flat child array with fixed stride N, plus record-level nulls."""
struct FixedSizeListVector{N, T, ET} <: AbstractVector{ET}
    data::Vector{T}
    nulls::BitVector    # true = record is null
    len::Int
end

Base.size(v::FixedSizeListVector) = (v.len,)
Base.IndexStyle(::Type{<:FixedSizeListVector}) = Base.IndexLinear()
@Base.propagate_inbounds function Base.getindex(v::FixedSizeListVector{N,T,ET}, i::Int) where {N,T,ET}
    @boundscheck checkbounds(v, i)
    v.nulls[i] && return missing
    FixedSizeView{N,T}(v.data, (i - 1) * N)
end

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
    (; schema, fsl, field_meta) = parse_arrow_schema(pf.metadata.key_value_metadata)

    if columns !== nothing
        leaf_columns = filter(leaf_columns) do (path, node)
            col_name = join(path, ".")
            col_name in columns || path[1] in columns || path[end] in columns
        end
    end

    row_groups = pf.metadata.row_groups

    # Per-column parallelism: each column reads its own row groups from the shared mmap'd data.
    # NUMA optimization opportunity: with ThreadPinning.jl, per-RG decompression/decoding tasks
    # could be pinned to NUMA-local threads, keeping data close to where it's consumed downstream.
    tasks = map(leaf_columns) do (path, node)
        Threads.@spawn begin
            top_name = path[1]
            col_name = join(path, ".")
            name = node.max_rep_level > 0 ? top_name : col_name
            nullable = _column_has_nulls(row_groups, path, node)

            # Per-row-group parallelism within each column
            chunk_tasks = map(row_groups) do rg
                Threads.@spawn begin
                    pages = _read_pages_for_rg(pf.data, rg, path, node)
                    _assemble_to_arrow(pages, node, schema_tree, path, fsl, field_meta, top_name; nullable)
                end
            end
            chunks = fetch.(chunk_tasks)

            isempty(chunks) && return nothing
            column = length(chunks) == 1 ? only(chunks) : ChainedVector(chunks)
            (Symbol(name), column)
        end
    end

    # Collect results in column order
    col_names = Symbol[]
    col_vectors = AbstractVector[]
    for (i, task) in enumerate(tasks)
        result = try
            fetch(task)
        catch e
            col_name = join(first(leaf_columns[i]), ".")
            @warn "Failed to read column $col_name" exception=(e, catch_backtrace())
            nothing
        end
        result === nothing && continue
        push!(col_names, result[1])
        push!(col_vectors, result[2])
    end

    col_types = Type[eltype(v) for v in col_vectors]
    lookup = Dict{Symbol,AbstractVector}(zip(col_names, col_vectors))
    meta = _parse_kv_metadata(pf.metadata.key_value_metadata)
    schema_ref = schema !== nothing ? Ref(schema) : Ref{Arrow.Meta.Schema}()
    Arrow.Table(col_names, col_types, col_vectors, lookup,
        schema_ref,
        Ref{Union{Nothing,Base.ImmutableDict{String,String}}}(meta))
end

"""
    _column_has_nulls(row_groups, path, node) -> Bool

Check column chunk statistics across all row groups to determine if a column
actually contains nulls. Falls back to schema-based conservative check
(`multi_rg && max_def > 0`) when statistics are unavailable.
"""
function _column_has_nulls(row_groups::Vector{RowGroup}, path::Vector{String}, node::SchemaNode)
    node.max_def_level == 0 && return false
    for rg in row_groups
        idx = findfirst(c -> c.meta_data !== nothing && c.meta_data.path_in_schema == path, rg.columns)
        idx === nothing && continue
        stats = rg.columns[idx].meta_data.statistics
        if stats === nothing || stats.null_count === nothing
            # Statistics unavailable — fall back to conservative multi-RG check.
            # For single-RG files, actual null presence drives the type via any(nulls).
            return length(row_groups) > 1
        end
        stats.null_count > 0 && return true
    end
    false
end

"""Read and decode all pages from one row group's column chunk."""
function _read_pages_for_rg(data::Vector{UInt8}, rg::RowGroup, column_path::Vector{String}, node::SchemaNode)
    idx = findfirst(c -> c.meta_data !== nothing && c.meta_data.path_in_schema == column_path, rg.columns)
    idx === nothing && error("Column chunk not found: $(join(column_path, "."))")
    type_length = Int(something(node.element.type_length, 0))
    reader = ColumnReader(data, rg.columns[idx].meta_data, node, type_length)
    read_all_pages(reader)
end

"""Assemble one row group's decoded pages into an Arrow array chunk."""
function _assemble_to_arrow(pages::Vector{<:DecodedPage}, node::SchemaNode, schema_tree::SchemaNode,
                            column_path::Vector{String}, fsl_info, field_meta, top_name::String; nullable::Bool=false)
    is_nested = node.max_rep_level > 0
    ptype = node.element.type
    max_def = node.max_def_level
    meta = get(field_meta, top_name, nothing)

    if is_nested
        # Fast path: FSL columns with no nulls — bypass collect_page_data entirely
        if haskey(fsl_info, top_name) && _fsl_no_nulls(pages, max_def)
            return _assemble_fsl_dense(pages, ptype, node.element, fsl_info[top_name]; nullable)
        end

        all_rep, all_def, raw = collect_page_data(pages, max_def)
        converted = convert_primitive_values(raw, ptype, node.element.converted_type)
        def_thresholds = compute_def_thresholds(schema_tree, column_path)

        if haskey(fsl_info, top_name)
            assemble_fsl_direct(all_rep, all_def, converted, max_def, fsl_info[top_name],
                                node.element, def_thresholds; nullable)
        else
            _to_arrow_nested(all_rep, all_def, converted, max_def, node.max_rep_level,
                             def_thresholds, ptype, node.element.converted_type; nullable, meta)
        end
    else
        # All flat columns: bool, bytes, numeric
        values, nulls = assemble_flat_column(pages, max_def)
        converted = convert_primitive_values(values, ptype, node.element.converted_type)
        _build_leaf_array(converted, nulls, ptype, node.element.converted_type; nullable, meta)
    end
end

"""Invert a Parquet nulls BitVector into an Arrow ValidityBitmap."""
function _validity(nulls::BitVector)
    nc = count(nulls)
    nc == 0 && return Arrow.ValidityBitmap(UInt8[], 1, length(nulls), 0)
    bytes = Vector{UInt8}(reinterpret(UInt8, .~nulls.chunks))
    Arrow.ValidityBitmap(bytes, 1, length(nulls), nc)
end

"""Build the appropriate Arrow leaf array from flat leaf values and nulls."""
function _build_leaf_array(leaf_values::AbstractVector{T}, leaf_nulls::BitVector, ptype, ctype;
                           nullable::Bool=false, meta=nothing) where T
    v = _validity(leaf_nulls)
    has_nulls = v.nc > 0 || nullable
    n = length(leaf_nulls)

    if ptype == BYTE_ARRAY
        # String/bytes: build Arrow.List with flat byte data + offsets
        is_utf8 = ctype == CT_UTF8
        flat = UInt8[]
        offsets = Int32[0]
        for i in 1:n
            if !leaf_nulls[i]
                append!(flat, leaf_values[i])
            end
            push!(offsets, Int32(length(flat)))
        end
        BT = is_utf8 ? String : Vector{UInt8}
        ET = has_nulls ? Union{Missing,BT} : BT
        return Arrow.List{ET, Int32, Vector{UInt8}}(UInt8[], v, Arrow.Offsets(UInt8[], offsets), flat, n, meta)
    elseif ptype == BOOLEAN
        bytes = zeros(UInt8, cld(n, 8))
        for i in 1:n
            if !leaf_nulls[i] && leaf_values[i]
                bytes[((i-1) >> 3) + 1] |= UInt8(1) << ((i-1) & 7)
            end
        end
        ET = has_nulls ? Union{Missing,Bool} : Bool
        return Arrow.BoolVector{ET}(bytes, 1, v, Int64(n), meta)
    else
        ET = has_nulls ? Union{Missing,T} : T
        return Arrow.Primitive(ET, UInt8[], v, leaf_values, n, meta)
    end
end

"""Build Arrow.List directly from rep/def levels — single pass, no intermediate Vector{Vector{T}}."""
function _to_arrow_nested(all_rep, all_def, values::AbstractVector{T}, max_def, max_rep,
                          def_thresholds, ptype, ctype; nullable::Bool=false, meta=nothing) where T
    # Innermost threshold: min def_level for a leaf element to exist
    inner_threshold = length(def_thresholds) >= max_rep ? def_thresholds[max_rep] : max_rep

    # Offset arrays for each nesting depth (1-indexed, k=1 is outermost list)
    offsets = [Int32[0] for _ in 1:max_rep]
    child_count = zeros(Int32, max_rep)

    # Record-level nulls (top level)
    num_records = count(==(0), all_rep)
    record_nulls = falses(num_records)

    # Flat leaf data
    leaf_values = T[]
    leaf_nulls = BitVector()

    record_idx = 0
    value_idx = 1

    for i in eachindex(all_rep)
        rep = all_rep[i]
        def = all_def[i]

        # Finalization: when rep = j, push offsets at levels j+1 .. max_rep
        # For rep=0, this means all levels 1..max_rep get finalized
        if rep == 0
            # Finalize all levels for previous record
            if record_idx > 0
                for k in max_rep:-1:1
                    push!(offsets[k], child_count[k])
                end
            end
            record_idx += 1

            # Null record: def=0 means the entire record is null
            if def == 0 && max_def > 0
                record_nulls[record_idx] = true
                # Still need to push offset entries for this null record at the end
                # (handled by the finalization on next rep=0 or after loop)
                # Increment child counts for levels that get empty slices: none
                continue
            end
        else
            # Finalize levels from max_rep down to rep+1
            for k in max_rep:-1:(rep + 1)
                push!(offsets[k], child_count[k])
            end
        end

        # Item creation: new items at levels max(1,rep)..max_rep
        # Level k item exists when def >= def_thresholds[k]
        for k in max(1, rep):max_rep
            if k <= length(def_thresholds) && def >= def_thresholds[k]
                if k < max_rep
                    child_count[k] += 1
                end
            end
        end
        # The innermost level (max_rep) always gets a child count bump from the leaf push below

        # Leaf handling
        if def == max_def
            push!(leaf_values, values[value_idx])
            push!(leaf_nulls, false)
            child_count[max_rep] += 1
            value_idx += 1
        elseif def >= inner_threshold
            # Leaf element exists but value is null — push placeholder
            push!(leaf_values, value_idx <= length(values) ? values[1] : zero(T))
            push!(leaf_nulls, true)
            child_count[max_rep] += 1
        end
        # def < inner_threshold: intermediate empty list, no leaf push
    end

    # Final finalization for last record
    if record_idx > 0
        for k in max_rep:-1:1
            push!(offsets[k], child_count[k])
        end
    end

    # Build bottom-up: leaf array → wrap with List at each level
    child = _build_leaf_array(leaf_values, leaf_nulls, ptype, ctype; nullable)

    for k in max_rep:-1:1
        ST = SubArray{eltype(child), 1, typeof(child), Tuple{UnitRange{Int64}}, true}
        offs = Arrow.Offsets(UInt8[], offsets[k])
        n = length(offsets[k]) - 1

        if k == 1
            # Top level: apply record nulls and field metadata
            v = _validity(record_nulls)
            ET = (v.nc > 0 || nullable) ? Union{Missing,ST} : ST
            child = Arrow.List{ET, Int32, typeof(child)}(UInt8[], v, offs, child, n, meta)
        else
            # Intermediate levels: all-valid
            v = Arrow.ValidityBitmap(UInt8[], 1, n, 0)
            child = Arrow.List{ST, Int32, typeof(child)}(UInt8[], v, offs, child, n, nothing)
        end
    end

    child
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
    if ctype == CT_DATE && ptype == INT32
        [Date(1970, 1, 1) + Day(v) for v in values]
    elseif ctype == CT_TIMESTAMP_MILLIS && ptype == INT64
        [DateTime(1970, 1, 1) + Millisecond(v) for v in values]
    elseif ctype == CT_TIMESTAMP_MICROS && ptype == INT64
        [DateTime(1970, 1, 1) + Microsecond(v) for v in values]
    elseif (T = _converted_int_type(ctype)) !== nothing && T !== eltype(values)
        T.(values)
    else
        values
    end
end

"""Map ConvertedType integer annotations to Julia types. Returns nothing if no conversion needed."""
function _converted_int_type(ctype)
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

"""Convert nested values into a FixedSizeListVector (flat child array + record-level nulls)."""
function convert_fixed_size_list(values, nulls::BitVector, elem::SchemaElement, list_size::Int; nullable::Bool=false)
    T = element_julia_type(elem.type, elem.converted_type)
    nrows = length(nulls)
    data = Vector{T}(undef, list_size * nrows)
    val_idx = 0
    @inbounds for row in 1:nrows
        base = (row - 1) * list_size
        if nulls[row]
            for j in 1:list_size; data[base + j] = zero(T); end
        else
            val_idx += 1
            inner = values[val_idx]
            for j in 1:list_size
                v = inner[j]
                data[base + j] = v === missing ? zero(T) : T(v)
            end
        end
    end
    has_nulls = any(nulls) || nullable
    ET = has_nulls ? Union{Missing, FixedSizeView{list_size, T}} : FixedSizeView{list_size, T}
    FixedSizeListVector{list_size, T, ET}(data, nulls, nrows)
end

"""
Direct FSL assembly: scatter values from rep/def levels into a flat buffer
in a single pass, bypassing intermediate Vector{Vector{T}} creation.
"""
function assemble_fsl_direct(all_rep, all_def, values::AbstractVector{V},
                             max_def::Int, list_size::Int, elem::SchemaElement,
                             def_thresholds::Vector{Int}; nullable::Bool=false) where V
    T = element_julia_type(elem.type, elem.converted_type)
    num_records = count(==(0), all_rep)
    data = Vector{T}(undef, list_size * num_records)
    nulls = falses(num_records)

    inner_threshold = length(def_thresholds) >= 1 ? def_thresholds[1] : 1

    record_idx = 0
    value_idx = 1
    slot_idx = 0  # position within current record's list

    @inbounds for i in eachindex(all_rep)
        rep = all_rep[i]
        def = all_def[i]

        if rep == 0
            # New record
            record_idx += 1
            slot_idx = 0
            base = (record_idx - 1) * list_size

            if def == 0 && max_def > 0
                nulls[record_idx] = true
                # Zero-fill the null record's slots
                for j in 1:list_size
                    data[base + j] = zero(T)
                end
                continue
            end
        end

        base = (record_idx - 1) * list_size

        if def == max_def
            slot_idx += 1
            if slot_idx <= list_size
                data[base + slot_idx] = T(values[value_idx])
            end
            value_idx += 1
        elseif def >= inner_threshold
            # Null leaf value
            slot_idx += 1
            if slot_idx <= list_size
                data[base + slot_idx] = zero(T)
            end
        end
    end

    has_nulls = any(nulls) || nullable
    ET = has_nulls ? Union{Missing, FixedSizeView{list_size, T}} : FixedSizeView{list_size, T}
    FixedSizeListVector{list_size, T, ET}(data, nulls, num_records)
end

"""Check if all pages in an FSL column have no nulls (all defs at max or no def levels)."""
function _fsl_no_nulls(pages::Vector{<:DecodedPage}, max_def::Int)
    for page in pages
        if page.def_levels !== nothing
            all(==(max_def), page.def_levels) || return false
        end
    end
    true
end

"""
Dense FSL assembly: when there are no nulls, page values are already contiguous.
Copy them directly into the flat FSL buffer — no rep/def level processing needed.
"""
function _assemble_fsl_dense(pages::Vector{<:DecodedPage}, ptype, elem::SchemaElement,
                             list_size::Int; nullable::Bool=false)
    T = element_julia_type(ptype, elem.converted_type)
    # Count records from rep levels
    num_records = sum(pages) do page
        page.rep_levels === nothing ? page.num_values : count(==(0), page.rep_levels)
    end

    data = Vector{T}(undef, list_size * num_records)
    nulls = falses(num_records)

    # Convert and copy page values in bulk using a function barrier for type stability
    _fsl_dense_copy!(data, pages, ptype, elem.converted_type)

    ET = nullable ? Union{Missing, FixedSizeView{list_size, T}} : FixedSizeView{list_size, T}
    FixedSizeListVector{list_size, T, ET}(data, nulls, num_records)
end

"""Type-stable inner loop: convert page values and copyto! into the flat buffer."""
function _fsl_dense_copy!(data::Vector{T}, pages::Vector{<:DecodedPage}, ptype, ctype) where T
    pos = 1
    for page in pages
        converted = convert_primitive_values(page.values, ptype, ctype)
        n = length(converted)
        if eltype(converted) === T
            copyto!(data, pos, converted, 1, n)
        else
            @inbounds for i in 1:n
                data[pos + i - 1] = T(converted[i])
            end
        end
        pos += n
    end
end


"""Get Julia type for a Parquet primitive type."""
function element_julia_type(ptype, ctype)
    if ctype == CT_UTF8 && ptype == BYTE_ARRAY
        String
    elseif ctype == CT_DATE && ptype == INT32
        Date
    elseif ctype in (CT_TIMESTAMP_MILLIS, CT_TIMESTAMP_MICROS) && ptype == INT64
        DateTime
    elseif (T = _converted_int_type(ctype)) !== nothing
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
