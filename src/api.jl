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

# ── Nested column containers ─────────────────────────────────────────────────

"""
Column wrapper adding named field access to nested Arrow containers, which store
fields positionally. `col[i]` delegates row access to the wrapped array;
`col.fieldname` yields the field's full child column — zero-copy for a single row
group, chained across per-row-group chunks otherwise.
"""
struct NestedColumn{kind, T, fnames, D<:AbstractVector{T}} <: AbstractVector{T}
    _data::D    # Arrow.Struct / Arrow.List-over-Struct, or ChainedVector of chunks
end

"""Struct column: rows are lazy `NamedTuple`s; `col.fieldname` is the child column."""
const StructColumn{T, fnames, D} = NestedColumn{:struct, T, fnames, D}

"""List-of-structs column: rows are lazy vectors of `NamedTuple`s; `col.fieldname`
is a ragged per-field list sharing the parent's offsets and validity."""
const ListOfStructsColumn{T, fnames, D} = NestedColumn{:list_of_structs, T, fnames, D}

StructColumn(data::AbstractVector{T}, fnames::Tuple{Vararg{Symbol}}) where T =
    NestedColumn{:struct, T, fnames, typeof(data)}(data)
ListOfStructsColumn(data::AbstractVector{T}, fnames::Tuple{Vararg{Symbol}}) where T =
    NestedColumn{:list_of_structs, T, fnames, typeof(data)}(data)

Base.size(c::NestedColumn) = size(getfield(c, :_data))
Base.IndexStyle(::Type{<:NestedColumn}) = Base.IndexLinear()
@Base.propagate_inbounds Base.getindex(c::NestedColumn, i::Int) = getfield(c, :_data)[i]
Arrow.getmetadata(c::NestedColumn) = Arrow.getmetadata(_first_chunk(getfield(c, :_data)))

Base.propertynames(::NestedColumn{kind, T, fnames}) where {kind, T, fnames} = fnames
function Base.getproperty(c::NestedColumn{kind, T, fnames}, name::Symbol) where {kind, T, fnames}
    j = findfirst(==(name), fnames)
    j === nothing && return getfield(c, name)
    _child_column(getfield(c, :_data), j)
end

"""Project field `j` out of a nested container: struct → child column, list-over-struct → ragged list."""
_child_column(s::Arrow.Struct, j::Int) = _wrap_nested(s.data[j])
_child_column(l::Arrow.List, j::Int) = _wrap_nested(_member_list(l, j))
function _child_column(cv::ChainedVector, j::Int)
    first(cv.arrays) isa Arrow.Struct ?
        _wrap_nested(ChainedVector([s.data[j] for s in cv.arrays])) :
        _wrap_nested(ChainedVector([_member_list(l, j) for l in cv.arrays]))
end

"""
Ragged view of one struct member: the member's child array under the same list levels,
sharing each level's offsets and validity. The struct may sit under several list levels.
"""
function _member_list(l::Arrow.List, j::Int)
    child = l.data isa Arrow.Struct ? l.data.data[j] : _member_list(l.data, j)
    _make_list(child, l.validity, l.offsets.offsets, length(l), Missing <: eltype(l))
end

"""The struct array reached from `v` through any number of list levels, or `nothing`."""
_inner_struct(s::Arrow.Struct) = s
_inner_struct(l::Arrow.List) = _inner_struct(l.data)
_inner_struct(::Any) = nothing

"""
Wrap an array whose elements are structs, directly (`StructColumn`) or through list levels
(`ListOfStructsColumn`), so named access composes (`tbl.a.b.c`). Other arrays are returned as is.
"""
function _wrap_nested(v::AbstractVector)
    v isa ChainedVector && isempty(v.arrays) && return v
    chunk = _first_chunk(v)
    s = _inner_struct(chunk)
    s === nothing && return v
    fnames = _struct_fnames(typeof(s))
    chunk isa Arrow.Struct ? StructColumn(v, fnames) : ListOfStructsColumn(v, fnames)
end
_struct_fnames(::Type{<:Arrow.Struct{T, S, fnames}}) where {T, S, fnames} = fnames

_first_chunk(v::AbstractVector) = v
_first_chunk(cv::ChainedVector) = first(cv.arrays)

"""
    ColumnReadError(column, cause)

Thrown by `read_parquet` when a column cannot be read, for example because it uses an
encoding or type that is not supported yet. `cause` is the original exception. The other
columns can still be read by passing `columns=` without the failing one.
"""
struct ColumnReadError <: Exception
    column::String
    cause::Exception
end

function Base.showerror(io::IO, e::ColumnReadError)
    print(io, "ColumnReadError: failed to read column \"", e.column, "\". To read the other columns, ",
          "pass `columns=` without it. Caused by: ")
    showerror(io, e.cause)
end

# Column tasks nest: unwrap to the exception that actually failed
_root_cause(e) = e isa TaskFailedException ? _root_cause(e.task.exception) : e

"""
    read_parquet(path::String; columns=nothing) -> Arrow.Table

Read a Parquet file and return an Arrow.Table (Tables.jl-compatible).

`columns` selects what to read, by the dotted paths used to reach the data: `"id"` for a
column, `"wf.values"` for a struct member, `"particles.pt"` for a member of a list of
structs, `"m.key"` for a map's keys. A name selects everything under it; selecting a
member returns its column with only the selected parts. Leaves that are not selected are
not decoded. A name that matches nothing is an `ArgumentError`.

A column's element type admits `Missing` exactly where a null occurs in the data read.

A column that cannot be read throws a [`ColumnReadError`](@ref) naming it; nothing is
skipped silently.
"""
function read_parquet(path::String; columns::Union{AbstractVector{<:AbstractString}, Nothing}=nothing)
    pf = open_parquet(path)
    try
        return read_parquet(pf; columns=columns)
    finally
        close(pf)
    end
end

"""Read and decode all pages from one row group's column chunk."""
function _read_pages_for_rg(data::Vector{UInt8}, rg::RowGroup, column_path::Vector{String}, node::SchemaNode)
    idx = findfirst(c -> c.meta_data !== nothing && c.meta_data.path_in_schema == column_path, rg.columns)
    idx === nothing && error("Column chunk not found: $(join(column_path, "."))")
    type_length = Int(something(node.element.type_length, 0))
    reader = ColumnReader(data, rg.columns[idx].meta_data, node, type_length)
    pages = read_all_pages(reader)
    isempty(pages) ? _empty_pages(node) : pages
end

# Zero-row file with no row groups at all
_read_pages_for_rg(::Vector{UInt8}, ::Nothing, ::Vector{String}, node::SchemaNode) = _empty_pages(node)

"""
One empty page of the leaf's physical type, standing in for a column chunk without
pages (zero-row row group or file) so the usual assembly yields typed empty columns.
"""
function _empty_pages(node::SchemaNode)
    type_length = Int(something(node.element.type_length, 0))
    values = decode_plain(node.element.type, UInt8[], 0, type_length)
    [DecodedPage(values, node.max_def_level > 0 ? Int[] : nothing,
                 node.max_rep_level > 0 ? Int[] : nothing, 0)]
end

"""Concatenate def levels across pages (pages without def levels are all-present)."""
function _page_defs(pages::Vector{<:DecodedPage}, max_def::Int)
    total = sum(p.num_values for p in pages; init=0)
    out = Vector{Int}(undef, total)
    pos = 1
    for p in pages
        if p.def_levels === nothing
            fill!(@view(out[pos:pos+p.num_values-1]), max_def)
        else
            copyto!(out, pos, p.def_levels, 1, p.num_values)
        end
        pos += p.num_values
    end
    out
end

"""Def levels at record starts (rep == 0) of a repeated member's level streams."""
function _record_defs(all_rep, all_def)
    out = Vector{Int}(undef, count(==(0), all_rep))
    rec = 0
    @inbounds for i in eachindex(all_rep)
        all_rep[i] == 0 || continue
        rec += 1
        out[rec] = all_def[i]
    end
    out
end

"""Invert a Parquet nulls BitVector into an Arrow ValidityBitmap."""
function _validity(nulls::BitVector)
    nc = count(nulls)
    nc == 0 && return Arrow.ValidityBitmap(UInt8[], 1, length(nulls), 0)
    bytes = Vector{UInt8}(reinterpret(UInt8, .~nulls.chunks))
    Arrow.ValidityBitmap(bytes, 1, length(nulls), nc)
end

"""Build an Arrow.Struct from child columns and group-null bits, widening eltype when nullable."""
function _make_struct(children::Tuple, fnames, nulls::BitVector, nullable::Bool, meta)
    NT = NamedTuple{fnames, Tuple{map(eltype, children)...}}
    v = _validity(nulls)
    ET = (v.nc > 0 || nullable) ? Union{Missing, NT} : NT
    Arrow.Struct{ET, typeof(children), fnames}(v, children, length(nulls), meta)
end

"""Build an Arrow.List over `child` from 0-based offsets, widening eltype when `withmissing`."""
function _make_list(child::AbstractVector, v::Arrow.ValidityBitmap, offsets::Vector{Int32},
                    n::Integer, withmissing::Bool, meta=nothing)
    ST = SubArray{eltype(child), 1, typeof(child), Tuple{UnitRange{Int64}}, true}
    ET = withmissing ? Union{Missing, ST} : ST
    Arrow.List{ET, Int32, typeof(child)}(UInt8[], v, Arrow.Offsets(UInt8[], offsets), child, Int(n), meta)
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

"""
What a leaf's values mean beyond its physical type: its ConvertedType, or a
`TimestampType` for timestamps. `logicalType` wins; without it, TIMESTAMP_MILLIS /
TIMESTAMP_MICROS mean UTC-adjusted (per the spec, and as pyarrow reads them).
"""
function leaf_annotation(elem::SchemaElement)
    elem.logical_type !== nothing && return elem.logical_type
    elem.converted_type == CT_TIMESTAMP_MILLIS && return TimestampType(true, :MILLIS)
    elem.converted_type == CT_TIMESTAMP_MICROS && return TimestampType(true, :MICROS)
    elem.converted_type
end

const ARROW_TIME_UNITS = (MILLIS = Arrow.Meta.TimeUnit.MILLISECOND, MICROS = Arrow.Meta.TimeUnit.MICROSECOND,
                          NANOS = Arrow.Meta.TimeUnit.NANOSECOND)

"""
Julia type for a timestamp column: `DateTime` when that is lossless (naive
milliseconds), otherwise `Arrow.Timestamp{unit, tz}` with `tz` `:UTC` or `nothing`.
"""
timestamp_julia_type(ts::TimestampType) =
    ts.unit == :MILLIS && !ts.is_adjusted_to_utc ? DateTime :
    Arrow.Timestamp{ARROW_TIME_UNITS[ts.unit], ts.is_adjusted_to_utc ? :UTC : nothing}

"""Convert primitive values based on the Parquet type and the leaf's annotation (see `leaf_annotation`)."""
function convert_primitive_values(values, ptype, ctype)
    if ctype == CT_DATE && ptype == INT32
        [Date(1970, 1, 1) + Day(v) for v in values]
    elseif ctype isa TimestampType && ptype == INT64
        T = timestamp_julia_type(ctype)
        # Arrow.Timestamp wraps one Int64, so the decoded values are reinterpreted in place
        T === DateTime ? [DateTime(1970, 1, 1) + Millisecond(v) for v in values] : reinterpret(T, values)
    elseif (T = _converted_int_type(ctype)) !== nothing && T !== eltype(values)
        # Bit-truncating, not checked: unsigned values are stored in signed physical
        # types, and null slots hold arbitrary bits
        values .% T
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

"""
Direct FSL assembly: scatter values from rep/def levels into a flat buffer
in a single pass, bypassing intermediate Vector{Vector{T}} creation.
"""
function assemble_fsl_direct(all_rep, all_def, values::AbstractVector{V},
                             max_def::Int, list_size::Int, elem::SchemaElement,
                             def_thresholds::Vector{Int}; nullable::Bool=false,
                             record_null_def::Int = max_def > 0 ? 1 : 0) where V
    T = element_julia_type(elem.type, leaf_annotation(elem))
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

            # Null record: def == 0 for a top-level column; inside a struct, any def
            # below the list member's own def level (struct null or list null)
            if def < record_null_def
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
    T = element_julia_type(ptype, leaf_annotation(elem))
    # Count records from rep levels
    num_records = sum(pages) do page
        page.rep_levels === nothing ? page.num_values : count(==(0), page.rep_levels)
    end

    data = Vector{T}(undef, list_size * num_records)
    nulls = falses(num_records)

    # Convert and copy page values in bulk using a function barrier for type stability
    _fsl_dense_copy!(data, pages, ptype, leaf_annotation(elem))

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
    elseif ctype isa TimestampType && ptype == INT64
        timestamp_julia_type(ctype)
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
