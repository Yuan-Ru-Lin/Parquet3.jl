# The array types `read_parquet` returns, and the builders that wrap decoded buffers in them.

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
A `FixedSizeView` whose elements can be null: the same flat array, plus the column's
element-level null bits (Arrow's validity of a fixed-size list's child array). `v[j]` is
`missing` for a null element.
"""
struct NullableFixedSizeView{N, T} <: AbstractVector{Union{Missing, T}}
    parent::Vector{T}
    nulls::BitVector    # one per element of `parent`; true = null
    offset::Int
end

Base.size(::NullableFixedSizeView{N}) where N = (N,)
Base.IndexStyle(::Type{<:NullableFixedSizeView}) = Base.IndexLinear()
@Base.propagate_inbounds function Base.getindex(v::NullableFixedSizeView, i::Int)
    @boundscheck checkbounds(v, i)
    @inbounds v.nulls[v.offset + i] ? missing : v.parent[v.offset + i]
end

ArrowTypes.ArrowKind(::Type{NullableFixedSizeView{N,T}}) where {N,T} = ArrowTypes.FixedSizeListKind{N,Union{Missing,T}}()

"""
Fixed-size list column with null elements: `FixedSizeListVector`'s flat array and
record-level nulls, plus one null bit per element. A column is given this type only when
a null element occurs in it; otherwise it is a `FixedSizeListVector`.
"""
struct NullableFixedSizeListVector{N, T, ET} <: AbstractVector{ET}
    data::Vector{T}
    element_nulls::BitVector    # one per element of `data`; true = null
    nulls::BitVector            # true = record is null
    len::Int
end

Base.size(v::NullableFixedSizeListVector) = (v.len,)
Base.IndexStyle(::Type{<:NullableFixedSizeListVector}) = Base.IndexLinear()
@Base.propagate_inbounds function Base.getindex(v::NullableFixedSizeListVector{N,T}, i::Int) where {N,T}
    @boundscheck checkbounds(v, i)
    v.nulls[i] && return missing
    NullableFixedSizeView{N,T}(v.data, v.element_nulls, (i - 1) * N)
end

# ── Map types ────────────────────────────────────────────────────────────────

"""
    MapView{K, V} <: AbstractDict{K, V}

One row of a map column: a zero-copy view of that row's keys and values. It is what a
map column returns on index access, as `FixedSizeView` is for a fixed-size list.

Entries are kept as stored: iteration yields `key => value` pairs in file order, including
duplicate keys, and `keys(m)` / `values(m)` are views into the column's key and value
arrays. Lookup (`m[k]`, `get`, `haskey`) is a linear scan, which suits the small maps
Parquet files hold; when a key occurs more than once the last entry wins, as the Parquet
format specifies and as `Dict(m)` gives. Use `Dict(m)` for a hashed copy.
"""
struct MapView{K, V, KA <: AbstractVector{K}, VA <: AbstractVector{V}} <: AbstractDict{K, V}
    keys::KA
    values::VA
end

Base.length(m::MapView) = length(m.keys)
Base.keys(m::MapView) = m.keys
Base.values(m::MapView) = m.values
Base.iterate(m::MapView, i::Int = 1) = i > length(m.keys) ? nothing : (m.keys[i] => m.values[i], i + 1)
function Base.get(m::MapView, key, default)
    i = findlast(isequal(key), m.keys)
    i === nothing ? default : m.values[i]
end
# Short form, without the array type parameters
function Base.show(io::IO, m::MapView)
    print(io, "MapView(")
    join(io, (sprint(show, k => v; context = io) for (k, v) in m), ", ")
    print(io, ")")
end
Base.show(io::IO, ::MIME"text/plain", m::MapView) = show(io, m)

"""
Map column chunk: Arrow's layout for a map — a list of key/value entries, here an
`Arrow.List` over an `Arrow.Struct` of the key and value arrays — presented as
`MapView`s. It is an array type of its own so that a map nested in a struct or a list
presents the same way as a top-level one.
"""
struct MapVector{ET, L <: AbstractVector} <: AbstractVector{ET}
    entries::L
end

function MapVector(entries::Arrow.List)
    ks, vs = entries.data.data
    view_type(a) = SubArray{eltype(a), 1, typeof(a), Tuple{UnitRange{Int64}}, true}
    M = MapView{eltype(ks), eltype(vs), view_type(ks), view_type(vs)}
    MapVector{Missing <: eltype(entries) ? Union{Missing, M} : M, typeof(entries)}(entries)
end

Base.size(m::MapVector) = size(m.entries)
Base.IndexStyle(::Type{<:MapVector}) = Base.IndexLinear()
@Base.propagate_inbounds function Base.getindex(m::MapVector, i::Int)
    row = m.entries[i]              # a view of the entries struct array, or missing
    row === missing && return missing
    ks, vs = parent(row).data
    range = only(parentindices(row))
    MapView(view(ks, range), view(vs, range))
end
Arrow.getmetadata(m::MapVector) = Arrow.getmetadata(m.entries)

# ── Arrow array builders ─────────────────────────────────────────────────────

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
        # Binary uses Arrow.jl's own element type for it, so Arrow.write accepts the array
        BT = is_utf8 ? String : Base.CodeUnits{UInt8, String}
        ET = has_nulls ? Union{Missing,BT} : BT
        return Arrow.List{ET, Int32, Vector{UInt8}}(UInt8[], v, Arrow.Offsets(UInt8[], offsets), flat, n, meta)
    elseif ptype == BOOLEAN
        # Null slots hold no value (their bytes may be anything), so they are masked out
        bytes = packed_bits(BitVector(leaf_values) .& .!leaf_nulls)
        ET = has_nulls ? Union{Missing,Bool} : Bool
        return Arrow.BoolVector{ET}(bytes, 1, v, Int64(n), meta)
    else
        ET = has_nulls ? Union{Missing,T} : T
        return Arrow.Primitive(ET, UInt8[], v, leaf_values, n, meta)
    end
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

"""Map column: rows are `MapView`s; `col.key` and `col.value` are the keys and values of
every row as ragged lists sharing the map's offsets and validity."""
const MapColumn{T, fnames, D} = NestedColumn{:map, T, fnames, D}

"""List column whose items are fixed-size lists, at any list depth. It behaves as the list
it wraps; the wrapper is what lets `Arrow.write` take the column without re-encoding it."""
const ListColumn{T, D} = NestedColumn{:list, T, (), D}

ListColumn(data::AbstractVector{T}) where T = NestedColumn{:list, T, (), typeof(data)}(data)
MapColumn(data::AbstractVector{T}, fnames::Tuple{Vararg{Symbol}}) where T =
    NestedColumn{:map, T, fnames, typeof(data)}(data)
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
    j === nothing || return _child_column(getfield(c, :_data), j)
    # Arrow.jl splits a table into record batches by taking `column.arrays[i]` of every
    # column once the first one is chunked; answer with this column's per-row-group chunks.
    name === :arrays && return [_wrap_nested(chunk) for chunk in _chunks(getfield(c, :_data))]
    getfield(c, name)
end

"""Project field `j` out of a nested container: struct → child column, list-over-struct → ragged list."""
_child_column(s::Arrow.Struct, j::Int) = _wrap_nested(s.data[j])
_child_column(l::Union{Arrow.List, MapVector}, j::Int) = _wrap_nested(_member_list(l, j))
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
_member_list(m::MapVector, j::Int) = _member_list(m.entries, j)

"""The struct array reached from `v` through any number of list levels, or `nothing`."""
_inner_struct(s::Arrow.Struct) = s
_inner_struct(l::Arrow.List) = _inner_struct(l.data)
_inner_struct(m::MapVector) = _inner_struct(m.entries)
_inner_struct(::Any) = nothing

"""Whether `v` is a fixed-size list array, looking through any number of list levels."""
_fixed_size_items(::Union{FixedSizeListVector, NullableFixedSizeListVector}) = true
_fixed_size_items(l::Arrow.List) = _fixed_size_items(l.data)
_fixed_size_items(::Any) = false

"""
Wrap an array whose elements are structs, directly (`StructColumn`) or through list levels
(`ListOfStructsColumn`), or maps (`MapColumn`), so named access composes (`tbl.a.b.c`).
A list of fixed-size lists becomes a `ListColumn`. Other arrays are returned as is.
"""
function _wrap_nested(v::AbstractVector)
    v isa ChainedVector && isempty(v.arrays) && return v
    chunk = _first_chunk(v)
    s = _inner_struct(chunk)
    s === nothing && return chunk isa Arrow.List && _fixed_size_items(chunk) ? ListColumn(v) : v
    fnames = _struct_fnames(typeof(s))
    chunk isa Arrow.Struct ? StructColumn(v, fnames) :
    chunk isa MapVector ? MapColumn(v, fnames) : ListOfStructsColumn(v, fnames)
end
_struct_fnames(::Type{<:Arrow.Struct{T, S, fnames}}) where {T, S, fnames} = fnames

_first_chunk(v::AbstractVector) = v
_first_chunk(cv::ChainedVector) = first(cv.arrays)
_chunks(v::AbstractVector) = [v]
_chunks(cv::ChainedVector) = cv.arrays

# ── Arrow.write ──────────────────────────────────────────────────────────────
#
# Arrow.write takes any array that is one of Arrow.jl's own types as it is, and re-encodes
# everything else row by row. Our arrays are Arrow.jl arrays underneath, wrapped
# (NestedColumn), or laid out the way Arrow lays them out (FixedSizeListVector, MapVector).
# `_arrow_native` hands Arrow.jl the equivalent array of its own types over the same
# buffers, so nothing is re-encoded.

"""
An array of Arrow.jl's own types equivalent to `x`, sharing its buffers. Arrays that
already are one are returned as they are.
"""
_arrow_native(x::AbstractVector) = x
_arrow_native(c::NestedColumn) = _arrow_native(getfield(c, :_data))

# Several row groups in one Arrow array: the chunks' buffers have to be joined
_arrow_native(cv::ChainedVector) = _arrow_concat([_arrow_native(chunk) for chunk in cv.arrays])

_arrow_native(l::Arrow.List{T, O, Vector{UInt8}}) where {T, O} = l       # strings and binary
_arrow_native(l::Arrow.List) = _list_over(l, _arrow_native(l.data))

function _arrow_native(s::Arrow.Struct{T, S, fnames}) where {T, S, fnames}
    children = map(_arrow_native, s.data)
    all(children .=== s.data) ? s : _struct_over(s, children)
end

"""
The child array of a fixed-size list for Arrow.jl. Numbers and `Arrow.Timestamp`s are
stored as Arrow stores them, so the flat vector is reused; `Bool` (bit-packed in Arrow),
`Date` and `DateTime` (other integers in Arrow) are encoded by Arrow.jl, which copies.
"""
_fixed_size_child(data::Vector{T}, nulls::Union{BitVector, Nothing}) where {T <: Union{Integer, AbstractFloat, Arrow.Timestamp}} =
    Arrow.Primitive(nulls === nothing ? T : Union{Missing, T}, UInt8[],
                    _validity(nulls === nothing ? falses(length(data)) : nulls), data, length(data), nothing)
_fixed_size_child(data::Vector{Bool}, nulls::Union{BitVector, Nothing}) = _fixed_size_child_encoded(data, nulls)
_fixed_size_child(data::Vector, nulls::Union{BitVector, Nothing}) = _fixed_size_child_encoded(data, nulls)
_fixed_size_child_encoded(data::Vector{T}, nulls) where T =
    Arrow.toarrowvector(nulls === nothing ? data : Union{Missing, T}[n ? missing : d for (d, n) in zip(data, nulls)])

function _fixed_size_over(child::AbstractVector, N::Int, nulls::BitVector, len::Int, nullable::Bool)
    E = NTuple{N, eltype(child)}
    Arrow.FixedSizeList{nullable ? Union{Missing, E} : E, typeof(child)}(UInt8[], _validity(nulls), child, len, nothing)
end

_arrow_native(v::FixedSizeListVector{N, T, ET}) where {N, T, ET} =
    _fixed_size_over(_fixed_size_child(v.data, nothing), N, v.nulls, v.len, Missing <: ET)
_arrow_native(v::NullableFixedSizeListVector{N, T, ET}) where {N, T, ET} =
    _fixed_size_over(_fixed_size_child(v.data, v.element_nulls), N, v.nulls, v.len, Missing <: ET)

function _arrow_native(m::MapVector)
    l = m.entries
    entries = _arrow_native(l.data)
    D = Dict{eltype(entries.data[1]), eltype(entries.data[2])}
    Arrow.Map{Missing <: eltype(m) ? Union{Missing, D} : D, Int32, typeof(entries)}(l.validity, l.offsets, entries, l.ℓ, l.metadata)
end

"""`l` with `child` in place of its elements' array (same offsets and validity)."""
function _list_over(l::Arrow.List{T, O}, child::AbstractVector) where {T, O}
    child === l.data && return l
    E = SubArray{eltype(child), 1, typeof(child), Tuple{UnitRange{Int64}}, true}
    Arrow.List{Missing <: T ? Union{Missing, E} : E, O, typeof(child)}(l.arrow, l.validity, l.offsets, child, l.ℓ, l.metadata)
end

"""`s` with `children` in place of its members' arrays (same validity)."""
function _struct_over(s::Arrow.Struct{T, S, fnames}, children::Tuple) where {T, S, fnames}
    NT = NamedTuple{fnames, Tuple{map(eltype, children)...}}
    Arrow.Struct{Missing <: T ? Union{Missing, NT} : NT, typeof(children), fnames}(s.validity, children, s.ℓ, s.metadata)
end

Arrow.arrowvector(x::Union{NestedColumn, FixedSizeListVector, NullableFixedSizeListVector, MapVector}, i, nl, fi, de, ded, meta; kw...) = _arrow_native(x)

"""
Join Arrow.jl arrays of one type — a column's row-group chunks — into one array. Needed
when a chunked column is written as a single Arrow record batch; buffers are copied once,
column by column, not re-encoded row by row.
"""
function _arrow_concat(xs::Vector)
    length(xs) == 1 && return only(xs)
    _concat(xs, _validity(reduce(vcat, [BitVector(!x.validity[i] for i in 1:length(x)) for x in xs])), sum(length, xs))
end

_concat(xs::Vector{<:Arrow.Primitive{T}}, validity, n) where T =
    Arrow.Primitive(T, UInt8[], validity, reduce(vcat, [collect(x.data) for x in xs]), n, first(xs).metadata)

_concat(xs::Vector{<:Arrow.BoolVector{T}}, validity, n) where T =
    Arrow.BoolVector{T}(packed_bits(reduce(vcat, [BitVector(coalesce.(x, false)) for x in xs])), 1, validity, n, first(xs).metadata)

# Offsets of the joined list: each chunk's offsets, shifted by the items before it
function _joined_offsets(xs, item_count)
    offsets, shift = Int32[0], Int32(0)
    for x in xs
        own = x.offsets.offsets
        append!(offsets, @view(own[2:end]) .- first(own) .+ shift)
        shift += Int32(item_count(x))
    end
    Arrow.Offsets(UInt8[], offsets)
end
_used_bytes(x) = @view x.data[first(x.offsets.offsets) + 1 : last(x.offsets.offsets)]

function _concat(xs::Vector{<:Arrow.List{T, O, Vector{UInt8}}}, validity, n) where {T, O}
    Arrow.List{T, O, Vector{UInt8}}(UInt8[], validity, _joined_offsets(xs, x -> length(_used_bytes(x))),
                                    reduce(vcat, map(_used_bytes, xs)), n, first(xs).metadata)
end

function _concat(xs::Vector{<:Arrow.List}, validity, n)
    child = _arrow_concat([x.data for x in xs])
    first_list = first(xs)
    l = typeof(first_list)(UInt8[], validity, _joined_offsets(xs, x -> length(x.data)), first_list.data, n, first_list.metadata)
    _list_over(l, child)
end

function _concat(xs::Vector{<:Arrow.Struct}, validity, n)
    first_struct = first(xs)
    children = Tuple(_arrow_concat([x.data[j] for x in xs]) for j in eachindex(first_struct.data))
    _struct_over(typeof(first_struct)(validity, first_struct.data, n, first_struct.metadata), children)
end

function _concat(xs::Vector{<:Arrow.FixedSizeList{T}}, validity, n) where T
    child = _arrow_concat([x.data for x in xs])
    Arrow.FixedSizeList{T, typeof(child)}(UInt8[], validity, child, n, first(xs).metadata)
end

function _concat(xs::Vector{<:Arrow.Map{T, O}}, validity, n) where {T, O}
    entries = _arrow_concat([x.data for x in xs])
    Arrow.Map{T, O, typeof(entries)}(validity, _joined_offsets(xs, x -> length(x.data)), entries, n, first(xs).metadata)
end
