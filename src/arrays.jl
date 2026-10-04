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
        BT = is_utf8 ? String : Vector{UInt8}
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
    j === nothing && return getfield(c, name)
    _child_column(getfield(c, :_data), j)
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

"""
Wrap an array whose elements are structs, directly (`StructColumn`) or through list levels
(`ListOfStructsColumn`), or maps (`MapColumn`), so named access composes (`tbl.a.b.c`).
Other arrays are returned as is.
"""
function _wrap_nested(v::AbstractVector)
    v isa ChainedVector && isempty(v.arrays) && return v
    chunk = _first_chunk(v)
    s = _inner_struct(chunk)
    s === nothing && return v
    fnames = _struct_fnames(typeof(s))
    chunk isa Arrow.Struct ? StructColumn(v, fnames) :
    chunk isa MapVector ? MapColumn(v, fnames) : ListOfStructsColumn(v, fnames)
end
_struct_fnames(::Type{<:Arrow.Struct{T, S, fnames}}) where {T, S, fnames} = fnames

_first_chunk(v::AbstractVector) = v
_first_chunk(cv::ChainedVector) = first(cv.arrays)
