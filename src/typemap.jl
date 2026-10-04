# Type mapping between Parquet and Julia, in both directions: what a leaf's physical
# type and annotation mean as a Julia type (reading), and which physical type and
# annotation a Julia type is written as (writing).

# ── Reading: Parquet → Julia ──────────────────────────────────────────────

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

# ── Writing: Julia → Parquet ──────────────────────────────────────────────

"""Map a Julia element type to (ParquetType, ConvertedType or nothing)."""
function writer_parquet_type(::Type{T}) where T
    T === Union{} && error("write_parquet: column eltype is Missing; use a typed " *
                           "column such as Union{Missing, Int64}[missing, ...]")
    T === Int32   && return (INT32, nothing)
    T === Int64   && return (INT64, nothing)
    # Narrow and unsigned integers: stored in INT32/INT64, annotated with a converted type
    T === Int8    && return (INT32, CT_INT_8)
    T === Int16   && return (INT32, CT_INT_16)
    T === UInt8   && return (INT32, CT_UINT_8)
    T === UInt16  && return (INT32, CT_UINT_16)
    T === UInt32  && return (INT32, CT_UINT_32)
    T === UInt64  && return (INT64, CT_UINT_64)
    T === Date    && return (INT32, CT_DATE)    # days since the Unix epoch
    # Timestamps carry a logicalType (see writer_logical_type). As pyarrow does, the
    # converted type is added only where it says the same thing: UTC millis or micros.
    if T === DateTime || T <: Arrow.Timestamp
        ts = writer_logical_type(T)
        ctype = !ts.is_adjusted_to_utc ? nothing :
                ts.unit == :MILLIS ? CT_TIMESTAMP_MILLIS : ts.unit == :MICROS ? CT_TIMESTAMP_MICROS : nothing
        return (INT64, ctype)
    end
    T === Float32 && return (FLOAT, nothing)
    T === Float64 && return (DOUBLE, nothing)
    T === Bool    && return (BOOLEAN, nothing)
    T <: AbstractString && return (BYTE_ARRAY, CT_UTF8)
    T === Vector{UInt8} && return (BYTE_ARRAY, nothing)
    error("write_parquet: unsupported column eltype $T " *
          "(supported: signed and unsigned integers up to 64 bits, Float32, Float64, Bool, String, " *
          "Date, DateTime, Arrow.Timestamp, Vector{UInt8}, " *
          "vectors or NamedTuples of those, and Missing unions)")
end

"""
The `logicalType` to write for element type `T`, or `nothing`. `DateTime` is a naive
millisecond timestamp. `Arrow.Timestamp{U, TZ}` keeps its unit; Parquet only has a UTC
flag, so any time zone other than `nothing` is written as UTC-adjusted.
"""
writer_logical_type(::Type) = nothing
writer_logical_type(::Type{DateTime}) = TimestampType(false, :MILLIS)
function writer_logical_type(::Type{Arrow.Timestamp{U, TZ}}) where {U, TZ}
    i = findfirst(==(U), values(ARROW_TIME_UNITS))
    i === nothing && error("write_parquet: Parquet has no timestamp unit $U (supported: milli-, micro-, nanoseconds)")
    TimestampType(TZ !== nothing, keys(ARROW_TIME_UNITS)[i])
end

"""
Integer-like values as stored in their physical Parquet type, INT32 or INT64: narrow and
unsigned integers bit-preserved, dates as days and datetimes as milliseconds since the
Unix epoch, Arrow timestamps as their count of units.
"""
physical_ints(values::Vector{<:Union{Int32, Int64}}) = values
physical_ints(values::Vector{<:Union{Int8, Int16, UInt8, UInt16, UInt32}}) = values .% Int32
physical_ints(values::Vector{UInt64}) = values .% Int64
physical_ints(values::Vector{Date}) = Int32[Dates.value(v - Date(1970, 1, 1)) for v in values]
physical_ints(values::Vector{DateTime}) = Int64[Dates.value(v - DateTime(1970, 1, 1)) for v in values]
physical_ints(values::Vector{<:Arrow.Timestamp}) = Int64[v.x for v in values]
