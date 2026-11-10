# Parquet metadata: table-driven Thrift (compact protocol) reading and writing via Thrift.jl.
#
# Each Thrift struct has one field table, `(field_id, field_name, type)`, used in both
# directions. A `ThriftType` says how a field is tagged on the wire and how its value is
# read and written. The tables are written by hand from parquet.thrift; fields that are
# not listed are skipped on read and never written.

import Thrift: julia_type

"""A Thrift field type: its wire type, and how to read and write a value of it."""
struct ThriftType{R, W}
    ttype::Int32
    read::R
    write::W
end

"""Read a Thrift struct into a `T` (built by keyword), using its field table."""
function read_thrift(p, ::Type{T}, fields) where T
    readStructBegin(p)
    kw = Dict{Symbol,Any}()
    while true
        _, ttype, fid = readFieldBegin(p)
        ttype == TType.STOP && break
        idx = findfirst(f -> first(f) == fid, fields)
        if idx !== nothing
            _, name, type = fields[idx]
            kw[name] = type.read(p)
        else
            skip(p, julia_type(ttype))
        end
        readFieldEnd(p)
    end
    readStructEnd(p)
    T(; kw...)
end

"""Write `obj` as a Thrift struct using its field table; fields that are `nothing` are omitted."""
function write_thrift(p, obj, fields)
    writeStructBegin(p, "")
    for (fid, name, type) in fields
        val = getfield(obj, name)
        val === nothing && continue
        writeFieldBegin(p, "", type.ttype, fid)
        type.write(p, val)
        writeFieldEnd(p)
    end
    writeFieldStop(p)
    writeStructEnd(p)
end

"""Serialize `obj` to compact-protocol bytes using its field table."""
function serialize_thrift(obj, fields)
    t = TMemoryTransport()
    write_thrift(TCompactProtocol(t), obj, fields)
    take!(t.buff)
end

function read_list(p, reader)
    _, n = readListBegin(p)
    items = [reader(p) for _ in 1:n]
    readListEnd(p)
    items
end

function write_list(p, etype::Int32, items, writer)
    writeListBegin(p, etype, length(items))
    foreach(item -> writer(p, item), items)
    writeListEnd(p)
end

"""Read a Thrift union, returning `reader(p, field_id)` for its one set member (`nothing` to skip it)."""
function read_union(reader, p)
    result = nothing
    readStructBegin(p)
    while true
        _, ttype, fid = readFieldBegin(p)
        ttype == TType.STOP && break
        value = reader(p, fid)
        value === nothing ? skip(p, julia_type(ttype)) : (result = value)
        readFieldEnd(p)
    end
    readStructEnd(p)
    result
end

"""Write a Thrift union with its single member `fid` (a struct written by `writer`)."""
function write_union(writer, p, fid::Integer)
    writeStructBegin(p, "")
    writeFieldBegin(p, "", TType.STRUCT, fid)
    writer(p)
    writeFieldEnd(p)
    writeFieldStop(p)
    writeStructEnd(p)
end

# ── Field types ───────────────────────────────────────────────────────────

const T_I32    = ThriftType(TType.I32, p -> read(p, Int32), (p, v) -> write(p, Int32(v)))
const T_I64    = ThriftType(TType.I64, p -> read(p, Int64), (p, v) -> write(p, Int64(v)))
const T_STRING = ThriftType(TType.STRING, p -> read(p, String), (p, v) -> write(p, v))
const T_BINARY = ThriftType(TType.STRING, p -> read(p, Vector{UInt8}), (p, v) -> write(p, v))
# In the compact protocol a bool field's value lives in its field header, hence writeBool
const T_BOOL   = ThriftType(TType.BOOL, p -> read(p, Bool), writeBool)

"""An enum, carried as its Int32 value."""
t_enum(::Type{E}) where E = ThriftType(TType.I32, p -> E(read(p, Int32)), (p, v) -> write(p, Int32(v)))

"""A nested struct of type `T` with its own field table."""
t_struct(::Type{T}, fields) where T =
    ThriftType(TType.STRUCT, p -> read_thrift(p, T, fields), (p, v) -> write_thrift(p, v, fields))

"""A list whose elements are of `type`."""
t_list(type::ThriftType) =
    ThriftType(TType.LIST, p -> read_list(p, type.read), (p, v) -> write_list(p, type.ttype, v, type.write))

# LogicalType is a union; only its TIMESTAMP member (field 8) is read or written. A
# TimeUnit is a union of empty structs, identified by the member's field id.
const TIME_UNITS = (:MILLIS, :MICROS, :NANOS)

_write_empty_struct(p) = (writeStructBegin(p, ""); writeFieldStop(p); writeStructEnd(p))

const T_TIME_UNIT = ThriftType(TType.STRUCT,
    p -> read_union((q, fid) -> fid in 1:3 ? (skip(q, julia_type(TType.STRUCT)); TIME_UNITS[fid]) : nothing, p),
    (p, v) -> write_union(_write_empty_struct, p, findfirst(==(v), TIME_UNITS)))

const TIMESTAMP_TYPE_FIELDS = [
    (1, :is_adjusted_to_utc, T_BOOL),
    (2, :unit,               T_TIME_UNIT),
]

const T_LOGICAL_TYPE = ThriftType(TType.STRUCT,
    p -> read_union((q, fid) -> fid == 8 ? read_thrift(q, TimestampType, TIMESTAMP_TYPE_FIELDS) : nothing, p),
    (p, ts) -> write_union(q -> write_thrift(q, ts, TIMESTAMP_TYPE_FIELDS), p, 8))

# ── Field tables ──────────────────────────────────────────────────────────

const SCHEMA_ELEMENT_FIELDS = [
    (1,  :type,            t_enum(ParquetType)),
    (2,  :type_length,     T_I32),
    (3,  :repetition_type, t_enum(FieldRepetitionType)),
    (4,  :name,            T_STRING),
    (5,  :num_children,    T_I32),
    (6,  :converted_type,  t_enum(ConvertedType)),
    (7,  :scale,           T_I32),
    (8,  :precision,       T_I32),
    (9,  :field_id,        T_I32),
    (10, :logical_type,    T_LOGICAL_TYPE),
]

const STATISTICS_FIELDS = [
    (1, :max,            T_BINARY),
    (2, :min,            T_BINARY),
    (3, :null_count,     T_I64),
    (4, :distinct_count, T_I64),
    (5, :max_value,      T_BINARY),
    (6, :min_value,      T_BINARY),
]

const COLUMN_METADATA_FIELDS = [
    (1,  :type,                    t_enum(ParquetType)),
    (2,  :encodings,               t_list(t_enum(Encoding))),
    (3,  :path_in_schema,          t_list(T_STRING)),
    (4,  :codec,                   t_enum(CompressionCodec)),
    (5,  :num_values,              T_I64),
    (6,  :total_uncompressed_size, T_I64),
    (7,  :total_compressed_size,   T_I64),
    (9,  :data_page_offset,        T_I64),
    (10, :index_page_offset,       T_I64),
    (11, :dictionary_page_offset,  T_I64),
    (12, :statistics,              t_struct(Statistics, STATISTICS_FIELDS)),
]

const COLUMN_CHUNK_FIELDS = [
    (1, :file_path,   T_STRING),
    (2, :file_offset, T_I64),
    (3, :meta_data,   t_struct(ColumnMetaData, COLUMN_METADATA_FIELDS)),
]

const ROW_GROUP_FIELDS = [
    (1, :columns,               t_list(t_struct(ColumnChunk, COLUMN_CHUNK_FIELDS))),
    (2, :total_byte_size,       T_I64),
    (3, :num_rows,              T_I64),
    (5, :file_offset,           T_I64),
    (6, :total_compressed_size, T_I64),
]

const KEY_VALUE_FIELDS = [
    (1, :key,   T_STRING),
    (2, :value, T_STRING),
]

const FILE_METADATA_FIELDS = [
    (1, :version,            T_I32),
    (2, :schema,             t_list(t_struct(SchemaElement, SCHEMA_ELEMENT_FIELDS))),
    (3, :num_rows,           T_I64),
    (4, :row_groups,         t_list(t_struct(RowGroup, ROW_GROUP_FIELDS))),
    (5, :key_value_metadata, t_list(t_struct(KeyValue, KEY_VALUE_FIELDS))),
    (6, :created_by,         T_STRING),
]

const DATA_PAGE_HEADER_FIELDS = [
    (1, :num_values,                T_I32),
    (2, :encoding,                  t_enum(Encoding)),
    (3, :definition_level_encoding, t_enum(Encoding)),
    (4, :repetition_level_encoding, t_enum(Encoding)),
    (5, :statistics,                t_struct(Statistics, STATISTICS_FIELDS)),
]

const DATA_PAGE_HEADER_V2_FIELDS = [
    (1, :num_values,                    T_I32),
    (2, :num_nulls,                     T_I32),
    (3, :num_rows,                      T_I32),
    (4, :encoding,                      t_enum(Encoding)),
    (5, :definition_levels_byte_length, T_I32),
    (6, :repetition_levels_byte_length, T_I32),
    (7, :is_compressed,                 T_BOOL),
    (8, :statistics,                    t_struct(Statistics, STATISTICS_FIELDS)),
]

const DICTIONARY_PAGE_HEADER_FIELDS = [
    (1, :num_values, T_I32),
    (2, :encoding,   t_enum(Encoding)),
    (3, :is_sorted,  T_BOOL),
]

const PAGE_HEADER_FIELDS = [
    (1, :type,                   t_enum(PageType)),
    (2, :uncompressed_page_size, T_I32),
    (3, :compressed_page_size,   T_I32),
    (4, :crc,                    T_I32),
    (5, :data_page_header,       t_struct(DataPageHeader, DATA_PAGE_HEADER_FIELDS)),
    (7, :dictionary_page_header, t_struct(DictionaryPageHeader, DICTIONARY_PAGE_HEADER_FIELDS)),
    (8, :data_page_header_v2,    t_struct(DataPageHeaderV2, DATA_PAGE_HEADER_V2_FIELDS)),
]

parse_file_metadata(data::Vector{UInt8}) =
    read_thrift(TCompactProtocol(TMemoryTransport(data)), FileMetaData, FILE_METADATA_FIELDS)
