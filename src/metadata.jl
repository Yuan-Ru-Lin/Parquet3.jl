# Parquet metadata parsing — table-driven Thrift deserialization via Thrift.jl

import Thrift: julia_type

"""Generic Thrift struct reader driven by a field table.
Each entry in `fields` is `(field_id, kwarg_name, reader_function)`."""
function read_thrift(p, ::Type{T}, fields) where T
    readStructBegin(p)
    kw = Dict{Symbol,Any}()
    while true
        _, ttype, fid = readFieldBegin(p)
        ttype == TType.STOP && break
        idx = findfirst(f -> first(f) == fid, fields)
        if idx !== nothing
            _, name, reader = fields[idx]
            kw[name] = reader(p)
        else
            skip(p, julia_type(ttype))
        end
        readFieldEnd(p)
    end
    readStructEnd(p)
    T(; kw...)
end

"""Read a Thrift list, calling `reader` for each element."""
function read_list(p, reader)
    _, n = readListBegin(p)
    items = [reader(p) for _ in 1:n]
    readListEnd(p)
    items
end

# ── LogicalType (union): only the TIMESTAMP member is parsed ──────────────

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

const TIME_UNITS = (:MILLIS, :MICROS, :NANOS)    # TimeUnit union field ids 1, 2, 3

# Each TimeUnit member is an empty struct; skip it and keep the field id
read_time_unit(p) = read_union((q, fid) -> fid in 1:3 ? (skip(q, julia_type(TType.STRUCT)); TIME_UNITS[fid]) : nothing, p)

const TIMESTAMP_TYPE_FIELDS = [
    (1, :is_adjusted_to_utc, p -> read(p, Bool)),
    (2, :unit,               read_time_unit),
]

read_logical_type(p) =
    read_union((q, fid) -> fid == 8 ? read_thrift(q, TimestampType, TIMESTAMP_TYPE_FIELDS) : nothing, p)

# ── Field tables ──────────────────────────────────────────────────────────

const SCHEMA_ELEMENT_FIELDS = [
    (1, :type,            p -> ParquetType(read(p, Int32))),
    (2, :type_length,     p -> read(p, Int32)),
    (3, :repetition_type, p -> FieldRepetitionType(read(p, Int32))),
    (4, :name,            p -> read(p, String)),
    (5, :num_children,    p -> read(p, Int32)),
    (6, :converted_type,  p -> ConvertedType(read(p, Int32))),
    (7, :scale,           p -> read(p, Int32)),
    (8, :precision,       p -> read(p, Int32)),
    (9, :field_id,        p -> read(p, Int32)),
    (10, :logical_type,   read_logical_type),
]

const STATISTICS_FIELDS = [
    (1, :max,            p -> read(p, Vector{UInt8})),
    (2, :min,            p -> read(p, Vector{UInt8})),
    (3, :null_count,     p -> read(p, Int64)),
    (4, :distinct_count, p -> read(p, Int64)),
    (5, :max_value,      p -> read(p, Vector{UInt8})),
    (6, :min_value,      p -> read(p, Vector{UInt8})),
]

const COLUMN_METADATA_FIELDS = [
    (1,  :type,                    p -> ParquetType(read(p, Int32))),
    (2,  :encodings,               p -> read_list(p, q -> Encoding(read(q, Int32)))),
    (3,  :path_in_schema,          p -> read_list(p, q -> read(q, String))),
    (4,  :codec,                   p -> CompressionCodec(read(p, Int32))),
    (5,  :num_values,              p -> read(p, Int64)),
    (6,  :total_uncompressed_size, p -> read(p, Int64)),
    (7,  :total_compressed_size,   p -> read(p, Int64)),
    (9,  :data_page_offset,        p -> read(p, Int64)),
    (10, :index_page_offset,       p -> read(p, Int64)),
    (11, :dictionary_page_offset,  p -> read(p, Int64)),
    (12, :statistics,              p -> read_thrift(p, Statistics, STATISTICS_FIELDS)),
]

const COLUMN_CHUNK_FIELDS = [
    (1, :file_path,  p -> read(p, String)),
    (2, :file_offset, p -> read(p, Int64)),
    (3, :meta_data,  p -> read_thrift(p, ColumnMetaData, COLUMN_METADATA_FIELDS)),
]

const ROW_GROUP_FIELDS = [
    (1, :columns,               p -> read_list(p, q -> read_thrift(q, ColumnChunk, COLUMN_CHUNK_FIELDS))),
    (2, :total_byte_size,       p -> read(p, Int64)),
    (3, :num_rows,              p -> read(p, Int64)),
    (6, :file_offset,           p -> read(p, Int64)),
    (7, :total_compressed_size, p -> read(p, Int64)),
]

const KEY_VALUE_FIELDS = [
    (1, :key,   p -> read(p, String)),
    (2, :value, p -> read(p, String)),
]

const FILE_METADATA_FIELDS = [
    (1, :version,            p -> read(p, Int32)),
    (2, :schema,             p -> read_list(p, q -> read_thrift(q, SchemaElement, SCHEMA_ELEMENT_FIELDS))),
    (3, :num_rows,           p -> read(p, Int64)),
    (4, :row_groups,         p -> read_list(p, q -> read_thrift(q, RowGroup, ROW_GROUP_FIELDS))),
    (5, :key_value_metadata, p -> read_list(p, q -> read_thrift(q, KeyValue, KEY_VALUE_FIELDS))),
    (6, :created_by,         p -> read(p, String)),
]

const DATA_PAGE_HEADER_FIELDS = [
    (1, :num_values,                  p -> read(p, Int32)),
    (2, :encoding,                    p -> Encoding(read(p, Int32))),
    (3, :definition_level_encoding,   p -> Encoding(read(p, Int32))),
    (4, :repetition_level_encoding,   p -> Encoding(read(p, Int32))),
    (5, :statistics,                  p -> read_thrift(p, Statistics, STATISTICS_FIELDS)),
]

const DATA_PAGE_HEADER_V2_FIELDS = [
    (1, :num_values,                      p -> read(p, Int32)),
    (2, :num_nulls,                       p -> read(p, Int32)),
    (3, :num_rows,                        p -> read(p, Int32)),
    (4, :encoding,                        p -> Encoding(read(p, Int32))),
    (5, :definition_levels_byte_length,   p -> read(p, Int32)),
    (6, :repetition_levels_byte_length,   p -> read(p, Int32)),
    (7, :is_compressed,                   p -> read(p, Bool)),
    (8, :statistics,                      p -> read_thrift(p, Statistics, STATISTICS_FIELDS)),
]

const DICTIONARY_PAGE_HEADER_FIELDS = [
    (1, :num_values, p -> read(p, Int32)),
    (2, :encoding,   p -> Encoding(read(p, Int32))),
    (3, :is_sorted,  p -> read(p, Bool)),
]

const PAGE_HEADER_FIELDS = [
    (1, :type,                    p -> PageType(read(p, Int32))),
    (2, :uncompressed_page_size,  p -> read(p, Int32)),
    (3, :compressed_page_size,    p -> read(p, Int32)),
    (4, :crc,                     p -> read(p, Int32)),
    (5, :data_page_header,        p -> read_thrift(p, DataPageHeader, DATA_PAGE_HEADER_FIELDS)),
    (7, :dictionary_page_header,  p -> read_thrift(p, DictionaryPageHeader, DICTIONARY_PAGE_HEADER_FIELDS)),
    (8, :data_page_header_v2,     p -> read_thrift(p, DataPageHeaderV2, DATA_PAGE_HEADER_V2_FIELDS)),
]

# ── Thrift writing (mirror of the table-driven reader) ───────────────────

"""Generic Thrift struct writer driven by a field table.
Each entry is `(field_id, ttype, getter, writer)`; fields whose getter returns
`nothing` are omitted (Thrift optional-field semantics)."""
function write_thrift(p, obj, fields)
    writeStructBegin(p, "")
    for (fid, ttype, getter, writer) in fields
        val = getter(obj)
        val === nothing && continue
        writeFieldBegin(p, "", ttype, fid)
        writer(p, val)
        writeFieldEnd(p)
    end
    writeFieldStop(p)
    writeStructEnd(p)
end

"""Write a Thrift list of `items`, calling `writer` for each element."""
function write_list(p, etype::Int32, items, writer)
    writeListBegin(p, etype, length(items))
    for item in items
        writer(p, item)
    end
    writeListEnd(p)
end

"""Serialize `obj` to compact-protocol bytes using its write field table."""
function serialize_thrift(obj, fields)
    t = TMemoryTransport()
    write_thrift(TCompactProtocol(t), obj, fields)
    take!(t.buff)
end

_w_enum(p, v) = write(p, Int32(v))

# Write tables cover only the fields the writer emits (readers of our files
# treat missing optional fields per Thrift semantics).

"""Write a Thrift union with its single member `fid` (a struct written by `writer`)."""
function write_union(writer, p, fid::Integer)
    writeStructBegin(p, "")
    writeFieldBegin(p, "", TType.STRUCT, fid)
    writer(p)
    writeFieldEnd(p)
    writeFieldStop(p)
    writeStructEnd(p)
end

_write_empty_struct(p) = (writeStructBegin(p, ""); writeFieldStop(p); writeStructEnd(p))

const TIMESTAMP_TYPE_W = [
    # In the compact protocol a bool field's value lives in its field header, hence writeBool
    (1, TType.BOOL,   o -> o.is_adjusted_to_utc, writeBool),
    (2, TType.STRUCT, o -> o.unit, (p, v) -> write_union(_write_empty_struct, p, findfirst(==(v), TIME_UNITS))),
]

write_logical_type(p, ts::TimestampType) = write_union(q -> write_thrift(q, ts, TIMESTAMP_TYPE_W), p, 8)

const SCHEMA_ELEMENT_W = [
    (1, TType.I32,    o -> o.type,            _w_enum),
    (3, TType.I32,    o -> o.repetition_type, _w_enum),
    (4, TType.STRING, o -> o.name,            (p, v) -> write(p, v)),
    (5, TType.I32,    o -> o.num_children,    (p, v) -> write(p, Int32(v))),
    (6, TType.I32,    o -> o.converted_type,  _w_enum),
    (10, TType.STRUCT, o -> o.logical_type,   write_logical_type),
]

const STATISTICS_W = [
    (3, TType.I64, o -> o.null_count, (p, v) -> write(p, Int64(v))),
]

const COLUMN_METADATA_W = [
    (1,  TType.I32,    o -> o.type,                    _w_enum),
    (2,  TType.LIST,   o -> o.encodings,               (p, v) -> write_list(p, TType.I32, v, _w_enum)),
    (3,  TType.LIST,   o -> o.path_in_schema,          (p, v) -> write_list(p, TType.STRING, v, (q, s) -> write(q, s))),
    (4,  TType.I32,    o -> o.codec,                   _w_enum),
    (5,  TType.I64,    o -> o.num_values,              (p, v) -> write(p, Int64(v))),
    (6,  TType.I64,    o -> o.total_uncompressed_size, (p, v) -> write(p, Int64(v))),
    (7,  TType.I64,    o -> o.total_compressed_size,   (p, v) -> write(p, Int64(v))),
    (9,  TType.I64,    o -> o.data_page_offset,        (p, v) -> write(p, Int64(v))),
    (12, TType.STRUCT, o -> o.statistics,              (p, v) -> write_thrift(p, v, STATISTICS_W)),
]

const COLUMN_CHUNK_W = [
    (2, TType.I64,    o -> o.file_offset, (p, v) -> write(p, Int64(v))),
    (3, TType.STRUCT, o -> o.meta_data,   (p, v) -> write_thrift(p, v, COLUMN_METADATA_W)),
]

const ROW_GROUP_W = [
    (1, TType.LIST, o -> o.columns,         (p, v) -> write_list(p, TType.STRUCT, v, (q, c) -> write_thrift(q, c, COLUMN_CHUNK_W))),
    (2, TType.I64,  o -> o.total_byte_size, (p, v) -> write(p, Int64(v))),
    (3, TType.I64,  o -> o.num_rows,        (p, v) -> write(p, Int64(v))),
]

const KEY_VALUE_W = [
    (1, TType.STRING, o -> o.key,   (p, v) -> write(p, v)),
    (2, TType.STRING, o -> o.value, (p, v) -> write(p, v)),
]

const FILE_METADATA_W = [
    (1, TType.I32,    o -> o.version,    (p, v) -> write(p, Int32(v))),
    (2, TType.LIST,   o -> o.schema,     (p, v) -> write_list(p, TType.STRUCT, v, (q, s) -> write_thrift(q, s, SCHEMA_ELEMENT_W))),
    (3, TType.I64,    o -> o.num_rows,   (p, v) -> write(p, Int64(v))),
    (4, TType.LIST,   o -> o.row_groups, (p, v) -> write_list(p, TType.STRUCT, v, (q, r) -> write_thrift(q, r, ROW_GROUP_W))),
    (5, TType.LIST,   o -> o.key_value_metadata, (p, v) -> write_list(p, TType.STRUCT, v, (q, kv) -> write_thrift(q, kv, KEY_VALUE_W))),
    (6, TType.STRING, o -> o.created_by, (p, v) -> write(p, v)),
]

const DATA_PAGE_HEADER_W = [
    (1, TType.I32, o -> o.num_values,                (p, v) -> write(p, Int32(v))),
    (2, TType.I32, o -> o.encoding,                  _w_enum),
    (3, TType.I32, o -> o.definition_level_encoding, _w_enum),
    (4, TType.I32, o -> o.repetition_level_encoding, _w_enum),
]

const PAGE_HEADER_W = [
    (1, TType.I32,    o -> o.type,                   _w_enum),
    (2, TType.I32,    o -> o.uncompressed_page_size, (p, v) -> write(p, Int32(v))),
    (3, TType.I32,    o -> o.compressed_page_size,   (p, v) -> write(p, Int32(v))),
    (5, TType.STRUCT, o -> o.data_page_header,       (p, v) -> write_thrift(p, v, DATA_PAGE_HEADER_W)),
]

# ── Public API (matches old signatures) ──────────────────────────────────

parse_file_metadata(data::Vector{UInt8}) =
    read_thrift(TCompactProtocol(TMemoryTransport(data)), FileMetaData, FILE_METADATA_FIELDS)

parse_page_header(data::Vector{UInt8}) =
    read_thrift(TCompactProtocol(TMemoryTransport(data)), PageHeader, PAGE_HEADER_FIELDS)
