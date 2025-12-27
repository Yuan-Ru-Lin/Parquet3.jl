# Parquet types and enums based on parquet.thrift specification

"""Parquet physical types"""
@enum ParquetType::Int32 begin
    BOOLEAN = 0
    INT32 = 1
    INT64 = 2
    INT96 = 3
    FLOAT = 4
    DOUBLE = 5
    BYTE_ARRAY = 6
    FIXED_LEN_BYTE_ARRAY = 7
end

"""Converted types for logical type mapping"""
@enum ConvertedType::Int32 begin
    CT_NONE = -1
    CT_UTF8 = 0
    CT_MAP = 1
    CT_MAP_KEY_VALUE = 2
    CT_LIST = 3
    CT_ENUM = 4
    CT_DECIMAL = 5
    CT_DATE = 6
    CT_TIME_MILLIS = 7
    CT_TIME_MICROS = 8
    CT_TIMESTAMP_MILLIS = 9
    CT_TIMESTAMP_MICROS = 10
    CT_UINT_8 = 11
    CT_UINT_16 = 12
    CT_UINT_32 = 13
    CT_UINT_64 = 14
    CT_INT_8 = 15
    CT_INT_16 = 16
    CT_INT_32 = 17
    CT_INT_64 = 18
    CT_JSON = 19
    CT_BSON = 20
    CT_INTERVAL = 21
end

"""Field repetition types"""
@enum FieldRepetitionType::Int32 begin
    REQUIRED = 0
    OPTIONAL = 1
    REPEATED = 2
end

"""Encoding types"""
@enum Encoding::Int32 begin
    PLAIN = 0
    # GROUP_VAR_INT = 1  # deprecated
    PLAIN_DICTIONARY = 2
    RLE = 3
    BIT_PACKED = 4
    DELTA_BINARY_PACKED = 5
    DELTA_LENGTH_BYTE_ARRAY = 6
    DELTA_BYTE_ARRAY = 7
    RLE_DICTIONARY = 8
    BYTE_STREAM_SPLIT = 9
end

"""Compression codecs"""
@enum CompressionCodec::Int32 begin
    UNCOMPRESSED = 0
    SNAPPY = 1
    GZIP = 2
    LZO = 3
    BROTLI = 4
    LZ4 = 5
    ZSTD = 6
    LZ4_RAW = 7
end

"""Page types"""
@enum PageType::Int32 begin
    DATA_PAGE = 0
    INDEX_PAGE = 1
    DICTIONARY_PAGE = 2
    DATA_PAGE_V2 = 3
end

"""Boundary order for statistics"""
@enum BoundaryOrder::Int32 begin
    UNORDERED = 0
    ASCENDING = 1
    DESCENDING = 2
end

# Thrift field IDs for Parquet structures
const PARQUET_MAGIC = UInt8[0x50, 0x41, 0x52, 0x31]  # "PAR1"

"""Schema element in Parquet file"""
mutable struct SchemaElement
    type::Union{ParquetType, Nothing}
    type_length::Union{Int32, Nothing}
    repetition_type::Union{FieldRepetitionType, Nothing}
    name::String
    num_children::Union{Int32, Nothing}
    converted_type::Union{ConvertedType, Nothing}
    scale::Union{Int32, Nothing}
    precision::Union{Int32, Nothing}
    field_id::Union{Int32, Nothing}
    # logical_type omitted for simplicity initially
end

SchemaElement() = SchemaElement(nothing, nothing, nothing, "", nothing, nothing, nothing, nothing, nothing)

"""Statistics for a column chunk or page"""
mutable struct Statistics
    max::Union{Vector{UInt8}, Nothing}
    min::Union{Vector{UInt8}, Nothing}
    null_count::Union{Int64, Nothing}
    distinct_count::Union{Int64, Nothing}
    max_value::Union{Vector{UInt8}, Nothing}
    min_value::Union{Vector{UInt8}, Nothing}
end

Statistics() = Statistics(nothing, nothing, nothing, nothing, nothing, nothing)

"""Page encoding statistics"""
mutable struct PageEncodingStats
    page_type::PageType
    encoding::Encoding
    count::Int32
end

"""Column metadata within a row group"""
mutable struct ColumnMetaData
    type::ParquetType
    encodings::Vector{Encoding}
    path_in_schema::Vector{String}
    codec::CompressionCodec
    num_values::Int64
    total_uncompressed_size::Int64
    total_compressed_size::Int64
    key_value_metadata::Union{Vector{Pair{String,String}}, Nothing}
    data_page_offset::Int64
    index_page_offset::Union{Int64, Nothing}
    dictionary_page_offset::Union{Int64, Nothing}
    statistics::Union{Statistics, Nothing}
    encoding_stats::Union{Vector{PageEncodingStats}, Nothing}
end

ColumnMetaData() = ColumnMetaData(
    BOOLEAN, Encoding[], String[], UNCOMPRESSED,
    0, 0, 0, nothing, 0, nothing, nothing, nothing, nothing
)

"""Column chunk information"""
mutable struct ColumnChunk
    file_path::Union{String, Nothing}
    file_offset::Int64
    meta_data::Union{ColumnMetaData, Nothing}
    offset_index_offset::Union{Int64, Nothing}
    offset_index_length::Union{Int32, Nothing}
    column_index_offset::Union{Int64, Nothing}
    column_index_length::Union{Int32, Nothing}
end

ColumnChunk() = ColumnChunk(nothing, 0, nothing, nothing, nothing, nothing, nothing)

"""Sorting column specification"""
mutable struct SortingColumn
    column_idx::Int32
    descending::Bool
    nulls_first::Bool
end

"""Row group metadata"""
mutable struct RowGroup
    columns::Vector{ColumnChunk}
    total_byte_size::Int64
    num_rows::Int64
    sorting_columns::Union{Vector{SortingColumn}, Nothing}
    file_offset::Union{Int64, Nothing}
    total_compressed_size::Union{Int64, Nothing}
    ordinal::Union{Int16, Nothing}
end

RowGroup() = RowGroup(ColumnChunk[], 0, 0, nothing, nothing, nothing, nothing)

"""Key-value metadata"""
struct KeyValue
    key::String
    value::Union{String, Nothing}
end

"""File metadata (footer)"""
mutable struct FileMetaData
    version::Int32
    schema::Vector{SchemaElement}
    num_rows::Int64
    row_groups::Vector{RowGroup}
    key_value_metadata::Union{Vector{KeyValue}, Nothing}
    created_by::Union{String, Nothing}
    column_orders::Union{Vector{Int}, Nothing}  # simplified
end

FileMetaData() = FileMetaData(0, SchemaElement[], 0, RowGroup[], nothing, nothing, nothing)

"""Data page header"""
mutable struct DataPageHeader
    num_values::Int32
    encoding::Encoding
    definition_level_encoding::Encoding
    repetition_level_encoding::Encoding
    statistics::Union{Statistics, Nothing}
end

DataPageHeader() = DataPageHeader(0, PLAIN, RLE, RLE, nothing)

"""Data page header V2"""
mutable struct DataPageHeaderV2
    num_values::Int32
    num_nulls::Int32
    num_rows::Int32
    encoding::Encoding
    definition_levels_byte_length::Int32
    repetition_levels_byte_length::Int32
    is_compressed::Bool
    statistics::Union{Statistics, Nothing}
end

DataPageHeaderV2() = DataPageHeaderV2(0, 0, 0, PLAIN, 0, 0, true, nothing)

"""Dictionary page header"""
mutable struct DictionaryPageHeader
    num_values::Int32
    encoding::Encoding
    is_sorted::Bool
end

DictionaryPageHeader() = DictionaryPageHeader(0, PLAIN_DICTIONARY, false)

"""Page header"""
mutable struct PageHeader
    type::PageType
    uncompressed_page_size::Int32
    compressed_page_size::Int32
    crc::Union{Int32, Nothing}
    data_page_header::Union{DataPageHeader, Nothing}
    index_page_header::Nothing  # not commonly used
    dictionary_page_header::Union{DictionaryPageHeader, Nothing}
    data_page_header_v2::Union{DataPageHeaderV2, Nothing}
end

PageHeader() = PageHeader(DATA_PAGE, 0, 0, nothing, nothing, nothing, nothing, nothing)
