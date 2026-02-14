# Parquet types and enums

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

@enum FieldRepetitionType::Int32 REQUIRED=0 OPTIONAL=1 REPEATED=2

@enum Encoding::Int32 begin
    PLAIN = 0
    PLAIN_DICTIONARY = 2
    RLE = 3
    BIT_PACKED = 4
    DELTA_BINARY_PACKED = 5
    DELTA_LENGTH_BYTE_ARRAY = 6
    DELTA_BYTE_ARRAY = 7
    RLE_DICTIONARY = 8
    BYTE_STREAM_SPLIT = 9
end

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

@enum PageType::Int32 DATA_PAGE=0 INDEX_PAGE=1 DICTIONARY_PAGE=2 DATA_PAGE_V2=3

const PARQUET_MAGIC = UInt8[0x50, 0x41, 0x52, 0x31]  # "PAR1"

#=============================================================================
# Metadata structs using @kwdef for clean keyword constructors
=============================================================================#

@kwdef struct SchemaElement
    type::Union{ParquetType, Nothing} = nothing
    type_length::Union{Int32, Nothing} = nothing
    repetition_type::Union{FieldRepetitionType, Nothing} = nothing
    name::String = ""
    num_children::Union{Int32, Nothing} = nothing
    converted_type::Union{ConvertedType, Nothing} = nothing
    scale::Union{Int32, Nothing} = nothing
    precision::Union{Int32, Nothing} = nothing
    field_id::Union{Int32, Nothing} = nothing
end

@kwdef struct Statistics
    max::Union{Vector{UInt8}, Nothing} = nothing
    min::Union{Vector{UInt8}, Nothing} = nothing
    null_count::Union{Int64, Nothing} = nothing
    distinct_count::Union{Int64, Nothing} = nothing
    max_value::Union{Vector{UInt8}, Nothing} = nothing
    min_value::Union{Vector{UInt8}, Nothing} = nothing
end

@kwdef struct ColumnMetaData
    type::ParquetType = BOOLEAN
    encodings::Vector{Encoding} = Encoding[]
    path_in_schema::Vector{String} = String[]
    codec::CompressionCodec = UNCOMPRESSED
    num_values::Int64 = 0
    total_uncompressed_size::Int64 = 0
    total_compressed_size::Int64 = 0
    data_page_offset::Int64 = 0
    index_page_offset::Union{Int64, Nothing} = nothing
    dictionary_page_offset::Union{Int64, Nothing} = nothing
    statistics::Union{Statistics, Nothing} = nothing
end

@kwdef struct ColumnChunk
    file_path::Union{String, Nothing} = nothing
    file_offset::Int64 = 0
    meta_data::Union{ColumnMetaData, Nothing} = nothing
end

@kwdef struct RowGroup
    columns::Vector{ColumnChunk} = ColumnChunk[]
    total_byte_size::Int64 = 0
    num_rows::Int64 = 0
    file_offset::Union{Int64, Nothing} = nothing
    total_compressed_size::Union{Int64, Nothing} = nothing
end

@kwdef struct KeyValue
    key::String = ""
    value::Union{String, Nothing} = nothing
end

@kwdef struct FileMetaData
    version::Int32 = 0
    schema::Vector{SchemaElement} = SchemaElement[]
    num_rows::Int64 = 0
    row_groups::Vector{RowGroup} = RowGroup[]
    key_value_metadata::Union{Vector{KeyValue}, Nothing} = nothing
    created_by::Union{String, Nothing} = nothing
end

@kwdef struct DataPageHeader
    num_values::Int32 = 0
    encoding::Encoding = PLAIN
    definition_level_encoding::Encoding = RLE
    repetition_level_encoding::Encoding = RLE
    statistics::Union{Statistics, Nothing} = nothing
end

@kwdef struct DataPageHeaderV2
    num_values::Int32 = 0
    num_nulls::Int32 = 0
    num_rows::Int32 = 0
    encoding::Encoding = PLAIN
    definition_levels_byte_length::Int32 = 0
    repetition_levels_byte_length::Int32 = 0
    is_compressed::Bool = true
    statistics::Union{Statistics, Nothing} = nothing
end

@kwdef struct DictionaryPageHeader
    num_values::Int32 = 0
    encoding::Encoding = PLAIN_DICTIONARY
    is_sorted::Bool = false
end

@kwdef struct PageHeader
    type::PageType = DATA_PAGE
    uncompressed_page_size::Int32 = 0
    compressed_page_size::Int32 = 0
    crc::Union{Int32, Nothing} = nothing
    data_page_header::Union{DataPageHeader, Nothing} = nothing
    dictionary_page_header::Union{DictionaryPageHeader, Nothing} = nothing
    data_page_header_v2::Union{DataPageHeaderV2, Nothing} = nothing
end

@kwdef struct SchemaNode
    element::SchemaElement
    children::Vector{SchemaNode} = SchemaNode[]
    max_def_level::Int = 0
    max_rep_level::Int = 0
    # Level at which this specific node contributes to def/rep
    own_def_level::Int = 0
    own_rep_level::Int = 0
end
