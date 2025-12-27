# Parquet metadata parsing using Thrift decoder

"""Parse a SchemaElement from Thrift"""
function parse_schema_element(d::ThriftDecoder)::SchemaElement
    elem = SchemaElement()
    push_struct(d)

    while true
        type_id, field_id = read_field_header(d)
        type_id == THRIFT_STOP && break

        if field_id == 1 && type_id == THRIFT_I32  # type
            elem.type = ParquetType(Int32(read_zigzag(d)))
        elseif field_id == 2 && type_id == THRIFT_I32  # type_length
            elem.type_length = Int32(read_zigzag(d))
        elseif field_id == 3 && type_id == THRIFT_I32  # repetition_type
            elem.repetition_type = FieldRepetitionType(Int32(read_zigzag(d)))
        elseif field_id == 4 && type_id == THRIFT_BINARY  # name
            elem.name = read_string(d)
        elseif field_id == 5 && type_id == THRIFT_I32  # num_children
            elem.num_children = Int32(read_zigzag(d))
        elseif field_id == 6 && type_id == THRIFT_I32  # converted_type
            elem.converted_type = ConvertedType(Int32(read_zigzag(d)))
        elseif field_id == 7 && type_id == THRIFT_I32  # scale
            elem.scale = Int32(read_zigzag(d))
        elseif field_id == 8 && type_id == THRIFT_I32  # precision
            elem.precision = Int32(read_zigzag(d))
        elseif field_id == 9 && type_id == THRIFT_I32  # field_id
            elem.field_id = Int32(read_zigzag(d))
        else
            skip_value(d, type_id)
        end
    end

    pop_struct(d)
    elem
end

"""Parse Statistics from Thrift"""
function parse_statistics(d::ThriftDecoder)::Statistics
    stats = Statistics()
    push_struct(d)

    while true
        type_id, field_id = read_field_header(d)
        type_id == THRIFT_STOP && break

        if field_id == 1 && type_id == THRIFT_BINARY  # max
            stats.max = read_binary(d)
        elseif field_id == 2 && type_id == THRIFT_BINARY  # min
            stats.min = read_binary(d)
        elseif field_id == 3 && type_id == THRIFT_I64  # null_count
            stats.null_count = read_zigzag(d)
        elseif field_id == 4 && type_id == THRIFT_I64  # distinct_count
            stats.distinct_count = read_zigzag(d)
        elseif field_id == 5 && type_id == THRIFT_BINARY  # max_value
            stats.max_value = read_binary(d)
        elseif field_id == 6 && type_id == THRIFT_BINARY  # min_value
            stats.min_value = read_binary(d)
        else
            skip_value(d, type_id)
        end
    end

    pop_struct(d)
    stats
end

"""Parse ColumnMetaData from Thrift"""
function parse_column_metadata(d::ThriftDecoder)::ColumnMetaData
    meta = ColumnMetaData()
    push_struct(d)

    while true
        type_id, field_id = read_field_header(d)
        type_id == THRIFT_STOP && break

        if field_id == 1 && type_id == THRIFT_I32  # type
            meta.type = ParquetType(Int32(read_zigzag(d)))
        elseif field_id == 2 && type_id == THRIFT_LIST  # encodings
            elem_type, size = read_list_header(d)
            meta.encodings = [Encoding(Int32(read_zigzag(d))) for _ in 1:size]
        elseif field_id == 3 && type_id == THRIFT_LIST  # path_in_schema
            elem_type, size = read_list_header(d)
            meta.path_in_schema = [read_string(d) for _ in 1:size]
        elseif field_id == 4 && type_id == THRIFT_I32  # codec
            meta.codec = CompressionCodec(Int32(read_zigzag(d)))
        elseif field_id == 5 && type_id == THRIFT_I64  # num_values
            meta.num_values = read_zigzag(d)
        elseif field_id == 6 && type_id == THRIFT_I64  # total_uncompressed_size
            meta.total_uncompressed_size = read_zigzag(d)
        elseif field_id == 7 && type_id == THRIFT_I64  # total_compressed_size
            meta.total_compressed_size = read_zigzag(d)
        elseif field_id == 9 && type_id == THRIFT_I64  # data_page_offset
            meta.data_page_offset = read_zigzag(d)
        elseif field_id == 10 && type_id == THRIFT_I64  # index_page_offset
            meta.index_page_offset = read_zigzag(d)
        elseif field_id == 11 && type_id == THRIFT_I64  # dictionary_page_offset
            meta.dictionary_page_offset = read_zigzag(d)
        elseif field_id == 12 && type_id == THRIFT_STRUCT  # statistics
            meta.statistics = parse_statistics(d)
        else
            skip_value(d, type_id)
        end
    end

    pop_struct(d)
    meta
end

"""Parse ColumnChunk from Thrift"""
function parse_column_chunk(d::ThriftDecoder)::ColumnChunk
    chunk = ColumnChunk()
    push_struct(d)

    while true
        type_id, field_id = read_field_header(d)
        type_id == THRIFT_STOP && break

        if field_id == 1 && type_id == THRIFT_BINARY  # file_path
            chunk.file_path = read_string(d)
        elseif field_id == 2 && type_id == THRIFT_I64  # file_offset
            chunk.file_offset = read_zigzag(d)
        elseif field_id == 3 && type_id == THRIFT_STRUCT  # meta_data
            chunk.meta_data = parse_column_metadata(d)
        elseif field_id == 4 && type_id == THRIFT_I64  # offset_index_offset
            chunk.offset_index_offset = read_zigzag(d)
        elseif field_id == 5 && type_id == THRIFT_I32  # offset_index_length
            chunk.offset_index_length = Int32(read_zigzag(d))
        elseif field_id == 6 && type_id == THRIFT_I64  # column_index_offset
            chunk.column_index_offset = read_zigzag(d)
        elseif field_id == 7 && type_id == THRIFT_I32  # column_index_length
            chunk.column_index_length = Int32(read_zigzag(d))
        else
            skip_value(d, type_id)
        end
    end

    pop_struct(d)
    chunk
end

"""Parse RowGroup from Thrift"""
function parse_row_group(d::ThriftDecoder)::RowGroup
    rg = RowGroup()
    push_struct(d)

    while true
        type_id, field_id = read_field_header(d)
        type_id == THRIFT_STOP && break

        if field_id == 1 && type_id == THRIFT_LIST  # columns
            elem_type, size = read_list_header(d)
            rg.columns = [parse_column_chunk(d) for _ in 1:size]
        elseif field_id == 2 && type_id == THRIFT_I64  # total_byte_size
            rg.total_byte_size = read_zigzag(d)
        elseif field_id == 3 && type_id == THRIFT_I64  # num_rows
            rg.num_rows = read_zigzag(d)
        elseif field_id == 6 && type_id == THRIFT_I64  # file_offset
            rg.file_offset = read_zigzag(d)
        elseif field_id == 7 && type_id == THRIFT_I64  # total_compressed_size
            rg.total_compressed_size = read_zigzag(d)
        elseif field_id == 8 && type_id == THRIFT_I16  # ordinal
            rg.ordinal = Int16(read_zigzag(d))
        else
            skip_value(d, type_id)
        end
    end

    pop_struct(d)
    rg
end

"""Parse KeyValue from Thrift"""
function parse_key_value(d::ThriftDecoder)::KeyValue
    key = ""
    value = nothing
    push_struct(d)

    while true
        type_id, field_id = read_field_header(d)
        type_id == THRIFT_STOP && break

        if field_id == 1 && type_id == THRIFT_BINARY
            key = read_string(d)
        elseif field_id == 2 && type_id == THRIFT_BINARY
            value = read_string(d)
        else
            skip_value(d, type_id)
        end
    end

    pop_struct(d)
    KeyValue(key, value)
end

"""Parse FileMetaData from Thrift"""
function parse_file_metadata(d::ThriftDecoder)::FileMetaData
    meta = FileMetaData()
    push_struct(d)

    while true
        type_id, field_id = read_field_header(d)
        type_id == THRIFT_STOP && break

        if field_id == 1 && type_id == THRIFT_I32  # version
            meta.version = Int32(read_zigzag(d))
        elseif field_id == 2 && type_id == THRIFT_LIST  # schema
            elem_type, size = read_list_header(d)
            meta.schema = [parse_schema_element(d) for _ in 1:size]
        elseif field_id == 3 && type_id == THRIFT_I64  # num_rows
            meta.num_rows = read_zigzag(d)
        elseif field_id == 4 && type_id == THRIFT_LIST  # row_groups
            elem_type, size = read_list_header(d)
            meta.row_groups = [parse_row_group(d) for _ in 1:size]
        elseif field_id == 5 && type_id == THRIFT_LIST  # key_value_metadata
            elem_type, size = read_list_header(d)
            meta.key_value_metadata = [parse_key_value(d) for _ in 1:size]
        elseif field_id == 6 && type_id == THRIFT_BINARY  # created_by
            meta.created_by = read_string(d)
        else
            skip_value(d, type_id)
        end
    end

    pop_struct(d)
    meta
end

"""Parse DataPageHeader from Thrift"""
function parse_data_page_header(d::ThriftDecoder)::DataPageHeader
    hdr = DataPageHeader()
    push_struct(d)

    while true
        type_id, field_id = read_field_header(d)
        type_id == THRIFT_STOP && break

        if field_id == 1 && type_id == THRIFT_I32  # num_values
            hdr.num_values = Int32(read_zigzag(d))
        elseif field_id == 2 && type_id == THRIFT_I32  # encoding
            hdr.encoding = Encoding(Int32(read_zigzag(d)))
        elseif field_id == 3 && type_id == THRIFT_I32  # definition_level_encoding
            hdr.definition_level_encoding = Encoding(Int32(read_zigzag(d)))
        elseif field_id == 4 && type_id == THRIFT_I32  # repetition_level_encoding
            hdr.repetition_level_encoding = Encoding(Int32(read_zigzag(d)))
        elseif field_id == 5 && type_id == THRIFT_STRUCT  # statistics
            hdr.statistics = parse_statistics(d)
        else
            skip_value(d, type_id)
        end
    end

    pop_struct(d)
    hdr
end

"""Parse DataPageHeaderV2 from Thrift"""
function parse_data_page_header_v2(d::ThriftDecoder)::DataPageHeaderV2
    hdr = DataPageHeaderV2()
    push_struct(d)

    while true
        type_id, field_id = read_field_header(d)
        type_id == THRIFT_STOP && break

        if field_id == 1 && type_id == THRIFT_I32  # num_values
            hdr.num_values = Int32(read_zigzag(d))
        elseif field_id == 2 && type_id == THRIFT_I32  # num_nulls
            hdr.num_nulls = Int32(read_zigzag(d))
        elseif field_id == 3 && type_id == THRIFT_I32  # num_rows
            hdr.num_rows = Int32(read_zigzag(d))
        elseif field_id == 4 && type_id == THRIFT_I32  # encoding
            hdr.encoding = Encoding(Int32(read_zigzag(d)))
        elseif field_id == 5 && type_id == THRIFT_I32  # definition_levels_byte_length
            hdr.definition_levels_byte_length = Int32(read_zigzag(d))
        elseif field_id == 6 && type_id == THRIFT_I32  # repetition_levels_byte_length
            hdr.repetition_levels_byte_length = Int32(read_zigzag(d))
        elseif field_id == 7 && (type_id == THRIFT_TRUE || type_id == THRIFT_FALSE)  # is_compressed
            hdr.is_compressed = type_id == THRIFT_TRUE
        elseif field_id == 8 && type_id == THRIFT_STRUCT  # statistics
            hdr.statistics = parse_statistics(d)
        else
            skip_value(d, type_id)
        end
    end

    pop_struct(d)
    hdr
end

"""Parse DictionaryPageHeader from Thrift"""
function parse_dictionary_page_header(d::ThriftDecoder)::DictionaryPageHeader
    hdr = DictionaryPageHeader()
    push_struct(d)

    while true
        type_id, field_id = read_field_header(d)
        type_id == THRIFT_STOP && break

        if field_id == 1 && type_id == THRIFT_I32  # num_values
            hdr.num_values = Int32(read_zigzag(d))
        elseif field_id == 2 && type_id == THRIFT_I32  # encoding
            hdr.encoding = Encoding(Int32(read_zigzag(d)))
        elseif field_id == 3 && (type_id == THRIFT_TRUE || type_id == THRIFT_FALSE)  # is_sorted
            hdr.is_sorted = type_id == THRIFT_TRUE
        else
            skip_value(d, type_id)
        end
    end

    pop_struct(d)
    hdr
end

"""Parse PageHeader from Thrift"""
function parse_page_header(d::ThriftDecoder)::PageHeader
    hdr = PageHeader()
    push_struct(d)

    while true
        type_id, field_id = read_field_header(d)
        type_id == THRIFT_STOP && break

        if field_id == 1 && type_id == THRIFT_I32  # type
            hdr.type = PageType(Int32(read_zigzag(d)))
        elseif field_id == 2 && type_id == THRIFT_I32  # uncompressed_page_size
            hdr.uncompressed_page_size = Int32(read_zigzag(d))
        elseif field_id == 3 && type_id == THRIFT_I32  # compressed_page_size
            hdr.compressed_page_size = Int32(read_zigzag(d))
        elseif field_id == 4 && type_id == THRIFT_I32  # crc
            hdr.crc = Int32(read_zigzag(d))
        elseif field_id == 5 && type_id == THRIFT_STRUCT  # data_page_header
            hdr.data_page_header = parse_data_page_header(d)
        elseif field_id == 7 && type_id == THRIFT_STRUCT  # dictionary_page_header
            hdr.dictionary_page_header = parse_dictionary_page_header(d)
        elseif field_id == 8 && type_id == THRIFT_STRUCT  # data_page_header_v2
            hdr.data_page_header_v2 = parse_data_page_header_v2(d)
        else
            skip_value(d, type_id)
        end
    end

    pop_struct(d)
    hdr
end
