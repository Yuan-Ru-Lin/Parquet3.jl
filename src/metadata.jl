# Parquet metadata parsing - builds immutable structs

function parse_schema_element(d::ThriftDecoder)::SchemaElement
    type = nothing
    type_length = nothing
    repetition_type = nothing
    name = ""
    num_children = nothing
    converted_type = nothing
    scale = nothing
    precision = nothing
    field_id = nothing

    push_struct(d)
    while true
        type_id, fid = read_field_header(d)
        type_id == THRIFT_STOP && break

        if fid == 1 && type_id == THRIFT_I32
            type = ParquetType(Int32(read_zigzag(d)))
        elseif fid == 2 && type_id == THRIFT_I32
            type_length = Int32(read_zigzag(d))
        elseif fid == 3 && type_id == THRIFT_I32
            repetition_type = FieldRepetitionType(Int32(read_zigzag(d)))
        elseif fid == 4 && type_id == THRIFT_BINARY
            name = read_string(d)
        elseif fid == 5 && type_id == THRIFT_I32
            num_children = Int32(read_zigzag(d))
        elseif fid == 6 && type_id == THRIFT_I32
            converted_type = ConvertedType(Int32(read_zigzag(d)))
        elseif fid == 7 && type_id == THRIFT_I32
            scale = Int32(read_zigzag(d))
        elseif fid == 8 && type_id == THRIFT_I32
            precision = Int32(read_zigzag(d))
        elseif fid == 9 && type_id == THRIFT_I32
            field_id = Int32(read_zigzag(d))
        else
            skip_value(d, type_id)
        end
    end
    pop_struct(d)

    SchemaElement(type, type_length, repetition_type, name, num_children,
                  converted_type, scale, precision, field_id)
end

function parse_statistics(d::ThriftDecoder)::Statistics
    max = min = null_count = distinct_count = max_value = min_value = nothing

    push_struct(d)
    while true
        type_id, fid = read_field_header(d)
        type_id == THRIFT_STOP && break

        if fid == 1 && type_id == THRIFT_BINARY
            max = read_binary(d)
        elseif fid == 2 && type_id == THRIFT_BINARY
            min = read_binary(d)
        elseif fid == 3 && type_id == THRIFT_I64
            null_count = read_zigzag(d)
        elseif fid == 4 && type_id == THRIFT_I64
            distinct_count = read_zigzag(d)
        elseif fid == 5 && type_id == THRIFT_BINARY
            max_value = read_binary(d)
        elseif fid == 6 && type_id == THRIFT_BINARY
            min_value = read_binary(d)
        else
            skip_value(d, type_id)
        end
    end
    pop_struct(d)

    Statistics(max, min, null_count, distinct_count, max_value, min_value)
end

function parse_column_metadata(d::ThriftDecoder)::ColumnMetaData
    type = BOOLEAN
    encodings = Encoding[]
    path_in_schema = String[]
    codec = UNCOMPRESSED
    num_values = Int64(0)
    total_uncompressed_size = Int64(0)
    total_compressed_size = Int64(0)
    data_page_offset = Int64(0)
    index_page_offset = nothing
    dictionary_page_offset = nothing
    statistics = nothing

    push_struct(d)
    while true
        type_id, fid = read_field_header(d)
        type_id == THRIFT_STOP && break

        if fid == 1 && type_id == THRIFT_I32
            type = ParquetType(Int32(read_zigzag(d)))
        elseif fid == 2 && type_id == THRIFT_LIST
            _, size = read_list_header(d)
            encodings = [Encoding(Int32(read_zigzag(d))) for _ in 1:size]
        elseif fid == 3 && type_id == THRIFT_LIST
            _, size = read_list_header(d)
            path_in_schema = [read_string(d) for _ in 1:size]
        elseif fid == 4 && type_id == THRIFT_I32
            codec = CompressionCodec(Int32(read_zigzag(d)))
        elseif fid == 5 && type_id == THRIFT_I64
            num_values = read_zigzag(d)
        elseif fid == 6 && type_id == THRIFT_I64
            total_uncompressed_size = read_zigzag(d)
        elseif fid == 7 && type_id == THRIFT_I64
            total_compressed_size = read_zigzag(d)
        elseif fid == 9 && type_id == THRIFT_I64
            data_page_offset = read_zigzag(d)
        elseif fid == 10 && type_id == THRIFT_I64
            index_page_offset = read_zigzag(d)
        elseif fid == 11 && type_id == THRIFT_I64
            dictionary_page_offset = read_zigzag(d)
        elseif fid == 12 && type_id == THRIFT_STRUCT
            statistics = parse_statistics(d)
        else
            skip_value(d, type_id)
        end
    end
    pop_struct(d)

    ColumnMetaData(type, encodings, path_in_schema, codec, num_values,
                   total_uncompressed_size, total_compressed_size, data_page_offset,
                   index_page_offset, dictionary_page_offset, statistics)
end

function parse_column_chunk(d::ThriftDecoder)::ColumnChunk
    file_path = nothing
    file_offset = Int64(0)
    meta_data = nothing

    push_struct(d)
    while true
        type_id, fid = read_field_header(d)
        type_id == THRIFT_STOP && break

        if fid == 1 && type_id == THRIFT_BINARY
            file_path = read_string(d)
        elseif fid == 2 && type_id == THRIFT_I64
            file_offset = read_zigzag(d)
        elseif fid == 3 && type_id == THRIFT_STRUCT
            meta_data = parse_column_metadata(d)
        else
            skip_value(d, type_id)
        end
    end
    pop_struct(d)

    ColumnChunk(file_path, file_offset, meta_data)
end

function parse_row_group(d::ThriftDecoder)::RowGroup
    columns = ColumnChunk[]
    total_byte_size = Int64(0)
    num_rows = Int64(0)
    file_offset = nothing
    total_compressed_size = nothing

    push_struct(d)
    while true
        type_id, fid = read_field_header(d)
        type_id == THRIFT_STOP && break

        if fid == 1 && type_id == THRIFT_LIST
            _, size = read_list_header(d)
            columns = [parse_column_chunk(d) for _ in 1:size]
        elseif fid == 2 && type_id == THRIFT_I64
            total_byte_size = read_zigzag(d)
        elseif fid == 3 && type_id == THRIFT_I64
            num_rows = read_zigzag(d)
        elseif fid == 6 && type_id == THRIFT_I64
            file_offset = read_zigzag(d)
        elseif fid == 7 && type_id == THRIFT_I64
            total_compressed_size = read_zigzag(d)
        else
            skip_value(d, type_id)
        end
    end
    pop_struct(d)

    RowGroup(columns, total_byte_size, num_rows, file_offset, total_compressed_size)
end

function parse_key_value(d::ThriftDecoder)::KeyValue
    key = ""
    value = nothing

    push_struct(d)
    while true
        type_id, fid = read_field_header(d)
        type_id == THRIFT_STOP && break

        if fid == 1 && type_id == THRIFT_BINARY
            key = read_string(d)
        elseif fid == 2 && type_id == THRIFT_BINARY
            value = read_string(d)
        else
            skip_value(d, type_id)
        end
    end
    pop_struct(d)

    KeyValue(key, value)
end

function parse_file_metadata(d::ThriftDecoder)::FileMetaData
    version = Int32(0)
    schema = SchemaElement[]
    num_rows = Int64(0)
    row_groups = RowGroup[]
    key_value_metadata = nothing
    created_by = nothing

    push_struct(d)
    while true
        type_id, fid = read_field_header(d)
        type_id == THRIFT_STOP && break

        if fid == 1 && type_id == THRIFT_I32
            version = Int32(read_zigzag(d))
        elseif fid == 2 && type_id == THRIFT_LIST
            _, size = read_list_header(d)
            schema = [parse_schema_element(d) for _ in 1:size]
        elseif fid == 3 && type_id == THRIFT_I64
            num_rows = read_zigzag(d)
        elseif fid == 4 && type_id == THRIFT_LIST
            _, size = read_list_header(d)
            row_groups = [parse_row_group(d) for _ in 1:size]
        elseif fid == 5 && type_id == THRIFT_LIST
            _, size = read_list_header(d)
            key_value_metadata = [parse_key_value(d) for _ in 1:size]
        elseif fid == 6 && type_id == THRIFT_BINARY
            created_by = read_string(d)
        else
            skip_value(d, type_id)
        end
    end
    pop_struct(d)

    FileMetaData(version, schema, num_rows, row_groups, key_value_metadata, created_by)
end

function parse_data_page_header(d::ThriftDecoder)::DataPageHeader
    num_values = Int32(0)
    encoding = PLAIN
    def_encoding = RLE
    rep_encoding = RLE
    statistics = nothing

    push_struct(d)
    while true
        type_id, fid = read_field_header(d)
        type_id == THRIFT_STOP && break

        if fid == 1 && type_id == THRIFT_I32
            num_values = Int32(read_zigzag(d))
        elseif fid == 2 && type_id == THRIFT_I32
            encoding = Encoding(Int32(read_zigzag(d)))
        elseif fid == 3 && type_id == THRIFT_I32
            def_encoding = Encoding(Int32(read_zigzag(d)))
        elseif fid == 4 && type_id == THRIFT_I32
            rep_encoding = Encoding(Int32(read_zigzag(d)))
        elseif fid == 5 && type_id == THRIFT_STRUCT
            statistics = parse_statistics(d)
        else
            skip_value(d, type_id)
        end
    end
    pop_struct(d)

    DataPageHeader(num_values, encoding, def_encoding, rep_encoding, statistics)
end

function parse_data_page_header_v2(d::ThriftDecoder)::DataPageHeaderV2
    num_values = Int32(0)
    num_nulls = Int32(0)
    num_rows = Int32(0)
    encoding = PLAIN
    def_bytes = Int32(0)
    rep_bytes = Int32(0)
    is_compressed = true
    statistics = nothing

    push_struct(d)
    while true
        type_id, fid = read_field_header(d)
        type_id == THRIFT_STOP && break

        if fid == 1 && type_id == THRIFT_I32
            num_values = Int32(read_zigzag(d))
        elseif fid == 2 && type_id == THRIFT_I32
            num_nulls = Int32(read_zigzag(d))
        elseif fid == 3 && type_id == THRIFT_I32
            num_rows = Int32(read_zigzag(d))
        elseif fid == 4 && type_id == THRIFT_I32
            encoding = Encoding(Int32(read_zigzag(d)))
        elseif fid == 5 && type_id == THRIFT_I32
            def_bytes = Int32(read_zigzag(d))
        elseif fid == 6 && type_id == THRIFT_I32
            rep_bytes = Int32(read_zigzag(d))
        elseif fid == 7 && type_id in (THRIFT_TRUE, THRIFT_FALSE)
            is_compressed = type_id == THRIFT_TRUE
        elseif fid == 8 && type_id == THRIFT_STRUCT
            statistics = parse_statistics(d)
        else
            skip_value(d, type_id)
        end
    end
    pop_struct(d)

    DataPageHeaderV2(num_values, num_nulls, num_rows, encoding, def_bytes, rep_bytes, is_compressed, statistics)
end

function parse_dictionary_page_header(d::ThriftDecoder)::DictionaryPageHeader
    num_values = Int32(0)
    encoding = PLAIN_DICTIONARY
    is_sorted = false

    push_struct(d)
    while true
        type_id, fid = read_field_header(d)
        type_id == THRIFT_STOP && break

        if fid == 1 && type_id == THRIFT_I32
            num_values = Int32(read_zigzag(d))
        elseif fid == 2 && type_id == THRIFT_I32
            encoding = Encoding(Int32(read_zigzag(d)))
        elseif fid == 3 && type_id in (THRIFT_TRUE, THRIFT_FALSE)
            is_sorted = type_id == THRIFT_TRUE
        else
            skip_value(d, type_id)
        end
    end
    pop_struct(d)

    DictionaryPageHeader(num_values, encoding, is_sorted)
end

function parse_page_header(d::ThriftDecoder)::PageHeader
    type = DATA_PAGE
    uncompressed_size = Int32(0)
    compressed_size = Int32(0)
    crc = nothing
    data_page_header = nothing
    dictionary_page_header = nothing
    data_page_header_v2 = nothing

    push_struct(d)
    while true
        type_id, fid = read_field_header(d)
        type_id == THRIFT_STOP && break

        if fid == 1 && type_id == THRIFT_I32
            type = PageType(Int32(read_zigzag(d)))
        elseif fid == 2 && type_id == THRIFT_I32
            uncompressed_size = Int32(read_zigzag(d))
        elseif fid == 3 && type_id == THRIFT_I32
            compressed_size = Int32(read_zigzag(d))
        elseif fid == 4 && type_id == THRIFT_I32
            crc = Int32(read_zigzag(d))
        elseif fid == 5 && type_id == THRIFT_STRUCT
            data_page_header = parse_data_page_header(d)
        elseif fid == 7 && type_id == THRIFT_STRUCT
            dictionary_page_header = parse_dictionary_page_header(d)
        elseif fid == 8 && type_id == THRIFT_STRUCT
            data_page_header_v2 = parse_data_page_header_v2(d)
        else
            skip_value(d, type_id)
        end
    end
    pop_struct(d)

    PageHeader(type, uncompressed_size, compressed_size, crc,
               data_page_header, dictionary_page_header, data_page_header_v2)
end
