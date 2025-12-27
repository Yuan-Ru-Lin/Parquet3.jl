# Page reader for Parquet column data

"""
    ColumnReader

Reads pages from a column chunk and decodes the data.
"""
mutable struct ColumnReader
    io::IO
    meta::ColumnMetaData
    schema_node::SchemaNode
    type_length::Int  # For FIXED_LEN_BYTE_ARRAY

    # Current state
    dictionary::Union{DictionaryDecoder, Nothing}
    pages_read::Int
    values_read::Int64
end

function ColumnReader(io::IO, meta::ColumnMetaData, schema_node::SchemaNode, type_length::Int=0)
    ColumnReader(io, meta, schema_node, type_length, nothing, 0, 0)
end

"""Read a page header from the current position."""
function read_page_header(io::IO)::PageHeader
    # Read enough bytes for the header (headers are typically small)
    # We need to read incrementally since we don't know the size
    start_pos = position(io)

    # Read up to 1KB for header (should be more than enough)
    header_data = read(io, min(1024, filesize(io) - start_pos))

    decoder = ThriftDecoder(header_data)
    header = parse_page_header(decoder)

    # Seek back to just after the header
    seek(io, start_pos + decoder.pos - 1)

    header
end

"""
    DecodedPage

Represents decoded data from a page.
"""
struct DecodedPage{T}
    values::Vector{T}
    def_levels::Union{Vector{Int}, Nothing}
    rep_levels::Union{Vector{Int}, Nothing}
    num_values::Int
end

"""Read and decode definition levels."""
function read_levels(data::Vector{UInt8}, count::Int, max_level::Int, encoding::Encoding)::Tuple{Vector{Int}, Int}
    if max_level == 0
        return (zeros(Int, count), 0)
    end

    bit_width = ceil(Int, log2(max_level + 1))
    bit_width = max(1, bit_width)

    if encoding == RLE
        # RLE/Bit-packed hybrid with length prefix
        len = ltoh(reinterpret(UInt32, data[1:4])[1])
        levels_data = data[5:4+len]
        levels = decode_rle_bitpacked(levels_data, count, bit_width)
        return (Int.(levels), 4 + Int(len))
    else
        # BIT_PACKED (deprecated but may appear)
        bytes_needed = cld(count * bit_width, 8)
        levels = unpack_bits(data[1:bytes_needed], count, bit_width)
        return (Int.(levels), bytes_needed)
    end
end

"""Decode values from a data page based on encoding and type."""
function decode_values(
    data::Vector{UInt8},
    count::Int,
    parquet_type::ParquetType,
    encoding::Encoding,
    type_length::Int,
    dictionary::Union{DictionaryDecoder, Nothing}
)
    if encoding == PLAIN
        return decode_plain_values(data, count, parquet_type, type_length)
    elseif encoding == PLAIN_DICTIONARY || encoding == RLE_DICTIONARY
        if dictionary === nothing
            error("Dictionary encoding but no dictionary available")
        end
        return decode_dictionary_page(dictionary, data, count)
    elseif encoding == DELTA_BINARY_PACKED
        return decode_delta_binary_packed(data, count)
    elseif encoding == DELTA_LENGTH_BYTE_ARRAY
        return decode_delta_length_byte_array(data, count)
    elseif encoding == DELTA_BYTE_ARRAY
        return decode_delta_byte_array(data, count)
    elseif encoding == BYTE_STREAM_SPLIT
        if parquet_type == FLOAT
            return decode_byte_stream_split_float(data, count)
        elseif parquet_type == DOUBLE
            return decode_byte_stream_split_double(data, count)
        else
            error("BYTE_STREAM_SPLIT only supported for FLOAT/DOUBLE")
        end
    else
        error("Unsupported encoding: $encoding")
    end
end

"""Decode plain-encoded values based on type."""
function decode_plain_values(data::Vector{UInt8}, count::Int, parquet_type::ParquetType, type_length::Int)
    if parquet_type == BOOLEAN
        return decode_plain_boolean(data, count)
    elseif parquet_type == INT32
        return decode_plain_int32(data, count)
    elseif parquet_type == INT64
        return decode_plain_int64(data, count)
    elseif parquet_type == INT96
        return decode_plain_int96(data, count)
    elseif parquet_type == FLOAT
        return decode_plain_float(data, count)
    elseif parquet_type == DOUBLE
        return decode_plain_double(data, count)
    elseif parquet_type == BYTE_ARRAY
        return decode_plain_byte_array(data, count)
    elseif parquet_type == FIXED_LEN_BYTE_ARRAY
        return decode_plain_fixed_byte_array(data, count, type_length)
    else
        error("Unknown parquet type: $parquet_type")
    end
end

"""Read and decode a single page from a column chunk."""
function read_page(reader::ColumnReader)::Union{DecodedPage, Nothing}
    meta = reader.meta

    # Calculate end of column chunk data
    chunk_start = something(meta.dictionary_page_offset, meta.data_page_offset)
    chunk_end = chunk_start + meta.total_compressed_size

    if position(reader.io) >= chunk_end
        return nothing
    end

    # Read page header
    header = read_page_header(reader.io)

    # Read page data
    page_data = read(reader.io, header.compressed_page_size)

    # Decompress if needed
    if header.compressed_page_size != header.uncompressed_page_size
        page_data = decompress(page_data, meta.codec, Int(header.uncompressed_page_size))
    end

    if header.type == DICTIONARY_PAGE
        # Parse dictionary page
        dict_header = header.dictionary_page_header
        num_values = dict_header.num_values

        # Find type_length for FIXED_LEN_BYTE_ARRAY
        type_length = reader.type_length

        reader.dictionary = DictionaryDecoder(page_data, Int(num_values), meta.type, type_length)
        reader.pages_read += 1

        # Recursively read next page (should be data page)
        return read_page(reader)

    elseif header.type == DATA_PAGE
        data_header = header.data_page_header
        num_values = data_header.num_values

        pos = 1
        def_levels = nothing
        rep_levels = nothing

        max_def = reader.schema_node.max_def_level
        max_rep = reader.schema_node.max_rep_level

        # Read repetition levels
        if max_rep > 0
            rep_levels, bytes_read = read_levels(
                page_data[pos:end], Int(num_values), max_rep,
                data_header.repetition_level_encoding
            )
            pos += bytes_read
        end

        # Read definition levels
        if max_def > 0
            def_levels, bytes_read = read_levels(
                page_data[pos:end], Int(num_values), max_def,
                data_header.definition_level_encoding
            )
            pos += bytes_read
        end

        # Count non-null values
        num_non_null = if def_levels !== nothing
            count(d -> d == max_def, def_levels)
        else
            Int(num_values)
        end

        # Decode values
        values = decode_values(
            page_data[pos:end],
            num_non_null,
            meta.type,
            data_header.encoding,
            reader.type_length,
            reader.dictionary
        )

        reader.pages_read += 1
        reader.values_read += num_values

        return DecodedPage(values, def_levels, rep_levels, Int(num_values))

    elseif header.type == DATA_PAGE_V2
        data_header = header.data_page_header_v2
        num_values = data_header.num_values

        pos = 1
        def_levels = nothing
        rep_levels = nothing

        max_def = reader.schema_node.max_def_level
        max_rep = reader.schema_node.max_rep_level

        # In V2, rep and def levels are not compressed
        rep_bytes = data_header.repetition_levels_byte_length
        def_bytes = data_header.definition_levels_byte_length

        # Read repetition levels (RLE encoded, no length prefix in V2)
        if max_rep > 0 && rep_bytes > 0
            bit_width = ceil(Int, log2(max_rep + 1))
            bit_width = max(1, bit_width)
            rep_levels = Int.(decode_rle_bitpacked(page_data[pos:pos+rep_bytes-1], Int(num_values), bit_width))
            pos += rep_bytes
        end

        # Read definition levels
        if max_def > 0 && def_bytes > 0
            bit_width = ceil(Int, log2(max_def + 1))
            bit_width = max(1, bit_width)
            def_levels = Int.(decode_rle_bitpacked(page_data[pos:pos+def_bytes-1], Int(num_values), bit_width))
            pos += def_bytes
        end

        # Decompress data portion if needed
        data_portion = page_data[pos:end]
        if data_header.is_compressed && meta.codec != UNCOMPRESSED
            expected_size = header.uncompressed_page_size - rep_bytes - def_bytes
            data_portion = decompress(data_portion, meta.codec, Int(expected_size))
        end

        # Count non-null values
        num_non_null = Int(num_values) - Int(data_header.num_nulls)

        # Decode values
        values = decode_values(
            data_portion,
            num_non_null,
            meta.type,
            data_header.encoding,
            reader.type_length,
            reader.dictionary
        )

        reader.pages_read += 1
        reader.values_read += num_values

        return DecodedPage(values, def_levels, rep_levels, Int(num_values))
    else
        # Skip unknown page types
        return read_page(reader)
    end
end

"""Read all pages from a column chunk."""
function read_all_pages(reader::ColumnReader)::Vector{DecodedPage}
    pages = DecodedPage[]

    # Seek to start of column data
    start_offset = something(reader.meta.dictionary_page_offset, reader.meta.data_page_offset)
    seek(reader.io, start_offset)

    while true
        page = read_page(reader)
        page === nothing && break
        push!(pages, page)
    end

    pages
end

"""
    assemble_column(pages::Vector{DecodedPage}, max_def::Int) -> (values, nulls)

Assemble column values from decoded pages, handling nulls based on definition levels.
Returns a vector of values and a BitVector indicating null positions.
"""
function assemble_column(pages::Vector{DecodedPage{T}}, max_def::Int) where T
    total_values = sum(p.num_values for p in pages)
    values = Vector{T}(undef, total_values)
    nulls = falses(total_values)

    out_idx = 1
    for page in pages
        value_idx = 1
        for i in 1:page.num_values
            if page.def_levels !== nothing
                def = page.def_levels[i]
                if def < max_def
                    # Null value
                    nulls[out_idx] = true
                    out_idx += 1
                    continue
                end
            end

            values[out_idx] = page.values[value_idx]
            value_idx += 1
            out_idx += 1
        end
    end

    (values, nulls)
end

"""
    assemble_nested(pages::Vector{DecodedPage}, max_def::Int, max_rep::Int)

Assemble nested data from decoded pages using repetition and definition levels.
Returns a vector of vectors (for repeated fields) or handles optionality.
"""
function assemble_nested(pages::Vector{DecodedPage{T}}, max_def::Int, max_rep::Int) where T
    if max_rep == 0
        # Not repeated, just handle nulls
        return assemble_column(pages, max_def)
    end

    # Repeated field: build list structure
    result = Vector{Vector{Union{T, Nothing}}}()
    current_list = Union{T, Nothing}[]

    for page in pages
        value_idx = 1
        for i in 1:page.num_values
            rep_level = page.rep_levels !== nothing ? page.rep_levels[i] : 0
            def_level = page.def_levels !== nothing ? page.def_levels[i] : max_def

            if rep_level == 0 && !isempty(current_list)
                # New record, save previous list
                push!(result, current_list)
                current_list = Union{T, Nothing}[]
            end

            if def_level < max_def
                # Null value
                push!(current_list, nothing)
            else
                push!(current_list, page.values[value_idx])
                value_idx += 1
            end
        end
    end

    # Don't forget the last list
    if !isempty(current_list)
        push!(result, current_list)
    end

    result
end
