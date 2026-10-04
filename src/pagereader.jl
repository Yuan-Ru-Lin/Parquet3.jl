# Page reader for Parquet column data

mutable struct ColumnReader
    data::Vector{UInt8}    # shared mmapped bytes (read-only)
    offset::Int            # current read position (0-based byte offset)
    meta::ColumnMetaData
    schema_node::SchemaNode
    type_length::Int
    dictionary::Union{DictionaryDecoder, Nothing}
end

ColumnReader(data::Vector{UInt8}, meta::ColumnMetaData, node::SchemaNode, type_len::Int=0) =
    ColumnReader(data, 0, meta, node, type_len, nothing)

struct DecodedPage{T, V<:AbstractVector{T}}
    values::V
    def_levels::Union{Vector{Int}, Nothing}
    rep_levels::Union{Vector{Int}, Nothing}
    num_values::Int
end

"""
Parse the page header at `offset`; returns it with the number of bytes it occupies.

A header's size is unknown until it is parsed, and it has no upper bound: min/max
statistics hold whole values. Thrift needs a contiguous buffer, so the header is parsed
from a window that doubles until it fits, up to `limit` (the end of the column chunk).
"""
function read_page_header(data::Vector{UInt8}, offset::Int, limit::Int = length(data))::Tuple{PageHeader, Int}
    limit = min(limit, length(data))
    window = 1024
    while true
        stop = min(offset + window, limit)
        t = TMemoryTransport(data[offset+1 : stop])
        try
            header = read_thrift(TCompactProtocol(t), PageHeader, PAGE_HEADER_FIELDS)
            return (header, position(t.buff))
        catch e
            # Ran off the end of the window: retry with a larger one, unless there is no more
            (e isa EOFError && stop < limit) || rethrow()
            window *= 2
        end
    end
end

function read_levels(data::AbstractVector{UInt8}, count::Int, max_level::Int, encoding::Encoding)
    max_level == 0 && return (zeros(Int, count), 0)

    bit_width = max(1, ceil(Int, log2(max_level + 1)))

    if encoding == RLE
        len = ltoh(reinterpret(UInt32, @view data[1:4])[1])
        levels = decode_rle_bitpacked(@view(data[5:4+len]), count, bit_width)
        return (Int.(levels), 4 + Int(len))
    else
        bytes = cld(count * bit_width, 8)
        levels = unpack_bits(@view(data[1:bytes]), count, bit_width)
        return (Int.(levels), bytes)
    end
end

function decode_values(data, count, ptype, encoding, type_len, dict)
    if encoding == PLAIN
        decode_plain(ptype, data, count, type_len)
    elseif encoding in (PLAIN_DICTIONARY, RLE_DICTIONARY)
        dict === nothing && error("No dictionary for dictionary encoding")
        decode_dictionary(dict, data, count)
    elseif encoding == DELTA_BINARY_PACKED
        vals, _ = decode_delta_binary_packed(data, count)
        # Truncating, not checked: INT32 deltas wrap around in 32 bits
        ptype == INT32 ? vals .% Int32 : vals
    elseif encoding == DELTA_LENGTH_BYTE_ARRAY
        decode_delta_length_byte_array(data, count)
    elseif encoding == BYTE_STREAM_SPLIT
        ptype == FLOAT ? decode_byte_stream_split_float32(data, count) :
                         decode_byte_stream_split_float64(data, count)
    else
        error("Unsupported encoding: $encoding")
    end
end

function read_page(reader::ColumnReader)
    meta = reader.meta
    chunk_end = something(meta.dictionary_page_offset, meta.data_page_offset) + meta.total_compressed_size
    reader.offset >= chunk_end && return nothing

    header, bytes_consumed = read_page_header(reader.data, reader.offset, Int(chunk_end))
    reader.offset += bytes_consumed
    page_range = reader.offset+1 : reader.offset+header.compressed_page_size
    reader.offset += header.compressed_page_size

    # Decompress for DICTIONARY_PAGE and DATA_PAGE (v1) — entire page is compressed.
    # DATA_PAGE_V2 handles decompression of data portion separately.
    if header.type != DATA_PAGE_V2 && meta.codec != UNCOMPRESSED
        page_data = decompress(reader.data[page_range], meta.codec, Int(header.uncompressed_page_size))
    else
        page_data = @view reader.data[page_range]
    end

    if header.type == DICTIONARY_PAGE
        dh = header.dictionary_page_header
        reader.dictionary = DictionaryDecoder(page_data, Int(dh.num_values), meta.type, reader.type_length)
        return read_page(reader)
    end

    max_def = reader.schema_node.max_def_level
    max_rep = reader.schema_node.max_rep_level

    if header.type == DATA_PAGE
        dh = header.data_page_header
        nv = Int(dh.num_values)
        pos = 1

        rep_levels = nothing
        def_levels = nothing

        if max_rep > 0
            rep_levels, bytes = read_levels(@view(page_data[pos:end]), nv, max_rep, dh.repetition_level_encoding)
            pos += bytes
        end
        if max_def > 0
            def_levels, bytes = read_levels(@view(page_data[pos:end]), nv, max_def, dh.definition_level_encoding)
            pos += bytes
        end

        non_null = def_levels === nothing ? nv : count(==(max_def), def_levels)
        values = decode_values(@view(page_data[pos:end]), non_null, meta.type, dh.encoding, reader.type_length, reader.dictionary)

        return DecodedPage(values, def_levels, rep_levels, nv)

    elseif header.type == DATA_PAGE_V2
        dh = header.data_page_header_v2
        nv = Int(dh.num_values)
        pos = 1

        rep_levels = nothing
        def_levels = nothing

        if max_rep > 0 && dh.repetition_levels_byte_length > 0
            bw = max(1, ceil(Int, log2(max_rep + 1)))
            rep_levels = Int.(decode_rle_bitpacked(@view(page_data[pos:pos+dh.repetition_levels_byte_length-1]), nv, bw))
            pos += dh.repetition_levels_byte_length
        end
        if max_def > 0 && dh.definition_levels_byte_length > 0
            bw = max(1, ceil(Int, log2(max_def + 1)))
            def_levels = Int.(decode_rle_bitpacked(@view(page_data[pos:pos+dh.definition_levels_byte_length-1]), nv, bw))
            pos += dh.definition_levels_byte_length
        end

        data_part = @view page_data[pos:end]
        if dh.is_compressed && meta.codec != UNCOMPRESSED
            expected = header.uncompressed_page_size - dh.repetition_levels_byte_length - dh.definition_levels_byte_length
            data_part = decompress(collect(data_part), meta.codec, Int(expected))
        end

        non_null = nv - Int(dh.num_nulls)
        values = decode_values(data_part, non_null, meta.type, dh.encoding, reader.type_length, reader.dictionary)

        return DecodedPage(values, def_levels, rep_levels, nv)
    else
        return read_page(reader)
    end
end

function read_all_pages(reader::ColumnReader)
    reader.offset = something(reader.meta.dictionary_page_offset, reader.meta.data_page_offset)

    pages = DecodedPage[]
    while true
        page = read_page(reader)
        page === nothing && break
        push!(pages, page)
    end
    pages
end

"""Assemble a flat (non-repeated) column."""
function assemble_flat_column(pages::Vector{<:DecodedPage}, max_def::Int)
    total = sum(p.num_values for p in pages)
    T = eltype(first(pages).values)
    values = Vector{T}(undef, total)
    nulls = falses(total)

    out = 1
    for page in pages
        if page.def_levels === nothing
            # Non-nullable: bulk copy entire page values
            n = length(page.values)
            copyto!(values, out, page.values, 1, n)
            out += n
        else
            # Nullable: element-by-element with null check
            val = 1
            @inbounds for i in 1:page.num_values
                if page.def_levels[i] < max_def
                    nulls[out] = true
                else
                    values[out] = page.values[val]
                    val += 1
                end
                out += 1
            end
        end
    end

    (values, nulls)
end

"""Collect rep/def levels and raw values from decoded pages."""
function collect_page_data(pages::Vector{<:DecodedPage}, max_def::Int)
    T = eltype(first(pages).values)
    total_levels = sum(p.num_values for p in pages)
    total_values = sum(length(p.values) for p in pages)

    all_rep = Vector{Int}(undef, total_levels)
    all_def = Vector{Int}(undef, total_levels)
    all_values = Vector{T}(undef, total_values)

    level_pos = 1
    value_pos = 1

    for page in pages
        nv = page.num_values

        if page.rep_levels !== nothing
            copyto!(all_rep, level_pos, page.rep_levels, 1, nv)
        else
            fill!(@view(all_rep[level_pos:level_pos+nv-1]), 0)
        end

        if page.def_levels !== nothing
            copyto!(all_def, level_pos, page.def_levels, 1, nv)
        else
            fill!(@view(all_def[level_pos:level_pos+nv-1]), max_def)
        end

        vlen = length(page.values)
        copyto!(all_values, value_pos, page.values, 1, vlen)

        level_pos += nv
        value_pos += vlen
    end

    (all_rep, all_def, all_values)
end
