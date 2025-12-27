# Parquet encoding implementations: Plain, RLE, Bit-packed, Dictionary, Delta

#=============================================================================
# Plain Encoding
=============================================================================#

"""Read plain-encoded boolean values."""
function decode_plain_boolean(data::Vector{UInt8}, count::Int)::Vector{Bool}
    result = Vector{Bool}(undef, count)
    for i in 1:count
        byte_idx = ((i - 1) >> 3) + 1
        bit_idx = (i - 1) & 7
        result[i] = (data[byte_idx] >> bit_idx) & 1 == 1
    end
    result
end

"""Read plain-encoded Int32 values."""
function decode_plain_int32(data::Vector{UInt8}, count::Int)::Vector{Int32}
    reinterpret(Int32, data)[1:count]
end

"""Read plain-encoded Int64 values."""
function decode_plain_int64(data::Vector{UInt8}, count::Int)::Vector{Int64}
    reinterpret(Int64, data)[1:count]
end

"""Read plain-encoded Int96 values (returned as 12-byte arrays)."""
function decode_plain_int96(data::Vector{UInt8}, count::Int)::Vector{Vector{UInt8}}
    [data[(i-1)*12+1:i*12] for i in 1:count]
end

"""Read plain-encoded Float32 values."""
function decode_plain_float(data::Vector{UInt8}, count::Int)::Vector{Float32}
    reinterpret(Float32, data)[1:count]
end

"""Read plain-encoded Float64 values."""
function decode_plain_double(data::Vector{UInt8}, count::Int)::Vector{Float64}
    reinterpret(Float64, data)[1:count]
end

"""Read plain-encoded byte arrays (variable length)."""
function decode_plain_byte_array(data::Vector{UInt8}, count::Int)::Vector{Vector{UInt8}}
    result = Vector{Vector{UInt8}}(undef, count)
    pos = 1
    for i in 1:count
        len = ltoh(reinterpret(UInt32, data[pos:pos+3])[1])
        pos += 4
        result[i] = data[pos:pos+len-1]
        pos += len
    end
    result
end

"""Read plain-encoded fixed-length byte arrays."""
function decode_plain_fixed_byte_array(data::Vector{UInt8}, count::Int, type_length::Int)::Vector{Vector{UInt8}}
    [data[(i-1)*type_length+1:i*type_length] for i in 1:count]
end

#=============================================================================
# Bit-packing utilities
=============================================================================#

"""Read bit-packed values with given bit width."""
function unpack_bits(data::Vector{UInt8}, count::Int, bit_width::Int)::Vector{UInt32}
    if bit_width == 0
        return zeros(UInt32, count)
    end

    result = Vector{UInt32}(undef, count)
    bit_pos = 0
    data_idx = 1
    current_byte = UInt32(data[1])

    for i in 1:count
        value = UInt32(0)
        bits_read = 0

        while bits_read < bit_width
            bits_in_byte = 8 - (bit_pos & 7)
            bits_to_read = min(bits_in_byte, bit_width - bits_read)

            byte_idx = (bit_pos >> 3) + 1
            if byte_idx > length(data)
                break
            end

            mask = (UInt32(1) << bits_to_read) - 1
            shift = bit_pos & 7
            extracted = (UInt32(data[byte_idx]) >> shift) & mask

            value |= extracted << bits_read
            bits_read += bits_to_read
            bit_pos += bits_to_read
        end

        result[i] = value
    end

    result
end

#=============================================================================
# RLE / Bit-packed Hybrid Encoding
=============================================================================#

"""
Decode RLE/Bit-packed hybrid encoding.
Returns a vector of decoded values.

Format:
- If bit_width is 0, all values are 0
- Otherwise, data consists of groups, each starting with a header:
  - If header & 1 == 0: RLE run (header >> 1 = run length)
  - If header & 1 == 1: Bit-packed run (header >> 1 = number of 8-value groups)
"""
function decode_rle_bitpacked(data::Vector{UInt8}, count::Int, bit_width::Int)::Vector{UInt32}
    if bit_width == 0
        return zeros(UInt32, count)
    end

    result = UInt32[]
    sizehint!(result, count)
    pos = 1

    while length(result) < count && pos <= length(data)
        # Read header (varint)
        header = UInt32(0)
        shift = 0
        while pos <= length(data)
            b = data[pos]
            pos += 1
            header |= UInt32(b & 0x7f) << shift
            if (b & 0x80) == 0
                break
            end
            shift += 7
        end

        if (header & 1) == 0
            # RLE: repeated value
            run_length = header >> 1
            # Read the value (ceil(bit_width/8) bytes, little-endian)
            value_bytes = cld(bit_width, 8)
            if pos + value_bytes - 1 > length(data)
                break
            end

            value = UInt32(0)
            for i in 0:value_bytes-1
                value |= UInt32(data[pos + i]) << (8 * i)
            end
            pos += value_bytes

            for _ in 1:run_length
                length(result) >= count && break
                push!(result, value)
            end
        else
            # Bit-packed: groups of 8 values
            num_groups = header >> 1
            num_values = num_groups * 8
            bytes_needed = cld(num_values * bit_width, 8)

            if pos + bytes_needed - 1 > length(data)
                bytes_needed = length(data) - pos + 1
            end

            packed_data = data[pos:pos+bytes_needed-1]
            pos += bytes_needed

            unpacked = unpack_bits(packed_data, num_values, bit_width)
            for v in unpacked
                length(result) >= count && break
                push!(result, v)
            end
        end
    end

    # Ensure we have exactly count values
    if length(result) > count
        resize!(result, count)
    end

    result
end

"""Decode RLE-encoded boolean values (used for definition/repetition levels with bit_width=1)."""
function decode_rle_boolean(data::Vector{UInt8}, count::Int)::Vector{Bool}
    values = decode_rle_bitpacked(data, count, 1)
    Bool.(values)
end

#=============================================================================
# Dictionary Encoding
=============================================================================#

"""
Dictionary decoding: uses indices to look up values in a dictionary.
The dictionary is decoded from a dictionary page using plain encoding.
The indices are encoded using RLE/bit-packed hybrid encoding.
"""
struct DictionaryDecoder{T}
    dictionary::Vector{T}
end

"""Create a dictionary decoder from plain-encoded dictionary page data."""
function DictionaryDecoder(data::Vector{UInt8}, num_values::Int, parquet_type::ParquetType, type_length::Int=0)
    dict = if parquet_type == BOOLEAN
        decode_plain_boolean(data, num_values)
    elseif parquet_type == INT32
        decode_plain_int32(data, num_values)
    elseif parquet_type == INT64
        decode_plain_int64(data, num_values)
    elseif parquet_type == INT96
        decode_plain_int96(data, num_values)
    elseif parquet_type == FLOAT
        decode_plain_float(data, num_values)
    elseif parquet_type == DOUBLE
        decode_plain_double(data, num_values)
    elseif parquet_type == BYTE_ARRAY
        decode_plain_byte_array(data, num_values)
    elseif parquet_type == FIXED_LEN_BYTE_ARRAY
        decode_plain_fixed_byte_array(data, num_values, type_length)
    else
        error("Unsupported dictionary type: $parquet_type")
    end
    DictionaryDecoder(dict)
end

"""Decode dictionary-encoded data given indices."""
function decode_dictionary(decoder::DictionaryDecoder{T}, indices::Vector{UInt32})::Vector{T} where T
    [decoder.dictionary[idx + 1] for idx in indices]  # Parquet indices are 0-based
end

"""Decode dictionary-encoded data page (indices are RLE/bit-packed)."""
function decode_dictionary_page(decoder::DictionaryDecoder{T}, data::Vector{UInt8}, count::Int)::Vector{T} where T
    # First byte is bit width
    bit_width = data[1]
    indices = decode_rle_bitpacked(data[2:end], count, Int(bit_width))
    decode_dictionary(decoder, indices)
end

#=============================================================================
# Delta Binary Packed Encoding
=============================================================================#

"""
Decode delta binary packed encoding for INT32/INT64.
Used for sorted or nearly-sorted integer columns.

Format:
- Block size (varint)
- Miniblocks per block (varint)
- Total value count (varint)
- First value (zigzag varint)
- For each block:
  - Min delta (zigzag varint)
  - Bit widths for each miniblock (1 byte each)
  - Miniblock data (bit-packed deltas - min_delta)
"""
function decode_delta_binary_packed(data::Vector{UInt8}, count::Int)::Vector{Int64}
    pos = 1

    # Read varint helper
    function read_varint()::UInt64
        result = UInt64(0)
        shift = 0
        while pos <= length(data)
            b = data[pos]
            pos += 1
            result |= UInt64(b & 0x7f) << shift
            if (b & 0x80) == 0
                break
            end
            shift += 7
        end
        result
    end

    # Read zigzag varint helper
    function read_zigzag()::Int64
        n = read_varint()
        Int64((n >> 1) ⊻ (-(n & 1)))
    end

    block_size = Int(read_varint())
    miniblocks_per_block = Int(read_varint())
    total_count = Int(read_varint())
    first_value = read_zigzag()

    values_per_miniblock = block_size ÷ miniblocks_per_block

    result = Vector{Int64}(undef, min(count, total_count))
    result[1] = first_value
    value_idx = 2

    while value_idx <= length(result) && pos <= length(data)
        # Read min delta for block
        min_delta = read_zigzag()

        # Read bit widths for each miniblock
        bit_widths = [data[pos + i - 1] for i in 1:miniblocks_per_block]
        pos += miniblocks_per_block

        # Decode each miniblock
        for mb_idx in 1:miniblocks_per_block
            bit_width = Int(bit_widths[mb_idx])
            values_to_read = min(values_per_miniblock, length(result) - value_idx + 1)

            if values_to_read <= 0
                break
            end

            if bit_width == 0
                # All deltas are min_delta
                for _ in 1:values_to_read
                    if value_idx > length(result)
                        break
                    end
                    result[value_idx] = result[value_idx - 1] + min_delta
                    value_idx += 1
                end
            else
                # Unpack bit-packed deltas
                bytes_needed = cld(values_per_miniblock * bit_width, 8)
                if pos + bytes_needed - 1 > length(data)
                    break
                end

                packed_data = data[pos:pos+bytes_needed-1]
                pos += bytes_needed

                deltas = unpack_bits(packed_data, values_per_miniblock, bit_width)

                for i in 1:values_to_read
                    if value_idx > length(result)
                        break
                    end
                    delta = Int64(deltas[i]) + min_delta
                    result[value_idx] = result[value_idx - 1] + delta
                    value_idx += 1
                end
            end
        end
    end

    result
end

#=============================================================================
# Delta Length Byte Array Encoding
=============================================================================#

"""
Decode delta length byte array encoding.
Lengths are delta-encoded, followed by concatenated byte data.
"""
function decode_delta_length_byte_array(data::Vector{UInt8}, count::Int)::Vector{Vector{UInt8}}
    # First decode the lengths using delta binary packed
    lengths = decode_delta_binary_packed(data, count)

    # Find where the lengths end and data begins
    # We need to re-parse to find the position
    pos = 1

    function read_varint()::Tuple{UInt64, Int}
        result = UInt64(0)
        shift = 0
        start_pos = pos
        while pos <= length(data)
            b = data[pos]
            pos += 1
            result |= UInt64(b & 0x7f) << shift
            if (b & 0x80) == 0
                break
            end
            shift += 7
        end
        (result, pos - start_pos)
    end

    # Skip the delta header
    block_size, _ = read_varint()
    miniblocks_per_block, _ = read_varint()
    total_count, _ = read_varint()
    # Skip first value
    read_varint()

    values_per_miniblock = Int(block_size) ÷ Int(miniblocks_per_block)
    values_remaining = Int(total_count)

    while values_remaining > 0
        # Skip min delta
        read_varint()

        # Skip bit widths
        pos += Int(miniblocks_per_block)

        # Skip miniblock data
        for mb_idx in 1:miniblocks_per_block
            bit_width = data[pos - Int(miniblocks_per_block) + mb_idx - 1]
            bytes_needed = cld(values_per_miniblock * Int(bit_width), 8)
            pos += bytes_needed
            values_remaining -= values_per_miniblock
            if values_remaining <= 0
                break
            end
        end
    end

    # Now pos points to the start of byte data
    result = Vector{Vector{UInt8}}(undef, count)
    for i in 1:count
        len = Int(lengths[i])
        result[i] = data[pos:pos+len-1]
        pos += len
    end

    result
end

#=============================================================================
# Delta Byte Array Encoding
=============================================================================#

"""
Decode delta byte array encoding.
Each value shares a prefix with the previous value.
Format: prefix lengths (delta encoded), suffix lengths (delta encoded), suffix data
"""
function decode_delta_byte_array(data::Vector{UInt8}, count::Int)::Vector{Vector{UInt8}}
    pos = 1

    function read_varint()::UInt64
        result = UInt64(0)
        shift = 0
        while pos <= length(data)
            b = data[pos]
            pos += 1
            result |= UInt64(b & 0x7f) << shift
            if (b & 0x80) == 0
                break
            end
            shift += 7
        end
        result
    end

    # Decode prefix lengths
    prefix_lengths = decode_delta_binary_packed(data, count)

    # Skip to suffix lengths (need to find position after prefix lengths block)
    # Re-parse prefix block to find end position
    block_size = Int(read_varint())
    miniblocks_per_block = Int(read_varint())
    total_count = Int(read_varint())
    read_varint()  # first value

    values_per_miniblock = block_size ÷ miniblocks_per_block
    values_remaining = total_count

    while values_remaining > 0 && pos <= length(data)
        read_varint()  # min delta
        start_bit_widths = pos
        pos += miniblocks_per_block

        for mb_idx in 1:miniblocks_per_block
            bit_width = data[start_bit_widths + mb_idx - 1]
            bytes_needed = cld(values_per_miniblock * Int(bit_width), 8)
            pos += bytes_needed
            values_remaining -= values_per_miniblock
            values_remaining <= 0 && break
        end
    end

    # Decode suffix lengths and data
    suffixes = decode_delta_length_byte_array(data[pos:end], count)

    # Reconstruct values
    result = Vector{Vector{UInt8}}(undef, count)
    result[1] = suffixes[1]  # First value has no prefix

    for i in 2:count
        prefix_len = Int(prefix_lengths[i])
        if prefix_len > 0
            result[i] = vcat(result[i-1][1:prefix_len], suffixes[i])
        else
            result[i] = suffixes[i]
        end
    end

    result
end

#=============================================================================
# Byte Stream Split Encoding
=============================================================================#

"""
Decode byte stream split encoding for FLOAT/DOUBLE.
Bytes are interleaved: all first bytes, then all second bytes, etc.
"""
function decode_byte_stream_split(data::Vector{UInt8}, count::Int, type_size::Int)::Vector{UInt8}
    result = Vector{UInt8}(undef, count * type_size)

    for i in 1:count
        for j in 1:type_size
            src_idx = (j - 1) * count + i
            dst_idx = (i - 1) * type_size + j
            result[dst_idx] = data[src_idx]
        end
    end

    result
end

"""Decode byte stream split for Float32."""
function decode_byte_stream_split_float(data::Vector{UInt8}, count::Int)::Vector{Float32}
    reconstructed = decode_byte_stream_split(data, count, 4)
    reinterpret(Float32, reconstructed)
end

"""Decode byte stream split for Float64."""
function decode_byte_stream_split_double(data::Vector{UInt8}, count::Int)::Vector{Float64}
    reconstructed = decode_byte_stream_split(data, count, 8)
    reinterpret(Float64, reconstructed)
end
