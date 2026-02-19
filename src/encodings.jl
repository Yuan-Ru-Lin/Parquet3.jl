# Parquet encoding implementations

#=============================================================================
# Plain Encoding
=============================================================================#

"""
    decode_plain(type, data, count, type_length) -> Vector

Decode plain-encoded values. Uses if-else instead of Val dispatch
since ParquetType is a runtime value.
"""
function decode_plain(ptype::ParquetType, data::AbstractVector{UInt8}, count::Int, type_length::Int=0)
    if ptype == BOOLEAN
        return decode_plain_boolean(data, count)
    elseif ptype == INT32
        return decode_plain_int32(data, count)
    elseif ptype == INT64
        return decode_plain_int64(data, count)
    elseif ptype == INT96
        return decode_plain_int96(data, count)
    elseif ptype == FLOAT
        return decode_plain_float32(data, count)
    elseif ptype == DOUBLE
        return decode_plain_float64(data, count)
    elseif ptype == BYTE_ARRAY
        return decode_plain_byte_array(data, count)
    elseif ptype == FIXED_LEN_BYTE_ARRAY
        return decode_plain_fixed_byte_array(data, count, type_length)
    else
        error("Unknown Parquet type: $ptype")
    end
end

function decode_plain_boolean(data::AbstractVector{UInt8}, count::Int)
    bv = BitVector(undef, count)
    fill!(bv.chunks, zero(UInt64))
    n = min(cld(count, 8), length(data))
    GC.@preserve bv data unsafe_copyto!(Ptr{UInt8}(pointer(bv.chunks)), pointer(data), n)
    bv
end

decode_plain_int32(data::AbstractVector{UInt8}, count::Int) =
    reinterpret(Int32, @view data[1:4count])

decode_plain_int64(data::AbstractVector{UInt8}, count::Int) =
    reinterpret(Int64, @view data[1:8count])

decode_plain_int96(data::AbstractVector{UInt8}, count::Int) =
    reinterpret(Int96, @view data[1:12count])

decode_plain_float32(data::AbstractVector{UInt8}, count::Int) =
    reinterpret(Float32, @view data[1:4count])

decode_plain_float64(data::AbstractVector{UInt8}, count::Int) =
    reinterpret(Float64, @view data[1:8count])

function decode_plain_byte_array(data::AbstractVector{UInt8}, count::Int)
    # Pass 1: compute element boundaries
    elem_ptr = Vector{Int}(undef, count + 1)
    elem_ptr[1] = 1
    pos = 1
    for i in 1:count
        len = Int(ltoh(reinterpret(UInt32, @view data[pos:pos+3])[1]))
        pos += 4 + len
        elem_ptr[i+1] = elem_ptr[i] + len
    end

    # Pass 2: copy data into flat buffer
    flat = Vector{UInt8}(undef, elem_ptr[end] - 1)
    pos = 1
    for i in 1:count
        len = elem_ptr[i+1] - elem_ptr[i]
        pos += 4
        copyto!(flat, elem_ptr[i], data, pos, len)
        pos += len
    end

    VectorOfVectors(flat, elem_ptr)
end

decode_plain_fixed_byte_array(data::AbstractVector{UInt8}, count::Int, type_length::Int) =
    nestedview(reshape(@view(data[1:type_length*count]), type_length, count))

#=============================================================================
# Bit Unpacking
=============================================================================#

"""
    unpack_bits(data, count, bit_width) -> Vector{UInt32}

Unpack `count` values from bit-packed data where each value is `bit_width` bits.
"""
function unpack_bits(data::AbstractVector{UInt8}, count::Int, bit_width::Int)
    bit_width == 0 && return zeros(UInt32, count)

    result = Vector{UInt32}(undef, count)
    bit_position = 0

    for i in 1:count
        value = UInt32(0)
        bits_remaining = bit_width

        while bits_remaining > 0
            byte_index = (bit_position >> 3) + 1
            byte_index > length(data) && break

            bit_offset = bit_position & 7
            bits_in_byte = 8 - bit_offset
            bits_to_read = min(bits_in_byte, bits_remaining)

            mask = (UInt32(1) << bits_to_read) - 1
            extracted = (UInt32(data[byte_index]) >> bit_offset) & mask

            value |= extracted << (bit_width - bits_remaining)
            bits_remaining -= bits_to_read
            bit_position += bits_to_read
        end

        result[i] = value
    end

    result
end

#=============================================================================
# RLE / Bit-packed Hybrid Encoding
=============================================================================#

"""
    decode_rle_bitpacked(data, count, bit_width) -> Vector{UInt32}

Decode RLE/bit-packed hybrid encoding used for repetition/definition levels
and dictionary indices.

Format:
- Each group starts with a varint header
- If header is even: RLE run, (header >> 1) repeated values follow
- If header is odd: bit-packed, (header >> 1) groups of 8 values follow
"""
function decode_rle_bitpacked(data::AbstractVector{UInt8}, count::Int, bit_width::Int)
    bit_width == 0 && return zeros(UInt32, count)

    result = Vector{UInt32}(undef, count)
    output_index = 0
    pos = 1
    data_length = length(data)

    while output_index < count && pos <= data_length
        # Read varint header
        header = UInt32(0)
        shift = 0
        while pos <= data_length
            byte = data[pos]
            pos += 1
            header |= UInt32(byte & 0x7f) << shift
            (byte & 0x80) == 0 && break
            shift += 7
        end

        is_bitpacked = (header & 1) == 1

        if is_bitpacked
            # Bit-packed: groups of 8 values
            num_groups = header >> 1
            num_values = num_groups * 8
            bytes_needed = cld(num_values * bit_width, 8)
            bytes_available = min(bytes_needed, data_length - pos + 1)

            packed_data = @view data[pos : pos + bytes_available - 1]
            pos += bytes_available

            unpacked = unpack_bits(packed_data, num_values, bit_width)
            for value in unpacked
                output_index >= count && break
                output_index += 1
                result[output_index] = value
            end
        else
            # RLE: repeated value
            run_length = header >> 1
            value_byte_count = cld(bit_width, 8)

            pos + value_byte_count - 1 > data_length && break

            value = UInt32(0)
            for i in 0 : value_byte_count - 1
                value |= UInt32(data[pos + i]) << (8 * i)
            end
            pos += value_byte_count

            for _ in 1:run_length
                output_index >= count && break
                output_index += 1
                result[output_index] = value
            end
        end
    end

    output_index < count && resize!(result, output_index)
    result
end

#=============================================================================
# Dictionary Encoding
=============================================================================#

struct DictionaryDecoder{T}
    dictionary::Vector{T}
end

function DictionaryDecoder(data::AbstractVector{UInt8}, num_values::Int, ptype::ParquetType, type_length::Int=0)
    dict_values = collect(decode_plain(ptype, data, num_values, type_length))
    DictionaryDecoder(dict_values)
end

"""
    decode_dictionary(decoder, data, count) -> Vector

Decode dictionary-encoded data. First byte is bit width, rest is RLE-encoded indices.
"""
function decode_dictionary(decoder::DictionaryDecoder{T}, data::AbstractVector{UInt8}, count::Int) where T
    bit_width = Int(data[1])
    indices = decode_rle_bitpacked(@view(data[2:end]), count, bit_width)

    result = Vector{T}(undef, length(indices))
    for i in eachindex(indices)
        result[i] = decoder.dictionary[indices[i] + 1]  # Parquet uses 0-based indices
    end
    result
end

#=============================================================================
# Delta Binary Packed Encoding
=============================================================================#

"""
    decode_delta_binary_packed(data, count) -> Vector{Int64}

Decode delta binary packed encoding for sorted/nearly-sorted integer columns.
"""
function decode_delta_binary_packed(data::AbstractVector{UInt8}, count::Int)
    pos = Ref(1)

    function read_varint()
        val = UInt64(0)
        shift = 0
        while pos[] <= length(data)
            byte = data[pos[]]
            pos[] += 1
            val |= UInt64(byte & 0x7f) << shift
            (byte & 0x80) == 0 && break
            shift += 7
        end
        val
    end

    function read_zigzag()
        n = read_varint()
        signed_n = reinterpret(Int64, n)
        (signed_n >> 1) ⊻ (-(signed_n & 1))
    end

    # Read header
    block_size = Int(read_varint())
    miniblocks_per_block = Int(read_varint())
    total_value_count = Int(read_varint())
    first_value = read_zigzag()

    values_per_miniblock = block_size ÷ miniblocks_per_block

    result = Vector{Int64}(undef, min(count, total_value_count))
    result[1] = first_value
    value_index = 2

    # Decode blocks
    while value_index <= length(result) && pos[] <= length(data)
        min_delta = read_zigzag()

        # Read bit widths for each miniblock
        bit_widths = data[pos[] : pos[] + miniblocks_per_block - 1]
        pos[] += miniblocks_per_block

        # Decode each miniblock
        for miniblock in 1:miniblocks_per_block
            bit_width = Int(bit_widths[miniblock])
            values_to_read = min(values_per_miniblock, length(result) - value_index + 1)
            values_to_read <= 0 && break

            if bit_width == 0
                # All deltas equal min_delta
                for _ in 1:values_to_read
                    value_index > length(result) && break
                    result[value_index] = result[value_index - 1] + min_delta
                    value_index += 1
                end
            else
                bytes_needed = cld(values_per_miniblock * bit_width, 8)
                pos[] + bytes_needed - 1 > length(data) && break

                packed_data = @view data[pos[] : pos[] + bytes_needed - 1]
                pos[] += bytes_needed

                deltas = unpack_bits(packed_data, values_per_miniblock, bit_width)
                for i in 1:values_to_read
                    value_index > length(result) && break
                    delta = Int64(deltas[i]) + min_delta
                    result[value_index] = result[value_index - 1] + delta
                    value_index += 1
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
    decode_delta_length_byte_array(data, count) -> Vector{Vector{UInt8}}

Decode delta length byte array encoding. Lengths are delta-encoded,
followed by concatenated byte data.
"""
function decode_delta_length_byte_array(data::AbstractVector{UInt8}, count::Int)
    # First, decode the lengths
    lengths = decode_delta_binary_packed(data, count)

    # Find where the length data ends by re-parsing the header
    pos = 1

    function read_varint()
        result = UInt64(0)
        shift = 0
        while pos <= length(data)
            byte = data[pos]
            pos += 1
            result |= UInt64(byte & 0x7f) << shift
            (byte & 0x80) == 0 && break
            shift += 7
        end
        result
    end

    block_size = Int(read_varint())
    miniblocks_per_block = Int(read_varint())
    total_count = Int(read_varint())
    read_varint()  # first value

    values_per_miniblock = block_size ÷ miniblocks_per_block
    values_remaining = total_count

    # Skip through the delta blocks to find byte data start
    while values_remaining > 0 && pos <= length(data)
        read_varint()  # min_delta
        bit_widths_start = pos
        pos += miniblocks_per_block

        for mb in 1:miniblocks_per_block
            bit_width = data[bit_widths_start + mb - 1]
            bytes_needed = cld(values_per_miniblock * Int(bit_width), 8)
            pos += bytes_needed
            values_remaining -= values_per_miniblock
            values_remaining <= 0 && break
        end
    end

    # Byte data is contiguous from here — build VectorOfVectors as a zero-copy view
    elem_ptr = Vector{Int}(undef, count + 1)
    elem_ptr[1] = 1
    for i in 1:count
        elem_ptr[i+1] = elem_ptr[i] + Int(lengths[i])
    end

    total = elem_ptr[end] - 1
    VectorOfVectors(@view(data[pos : pos + total - 1]), elem_ptr)
end

#=============================================================================
# Byte Stream Split Encoding
=============================================================================#

"""
    decode_byte_stream_split_float32(data, count) -> Vector{Float32}

Decode byte stream split encoding for Float32. Bytes are interleaved:
all first bytes, then all second bytes, etc.
"""
function decode_byte_stream_split_float32(data::AbstractVector{UInt8}, count::Int)
    reconstructed = Vector{UInt8}(undef, count * 4)
    for i in 1:count
        for j in 1:4
            reconstructed[(i-1)*4 + j] = data[(j-1)*count + i]
        end
    end
    reinterpret(Float32, reconstructed)
end

"""
    decode_byte_stream_split_float64(data, count) -> Vector{Float64}

Decode byte stream split encoding for Float64.
"""
function decode_byte_stream_split_float64(data::AbstractVector{UInt8}, count::Int)
    reconstructed = Vector{UInt8}(undef, count * 8)
    for i in 1:count
        for j in 1:8
            reconstructed[(i-1)*8 + j] = data[(j-1)*count + i]
        end
    end
    reinterpret(Float64, reconstructed)
end
