# Parquet encoding implementations

#=============================================================================
# Plain Encoding
=============================================================================#

# Fixed-width physical types, whose PLAIN encoding is their little-endian bytes
const PLAIN_FIXED_TYPES = Dict(INT32 => Int32, INT64 => Int64, INT96 => Int96, FLOAT => Float32, DOUBLE => Float64)

"""
    decode_plain(type, data, count, type_length) -> Vector

Decode plain-encoded values. `ptype` is a runtime value, so this branches instead of
dispatching; fixed-width types are a reinterpreted view of the page bytes.
"""
function decode_plain(ptype::ParquetType, data::AbstractVector{UInt8}, count::Int, type_length::Int=0)
    ptype == BOOLEAN && return decode_plain_boolean(data, count)
    ptype == BYTE_ARRAY && return decode_plain_byte_array(data, count)
    ptype == FIXED_LEN_BYTE_ARRAY && return decode_plain_fixed_byte_array(data, count, type_length)
    T = get(() -> error("Unknown Parquet type: $ptype"), PLAIN_FIXED_TYPES, ptype)
    reinterpret(T, @view data[1:sizeof(T) * count])
end

function decode_plain_boolean(data::AbstractVector{UInt8}, count::Int)
    bv = BitVector(undef, count)
    fill!(bv.chunks, zero(UInt64))
    n = min(cld(count, 8), length(data))
    GC.@preserve bv data unsafe_copyto!(Ptr{UInt8}(pointer(bv.chunks)), pointer(data), n)
    bv
end

"""
RLE-encoded boolean values: the RLE/bit-packed hybrid at bit width 1, preceded by its
4-byte length. Unlike levels and dictionary indices, boolean values carry the length
prefix in both v1 and v2 data pages.
"""
function decode_rle_boolean(data::AbstractVector{UInt8}, count::Int)
    count == 0 && return falses(0)
    len = Int(ltoh(reinterpret(UInt32, data[1:4])[1]))
    BitVector(decode_rle_bitpacked(@view(data[5:4+len]), count, 1) .!= 0)
end

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

"""Bits needed to store a repetition or definition level up to `max_level`."""
level_bit_width(max_level::Integer) = ndigits(max_level, base = 2)

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
    _unpack_bits_into!(result, 1, data, count, bit_width)
    result
end

"""
    unpack_bits!(result, offset, data, count, bit_width)

Unpack `count` bit-packed values directly into `result` starting at `result[offset]`.
"""
function unpack_bits!(result::Vector{U}, offset::Int, data::AbstractVector{UInt8}, count::Int, bit_width::Int) where {U <: Union{UInt32, UInt64}}
    bit_width == 0 && (fill!(@view(result[offset:offset+count-1]), zero(U)); return)
    _unpack_bits_into!(result, offset, data, count, bit_width)
    nothing
end

"""
Inner loop shared by unpack_bits and unpack_bits!.
Uses an accumulator twice as wide as the values to extract them with shift+mask
instead of a byte-at-a-time inner loop (UInt64 for levels and indices, UInt128
for delta values, whose bit width can reach 64).
"""
function _unpack_bits_into!(result::Vector{U}, offset::Int, data::AbstractVector{UInt8}, count::Int, bit_width::Int) where {U <: Union{UInt32, UInt64}}
    A = widen(U)
    mask = (A(1) << bit_width) - A(1)
    data_len = length(data)
    accum = A(0)
    bits_in_accum = 0
    byte_pos = 1  # next byte to load from data

    @inbounds for i in 0:count-1
        # Ensure accumulator has enough bits
        while bits_in_accum < bit_width && byte_pos <= data_len
            accum |= A(data[byte_pos]) << bits_in_accum
            bits_in_accum += 8
            byte_pos += 1
        end

        result[offset + i] = (accum & mask) % U
        accum >>= bit_width
        bits_in_accum -= bit_width
    end
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
        header, pos = _read_varint(data, pos)
        header = Int(header)

        is_bitpacked = (header & 1) == 1

        if is_bitpacked
            # Bit-packed: groups of 8 values
            num_groups = header >> 1
            num_values = num_groups * 8
            bytes_needed = cld(num_values * bit_width, 8)
            bytes_available = min(bytes_needed, data_length - pos + 1)

            packed_data = @view data[pos : pos + bytes_available - 1]
            pos += bytes_available

            values_to_copy = min(num_values, count - output_index)
            # Ensure result has enough room (it should, but be safe)
            unpack_bits!(result, output_index + 1, packed_data, values_to_copy, bit_width)
            output_index += values_to_copy
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

"""Read a varint from `data` starting at position `pos`, return (value, new_pos)."""
function _read_varint(data::AbstractVector{UInt8}, pos::Int)
    val = UInt64(0)
    shift = 0
    len = length(data)
    @inbounds while pos <= len
        byte = data[pos]
        pos += 1
        val |= UInt64(byte & 0x7f) << shift
        (byte & 0x80) == 0 && break
        shift += 7
    end
    (val, pos)
end

"""Read a zigzag-encoded varint from `data` starting at `pos`, return (value, new_pos)."""
function _read_zigzag(data::AbstractVector{UInt8}, pos::Int)
    n, pos = _read_varint(data, pos)
    # Logical shift on the unsigned value, so the full Int64 range decodes
    (reinterpret(Int64, (n >> 1) ⊻ -(n & 0x01)), pos)
end

"""
    decode_delta_binary_packed(data, count) -> (Vector{Int64}, final_pos)

Decode delta binary packed encoding. Returns the decoded values and the
byte position after the last consumed byte (for use by delta_length_byte_array).

Deltas are added with wrap-around, as the encoding defines them; for an INT32 column
the caller truncates the result to 32 bits, which completes the 32-bit wrap-around.
"""
function decode_delta_binary_packed(data::AbstractVector{UInt8}, count::Int)
    pos = 1

    # Read header
    block_size, pos = _read_varint(data, pos); block_size = Int(block_size)
    miniblocks_per_block, pos = _read_varint(data, pos); miniblocks_per_block = Int(miniblocks_per_block)
    total_value_count, pos = _read_varint(data, pos); total_value_count = Int(total_value_count)
    first_value, pos = _read_zigzag(data, pos)

    values_per_miniblock = block_size ÷ miniblocks_per_block

    result = Vector{Int64}(undef, min(count, total_value_count))
    isempty(result) && return (result, pos)
    result[1] = first_value
    value_index = 2

    # Reusable buffer for unpacked deltas (avoids allocation per miniblock)
    delta_buf = Vector{UInt64}(undef, values_per_miniblock)

    # Decode blocks
    while value_index <= length(result) && pos <= length(data)
        min_delta, pos = _read_zigzag(data, pos)

        # Read bit widths for each miniblock
        bit_widths = @view data[pos : pos + miniblocks_per_block - 1]
        pos += miniblocks_per_block

        # Decode each miniblock
        for miniblock in 1:miniblocks_per_block
            bit_width = Int(bit_widths[miniblock])
            values_to_read = min(values_per_miniblock, length(result) - value_index + 1)
            values_to_read <= 0 && break

            if bit_width == 0
                # All deltas equal min_delta
                @inbounds for _ in 1:values_to_read
                    value_index > length(result) && break
                    result[value_index] = result[value_index - 1] + min_delta
                    value_index += 1
                end
            else
                bytes_needed = cld(values_per_miniblock * bit_width, 8)
                pos + bytes_needed - 1 > length(data) && break

                packed_data = @view data[pos : pos + bytes_needed - 1]
                pos += bytes_needed

                unpack_bits!(delta_buf, 1, packed_data, values_per_miniblock, bit_width)
                @inbounds for i in 1:values_to_read
                    value_index > length(result) && break
                    delta = (delta_buf[i] % Int64) + min_delta
                    result[value_index] = result[value_index - 1] + delta
                    value_index += 1
                end
            end
        end
    end

    (result, pos)
end

"""ZigZag varint (inverse of _read_zigzag)."""
_write_zigzag(io::IO, v::Integer) = (x = Int64(v); _write_varint(io, reinterpret(UInt64, (x << 1) ⊻ (x >> 63))))

"""
Bit-pack `values` LSB-first at `bit_width` bits each (inverse of unpack_bits!),
zero-padded to `count` values.
"""
function pack_bits(values::AbstractVector{<:Unsigned}, bit_width::Int, count::Int = length(values))
    out = zeros(UInt8, cld(count * bit_width, 8))
    accum, bits_in_accum, byte_pos = UInt128(0), 0, 1
    @inbounds for v in values
        accum |= UInt128(v) << bits_in_accum
        bits_in_accum += bit_width
        while bits_in_accum >= 8
            out[byte_pos] = accum % UInt8
            byte_pos += 1
            accum >>= 8
            bits_in_accum -= 8
        end
    end
    bits_in_accum > 0 && (out[byte_pos] = accum % UInt8)
    out
end

"""
    encode_delta_binary_packed(values::Vector{<:Union{Int32, Int64}}) -> Vector{UInt8}

DELTA_BINARY_PACKED (inverse of decode_delta_binary_packed): a header with the first
value, then blocks of 128 deltas. Each block stores its minimum delta, and each of its
4 miniblocks of 32 stores `delta - min_delta` bit-packed at the width of its largest
value. Deltas wrap around in the values' own integer width, so they always fit in it.
"""
function encode_delta_binary_packed(values::Vector{T}) where {T <: Union{Int32, Int64}}
    block_size, miniblocks = 128, 4
    miniblock_size = block_size ÷ miniblocks
    out = IOBuffer()
    foreach(v -> _write_varint(out, UInt64(v)), (block_size, miniblocks, length(values)))
    _write_zigzag(out, isempty(values) ? 0 : first(values))

    deltas = T[values[i] - values[i - 1] for i in 2:length(values)]
    for block in Iterators.partition(deltas, block_size)
        min_delta = minimum(block)
        _write_zigzag(out, min_delta)
        packed = map(Iterators.partition(block, miniblock_size)) do miniblock
            relative = [(d - min_delta) % unsigned(T) for d in miniblock]
            bit_width = 8 * sizeof(T) - leading_zeros(maximum(relative))
            (bit_width, pack_bits(relative, bit_width, miniblock_size))
        end
        # Bit widths of unused miniblocks in the last block are written as zero, with no data
        write(out, UInt8[i <= length(packed) ? first(packed[i]) : 0 for i in 1:miniblocks])
        foreach(p -> write(out, last(p)), packed)
    end
    take!(out)
end

#=============================================================================
# Delta Length Byte Array Encoding
=============================================================================#

"""
    decode_delta_length_byte_array(data, count) -> VectorOfVectors

Decode delta length byte array encoding. Lengths are delta-encoded,
followed by concatenated byte data.
"""
function decode_delta_length_byte_array(data::AbstractVector{UInt8}, count::Int)
    # Decode lengths; pos is the byte position right after the delta block
    lengths, pos = decode_delta_binary_packed(data, count)

    # Byte data is contiguous from here — build VectorOfVectors as a zero-copy view
    elem_ptr = Vector{Int}(undef, count + 1)
    elem_ptr[1] = 1
    for i in 1:count
        elem_ptr[i+1] = elem_ptr[i] + Int(lengths[i])
    end

    total = elem_ptr[end] - 1
    VectorOfVectors(@view(data[pos : pos + total - 1]), elem_ptr)
end

"""
    encode_delta_length_byte_array(values) -> Vector{UInt8}

DELTA_LENGTH_BYTE_ARRAY (inverse of decode_delta_length_byte_array): all lengths,
DELTA_BINARY_PACKED as INT32, followed by the values' bytes back to back.
"""
function encode_delta_length_byte_array(values::AbstractVector{<:Union{AbstractString, Vector{UInt8}}})
    out = IOBuffer()
    write(out, encode_delta_binary_packed(Int32[sizeof(v) for v in values]))
    foreach(v -> write(out, v), values)
    take!(out)
end

#=============================================================================
# Byte Stream Split Encoding
=============================================================================#

"""
    decode_byte_stream_split(T, data, count) -> AbstractVector{T}

Decode byte stream split encoding: byte 1 of every value, then byte 2 of every value, and
so on. Read as a `count`×K matrix whose columns are those streams, the values' bytes are
its rows. Inverse of `encode_byte_stream_split`.
"""
decode_byte_stream_split(::Type{T}, data::AbstractVector{UInt8}, count::Int) where {T <: Union{Float32, Float64}} =
    reinterpret(T, vec(permutedims(reshape(@view(data[1:sizeof(T) * count]), count, sizeof(T)))))

# ═══════════════════════════════════════════════════════════════════════════
# Encoders (write side) — mirrors of the decoders above
# ═══════════════════════════════════════════════════════════════════════════

"""PLAIN-encode fixed-width values (inverse of decode_plain for these types)."""
encode_plain(values::Vector{T}) where {T <: Union{Int32, Int64, Float32, Float64}} =
    collect(reinterpret(UInt8, values))

"""
Integer-like values as stored in their physical Parquet type, INT32 or INT64: narrow and
unsigned integers bit-preserved, dates as days and datetimes as milliseconds since the
Unix epoch, Arrow timestamps as their count of units.
"""
physical_ints(values::Vector{<:Union{Int32, Int64}}) = values
physical_ints(values::Vector{<:Union{Int8, Int16, UInt8, UInt16, UInt32}}) = values .% Int32
physical_ints(values::Vector{UInt64}) = values .% Int64
physical_ints(values::Vector{Date}) = Int32[Dates.value(v - Date(1970, 1, 1)) for v in values]
physical_ints(values::Vector{DateTime}) = Int64[Dates.value(v - DateTime(1970, 1, 1)) for v in values]
physical_ints(values::Vector{<:Arrow.Timestamp}) = Int64[v.x for v in values]

"""PLAIN-encode integer-like values through their physical type (see `physical_ints`)."""
encode_plain(values::Vector{<:Union{Int8, Int16, UInt8, UInt16, UInt32, UInt64, Date, DateTime, Arrow.Timestamp}}) =
    encode_plain(physical_ints(values))

"""
BYTE_STREAM_SPLIT-encode floats (inverse of decode_byte_stream_split):
byte 1 of every value, then byte 2 of every value, and so on. With the values' bytes as
the columns of a K×n matrix, that is its rows laid end to end.
"""
encode_byte_stream_split(values::Vector{T}) where {T <: Union{Float32, Float64}} =
    vec(permutedims(reshape(reinterpret(UInt8, values), sizeof(T), :)))

"""
The bytes of a bit vector, LSB-first: bit `i` is bit `(i-1) & 7` of byte `(i-1) >> 3`. This
is how a `BitVector` stores its chunks, and how Parquet (PLAIN booleans) and Arrow (boolean
arrays) pack booleans.
"""
packed_bits(bits::BitVector) = reinterpret(UInt8, bits.chunks)[1:cld(length(bits), 8)]

"""PLAIN-encode booleans, LSB-first bit-packed (inverse of decode_plain_boolean)."""
encode_plain(values::Vector{Bool}) = packed_bits(BitVector(values))

"""PLAIN-encode strings/byte arrays as 4-byte LE length + payload (inverse of decode_plain_byte_array)."""
function encode_plain(values::AbstractVector{<:Union{AbstractString, Vector{UInt8}}})
    out = IOBuffer()
    for v in values
        bytes = v isa AbstractString ? codeunits(v) : v
        write(out, htol(UInt32(length(bytes))))
        write(out, bytes)
    end
    take!(out)
end

"""LEB128 varint (inverse of _read_varint)."""
function _write_varint(io::IO, v::Unsigned)
    while true
        b = UInt8(v & 0x7f)
        v >>= 7
        v == 0 && return write(io, b)
        write(io, b | 0x80)
    end
end

"""
RLE/bit-packed hybrid encoding of levels (inverse of decode_rle_bitpacked).
Uses RLE runs only — header `run_length << 1` (even) followed by the run value
in `cld(bit_width, 8)` bytes — which is always a valid form of the hybrid.
"""
function encode_rle_bitpacked(levels::AbstractVector{<:Integer}, bit_width::Int)
    out = IOBuffer()
    value_bytes = cld(bit_width, 8)
    i = 1
    while i <= length(levels)
        v = levels[i]
        j = i
        while j < length(levels) && levels[j + 1] == v
            j += 1
        end
        _write_varint(out, UInt64(j - i + 1) << 1)
        for b in 0:value_bytes-1
            write(out, UInt8((v >> (8b)) & 0xff))
        end
        i = j + 1
    end
    take!(out)
end
