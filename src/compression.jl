# Compression codec implementations for Parquet
# Uses CodecZlib, CodecZstd, CodecLz4 packages when available

using CodecZlib: GzipDecompressor
using CodecZstd: ZstdDecompressor
using CodecLz4: LZ4FrameDecompressor

"""
    decompress(data::Vector{UInt8}, codec::CompressionCodec, uncompressed_size::Int) -> Vector{UInt8}

Decompress data using the specified codec.
"""
function decompress(data::Vector{UInt8}, codec::CompressionCodec, uncompressed_size::Int)::Vector{UInt8}
    if codec == UNCOMPRESSED
        return data
    elseif codec == SNAPPY
        return decompress_snappy(data, uncompressed_size)
    elseif codec == GZIP
        return decompress_gzip(data)
    elseif codec == ZSTD
        return decompress_zstd(data)
    elseif codec == LZ4 || codec == LZ4_RAW
        return decompress_lz4(data, uncompressed_size, codec == LZ4_RAW)
    else
        error("Unsupported compression codec: $codec")
    end
end

#=============================================================================
# Snappy Decompression (native implementation)
=============================================================================#

"""
Decompress Snappy-compressed data.
Snappy format: https://github.com/google/snappy/blob/main/format_description.txt
"""
function decompress_snappy(data::Vector{UInt8}, uncompressed_size::Int)::Vector{UInt8}
    result = Vector{UInt8}(undef, uncompressed_size)
    pos = 1
    out_pos = 1

    # Read uncompressed length (varint)
    decoded_len = 0
    shift = 0
    while pos <= length(data)
        b = data[pos]
        pos += 1
        decoded_len |= (b & 0x7f) << shift
        if (b & 0x80) == 0
            break
        end
        shift += 7
    end

    while pos <= length(data) && out_pos <= uncompressed_size
        tag = data[pos]
        pos += 1

        tag_type = tag & 0x03

        if tag_type == 0
            # Literal
            len = (tag >> 2) + 1

            if len <= 60
                # Length is in tag
            elseif len == 61
                len = Int(data[pos]) + 1
                pos += 1
            elseif len == 62
                len = Int(data[pos]) + (Int(data[pos+1]) << 8) + 1
                pos += 2
            elseif len == 63
                len = Int(data[pos]) + (Int(data[pos+1]) << 8) + (Int(data[pos+2]) << 16) + 1
                pos += 3
            else
                len = Int(data[pos]) + (Int(data[pos+1]) << 8) + (Int(data[pos+2]) << 16) + (Int(data[pos+3]) << 24) + 1
                pos += 4
            end

            copyto!(result, out_pos, data, pos, len)
            pos += len
            out_pos += len

        elseif tag_type == 1
            # Copy with 1-byte offset
            len = ((tag >> 2) & 0x07) + 4
            offset = Int(tag & 0xe0) << 3 | Int(data[pos])
            pos += 1

            for i in 1:len
                result[out_pos] = result[out_pos - offset]
                out_pos += 1
            end

        elseif tag_type == 2
            # Copy with 2-byte offset
            len = (tag >> 2) + 1
            offset = Int(data[pos]) | (Int(data[pos+1]) << 8)
            pos += 2

            for i in 1:len
                result[out_pos] = result[out_pos - offset]
                out_pos += 1
            end

        else  # tag_type == 3
            # Copy with 4-byte offset
            len = (tag >> 2) + 1
            offset = Int(data[pos]) | (Int(data[pos+1]) << 8) | (Int(data[pos+2]) << 16) | (Int(data[pos+3]) << 24)
            pos += 4

            for i in 1:len
                result[out_pos] = result[out_pos - offset]
                out_pos += 1
            end
        end
    end

    result
end

#=============================================================================
# Gzip Decompression
=============================================================================#

"""Decompress Gzip-compressed data."""
function decompress_gzip(data::Vector{UInt8})::Vector{UInt8}
    io = IOBuffer(data)
    decompressor = GzipDecompressor()
    decompressed = read(TranscodingStream(decompressor, io))
    decompressed
end

#=============================================================================
# Zstd Decompression
=============================================================================#

"""Decompress Zstd-compressed data."""
function decompress_zstd(data::Vector{UInt8})::Vector{UInt8}
    io = IOBuffer(data)
    decompressor = ZstdDecompressor()
    decompressed = read(TranscodingStream(decompressor, io))
    decompressed
end

#=============================================================================
# LZ4 Decompression
=============================================================================#

"""Decompress LZ4-compressed data."""
function decompress_lz4(data::Vector{UInt8}, uncompressed_size::Int, is_raw::Bool)::Vector{UInt8}
    if is_raw
        # LZ4_RAW: raw LZ4 block without frame header
        return decompress_lz4_raw(data, uncompressed_size)
    else
        # LZ4: LZ4 frame format (Hadoop style with block sizes)
        return decompress_lz4_hadoop(data, uncompressed_size)
    end
end

"""Decompress LZ4 raw block (no frame header)."""
function decompress_lz4_raw(data::Vector{UInt8}, uncompressed_size::Int)::Vector{UInt8}
    result = Vector{UInt8}(undef, uncompressed_size)
    pos = 1
    out_pos = 1

    while pos <= length(data) && out_pos <= uncompressed_size
        token = data[pos]
        pos += 1

        # Literal length
        lit_len = (token >> 4) & 0x0f
        if lit_len == 15
            while pos <= length(data)
                add = data[pos]
                pos += 1
                lit_len += add
                add < 255 && break
            end
        end

        # Copy literals
        if lit_len > 0
            copyto!(result, out_pos, data, pos, lit_len)
            pos += lit_len
            out_pos += lit_len
        end

        # Check if we're done
        out_pos > uncompressed_size && break
        pos > length(data) && break

        # Match offset (little-endian 16-bit)
        offset = Int(data[pos]) | (Int(data[pos+1]) << 8)
        pos += 2

        # Match length
        match_len = (token & 0x0f) + 4
        if (token & 0x0f) == 15
            while pos <= length(data)
                add = data[pos]
                pos += 1
                match_len += add
                add < 255 && break
            end
        end

        # Copy match
        for i in 1:match_len
            if out_pos > uncompressed_size
                break
            end
            result[out_pos] = result[out_pos - offset]
            out_pos += 1
        end
    end

    result
end

"""Decompress LZ4 Hadoop format (block sizes prepended)."""
function decompress_lz4_hadoop(data::Vector{UInt8}, uncompressed_size::Int)::Vector{UInt8}
    # Hadoop LZ4 format: 4-byte uncompressed size, then blocks
    # Each block: 4-byte compressed size, 4-byte uncompressed size, data
    result = UInt8[]
    sizehint!(result, uncompressed_size)

    pos = 1

    # Skip decompressed size header if present
    if length(data) >= 4
        # First 4 bytes might be total uncompressed size (big-endian in Hadoop)
        total_size = (Int(data[1]) << 24) | (Int(data[2]) << 16) | (Int(data[3]) << 8) | Int(data[4])
        if total_size == uncompressed_size
            pos = 5
        end
    end

    while pos <= length(data) && length(result) < uncompressed_size
        if pos + 8 > length(data)
            break
        end

        # Block compressed size (big-endian)
        compressed_size = (Int(data[pos]) << 24) | (Int(data[pos+1]) << 16) | (Int(data[pos+2]) << 8) | Int(data[pos+3])
        pos += 4

        # Block uncompressed size (big-endian)
        block_uncompressed = (Int(data[pos]) << 24) | (Int(data[pos+1]) << 16) | (Int(data[pos+2]) << 8) | Int(data[pos+3])
        pos += 4

        if compressed_size == block_uncompressed
            # Uncompressed block
            append!(result, data[pos:pos+compressed_size-1])
        else
            # Compressed block
            block_data = data[pos:pos+compressed_size-1]
            decompressed_block = decompress_lz4_raw(block_data, block_uncompressed)
            append!(result, decompressed_block)
        end

        pos += compressed_size
    end

    result
end

# Re-export TranscodingStreams for gzip/zstd
using TranscodingStreams: TranscodingStream
