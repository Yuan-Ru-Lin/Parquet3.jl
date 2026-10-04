# Compression codec implementations for Parquet
# Uses CodecZlib, CodecZstd, CodecLz4 packages when available

using CodecZlib: GzipDecompressor, GzipCompressor
using CodecZstd: ZstdDecompressor, ZstdCompressor
using CodecLz4: LZ4_decompress_safe, LZ4_compress_fast, LZ4_compressBound
using Snappy: Snappy

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

decompress_snappy(data::Vector{UInt8}, ::Int) = Snappy.uncompress(data)

"""
    compress(data::Vector{UInt8}, codec::CompressionCodec) -> Vector{UInt8}

Compress data using the specified codec (inverse of `decompress`).
"""
function compress(data::Vector{UInt8}, codec::CompressionCodec)::Vector{UInt8}
    codec == UNCOMPRESSED && return data
    codec == SNAPPY && return Snappy.compress(data)
    codec == GZIP && return transcode(GzipCompressor, data)
    codec == ZSTD && return transcode(ZstdCompressor, data)
    codec == LZ4_RAW && return compress_lz4_raw(data)
    error("Unsupported compression codec for writing: $codec")
end

"""Compress to a raw LZ4 block using liblz4 (inverse of decompress_lz4_raw)."""
function compress_lz4_raw(data::Vector{UInt8})::Vector{UInt8}
    result = Vector{UInt8}(undef, LZ4_compressBound(length(data)))
    n = LZ4_compress_fast(data, result, length(data), length(result))
    n <= 0 && error("LZ4 compression failed (return code: $n)")
    resize!(result, n)
end

#=============================================================================
# Gzip Decompression
=============================================================================#

# `transcode` initializes and finalizes the codec around the call. Reading from a
# TranscodingStream that is never closed leaves the native context allocated: ZSTD's is
# not garbage-collected, which leaked ~95 KB per page.

"""Decompress Gzip-compressed data."""
decompress_gzip(data::Vector{UInt8})::Vector{UInt8} = transcode(GzipDecompressor, data)

#=============================================================================
# Zstd Decompression
=============================================================================#

"""Decompress Zstd-compressed data."""
decompress_zstd(data::Vector{UInt8})::Vector{UInt8} = transcode(ZstdDecompressor, data)

#=============================================================================
# LZ4 Decompression (via CodecLz4 / liblz4)
=============================================================================#

"""Decompress raw LZ4 block using liblz4."""
function decompress_lz4_raw(data::Vector{UInt8}, uncompressed_size::Int)::Vector{UInt8}
    result = Vector{UInt8}(undef, uncompressed_size)
    ret = LZ4_decompress_safe(data, result, length(data), uncompressed_size)
    ret < 0 && error("LZ4 decompression failed (error code: $ret)")
    result
end

"""Decompress LZ4-compressed data (raw or Hadoop framing)."""
function decompress_lz4(data::Vector{UInt8}, uncompressed_size::Int, is_raw::Bool)::Vector{UInt8}
    is_raw && return decompress_lz4_raw(data, uncompressed_size)

    # Hadoop LZ4: 4-byte total size (big-endian), then blocks of
    # [4-byte compressed size, 4-byte uncompressed size, data] (all big-endian)
    result = UInt8[]
    sizehint!(result, uncompressed_size)
    pos = 1

    # Skip total size header if present
    if length(data) >= 4
        total_size = ntoh(reinterpret(Int32, data[1:4])[1])
        total_size == uncompressed_size && (pos = 5)
    end

    while pos + 7 <= length(data) && length(result) < uncompressed_size
        compressed_size = Int(ntoh(reinterpret(Int32, data[pos:pos+3])[1]))
        block_uncompressed = Int(ntoh(reinterpret(Int32, data[pos+4:pos+7])[1]))
        pos += 8

        block = @view data[pos:pos+compressed_size-1]
        if compressed_size == block_uncompressed
            append!(result, block)
        else
            append!(result, decompress_lz4_raw(Vector{UInt8}(block), block_uncompressed))
        end
        pos += compressed_size
    end

    result
end

using TranscodingStreams: transcode
