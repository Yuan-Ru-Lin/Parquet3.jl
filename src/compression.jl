# Page compression for Parquet, via ChunkCodecs.jl

using ChunkCodecLibZlib: GzipCodec, GzipEncodeOptions, encode, decode
using ChunkCodecLibZstd: ZstdCodec, ZstdEncodeOptions
using ChunkCodecLibLz4: LZ4BlockCodec, LZ4BlockEncodeOptions
using ChunkCodecLibSnappy: SnappyCodec, SnappyEncodeOptions
using ChunkCodecLibBrotli: BrotliCodec, BrotliEncodeOptions

"""ChunkCodecs decoder and encoder for each Parquet codec (LZ4 here is the raw block format)."""
const CHUNK_CODECS = Dict(
    SNAPPY  => (SnappyCodec(),   SnappyEncodeOptions()),
    GZIP    => (GzipCodec(),     GzipEncodeOptions()),
    BROTLI  => (BrotliCodec(),   BrotliEncodeOptions()),
    ZSTD    => (ZstdCodec(),     ZstdEncodeOptions()),
    LZ4_RAW => (LZ4BlockCodec(), LZ4BlockEncodeOptions()),
)

_chunk_codec(codec::CompressionCodec) =
    get(() -> error("Unsupported compression codec: $codec"), CHUNK_CODECS, codec)

"""
    decompress(data, codec::CompressionCodec, uncompressed_size::Int) -> Vector{UInt8}

Decompress a page. `uncompressed_size` is the size the page header declares; it bounds
the output, so a corrupt file cannot force a larger allocation.
"""
function decompress(data::AbstractVector{UInt8}, codec::CompressionCodec, uncompressed_size::Int)
    codec == UNCOMPRESSED && return data
    codec == LZ4 && return decompress_lz4_hadoop(data, uncompressed_size)
    decode(first(_chunk_codec(codec)), data; max_size = uncompressed_size)
end

"""
    compress(data, codec::CompressionCodec) -> Vector{UInt8}

Compress a page (inverse of `decompress`). The deprecated Hadoop-framed LZ4 is not written.
"""
compress(data::AbstractVector{UInt8}, codec::CompressionCodec) =
    codec == UNCOMPRESSED ? data : encode(last(_chunk_codec(codec)), data)

"""
Decompress the deprecated LZ4 codec: Hadoop framing around raw LZ4 blocks. An optional
4-byte total size, then blocks of [4-byte compressed size, 4-byte uncompressed size,
data], all big-endian. ChunkCodecs has no codec for this framing, only for the blocks.
"""
function decompress_lz4_hadoop(data::AbstractVector{UInt8}, uncompressed_size::Int)
    be_int(pos) = Int(ntoh(reinterpret(Int32, data[pos:pos+3])[1]))
    result = UInt8[]
    sizehint!(result, uncompressed_size)

    # Skip total size header if present
    pos = length(data) >= 4 && be_int(1) == uncompressed_size ? 5 : 1

    while pos + 7 <= length(data) && length(result) < uncompressed_size
        compressed_size, block_uncompressed = be_int(pos), be_int(pos + 4)
        pos += 8
        block = @view data[pos:pos+compressed_size-1]
        append!(result, compressed_size == block_uncompressed ? block :
                        decode(LZ4BlockCodec(), block; max_size = block_uncompressed))
        pos += compressed_size
    end

    result
end
