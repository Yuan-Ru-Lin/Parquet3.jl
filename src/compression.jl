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
Decompress the deprecated LZ4 codec. Hadoop wrote it as a sequence of frames, each
[4-byte uncompressed size][4-byte compressed size][raw LZ4 block], sizes big-endian; other
writers stored one raw LZ4 block with no framing under the same codec id. The Hadoop
framing is tried first and accepted only if it accounts for the input and the output
exactly; otherwise the data is taken as a single raw block (as Arrow C++ does).
ChunkCodecs has no codec for the framing, only for the blocks.
"""
function decompress_lz4_hadoop(data::AbstractVector{UInt8}, uncompressed_size::Int)
    framed = _lz4_hadoop_frames(data, uncompressed_size)
    framed !== nothing ? framed : decode(LZ4BlockCodec(), data; max_size = uncompressed_size)
end

"""The data decoded as Hadoop LZ4 frames, or `nothing` if it is not laid out that way."""
function _lz4_hadoop_frames(data::AbstractVector{UInt8}, uncompressed_size::Int)
    be_int(pos) = Int(ntoh(reinterpret(UInt32, data[pos:pos+3])[1]))
    result = UInt8[]
    sizehint!(result, uncompressed_size)
    pos = 1
    while pos <= length(data)
        pos + 7 <= length(data) || return nothing
        block_uncompressed, compressed_size = be_int(pos), be_int(pos + 4)
        pos += 8
        (compressed_size <= length(data) - pos + 1 && length(result) + block_uncompressed <= uncompressed_size) || return nothing
        block = try
            decode(LZ4BlockCodec(), @view(data[pos:pos+compressed_size-1]); max_size = block_uncompressed)
        catch
            return nothing
        end
        length(block) == block_uncompressed || return nothing
        append!(result, block)
        pos += compressed_size
    end
    length(result) == uncompressed_size ? result : nothing
end
