# Writing

What `write_parquet` accepts and what it writes. Limits are collected in [limitations.md](limitations.md); reading is in [reading.md](reading.md).

## Column types and nesting

`write_parquet(path, table)` writes flat columns of Int8–Int64, UInt8–UInt64, Float32/Float64, Bool, String, `Date`, `DateTime`, `Arrow.Timestamp` and `Vector{UInt8}`, lists (vector elements, written as `List<T>`), structs (`NamedTuple` elements, written as a group) and maps (`AbstractDict` elements, written as a Parquet MAP) of supported types nested to any depth — `struct{list}`, struct-of-struct, `list<struct>`, `list<list>`, … — and `Missing` unions at every level (single row group, null-count statistics).

`DateTime` is written as a naive millisecond timestamp and `Arrow.Timestamp{unit, tz}` with its unit and UTC flag, so timestamp columns from `read_parquet` write back unchanged (a named time zone becomes UTC, since Parquet only stores a UTC flag).

Multiple row groups are not yet written.

## Compression

Pages are compressed with Snappy by default; pass `compression = :gzip`, `:brotli`, `:zstd`, `:lz4`, or `:uncompressed` to change it.

All codecs go through [ChunkCodecs.jl](https://github.com/JuliaIO/ChunkCodecs.jl).

| Codec | Read | Write (`compression =`) | Implementation |
|-------|------|-------------------------|----------------|
| Snappy | yes | `:snappy` (default) | ChunkCodecLibSnappy |
| Gzip | yes | `:gzip` | ChunkCodecLibZlib |
| Brotli | yes | `:brotli` | ChunkCodecLibBrotli |
| Zstd | yes | `:zstd` | ChunkCodecLibZstd |
| LZ4 (raw) | yes | `:lz4` | ChunkCodecLibLz4 |
| LZ4 (deprecated codec id) | yes, in Hadoop's framing and as a single raw block | no | Custom Hadoop framing around ChunkCodecLibLz4 blocks |

## Encodings

Values are PLAIN-encoded by default. `encoding` accepts:

| `encoding =` | Applies to |
|---|---|
| `:plain` | everything |
| `:byte_stream_split` | Float32, Float64 |
| `:delta_binary_packed` | every type stored as an integer: Int8–Int64, UInt8–UInt64, Date, DateTime, Arrow.Timestamp |
| `:delta_length_byte_array` | String, `Vector{UInt8}` |

Dictionary-encoded files are read, but dictionary encoding is not yet written (planned for v0.3).

A single name applies to the whole table: it is used for every column whose type allows it, and the others stay PLAIN.

A `Dict` chooses per column, keyed by the path used to reach the data: `encoding = Dict("x" => :byte_stream_split, "wf.values" => :plain, "particles.pt" => :byte_stream_split)`. A key naming a struct or list covers everything under it; in a `Dict`, an encoding that does not fit the column's type, or a key matching no column, is an error.

## Fixed-size lists

`FixedSizeListVector` columns, at top level or nested in structs, lists and maps, keep their fixed size through `ARROW:schema` metadata, for this reader and for pyarrow.

The first write of each new table schema containing such a column takes 5–20 s (one-time compilation of the Arrow schema step; later writes of the same schema in the same session are fast, and tables without a fixed-size list are unaffected).

## What reads the output

Every shape the writer produces reads back with `read_parquet`.

Output is readable by pyarrow, with one exception: pyarrow cannot read a file in which a fixed-size list is null, or sits inside a null struct (it fails with "Expected all lists to be of size=N", also on such files from other writers); `read_parquet` reads them.
