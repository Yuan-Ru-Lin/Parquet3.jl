# Writing

What `write_parquet` accepts and what it writes. Limits are collected in [limitations.md](limitations.md); reading is in [reading.md](reading.md).

## Column types and nesting

`write_parquet(path, table)` writes flat columns of Int8–Int64, UInt8–UInt64, Float32/Float64, Bool, String, `Date`, `DateTime`, `Arrow.Timestamp` and `Vector{UInt8}`, lists (vector elements, written as `List<T>`), structs (`NamedTuple` elements, written as a group) and maps (`AbstractDict` elements, written as a Parquet MAP) of supported types nested to any depth — `struct{list}`, struct-of-struct, `list<struct>`, `list<list>`, … — and `Missing` unions at every level (null-count statistics).

`DateTime` is written as a naive millisecond timestamp and `Arrow.Timestamp{unit, tz}` with its unit and UTC flag, so timestamp columns from `read_parquet` write back unchanged (a named time zone becomes UTC, since Parquet only stores a UTC flag).

## Row groups
By default the table is one row group. `rowgroup_size = n` writes row groups of `n` rows (the last one shorter): each is shredded, encoded and written before the next, so the writer holds a few times one row group rather than the whole table, and `read_parquet` decodes the groups in parallel and presents them as one table. Choose `n` by the memory you can spend: with 26 M rows of five numeric columns, `rowgroup_size = 2_000_000` keeps the writer under a gigabyte where one row group needed four.

If writing fails partway (a value that cannot be written is found while the columns are processed), no partial file is left at the path. A file that was there before the call is then gone too, since writing starts by emptying it. When the path is a symbolic link, the file it points to is the one removed, and the link stays.

## Writing in batches
`open_parquet_writer` writes a file one row group at a time, for a producer that makes its table in batches and should not hold all of it:

```julia
open_parquet_writer("hits.parquet"; compression = :zstd) do w
    for batch in batches            # any Tables.jl table
        write_rowgroup!(w, batch)   # one row group, written before the next batch is made
    end
end
```

The first batch fixes the columns (names, order, types); an empty one only does that. Later batches must have the same columns; `Missing` may come and go. The footer is written when the writer is closed (`close(w)`, or the end of the `do` block), and until then the file is not a Parquet file. With the `do` form a block that throws leaves no file. A batch that cannot be written is an error that leaves the file as it was, and the writer can go on.

Several processes can write one file. All the work of a row group (shredding, encoding, compression) is in `encode_rowgroup(batch; compression, encoding)`, which returns the row group's bytes and metadata as plain data; `write_rowgroup!(w, encoded)` appends it by copying the bytes. So workers encode and one process appends, for example with Distributed.jl:

```julia
using Distributed
@everywhere using Parquet3
open_parquet_writer("hits.parquet") do w
    for encoded in pmap(i -> encode_rowgroup(simulate(i); compression = :zstd), 1:100)
        write_rowgroup!(w, encoded)
    end
end
```

(`pmap` collects before the loop runs; feed a `RemoteChannel` from the workers to append as results arrive.) Each row group keeps the compression and encodings it was encoded with.

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
| `:dictionary` | every type but `Bool` |

`:dictionary` writes, per row group, the column's distinct values once (a dictionary page) and an index per value, bit-packed at the width the number of distinct values needs. It pays for columns with few distinct values (detector or channel numbers, event numbers repeated over hits, categories, repeated strings). A column chunk whose distinct values would take more than 1 MiB is written PLAIN instead, as pyarrow does, so asking for it on a column of all-different values costs one scan and nothing in the file; smaller row groups keep more chunks under the limit. Dictionary encoding is never chosen for you: the default stays PLAIN.

A single name applies to the whole table: it is used for every column whose type allows it, and the others stay PLAIN.

A `Dict` chooses per column, keyed by the path used to reach the data: `encoding = Dict("x" => :byte_stream_split, "wf.values" => :plain, "particles.pt" => :byte_stream_split)`. A key naming a struct or list covers everything under it; in a `Dict`, an encoding that does not fit the column's type, or a key matching no column, is an error.

## Fixed-size lists

`FixedSizeListVector` columns, at top level or nested in structs, lists and maps, keep their fixed size through `ARROW:schema` metadata, for this reader and for pyarrow. A column of fixed-length vectors is written as one: an ArraysOfArrays `VectorOfSimilarVectors` (a `nestedview` of an `N × n` matrix, e.g. x, y, z rows of positions) over its buffer without a copy, or a `Vector` of isbits fixed-length vectors such as StaticArrays' `SVector{N, T}`; a `FixedSizeListVector` read back from a file is written as it is.

The first write of each new table schema containing such a column takes 5–20 s (one-time compilation of the Arrow schema step; later writes of the same schema in the same session are fast, and tables without a fixed-size list are unaffected).

## What reads the output

Every shape the writer produces reads back with `read_parquet`.

Output is readable by pyarrow, with one exception: pyarrow cannot read a file in which a fixed-size list is null, or sits inside a null struct (it fails with "Expected all lists to be of size=N", also on such files from other writers); `read_parquet` reads them.
