# Parquet3.jl

An alternative Parquet implementation in Julia, focused on nested data. Currently read-only, returning `Arrow.Table` (Tables.jl-compatible) with memory-mapped IO and per-RowGroup parallelism.

## Usage

```julia
using Parquet3

tbl = read_parquet("data.parquet")
tbl.column_name          # access a column
tbl = read_parquet("data.parquet"; columns=["id", "name"])  # read specific columns
```

`read_parquet` returns an `Arrow.Table`, which implements the Tables.jl interface:

```julia
using DataFrames
df = DataFrame(tbl)

using TypedTables
t = Table(tbl)
```

### Inspection

```julia
pf = open_parquet("data.parquet")
num_rows(pf)
num_row_groups(pf)
column_names(pf)
schema_string(pf)
close(pf)
```

## Supported Features

### Encodings

Plain, RLE/Bit-Packed, Dictionary (Plain Dictionary + RLE Dictionary), Delta Binary Packed, Delta Length Byte Array, Byte Stream Split.

### Compression

| Codec | Implementation |
|-------|---------------|
| Snappy | Snappy.jl (libsnappy) |
| Gzip | CodecZlib.jl |
| Zstd | CodecZstd.jl |
| LZ4 (raw) | CodecLz4.jl (liblz4) |
| LZ4 (Hadoop) | Custom framing + liblz4 for block decompression |

### Nested Types

- `List<T>` — returned as `Arrow.List` (Tables.jl-compatible, iterable as nested arrays)
- `List<List<T>>` and deeper — arbitrary nesting depth supported via nested `Arrow.List`
- `FixedSizeList<T>` — returned as `FixedSizeListVector{N,T}` (flat `Vector{T}` with fixed stride, zero-copy `FixedSizeView{N,T}` element access); requires `ARROW:schema` metadata written by Arrow-based tools (pyarrow, Arrow C++, etc.)

### Logical Types

ConvertedType annotations are respected: UTF8, Date, Timestamp (millis/micros), Int8/16/32/64, UInt8/16/32/64.

## Developer Notes

See [dev-note.md](dev-note.md) for architecture, design decisions, and known limitations.

