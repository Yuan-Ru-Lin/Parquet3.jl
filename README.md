# Parquet3.jl

A pure Julia Parquet file reader. Returns `Arrow.Table` (Tables.jl-compatible) with memory-mapped IO and per-RowGroup parallelism.

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

## Architecture

- **Memory-mapped IO**: Files are read via `Mmap.mmap`, producing a shared read-only `Vector{UInt8}`. No `IOStream` seek/read — safe for concurrent access.
- **Per-RowGroup parallelism**: Each column spawns tasks (`Threads.@spawn`) that process row groups in parallel. Results are composed via `ChainedVector` (zero-copy, no concatenation).
- **Arrow-native arrays**: Decoded Parquet pages are assembled directly into `Arrow.Primitive`, `Arrow.BoolVector`, and `Arrow.List` — matching Parquet's Dremel encoding to Arrow's offset-based layout in a single pass.

## Known Limitations

- Read-only. No write support.
- Without `ARROW:schema` metadata, `FixedSizeList` columns are read as regular variable-length lists since Parquet's schema does not encode the list size.
- `FixedSizeListVector` is not an `Arrow.ArrowVector` subtype. It registers `ArrowKind = FixedSizeListKind{N,T}` so `Arrow.write` can serialize it correctly, but:
  - Nested FixedSizeList (e.g., `FixedSizeList<FixedSizeList<T>>`) is not supported — only top-level FSL fields are detected from ARROW:schema.
  - Composition with other Arrow types (e.g., `List<FixedSizeList<T>>`) falls back to variable-length lists at all levels.
  - Reading back via `Arrow.read` returns Arrow.jl's native `FixedSizeList` (NTuple-based), not `FixedSizeListVector`.
- LZ4 Hadoop framing (used by older Spark/Hadoop writers) is implemented but not tested end-to-end — only the standard LZ4 raw/frame format is covered by the test suite.
- `open_parquet` / `read_parquet` on a non-existent path gives "File too small" instead of "File not found" (Mmap.mmap silently creates an empty file). Needs a guard in the public API.
