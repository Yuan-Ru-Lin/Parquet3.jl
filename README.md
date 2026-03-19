# Parquet3.jl

An alternative Parquet implementation in Julia, focused on nested data — a suitable representation for data typical of physics experiments, like waveform data and observables of multiple physics objects in an event. Currently read-only, returning `Arrow.Table` (Tables.jl-compatible) with memory-mapped IO and per-RowGroup parallelism.

## Demo

Create a Parquet file with nested columns using pyarrow:

```python
import pyarrow as pa
import pyarrow.parquet as pq

fsl_arr = pa.FixedSizeListArray.from_arrays(
    pa.array([1,2,3, 4,5,6, 7,8,9, 10,11,12], type=pa.int32()), 3
)
list_arr = pa.array([[100, 200], [300], [400, 500, 600], [700, 800]], type=pa.list_(pa.int32()))

tbl = pa.table({'waveform': fsl_arr, 'hits': list_arr})
pq.write_table(tbl, 'nested.parquet')
```

Read it with Parquet3.jl:

```julia
julia> using Parquet3

julia> tbl = read_parquet("nested.parquet")

julia> tbl.hits      # List<Int32> — variable-length, returned as Arrow.List
4-element Arrow.List{...}:
 Int32[100, 200]
 Int32[300]
 Int32[400, 500, 600]
 Int32[700, 800]

julia> tbl.waveform  # FixedSizeList<Int32>[3] — zero-copy views into flat array
4-element Parquet3.FixedSizeListVector{3, Int32, Parquet3.FixedSizeView{3, Int32}}:
 Int32[1, 2, 3]
 Int32[4, 5, 6]
 Int32[7, 8, 9]
 Int32[10, 11, 12]

julia> tbl.waveform[1]    # returns a lightweight view, not a copy
3-element Parquet3.FixedSizeView{3, Int32}:
 1
 2
 3
```

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

