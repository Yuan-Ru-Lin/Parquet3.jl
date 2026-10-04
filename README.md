# Parquet3.jl

An alternative Parquet implementation in Julia, focused on nested data — a suitable representation for data typical of physics experiments, like waveform data and observables of multiple physics objects in an event. Reading returns `Arrow.Table` (Tables.jl-compatible) with memory-mapped IO and per-RowGroup parallelism; writing of flat, list, and struct columns (nested to any depth) is supported.

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

# Write any Tables.jl-compatible table (flat, list, and struct columns; see Supported Features)
write_parquet("out.parquet", (id = Int32[1, 2], name = ["a", missing], hits = [[1.5, 2.5], Float64[]],
                             vertex = [(x = 0.1, y = 0.2), (x = 0.3, y = 0.4)]);
              compression = :zstd)   # default :snappy
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

### Writing

`write_parquet(path, table)` writes flat columns of Int8–Int64, UInt8–UInt64, Float32/Float64, Bool, String, `Date`, `DateTime`, `Arrow.Timestamp` and `Vector{UInt8}`, lists (vector elements, written as `List<T>`) and structs (`NamedTuple` elements, written as a group) of supported types nested to any depth — `struct{list}`, struct-of-struct, `list<struct>`, `list<list>`, … — and `Missing` unions at every level (PLAIN encoding, single row group, null-count statistics). `DateTime` is written as a naive millisecond timestamp and `Arrow.Timestamp{unit, tz}` with its unit and UTC flag, so timestamp columns from `read_parquet` write back unchanged (a named time zone becomes UTC, since Parquet only stores a UTC flag). Pages are compressed with Snappy by default; pass `compression = :gzip`, `:zstd`, `:lz4`, or `:uncompressed` to change it. Output is readable by pyarrow. Multiple row groups are not yet written. `FixedSizeListVector` columns, at top level or as struct members, keep their fixed size through `ARROW:schema` metadata, for this reader and for pyarrow. The first write of each new table schema containing such a column takes 5–20 s (one-time compilation of the Arrow schema step; later writes of the same schema in the same session are fast, and tables without a fixed-size list are unaffected). Shapes the reader does not assemble yet (e.g. `list<struct{list}>`) are written correctly but read back as flattened columns.

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
- `Struct` — returned as `StructColumn` (columnar wrapper over `Arrow.Struct`): `col[i]` gives a lazily-built `NamedTuple` row, `col.fieldname` gives the full child column zero-copy (chained across row groups). Members may be primitives, strings, lists (e.g. `waveform: {t0: float, dt: float, values: list<int32>}`), or nested structs — named access composes (`tbl.event.vertex.x`).
- `List<Struct>` — returned as `ListOfStructsColumn`: `col[i]` gives a lazy vector of `NamedTuple`s, `col.fieldname` gives that field as a ragged list column sharing the parent's offsets (e.g. `particles.pt`). Deeper combinations (`List<Struct{List}>`, maps) are not yet assembled and fall back to distinct flattened columns.
- `FixedSizeList<T>` — returned as `FixedSizeListVector{N,T}` (flat `Vector{T}` with fixed stride, zero-copy `FixedSizeView{N,T}` element access); requires `ARROW:schema` metadata written by Arrow-based tools (pyarrow, Arrow C++, etc.) Also restored as a struct member (e.g. `waveform: {t0, dt, values: fixed_size_list<int32>[1400]}`).

### Logical Types

ConvertedType annotations are respected: UTF8, Date, Int8/16/32/64, UInt8/16/32/64.

Timestamps are read from `logicalType` (falling back to the converted type) and returned in the most convenient type that loses nothing:

| Parquet timestamp | Returned as |
|---|---|
| milliseconds, not UTC-adjusted (naive) | `DateTime` |
| milliseconds, UTC-adjusted | `Arrow.Timestamp{MILLISECOND, :UTC}` |
| microseconds or nanoseconds | `Arrow.Timestamp{unit, tz}` with `tz` `:UTC` or `nothing` |

`Arrow.Timestamp` wraps the stored `Int64` (`ts.x`), so microsecond and nanosecond values are exact. A file with only the older converted type (TIMESTAMP_MILLIS / TIMESTAMP_MICROS) is UTC-adjusted by definition and reads as `Arrow.Timestamp{…, :UTC}`. The same rule applies inside lists, structs and lists of structs.

## Developer Notes

See [dev-note.md](dev-note.md) for architecture, design decisions, and known limitations.

