# Parquet3.jl

[![CI](https://github.com/Yuan-Ru-Lin/Parquet3.jl/actions/workflows/CI.yml/badge.svg)](https://github.com/Yuan-Ru-Lin/Parquet3.jl/actions/workflows/CI.yml)

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
4-element Parquet3.FixedSizeListVector{3, Int32, Parquet3.FixedSizeView{3, Int32, Int32, Nothing}, Nothing}:
 [1, 2, 3]
 [4, 5, 6]
 [7, 8, 9]
 [10, 11, 12]

julia> tbl.waveform[1]    # returns a lightweight view, not a copy
3-element Parquet3.FixedSizeView{3, Int32, Int32, Nothing}:
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
tbl = read_parquet("data.parquet"; columns=["wf.t0", "particles.pt"])  # only these members; the rest is not decoded

# Write any Tables.jl-compatible table (flat, list, struct and map columns; see docs/writing.md)
write_parquet("out.parquet", (id = Int32[1, 2], name = ["a", missing], hits = [[1.5, 2.5], Float64[]],
                             vertex = [(x = 0.1, y = 0.2), (x = 0.3, y = 0.4)]);
              compression = :zstd)   # default :snappy
```

`columns` takes the dotted paths you would use to reach the data; see [Selecting columns](docs/reading.md#selecting-columns). `write_parquet` also takes `encoding =`, for the whole table or per column; see [Encodings](docs/writing.md#encodings).

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

## What is supported

| | Summary | Details |
|---|---|---|
| Nested types | Any nesting of lists, structs and maps is read, to any depth; fixed-size lists (waveforms) come back as zero-copy views, under the rules in the details | [Returned types](docs/reading.md#returned-types) |
| Written column types | Int8–Int64, UInt8–UInt64, Float32/Float64, Bool, String, `Date`, `DateTime`, `Arrow.Timestamp`, `Vector{UInt8}`, and lists, structs and maps of these nested to any depth, with `Missing` at every level | [Column types and nesting](docs/writing.md#column-types-and-nesting) |
| Compression | Snappy, Gzip, Brotli, Zstd and LZ4 (raw), read and written | [Compression](docs/writing.md#compression) |
| Encodings read | Plain, RLE/Bit-Packed, Dictionary, Delta Binary Packed, Delta Length Byte Array, Byte Stream Split (float, double, int32, int64) | [Encodings read](docs/reading.md#encodings-read) |
| Encodings written | PLAIN (default), BYTE_STREAM_SPLIT, DELTA_BINARY_PACKED, DELTA_LENGTH_BYTE_ARRAY | [Encodings](docs/writing.md#encodings) |
| Logical types | UTF8, Date, Int8/16/32/64, UInt8/16/32/64, and timestamps with their unit and UTC flag | [Logical types](docs/reading.md#logical-types) |
| Arrow | The result can be written as Arrow IPC with `Arrow.write(path, tbl)` | [Tables.jl and Arrow](docs/reading.md#tablesjl-and-arrow) |

## Things to know

- A column's element type admits `Missing` exactly where a null occurs in the data that was read. See [Element types follow the data](docs/reading.md#element-types-follow-the-data).
- A column that cannot be read throws a `Parquet3.ColumnReadError` naming it; nothing is skipped silently. See [Errors](docs/reading.md#errors).
- pyarrow cannot read a file in which a fixed-size list is null, or sits inside a null struct; `read_parquet` reads them. See [pyarrow interop](docs/limitations.md#pyarrow-interop).
- Dictionary encoding and multiple row groups are not yet written. See [Known limitations](docs/limitations.md).

## Documentation

- [docs/reading.md](docs/reading.md): selecting columns, returned types, fixed-size lists, timestamps, Arrow.
- [docs/writing.md](docs/writing.md): column types, compression, encodings, what reads the output.
- [docs/limitations.md](docs/limitations.md): known limitations.
- [CHANGELOG.md](CHANGELOG.md): what changed in each version, including breaking changes.
- [dev-note.md](dev-note.md): architecture and design decisions.
