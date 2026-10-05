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

# Write any Tables.jl-compatible table (flat, list, and struct columns; see Supported Features)
write_parquet("out.parquet", (id = Int32[1, 2], name = ["a", missing], hits = [[1.5, 2.5], Float64[]],
                             vertex = [(x = 0.1, y = 0.2), (x = 0.3, y = 0.4)]);
              compression = :zstd)   # default :snappy
```

`columns` takes the dotted paths you would use to reach the data: `"id"` for a column, `"wf.values"` for a struct member, `"particles.pt"` for a member of a list of structs, `"m.key"` for a map's keys. A name selects everything under it. Selecting a member returns its column with only the selected parts (`wf` as a struct with only `t0`), and the other members are never decoded. A name that matches nothing is an `ArgumentError`. (pyarrow's `ParquetFile.read(columns=…)` prunes the same way; its `read_table` returns a selected struct member as a top-level column instead.)

A column's element type admits `Missing` exactly where a null occurs in the data that was read; it does not depend on the schema's "optional" flags or on the writer's statistics. A member of a struct counts as missing wherever its struct is.

A column that cannot be read (an encoding or type not supported yet) throws a `Parquet3.ColumnReadError` naming it; nothing is skipped silently. Pass `columns=` without that column, or without that member, to read the rest.

The result can be written as Arrow IPC with `Arrow.write(path, tbl)`: every column kind is handed to Arrow.jl over the buffers that were read, without re-encoding, and a multi-row-group file becomes one record batch per row group. Binary columns have Arrow.jl's binary element type (`Base.CodeUnits`, an `AbstractVector{UInt8}`).

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

`write_parquet(path, table)` writes flat columns of Int8–Int64, UInt8–UInt64, Float32/Float64, Bool, String, `Date`, `DateTime`, `Arrow.Timestamp` and `Vector{UInt8}`, lists (vector elements, written as `List<T>`), structs (`NamedTuple` elements, written as a group) and maps (`AbstractDict` elements, written as a Parquet MAP) of supported types nested to any depth — `struct{list}`, struct-of-struct, `list<struct>`, `list<list>`, … — and `Missing` unions at every level (single row group, null-count statistics). `DateTime` is written as a naive millisecond timestamp and `Arrow.Timestamp{unit, tz}` with its unit and UTC flag, so timestamp columns from `read_parquet` write back unchanged (a named time zone becomes UTC, since Parquet only stores a UTC flag). Pages are compressed with Snappy by default; pass `compression = :gzip`, `:brotli`, `:zstd`, `:lz4`, or `:uncompressed` to change it. Output is readable by pyarrow, with one exception: pyarrow cannot read a file in which a fixed-size list is null, or sits inside a null struct (it fails with "Expected all lists to be of size=N", also on such files from other writers); `read_parquet` reads them. Multiple row groups are not yet written.

Values are PLAIN-encoded by default. `encoding` accepts:

| `encoding =` | Applies to |
|---|---|
| `:plain` | everything |
| `:byte_stream_split` | Float32, Float64 |
| `:delta_binary_packed` | every type stored as an integer: Int8–Int64, UInt8–UInt64, Date, DateTime, Arrow.Timestamp |
| `:delta_length_byte_array` | String, `Vector{UInt8}` |

Dictionary-encoded files are read, but dictionary encoding is not yet written (planned for v0.3).

A single name applies to the whole table: it is used for every column whose type allows it, and the others stay PLAIN. A `Dict` chooses per column, keyed by the path used to reach the data: `encoding = Dict("x" => :byte_stream_split, "wf.values" => :plain, "particles.pt" => :byte_stream_split)`. A key naming a struct or list covers everything under it; in a `Dict`, an encoding that does not fit the column's type, or a key matching no column, is an error. `FixedSizeListVector` columns, at top level or nested in structs, lists and maps, keep their fixed size through `ARROW:schema` metadata, for this reader and for pyarrow. The first write of each new table schema containing such a column takes 5–20 s (one-time compilation of the Arrow schema step; later writes of the same schema in the same session are fast, and tables without a fixed-size list are unaffected). Every shape the writer produces reads back with `read_parquet`.

### Encodings

Read: Plain, RLE/Bit-Packed (levels, dictionary indices, booleans), Dictionary (Plain Dictionary + RLE Dictionary), Delta Binary Packed, Delta Length Byte Array, Byte Stream Split (float, double, int32, int64). Not read yet: Delta Byte Array, and Byte Stream Split for fixed-length byte arrays.

### Compression

All codecs go through [ChunkCodecs.jl](https://github.com/JuliaIO/ChunkCodecs.jl).

| Codec | Read | Write (`compression =`) | Implementation |
|-------|------|-------------------------|----------------|
| Snappy | yes | `:snappy` (default) | ChunkCodecLibSnappy |
| Gzip | yes | `:gzip` | ChunkCodecLibZlib |
| Brotli | yes | `:brotli` | ChunkCodecLibBrotli |
| Zstd | yes | `:zstd` | ChunkCodecLibZstd |
| LZ4 (raw) | yes | `:lz4` | ChunkCodecLibLz4 |
| LZ4 (deprecated codec id) | not working yet (the parquet-testing files fail; see dev-note Known Limitations) | no | Custom Hadoop framing around ChunkCodecLibLz4 blocks |

### Nested Types

Any nesting of lists, structs and maps is read, to any depth, by one recursive reader that mirrors the writer.

| Parquet | Returned as | Access |
|---|---|---|
| `List<T>`, `List<List<T>>`, … | `Arrow.List` | `col[i]` is a zero-copy view of the row's items |
| struct | `StructColumn` | `col[i]` is a `NamedTuple`; `col.field` is the whole member column, zero-copy |
| `List<Struct>`, at any list depth, with any members | `ListOfStructsColumn` | `col[i]` is the row's structs; `col.field` is that member for every row as a ragged list sharing the offsets (`particles.pt`) |
| map | `MapColumn` | `col[i]` is a `MapView`, a zero-copy dictionary view of the row (`col[i]["k"]`, iteration in file order, `Dict(col[i])` for a hashed copy); `col.key` and `col.value` are all keys and all values, per row |
| `FixedSizeList<T>` | `FixedSizeListVector{N,T}` | `col[i]` is a zero-copy `FixedSizeView{N,T}` into one flat vector |
| `FixedSizeList<T>` with a null element somewhere in the column | the same `FixedSizeListVector`, with one null bit per element beside the flat vector | `col[i]` is a zero-copy `FixedSizeView{N, Union{Missing,T}}` and `col[i][j]` is `missing` for a null element |
| `List<FixedSizeList<T>>`, at any list depth | `ListColumn` | `col[i]` is a zero-copy view of the row's items, each a `FixedSizeView{N,T}` |

Named access composes through structs and lists: `tbl.event.vertex.x`, `tbl.s.hits.x`, `tbl.tracks.vertex.x` (one value per track, per row), `tbl.mm.value.key` (the keys of nested maps). A map nested in a struct, a list or another map presents the same way (`tbl.mm[i]["a"]["x"]`). When a key occurs twice in a row, lookup and `Dict(...)` take the last entry, as the Parquet format specifies; iteration shows both. Multi-row-group files chain the per-group chunks without copying.

`FixedSizeList` needs the `ARROW:schema` metadata that Arrow-based tools (pyarrow, Arrow C++, this package's writer) store; it is restored wherever it is declared with a fixed-width element (numbers, `Bool`, dates, timestamps; a fixed-size list of strings or bytes reads as an ordinary list): at top level, as a struct member (e.g. `waveform: {t0, dt, values: fixed_size_list<int32>[1400]}`), inside lists (`list<fixed_size_list>`, `list<struct<…>>`) and as a map value. The null bits are a type parameter of the two types (`Nothing` when the column has no null element), so a column has them only if a null element occurs in it and waveform columns without one pay nothing; both forms keep their fixed size through `write_parquet` and `Arrow.write`. `FixedSizeView{N, T}` names the views of a column whatever that parameter is. Of a fixed-size list of fixed-size lists only the inner level is restored; the outer one reads as a variable-length list. Legacy list layouts (2-level lists, bare repeated fields) and maps without values are read as pyarrow reads them.

### Logical Types

ConvertedType annotations are respected: UTF8, Date, Int8/16/32/64, UInt8/16/32/64.

Timestamps are read from `logicalType` (falling back to the converted type) and returned in the most convenient type that loses nothing:

| Parquet timestamp | Returned as |
|---|---|
| milliseconds, not UTC-adjusted (naive) | `DateTime` |
| milliseconds, UTC-adjusted | `Arrow.Timestamp{MILLISECOND, :UTC}` |
| microseconds or nanoseconds | `Arrow.Timestamp{unit, tz}` with `tz` `:UTC` or `nothing` |

`Arrow.Timestamp` wraps the stored `Int64` (`ts.x`), so microsecond and nanosecond values are exact. A file with only the older converted type (TIMESTAMP_MILLIS / TIMESTAMP_MICROS) is UTC-adjusted by definition and reads as `Arrow.Timestamp{…, :UTC}`. The same rule applies inside lists, structs and lists of structs.

Not converted, returned as stored: decimals (the unscaled integer; as pyarrow writes them, a `Vector{UInt8}` holding it big-endian), Float16 (two bytes), other fixed-length byte arrays, durations and times (`Int32`/`Int64` in the file's unit), and INT96 timestamps.

## Developer Notes

See [dev-note.md](dev-note.md) for architecture, design decisions, and known limitations.

