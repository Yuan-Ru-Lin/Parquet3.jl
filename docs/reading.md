# Reading

How `read_parquet` selects columns, what it returns, and how the result can be passed on. Limits are collected in [limitations.md](limitations.md); writing is in [writing.md](writing.md).

## Selecting columns

`columns` takes the dotted paths you would use to reach the data: `"id"` for a column, `"wf.values"` for a struct member, `"particles.pt"` for a member of a list of structs, `"m.key"` for a map's keys. A name selects everything under it.

Selecting a member returns its column with only the selected parts (`wf` as a struct with only `t0`), and the other members are never decoded. A name that matches nothing is an `ArgumentError`.

(pyarrow's `ParquetFile.read(columns=…)` prunes the same way; its `read_table` returns a selected struct member as a top-level column instead.)

## Element types follow the data

A column's element type admits `Missing` exactly where a null occurs in the data that was read; it does not depend on the schema's "optional" flags or on the writer's statistics. A member of a struct counts as missing wherever its struct is.

## Errors

A column that cannot be read (an encoding or type not supported yet) throws a `Parquet3.ColumnReadError` naming it; nothing is skipped silently. Pass `columns=` without that column, or without that member, to read the rest.

## Returned types

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

### Named access

Named access composes through structs and lists: `tbl.event.vertex.x`, `tbl.s.hits.x`, `tbl.tracks.vertex.x` (one value per track, per row), `tbl.mm.value.key` (the keys of nested maps). A map nested in a struct, a list or another map presents the same way (`tbl.mm[i]["a"]["x"]`).

When a key occurs twice in a row, lookup and `Dict(...)` take the last entry, as the Parquet format specifies; iteration shows both.

Multi-row-group files chain the per-group chunks without copying.

### Fixed-size lists

`FixedSizeList` needs the `ARROW:schema` metadata that Arrow-based tools (pyarrow, Arrow C++, this package's writer) store; it is restored wherever it is declared with a fixed-width element (numbers, `Bool`, dates, timestamps; a fixed-size list of strings or bytes reads as an ordinary list): at top level, as a struct member (e.g. `waveform: {t0, dt, values: fixed_size_list<int32>[1400]}`), inside lists (`list<fixed_size_list>`, `list<struct<…>>`) and as a map value.

The null bits are a type parameter of the two types (`Nothing` when the column has no null element), so a column has them only if a null element occurs in it and waveform columns without one pay nothing; both forms keep their fixed size through `write_parquet` and `Arrow.write`. `FixedSizeView{N, T}` names the views of a column without null elements, and `FixedSizeView{N, Union{Missing, T}}` those of a column with them, whatever that parameter is.

Of a fixed-size list of fixed-size lists only the inner level is restored; the outer one reads as a variable-length list.

### Other list and map layouts

Legacy list layouts (2-level lists, bare repeated fields) and maps without values are read as pyarrow reads them.

## Encodings read

Read: Plain, RLE/Bit-Packed (levels, dictionary indices, booleans), Dictionary (Plain Dictionary + RLE Dictionary), Delta Binary Packed, Delta Length Byte Array, Byte Stream Split (float, double, int32, int64). Not read yet: Delta Byte Array, and Byte Stream Split for fixed-length byte arrays.

## Logical types

ConvertedType annotations are respected: UTF8, Date, Int8/16/32/64, UInt8/16/32/64.

### Timestamps

Timestamps are read from `logicalType` (falling back to the converted type) and returned in the most convenient type that loses nothing:

| Parquet timestamp | Returned as |
|---|---|
| milliseconds, not UTC-adjusted (naive) | `DateTime` |
| milliseconds, UTC-adjusted | `Arrow.Timestamp{MILLISECOND, :UTC}` |
| microseconds or nanoseconds | `Arrow.Timestamp{unit, tz}` with `tz` `:UTC` or `nothing` |

`Arrow.Timestamp` wraps the stored `Int64` (`ts.x`), so microsecond and nanosecond values are exact. A file with only the older converted type (TIMESTAMP_MILLIS / TIMESTAMP_MICROS) is UTC-adjusted by definition and reads as `Arrow.Timestamp{…, :UTC}`. The same rule applies inside lists, structs and lists of structs.

### Types returned as stored

Not converted, returned as stored (and written back with their values but without the annotation: decimals, Float16 and fixed-length binary as plain binary, times and durations as plain integers; INT96 cannot be written): decimals (the unscaled integer; as pyarrow writes them, a `Vector{UInt8}` holding it big-endian), Float16 (two bytes), other fixed-length byte arrays, durations and times (`Int32`/`Int64` in the file's unit), and INT96 timestamps.

## Tables.jl and Arrow

The result can be written as Arrow IPC with `Arrow.write(path, tbl)`: every column kind is handed to Arrow.jl over the buffers that were read, without re-encoding, and a multi-row-group file becomes one record batch per row group. Binary columns have Arrow.jl's binary element type (`Base.CodeUnits`, an `AbstractVector{UInt8}`).
