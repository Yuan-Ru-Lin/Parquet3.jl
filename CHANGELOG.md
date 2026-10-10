# Changelog

## Unreleased

### Writing

- `write_parquet(path, tbl; rowgroup_size = n)` writes the table as row groups of `n` rows, each shredded, encoded and written before the next, so the writer's working set is a few times one row group instead of the whole table. The default is still one row group.
- `FixedSizeListVector(N, data)` builds a fixed-size-list column from a flat buffer for writing; the type is exported.

### Performance

Measured on a 26.4 M-row table (two Int64, two Float32 and one `FixedSizeList<Float32>[3]` column, zstd, Julia 1.12, 4 threads); the files written are byte-identical to before.

- Reading a flat column no longer boxes every value. `assemble_flat_column` copied optional columns (every column `write_parquet` writes) element by element through an abstractly typed page vector, about four allocations per value. The per-page copy is now a typed function barrier, and a page whose definition levels are all at the maximum is copied in bulk. One column: 4.7 s and 106 M allocations to 0.8 s and 1.5 k; all columns: 12.1 s and 397 M to about 3 s of work and 2 k.
- Definition and repetition levels are kept in the decoder's `UInt32` instead of being copied to `Int`, and a level stream that is a single RLE run of "present" (or "no repetition") over the whole page builds no buffer at all. The compressed bytes of a page are decompressed from a view, not a copy.
- Writing allocates per column, not per row: a flat column without `missing` and a `FixedSizeListVector` without nulls are appended in bulk, repetition levels are only kept under a list, level buffers are bytes, and a page is assembled once in a buffer of its exact size (PLAIN fixed-width values are copied by pointer) instead of through an `IOBuffer`, `take!` and `vcat`. Whole table: 25.7 s and 52.9 M allocations (18.5 GiB) to 7.8 s and 2 k (3.6 GiB).
- A column chunk larger than 2 GiB now fails with a message naming the column instead of an `InexactError` (one data page per column is still written).

## v0.2.0

Parquet3 now reads every nested shape Parquet can hold and writes Parquet files. pyarrow is the reference throughout: the tests compare what we read with what pyarrow reads, and have pyarrow read what we write.

### Reading

- Every nested shape: structs, lists, lists of structs, structs with list members, lists of lists, at any depth and in any combination. A struct column is one column (`tbl.event.vertex.x`), not several dotted ones; a list of structs gives each member as a ragged list (`tbl.particles.pt`).
- Maps: a map column's rows are `MapView`s, zero-copy dictionary views (`col[i]["key"]`).
- Fixed-size lists (waveforms) are restored wherever the file declares them: at top level, in structs, in lists and as map values. A null element inside one reads as `missing`.
- `columns=` selects by the path you would use to reach the data: `"id"`, `"wf.values"`, `"particles.pt"`, `"m.key"`.
- Element types say exactly where nulls occur: a column, member or element admits `Missing` only if the data has a null there.
- Timestamps keep their unit and UTC flag. Naive millisecond timestamps are `DateTime`; everything else is an `Arrow.Timestamp`, exact to the nanosecond.
- More encodings and layouts: DELTA_BINARY_PACKED, DELTA_LENGTH_BYTE_ARRAY, BYTE_STREAM_SPLIT (float, double, int32, int64), RLE booleans, data page v2 edge cases, legacy list layouts. 55 of the 64 Apache parquet-testing files read in full.
- Compression: Snappy, Gzip, Brotli, Zstd and LZ4 (the raw codec; the deprecated LZ4 codec does not work yet).

### Writing (new)

- `write_parquet(path, table)` writes any Tables.jl table: integers of 8 to 64 bits, signed and unsigned, floats, `Bool`, `String`, bytes, `Date`, `DateTime`, `Arrow.Timestamp`, and lists, structs (`NamedTuple`s) and maps (`AbstractDict`s) of those, nested to any depth, with `Missing` at any level.
- What `read_parquet` returns can be written back with the same values and, for the types the reader converts, the same types, including fixed-size lists, which keep their size for this package and for pyarrow. The exception is the types returned as stored (see Known limitations): their values are written back, their annotation is not.
- `compression = :snappy` (default), `:gzip`, `:brotli`, `:zstd`, `:lz4`, `:uncompressed`.
- `encoding = :plain` (default), `:byte_stream_split`, `:delta_binary_packed`, `:delta_length_byte_array`, for the whole table or per column.
- Null counts are written as pyarrow writes them.

### Arrow.write

- `Arrow.write` takes every column `read_parquet` returns and reuses its buffers. A file with several row groups becomes several Arrow record batches.

### Breaking changes

- **Nested columns.** Shapes that used to read as several flattened, dotted columns are now one nested column. Maps are a `MapColumn`.
- **`columns=`** takes short dotted paths (`"particles.pt"`). Parquet's own paths (`particles.list.element.pt`) and bare leaf names are no longer accepted, and a name that matches nothing is an `ArgumentError`.
- **A column that cannot be read is an error** (`ColumnReadError`, naming the column). It used to be skipped with a warning.
- **`Missing` only where a null occurs.** Element types no longer admit `Missing` just because the schema allows it, so the type of a column can differ between two files with the same schema.
- **An empty list that cannot be null reads as `[]`**, not `missing`.
- **Timestamps.** UTC-adjusted and sub-millisecond timestamps read as `Arrow.Timestamp`, not `DateTime`.
- **Binary columns** have element type `Base.CodeUnits{UInt8, String}` instead of `Vector{UInt8}`. Use `Vector(x)` where a `Vector{UInt8}` is required.
- **Fixed-size lists.**
  - A null element reads as `missing`; it used to read as 0.
  - `FixedSizeView` and `FixedSizeListVector` have more type parameters. `x isa FixedSizeView{N, T}` and `col isa FixedSizeListVector{N, T}` still work, but `FixedSizeView{N, T}` is no longer a concrete type, so `eltype(col) == FixedSizeView{N, T}` is now false; use `<:`.
  - A fixed-size list of strings or bytes reads as an ordinary list.
- **A missing file** is a `SystemError`, and nothing is created at the path.

### Known limitations

- pyarrow cannot read a file in which a fixed-size list is null (for example a null waveform) or sits inside a null struct; it fails with "Expected all lists to be of size=N". This is so for such files from any writer, pyarrow included. `write_parquet` writes them faithfully and `read_parquet` reads them.
- The writer writes one row group and one page per column, with no min/max statistics and no dictionary encoding.
- Not read yet: DELTA_BYTE_ARRAY, BYTE_STREAM_SPLIT for fixed-length byte arrays, the deprecated LZ4 codec, and more than 2 GB of strings in one column chunk.
- Decimals, Float16, durations, times and INT96 timestamps are returned as stored (raw bytes or integers), not converted. Written back, they keep their values but lose the annotation: decimals, Float16 and fixed-length binary become plain binary, times and durations plain integers. INT96 columns cannot be written.
- `Arrow.write` of rows collected out of a fixed-size list of `UInt8` (`collect(tbl.col)`) produces Arrow fixed-size binary; the column as read is written as a fixed-size list.
- Of a fixed-size list of fixed-size lists only the inner size is restored.
- The first write of a table that contains a fixed-size list takes 5–20 s (one-time compilation per table schema).
- `Arrow = "~2.8.1"`: the package uses Arrow.jl internals, so each Arrow minor release needs a check before the bound is raised.

The full list is in [docs/limitations.md](docs/limitations.md).

## v0.1.0

First release: a reader for flat columns, lists and fixed-size lists, returning an `Arrow.Table`.
