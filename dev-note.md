# Dev Notes

## Architecture

- **Memory-mapped IO**: Files are read via `Mmap.mmap`, producing a shared read-only `Vector{UInt8}`. No `IOStream` seek/read — safe for concurrent access.
- **Per-RowGroup parallelism**: Each column spawns tasks (`Threads.@spawn`) that process row groups in parallel. Results are composed via `ChainedVector` (zero-copy, no concatenation).
- **Arrow-native arrays**: Decoded Parquet pages are assembled directly into `Arrow.Primitive`, `Arrow.BoolVector`, and `Arrow.List` — matching Parquet's Dremel encoding to Arrow's offset-based layout in a single pass.

## FixedSizeList Design

`Arrow.FixedSizeList` uses `NTuple{N, T}` as its element type, so every `col[i]` access copies all `N` elements to create an `N`-tuple. This is unacceptable for waveform data where `N` could be of O(10^3–10^4).

This package uses two structs to circumvent the issue:

- **`FixedSizeListVector{N, T, ET}`** — the column container. Stores data in a flat `Vector{T}` with fixed stride `N`, plus a `BitVector` for record-level nulls.
- **`FixedSizeView{N, T}`** — a lightweight zero-copy view returned on index access. Carries only a pointer to the parent array and an offset (16 bytes), regardless of `N`.

`FixedSizeListVector` is not an `Arrow.ArrowVector` subtype. It registers `ArrowKind = FixedSizeListKind{N,T}` so `Arrow.write` can serialize it correctly, but:

- Nested FixedSizeList (e.g., `FixedSizeList<FixedSizeList<T>>`) is not supported — only top-level FSL fields are detected from ARROW:schema.
- Composition with other Arrow types (e.g., `List<FixedSizeList<T>>`) falls back to variable-length lists at all levels.
- If you write the table to an Arrow IPC file with `Arrow.write` and read it back with `Arrow.read`, FixedSizeList columns will come back as Arrow.jl's native `NTuple`-based `FixedSizeList`, not as `FixedSizeListVector`. The data is preserved, but the zero-copy view behavior is lost.

## Struct Design

Parquet has no physical struct storage — a group is pure schema nesting over independently
stored leaf columns, so struct support is an assembly feature. Each member leaf is decoded
with the existing page machinery, then wrapped in `Arrow.Struct` (a tuple of child columns
plus a validity bitmap; `NamedTuple` elements are materialized lazily on `getindex`).

`Arrow.Struct` stores fields positionally with no name-based access, so — following the
`FixedSizeListVector` precedent — the public container is our own `StructColumn` wrapper:
`col[i]` delegates row access to the wrapped `Arrow.Struct` (or `ChainedVector` of per-RG
chunks), and `getproperty` maps `col.fieldname` to the full child column, chaining chunks
per field for multi-RowGroup files. `Arrow.write` serializes it as a native struct column
(the `NamedTuple` eltype drives `ArrowTypes.StructKind` inference), verified by round-trip.

Null attribution comes from raw definition levels. For
`optional wf { optional t0; optional values (LIST) { repeated list { optional element }}}`
(max_def = 4 on the `values` leaf): def 0 = struct null, 1 = list null, 2 = empty list,
3 = element null, 4 = value. Struct validity is derived from a flat member's def levels
(`def <` the group's own def level), or from record starts (rep == 0) of a list member when
the struct has no flat fields. List members reuse `_to_arrow_nested` with a
`record_null_def` threshold: below it the record is a null list (for top-level list columns
the threshold is 1, preserving the old `def == 0` behavior).

Element-type stability across row-group chunks (required for `ChainedVector`) is decided
before reading data: a struct can only be null where *every* member is null, so if any flat
member's column-chunk statistics report zero nulls, the struct eltype excludes `Missing`.

Nested structs assemble recursively via a per-column plan (`_plan_struct`); one leftmost
leaf's def levels encode the nullness of every ancestor group, so each nesting level slices
its own validity from the same `record_defs` vector by comparing against its own def level.

`List<Struct>` inverts the decomposition: every member leaf carries an identical copy of the
list structure in its rep/def levels (a Dremel invariant), so offsets and list/element
validity are built once from the first member, remaining members contribute child arrays at
element granularity, and the result is `Arrow.List` over `Arrow.Struct`, wrapped in
`ListOfStructsColumn` (named field access returns a ragged per-field list sharing offsets).

Unsupported shapes (`List<Struct{List}>`, maps) fall back to flattened dotted columns; a
repeated leaf claims the bare top-level column name only when it is the sole leaf under that
top, so multi-leaf fallbacks can no longer silently collide on one name.

## Known Limitations

- Read-only. No write support.
- Without `ARROW:schema` metadata, `FixedSizeList` columns are read as regular variable-length lists since Parquet's schema does not encode the list size.
- LZ4 Hadoop framing (used by older Spark/Hadoop writers) is implemented but not tested end-to-end — only the standard LZ4 raw/frame format is covered by the test suite.
- `open_parquet` / `read_parquet` on a non-existent path gives "File too small" instead of "File not found" (Mmap.mmap silently creates an empty file). Needs a guard in the public API.
