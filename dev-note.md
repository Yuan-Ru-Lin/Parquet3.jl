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
3 = element null, 4 = value. Struct validity is derived from the first member's def levels
(`def <` the group's own def level), taken at record starts (rep == 0) when that member is
a list. List members reuse `_to_arrow_nested` with a
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

## Nested List Assembly

`_to_arrow_nested` turns rep/def levels into nested `Arrow.List`s in one pass. An entry
with rep = r continues the level-r list, so new lists open at levels r+1..max_rep; each
opening pushes a start offset and a validity bit for that level, provided its parent item
exists (`def >=` the parent's repeated-node threshold). A level-k list is null when def is
below its own group's def level, i.e. `def < thresholds[k] - 1`. Null leaf elements take a
slot in the child array that is never read.

A column chunk with no pages (zero-row file or row group) is given one empty page of the
leaf's physical type (`_empty_pages`), so every column kind assembles to a typed empty column.

## Writer Design

Writing mirrors the table-driven Thrift reading: `write_thrift` in `src/metadata.jl`
inverts `read_thrift` using Thrift.jl's exported compact-protocol writers, driven by
write field tables that emit only the fields we produce. Encoders in `src/encodings.jl`
are inverses of the decoders beside them (`encode_plain`, `encode_rle_bitpacked` —
RLE-runs-only, always a valid form of the hybrid encoding). `src/filewriter.jl`
assembles pages (length-prefixed RLE def levels + PLAIN values), column chunks, and
the footer. All columns are written OPTIONAL with null-count statistics so the
reader's statistics-based eltype derivation works on our own files.

Nested writing is one recursion over the column's element type. `_plan_node` builds the
schema subtree — `NamedTuple` → group, `AbstractVector` → 3-level LIST, otherwise a
primitive leaf, every node OPTIONAL — and gives each node the list of leaves below it.
`_shred!` then walks each row (Dremel shredding): `def` goes up by one for every present
optional node and once more on entering a list's items; an item's `rep` is its list's
depth, except the first item, which inherits the enclosing one. A null or empty value is
recorded in every leaf below the node where the path stopped. `_data_page` writes any
leaf. `NamedTuple` types must be concrete, since member types are read from the type. For example `List<primitive>` has
def 0 = null list, 1 = empty list, 2 = null element, 3 = value. `Vector{UInt8}` elements
are byte strings, not lists. `null_count` counts every level entry without a value
(including empty lists), matching pyarrow.

Current writer scope: flat, list, and struct columns nested to any depth (Int32/Int64/Float32/Float64/
Bool/String/bytes + Missing unions), PLAIN, uncompressed, one row group. Next steps are
in `tasks/todo.md`.

## Known Limitations

- Struct members cannot be selected individually: `columns=["s.a"]` warns and is ignored; select `"s"` and use `tbl.s.a`.
- `logicalType` annotations are not parsed, only `converted_type`. A LIST group carrying only `logicalType` would be read as a struct with a single member `list` (not observed in practice; pyarrow writes both).
- Without `ARROW:schema` metadata, `FixedSizeList` columns are read as regular variable-length lists since Parquet's schema does not encode the list size.
- LZ4 Hadoop framing (used by older Spark/Hadoop writers) is implemented but not tested end-to-end — only the standard LZ4 raw/frame format is covered by the test suite.
- `open_parquet` / `read_parquet` on a non-existent path gives "File too small" instead of "File not found" (Mmap.mmap silently creates an empty file). Needs a guard in the public API.
