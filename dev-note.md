# Dev Notes

## Architecture

- **Memory-mapped IO**: Files are read via `Mmap.mmap`, producing a shared read-only `Vector{UInt8}`. No `IOStream` seek/read — safe for concurrent access.
- **Per-RowGroup parallelism**: Each column spawns tasks (`Threads.@spawn`) that process row groups in parallel, and the leaves of a struct in parallel within a row group. Results are composed via `ChainedVector` (zero-copy, no concatenation).
- **Arrow-native arrays**: Decoded Parquet pages are assembled directly into `Arrow.Primitive`, `Arrow.BoolVector`, and `Arrow.List` — matching Parquet's Dremel encoding to Arrow's offset-based layout in a single pass. These are built through Arrow.jl's internal positional constructors, which are not public API, so the compat bound is tight (`Arrow = "~2.8.1"`, the 2.8 series only) and must be re-checked on each Arrow minor release.

## FixedSizeList Design

`Arrow.FixedSizeList` uses `NTuple{N, T}` as its element type, so every `col[i]` access copies all `N` elements to create an `N`-tuple. This is unacceptable for waveform data where `N` could be of O(10^3–10^4).

This package uses two structs to circumvent the issue:

- **`FixedSizeListVector{N, T, ET}`** — the column container. Stores data in a flat `Vector{T}` with fixed stride `N`, plus a `BitVector` for record-level nulls.
- **`FixedSizeView{N, T}`** — a lightweight zero-copy view returned on index access. Carries only a pointer to the parent array and an offset (16 bytes), regardless of `N`.

`FixedSizeListVector` is not an `Arrow.ArrowVector` subtype. It registers `ArrowKind = FixedSizeListKind{N,T}` so `Arrow.write` can serialize it correctly, but:

- FSL fields are detected from ARROW:schema at top level and inside structs (`parse_arrow_schema` keys a struct member by its dotted path, e.g. `"wf.values"`). Nested FixedSizeList (e.g., `FixedSizeList<FixedSizeList<T>>`) is not supported.
- Composition with other Arrow types (e.g., `List<FixedSizeList<T>>`) falls back to variable-length lists at all levels.
- If you write the table to an Arrow IPC file with `Arrow.write` and read it back with `Arrow.read`, FixedSizeList columns will come back as Arrow.jl's native `NTuple`-based `FixedSizeList`, not as `FixedSizeListVector`. The data is preserved, but the zero-copy view behavior is lost.

## Reader Design

The reader (`src/reader.jl`) is the inverse of the writer's `_plan_node` / `_shred!`:
schema tree → plan tree → prune to the selection → recursive assembly. It replaced three
shape-specific paths (leaf/list, struct, list<struct>) and a flattened fallback.

**Plan tree** (`plan_read_tree`). Three node kinds: `:leaf` (values and element
validity), `:list` (offsets and validity; one child), `:struct` (validity; its members).
A list covers the standard 3-level LIST, the legacy 2-level forms, a bare repeated field,
and MAP (a list of key/value structs; a map without values is a list of its keys, as in
pyarrow). Each node records the definition level from which it is non-null, its
repetition depth, and for a list the level from which an item exists. A node's `key` is
the path a user types (`"wf.values"`, `"particles.pt"`, `"m.key"`), without a list's
`list`/`element` or a map's `key_value` segments — the same keys as the writer's
`encoding` keyword.

**Pruning** (`prune_read_plan`). `columns=` keeps the selected leaves and their ancestors.
Assembly never knows it was handed a subset: structure comes from the leftmost *remaining*
leaf. Unselected leaves are not in the plan, so they are not decoded. An unmatched key is
an error.

**Slots.** A node has one slot per item of its nearest enclosing list, or one per row
outside lists. A level entry starts a slot when `rep <= slot_rep && def >= slot_def`,
where those are the enclosing list's repetition depth and item level. A slot is null at a
node when `def < node.def_level`, for any reason: the node is null or an ancestor is. A
struct member is therefore missing wherever its struct is, which is what `col.member`
must show for those rows (pyarrow's `flatten` semantics).

**One leaf's levels serve every ancestor.** All leaves under a node carry the same
structure above it (the Dremel invariant), so each list or struct takes its offsets and
validity from its leftmost leaf's levels (`_list_structure`, `_slot_nulls`), one short
pass per node. Every other leaf only places its values in slots (`_scatter_leaf`; without
null elements the decoded values are used as they are).

**Two stages.** Stage 1 (`_read_buffers`) runs per row group in parallel and returns raw
buffers per node; nothing in it depends on whether a type admits `Missing`. Stage 2
(`_wrap_buffers`) runs once all row groups are in: a node's type admits `Missing` exactly
when a null was decoded at that node in any row group, and every chunk is wrapped with
that one type, so `ChainedVector` composition is stable. Statistics are not consulted, so
element types do not depend on the writer. Consequence: types depend on the data read;
two files with the same schema can differ in `Missing`.

**FixedSizeList.** A list node that `ARROW:schema` declares fixed-size, with a primitive
element and outside other lists, is read into one flat vector: straight copies of the
page values when there are no nulls (the dense path, which is what makes waveforms fast),
a scatter by level otherwise. It reports one level entry per row to its parent, so
nothing above it does per-element work.

**Wrappers.** `_wrap_nested` (src/api.jl) gives named field access to any array whose
elements are structs, directly (`StructColumn`) or through list levels
(`ListOfStructsColumn`); `_member_list` projects a field through every list level,
sharing each level's offsets and validity. Both are one type, `NestedColumn`.
`Arrow.Struct` stores fields positionally with no name-based access, which is why the
wrapper exists; `Arrow.write` serializes a wrapped column by re-encoding it row by row,
not by reusing the buffers (see Known Limitations for the one shape that fails).

**A column chunk with no pages** (zero-row file or row group) gets one empty page of the
leaf's physical type (`_empty_pages`), so every shape assembles to a typed empty column,
with no `Missing` anywhere since no null was seen.

## Writer Design

Writing mirrors the table-driven Thrift reading: `write_thrift` in `src/metadata.jl`
inverts `read_thrift` using Thrift.jl's exported compact-protocol writers, driven by
write field tables that emit only the fields we produce. Encoders in `src/encodings.jl`
are inverses of the decoders beside them (`encode_plain`, `encode_rle_bitpacked` —
RLE-runs-only, always a valid form of the hybrid encoding). `src/filewriter.jl`
assembles pages (length-prefixed RLE def levels + PLAIN values), column chunks, and
the footer. All columns are written OPTIONAL, with null-count statistics for readers that use them.

Nested writing is one recursion over the column's element type. `_plan_node` builds the
schema subtree — `NamedTuple` → group, `AbstractVector` → 3-level LIST, otherwise a
primitive leaf, every node OPTIONAL — and gives each node the list of leaves below it.
`_shred!` then walks each row (Dremel shredding): `def` goes up by one for every present
optional node and once more on entering a list's items; an item's `rep` is its list's
depth, except the first item, which inherits the enclosing one. A null or empty value is
recorded in every leaf below the node where the path stopped. `_data_page` writes any
leaf. `NamedTuple` types must be concrete, since member types are read from the type. For example `List<primitive>` has
def 0 = null list, 1 = empty list, 2 = null element, 3 = value. `Vector{UInt8}` elements
are byte strings, not lists. `null_count` follows pyarrow leaf by leaf (`_null_count`), for readers that derive
nullability from it (ours did until the recursive reader; it now uses the decoded levels). pyarrow's rule is not written down anywhere; measured on
pyarrow 23 across 18 leaf positions it is: a leaf that is itself a list's element counts
every level entry without a value, null and empty lists included; a leaf below a struct
inside a list counts only the list's existing slots, so null and empty lists are left out;
outside lists the two agree. The test compares our statistics with pyarrow's for
every shape, so a change in pyarrow's behaviour would show up there.

Because dispatch is on element type, the reader's containers (`Arrow.List`, `StructColumn`,
`ListOfStructsColumn`, `FixedSizeListVector`, `ChainedVector` chunks) are written without
special cases.

A `FixedSizeListVector` is written as a plain LIST, as pyarrow does; the fixed size lives
in the `ARROW:schema` key-value entry. `_arrow_schema_kv` gets that entry from Arrow.jl
instead of building FlatBuffers by hand: it serializes a zero-row copy of the table and
keeps the first IPC message, which is the schema. The entry is written only when the table
has a FixedSizeList column, at top level or nested in structs/lists (`_has_fsl`): Arrow.jl compiles its schema code for each new set of column
types, which adds 5–20 s to a first `write_parquet` call (measured 2026-10-03), and no
other type we write needs it. Tables with a FixedSizeList column still pay that once per
session and table shape.

Narrow and unsigned integers are stored in INT32/INT64 with a converted-type annotation;
`encode_plain` converts with `% Int32` / `% Int64`, the inverse of the reader's `% T`.
`Date` is written as INT32 days since the Unix epoch with converted type DATE. Timestamps
are described under Timestamp Design.

Current writer scope: flat, list, and struct columns nested to any depth (Int8–Int64, UInt8–UInt64,
Float32/Float64, Bool, String, Date, DateTime, bytes + Missing unions), one row group. Each v1 data page
body (levels + values) is compressed as a whole by `compress`, the inverse of `decompress`
in `src/compression.jl`; Snappy is the default, as in pyarrow, whose codec names we follow
(`:lz4` means LZ4_RAW; the deprecated Hadoop-framed LZ4 is not written).

Compression is a lookup (`CHUNK_CODECS`: Parquet codec → ChunkCodecs decoder and encoder)
plus `encode` / `decode`. `decode` is given the page header's declared uncompressed size
as `max_size`, so a corrupt file cannot force a larger allocation; a page that expands
beyond its declared size is an error. ChunkCodecs releases native contexts itself, which
is what the earlier zstd leak (unclosed TranscodingStream) got wrong. The Hadoop LZ4
framing stays our own code, since ChunkCodecs has only the block codec. ChunkCodecLibSnappy
1.0.0 does not declare `is_thread_safe` (it defaults to `false`); its codec is a stateless
singleton over snappy's one-shot functions, and we share it across reader tasks. Next steps are
in `tasks/todo.md`.

## Writer Encodings

`write_parquet(...; encoding)` picks the value encoding per leaf; levels are always RLE.
Each encoder sits beside its decoder in `src/encodings.jl` as its inverse. `_plan_node`
gives every leaf a user-facing `key`: the schema path without a list's structural
`list`/`element` segments (`particles.list.element.pt` → `particles.pt`). The key is
threaded through planning, not derived by dropping names, so a struct field that happens
to be called `list` or `element` keeps its segment. `_leaf_encodings` resolves the keyword:
a single name is best-effort (PLAIN where the type does not allow it); a `Dict` is strict
(wrong type or unmatched key is an error) and the longest matching key wins. All columns
are planned before the file is opened, so these errors leave no partial file. The page
header's `encoding` and the column metadata's `encodings` state what was written.

Implemented: PLAIN, BYTE_STREAM_SPLIT (Float32/Float64: the K bytes of each value are the
columns of a K×n matrix, and the encoding is its rows laid end to end), and
DELTA_BINARY_PACKED for every leaf whose physical type is INT32 or INT64. `physical_ints`
maps narrow and unsigned integers, dates and timestamps to that physical type, for PLAIN
and delta alike. Blocks hold 128 deltas in 4 miniblocks of 32, as pyarrow writes them.

Delta arithmetic wraps around in the column's own width (so every delta fits in it), and
`delta - min_delta` is taken as unsigned. The decoder mirrors that: deltas are unpacked as
UInt64 (bit widths up to 64, through a UInt128 accumulator) and added with wrap-around, and
an INT32 column is truncated to 32 bits at the end. Before E2 the decoder unpacked into
UInt32 and converted with range checks, so it dropped pyarrow's INT32 columns with
wrap-around deltas and INT64 columns with deltas wider than 32 bits.

DELTA_LENGTH_BYTE_ARRAY (strings and bytes) is the lengths, delta-packed as INT32, followed
by the values' bytes back to back; it reuses the delta encoder.

Not written: dictionary encoding (read, but writing is deferred to v0.3; `:dictionary` is not
an accepted name), and
DELTA_BYTE_ARRAY, which the reader does not decode either.

## Timestamp Design

Julia's `DateTime` holds milliseconds and no zone, so it is exact only for a naive
millisecond timestamp. The reader therefore uses a mixed rule (`timestamp_julia_type`):
naive milliseconds → `DateTime`; everything else → `Arrow.Timestamp{U, TZ}` with `TZ`
`:UTC` or `nothing`. `Arrow.Timestamp` is a bits type wrapping one `Int64`, so the decoded
INT64 values are reinterpreted in place, with no per-value conversion.

`leaf_annotation(elem)` is the single place that decides what a leaf means: the parsed
`logicalType` TIMESTAMP if present, else TIMESTAMP_MILLIS / TIMESTAMP_MICROS read as
UTC-adjusted (the spec's meaning, and pyarrow's), else the plain converted type. Every
assembly path (flat, list, struct member, list<struct>, FixedSizeList) passes that
annotation where it used to pass the converted type, so nested timestamps need no
special cases.

Thrift: `logicalType` is SchemaElement field 10, a union. `read_union` reads the one set
member; only member 8 (TIMESTAMP: `isAdjustedToUTC` + a `TimeUnit` union of empty structs)
is parsed, the rest is skipped. In the compact protocol a bool field's value is carried in
the field header, so it is written with `writeBool`, not `write`.

The writer emits `logicalType` for `DateTime` (naive millis) and `Arrow.Timestamp`. As
pyarrow does, it adds the converted type only where it means the same thing: UTC millis
or micros. pyarrow then reports the same converted and logical types as for its own files.

## Known Limitations

- A column that cannot be read is an error: `read_parquet` throws `ColumnReadError`, naming the column and keeping the original exception as `cause`; nothing is skipped silently. The other columns, and the other members of the same struct or list, can be read with `columns=`. Columns in the Apache parquet-testing files that currently throw (9 of the 64 files; pinned by the test "every file reads fully or is a known gap"):
  - Not implemented: DELTA_BYTE_ARRAY encoding (`delta_byte_array`, `delta_encoding_optional_column`, `delta_encoding_required_column`: string columns); BYTE_STREAM_SPLIT for anything but FLOAT/DOUBLE (`byte_stream_split_extended`: float16, int32, fixed-length, decimal).
  - Not investigated: the deprecated LZ4 codec in both its Hadoop-framed and unframed forms (`hadoop_lz4_compressed`, `hadoop_lz4_compressed_larger`, `non_hadoop_lz4_compressed`).
  - A limit: more than 2 GB of string data in one column chunk overflows the 32-bit offsets (`large_string_map.brotli`: the map's keys; `columns=["arr.value"]` reads).
  - A malformed file, which pyarrow rejects too ("Unexpected end of stream"): `fixed_length_byte_array` declares its column required but its pages contain nulls (page 0 announces 100 values and holds 91), so the nulls cannot be located. It exists to test the page index. PLAIN fixed-length byte arrays themselves are read: `fixed_length_decimal`, `fixed_length_decimal_legacy`, and the `flba5_plain`, `float16_plain` and `decimal_plain` columns of `byte_stream_split_extended`.
  - Fixed for v0.2.0 (2026-10-04): `dictionary_page_offset = 0` (`dict-page-offset-zero`); an empty v2 data section (`datapage_v2_empty_datapage.snappy`); v2 pages that store repetition levels for a non-repeated column, and RLE-encoded booleans (`rle_boolean_encoding`, `datapage_v2.snappy`).
- The first `write_parquet` call for each new table schema that contains a FixedSizeList (top-level, or nested in structs or lists) takes 5–20 s. The `ARROW:schema` entry is produced by Arrow.jl's generic writer, which Julia compiles per table type. Tables without a FixedSizeList skip that path, and later writes of the same schema in the same session are fast. Hand-building the schema message would avoid it; that was decided against for v0.2.0.
- Parquet stores only a UTC flag for timestamps, not a time zone name. An `Arrow.Timestamp` with a named zone is written as UTC-adjusted and reads back as `:UTC`. pyarrow shows it as `tz=UTC` too, unless the file also has an `ARROW:schema` entry (i.e. a FixedSizeList is present), in which case pyarrow restores the zone name from there. The instants are the same either way.
- `Arrow.write` throws a `MethodError` for a struct column that has a list member and at least one null struct row (e.g. `wf: struct<t0, values: list<int32>>` with a null `wf`). Structs without null rows, structs without list members, and `List<Struct>` columns are written correctly. `write_parquet` is not affected. The cause is in the row-by-row re-encoding: for the null row Arrow.jl builds a default list whose type does not match our view-based element type.
- Of the `logicalType` union only the TIMESTAMP member is parsed; everything else still relies on `converted_type`. A LIST group carrying only `logicalType` would be read as a struct with a single member `list` (not observed in practice; pyarrow writes both). INT96 timestamps and TIME are not converted.
- A `FixedSizeList` is restored at top level and as a struct member (at any struct depth). Inside a list (e.g. `list<fixed_size_list>`, `list<struct<…fsl…>>`) it is read as a variable-length list and written back as one: values are correct, but the fixed size is lost.
- A map is read as a list of `(key, value)` structs, not as a `Dict`; written back, it is a list of structs, not a Parquet MAP.
- Without `ARROW:schema` metadata, `FixedSizeList` columns are read as regular variable-length lists since Parquet's schema does not encode the list size.
- `open_parquet` / `read_parquet` on a non-existent path gives "File too small" instead of "File not found" (Mmap.mmap silently creates an empty file). Needs a guard in the public API.
