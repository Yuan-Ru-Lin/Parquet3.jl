# Dev Notes

## Architecture

- **Memory-mapped IO**: Files are read via `Mmap.mmap`, producing a shared read-only `Vector{UInt8}`. No `IOStream` seek/read — safe for concurrent access.
- **Per-RowGroup parallelism**: Each column spawns tasks (`Threads.@spawn`) that process row groups in parallel, and the leaves of a struct in parallel within a row group. Results are composed via `ChainedVector` (zero-copy, no concatenation).
- **Arrow-native arrays**: Decoded Parquet pages are assembled directly into `Arrow.Primitive`, `Arrow.BoolVector`, and `Arrow.List` — matching Parquet's Dremel encoding to Arrow's offset-based layout in a single pass. These are built through Arrow.jl's internal positional constructors, which are not public API, so the compat bound is tight (`Arrow = "~2.8.1"`, the 2.8 series only) and must be re-checked on each Arrow minor release.

## FixedSizeList Design

`Arrow.FixedSizeList` uses `NTuple{N, T}` as its element type, so every `col[i]` access copies all `N` elements to create an `N`-tuple. This is unacceptable for waveform data where `N` could be of O(10^3–10^4).

This package uses two structs to circumvent the issue:

- **`FixedSizeListVector{N, T, ET, B}`** — the column container. Stores data in a flat `Vector{T}` with fixed stride `N`, a `BitVector` for record-level nulls, and element-level null bits of type `B`.
- **`FixedSizeView{N, E, T, B}`** — a lightweight zero-copy view returned on index access. Carries the parent array and an offset (16 bytes, regardless of `N`), and the null bits when the column has them. `E` is its element type.

An Arrow fixed-size list has two validity bitmaps: one for the lists and one for the elements of its child array. `B` is the type of the second, in the way `Arrow.Primitive{T, A}` is one type for every storage:

- `B === Nothing` when no element of the column is null. The field then holds `nothing`, which takes no space: the view is still two words and the column 24 bytes, and indexing is selected by dispatch, so it compiles to the same instructions as before the parameter existed (checked with `sizeof`, `fieldoffset` and `@code_native`; the benchmark gate holds). `E === T`.
- `B === BitVector` when a null element occurs. `E === Union{Missing, T}` and `v[j]` is `missing` where the bit is set. The values stay in the same flat `Vector{T}`; there is no `Vector{Union{Missing, T}}`.

Which form a column gets is settled after all its row groups are decoded, like nullability: one null element anywhere gives every chunk the bitmap. A null list and a null element are independent. `FixedSizeView{N, E}` and `FixedSizeListVector{N, T}` name the types without the trailing parameters (they are then families of types, not concrete ones; `write_parquet` accepts a column typed that way). Because a view is (storage, offset), a shape can be added later (the 2-D idea in the v0.3 list).

`FixedSizeListVector` is not an `Arrow.ArrowVector` subtype. It registers `ArrowKind = FixedSizeListKind{N,T}` so `Arrow.write` can serialize it correctly, but:

- FSL fields are detected from ARROW:schema at any depth: the reader walks the Arrow schema together with its read plan (struct ↔ struct, list ↔ list, map ↔ map), so a fixed-size list is found at top level, in structs, in lists (`List<FixedSizeList<T>>`, `List<Struct<…>>`) and as a map value.
- Of a `FixedSizeList<FixedSizeList<T>>` only the inner level is restored; the outer one reads as a variable-length list (see [docs/limitations.md](docs/limitations.md)). The element must be fixed-width (numbers, `Bool`, dates, timestamps); a fixed-size list of strings or bytes reads as an ordinary list.
- If you write the table to an Arrow IPC file with `Arrow.write` and read it back with `Arrow.read`, FixedSizeList columns will come back as Arrow.jl's native `NTuple`-based `FixedSizeList`, not as `FixedSizeListVector`. The data is preserved, but the zero-copy view behavior is lost.

## Source Layout

Files are included in dependency order (`src/Parquet3.jl`); each uses only what the files above it define.

| File | Contents |
|---|---|
| `types.jl` | enums, metadata structs, schema nodes |
| `metadata.jl` | Thrift field tables; reading and writing metadata |
| `typemap.jl` | Parquet type ↔ Julia type, both directions |
| `encodings.jl` | value and level encodings; each decoder is followed by its encoder |
| `compression.jl` | page compression |
| `filereader.jl` | opening a file, footer, schema tree |
| `pagereader.jl` | the pages of a column chunk |
| `arrow_schema.jl` | `ARROW:schema` metadata |
| `arrays.jl` | the array types returned (`FixedSizeListVector`, `MapVector`, the wrappers) and their builders |
| `reader.jl` | plan, prune, assemble: `read_parquet` |
| `filewriter.jl` | plan, shred, encode: `write_parquet` |

## Reader Design

The repetition/definition-level encoding of nested data, and the invariant used below, come from the Dremel paper: S. Melnik et al., "Dremel: Interactive Analysis of Web-Scale Datasets", PVLDB 3(1), 2010 — <https://research.google.com/pubs/archive/36632.pdf>.

The reader (`src/reader.jl`) is the inverse of the writer's `_plan_node` / `_shred!`: schema tree → plan tree → prune to the selection → recursive assembly. It replaced three shape-specific paths (leaf/list, struct, list<struct>) and a flattened fallback.

**Plan tree** (`plan_read_tree`). Three node kinds: `:leaf` (values and element validity), `:list` (offsets and validity; one child), `:struct` (validity; its members). A list covers the standard 3-level LIST, the legacy 2-level forms, a bare repeated field, and MAP (a list of key/value structs; a map without values is a list of its keys, as in pyarrow). Each node records the definition level from which it is non-null, its repetition depth, and for a list the level from which an item exists. A node's `key` is the path a user types (`"wf.values"`, `"particles.pt"`, `"m.key"`), without a list's `list`/`element` or a map's `key_value` segments — the same keys as the writer's `encoding` keyword.

**Pruning** (`prune_read_plan`). `columns=` keeps the selected leaves and their ancestors. Assembly never knows it was handed a subset: structure comes from the leftmost *remaining* leaf. Unselected leaves are not in the plan, so they are not decoded. An unmatched key is an error.

**Slots.** A node has one slot per item of its nearest enclosing list, or one per row outside lists. A level entry starts a slot when `rep <= slot_rep && def >= slot_def`, where those are the enclosing list's repetition depth and item level. A slot is null at a node when `def < node.def_level`, for any reason: the node is null or an ancestor is. A struct member is therefore missing wherever its struct is, which is what `col.member` must show for those rows (pyarrow's `flatten` semantics).

**One leaf's levels serve every ancestor.** All leaves under a node carry the same structure above it (the Dremel invariant), so each list or struct takes its offsets and validity from its leftmost leaf's levels (`_list_structure`, `_slot_nulls`), one short pass per node. Every other leaf only places its values in slots (`_scatter_leaf`; without null elements the decoded values are used as they are).

**Two stages.** Stage 1 (`_read_buffers`) runs per row group in parallel and returns raw buffers per node; nothing in it depends on whether a type admits `Missing`. Stage 2 (`_wrap_buffers`) runs once all row groups are in: a node's type admits `Missing` exactly when a null was decoded at that node in any row group, and every chunk is wrapped with that one type, so `ChainedVector` composition is stable. Statistics are not consulted, so element types do not depend on the writer. Consequence: types depend on the data read; two files with the same schema can differ in `Missing`.

**FixedSizeList.** A list node that `ARROW:schema` declares fixed-size, with a fixed-width element (`_fixed_width_leaf`), is read into one flat vector with one slot per row, or per item of the list it sits in: straight copies of the page values when there are no nulls (the dense path, which is what makes waveforms fast, and which holds at any depth since every level entry is then one element), a scatter by level otherwise. To its parents it reports the levels a plain leaf in its place would have: one entry per slot and per empty or null enclosing list, so nothing above it does per-element work. Outside lists that is one entry per row, and that case keeps its own branch so the waveform path is unchanged.

A null *element* is stored as zero bits in the flat vector. When the levels of a row group show one, the scatter path also records its position (the dense path cannot meet one: it runs only when every level is at its maximum). Stage 2 then gives every chunk of the column the element null bits (`_fixed_size_list`: `B === BitVector`), with an all-false bitmap for row groups that have none. Nothing is decoded twice and no value is copied.

**Wrappers.** `_wrap_nested` (src/arrays.jl) gives named field access to any array whose elements are structs, directly (`StructColumn`) or through list levels (`ListOfStructsColumn`); `_member_list` projects a field through every list level, sharing each level's offsets and validity. Both are one type, `NestedColumn`. `Arrow.Struct` stores fields positionally with no name-based access, which is why the wrapper exists. A list whose items are fixed-size lists is wrapped too (`ListColumn`, no field names): unwrapped, it would be an `Arrow.List` that `Arrow.write` takes as it is and then cannot serialize, because its child is a `FixedSizeListVector`.

**Maps.** A MAP group plans as a `:map` node: a list whose element is the key/value struct. It assembles exactly as a list (Arrow's own layout for a map) and stage 2 wraps the result in `MapVector`, whose rows are `MapView{K,V} <: AbstractDict{K,V}`: two views, of the row's keys and of its values. This follows the FixedSizeListVector / FixedSizeView precedent: Arrow's convention for what the data is, our own zero-copy view for how a row is shown (`Arrow.Map` would build a `Dict` on every access and hide the key and value columns). Index access costs the same for 2 entries as for 2000. Entries keep file order and duplicates; lookup is a linear scan from the end, so the last entry of a duplicated key wins, as the format specifies and as `Dict(view)` gives. `MapVector` is an array type of its own, so a map inside a struct, a list or another map presents the same way, and `_member_list` sees through it, which keeps `col.key` / `col.value`. A map without values, or one pruned to its keys or its values, is a plain list. The writer maps any `AbstractDict` element type to a MAP (required key, optional value), so a map column writes back as a map; `Arrow.write` turns it into an `Arrow.Map` through ArrowTypes' `MapKind`, with no code of ours.

**A column chunk with no pages** (zero-row file or row group) gets one empty page of the leaf's physical type (`_empty_pages`), so every shape assembles to a typed empty column, with no `Missing` anywhere since no null was seen.

## Arrow.write

`Arrow.write` takes an array of Arrow.jl's own types as it is and re-encodes anything else row by row. Everything `read_parquet` returns is an Arrow.jl array underneath, so `_arrow_native` (src/arrays.jl) hands Arrow.jl the equivalent array of its own types over the same buffers, and `Arrow.arrowvector` is defined for our three array types to call it:

| Returned by `read_parquet` | Given to Arrow.jl | Buffers |
|---|---|---|
| plain, string, list columns | themselves (`Arrow.Primitive`, `Arrow.BoolVector`, `Arrow.List`) | as they are |
| binary | itself; its element type is Arrow.jl's binary type, `Base.CodeUnits` | as they are |
| `StructColumn`, `ListOfStructsColumn` | the wrapped `Arrow.Struct` / `Arrow.List` | reused |
| `FixedSizeListVector`, with or without element null bits (at any depth) | `Arrow.FixedSizeList` over an `Arrow.Primitive` of the flat vector, which carries the element validity | reused for numbers and `Arrow.Timestamp`; `Bool`, `Date` and `DateTime` elements are stored differently by Arrow and are encoded by Arrow.jl (one copy) |
| `ListColumn` (list of fixed-size lists) | `Arrow.List` over that `Arrow.FixedSizeList`, same offsets | reused |
| `MapColumn` / `MapVector` | `Arrow.Map` over the same offsets and key/value struct | reused |

A parent is rebuilt (a new header, no data) only when one of its children changed type.

**Coupling to Arrow.jl, and the intent.** This package builds its own Arrow array types (view-based fixed-size lists, map views, wrappers with named access), so it has to implement Arrow.jl's internal interface for them. That is deliberate, not an accident: these types belong upstream, and the plan is to offer them to Arrow.jl rather than to remove the overloads (`tasks/todo.md`, "Upstream to Arrow.jl"). Until then the compat bound is `Arrow = "~2.8.1"`, and these internal names must be re-checked on each Arrow minor release:
- the method `Arrow.arrowvector(x, i, nl, fi, de, ded, meta; kw...)`, extended for our types;
- the `arrays` property Arrow.jl reads from every column when it splits a table whose first column is a `ChainedVector` (`Tables.partitions` of an `Arrow.Table`);
- the positional constructors and fields of `Arrow.Primitive`, `Arrow.BoolVector`, `Arrow.List`, `Arrow.Struct`, `Arrow.FixedSizeList`, `Arrow.Map`, `Arrow.Offsets`, `Arrow.ValidityBitmap`, and of `Arrow.Table` (built field by field in `read_parquet`);
- `Arrow.toarrowvector`, `Arrow.getmetadata`, `Arrow.tobuffer` (the schema message for `ARROW:schema`), `Arrow.FlatBuffers.getrootas` and the `Arrow.Meta` schema types (`Schema`, `Message`, `Field`, `Struct`, `List`, `LargeList`, `FixedSizeList`, `Map`, `TimeUnit`), used to read `ARROW:schema`;
- `ArrowTypes.ArrowKind` / `ArrowTypes.FixedSizeListKind` (public, listed for completeness);
- the behaviour that a fixed-size list of exactly `UInt8` becomes fixed-size binary, which the two workarounds above depend on.

Several row groups: `Arrow.write(io, table)` asks the table for partitions. Arrow.jl splits a table whose first column is a `ChainedVector` by taking `column.arrays[i]` of every column, so a wrapper answers `.arrays` with its per-row-group chunks. Each row group then becomes one Arrow record batch, with its buffers reused. Two cases join the chunks into one array instead (`_arrow_concat`: one copy per buffer, column by column, not row by row): a chunked wrapper column written on its own, outside its table, and a table whose first column is a wrapper, which Arrow.jl treats as a single partition.

Before this, wrapped columns were re-encoded row by row, which failed for a struct with a list member and a null row (Arrow.jl built a default list of the wrong type for it), and a multi-row-group table with a struct column could not be written at all.

Read back with `Arrow.Table`, a fixed-size list is Arrow.jl's `NTuple`-based array and a map is an `Arrow.Map` of `Dict`s; the zero-copy views are this package's.

## Writer Design

Writing mirrors the table-driven Thrift reading: `write_thrift` in `src/metadata.jl` inverts `read_thrift` using Thrift.jl's exported compact-protocol writers. Each Thrift struct has one hand-written field table, `(id, name, type)`, used in both directions; a `ThriftType` carries the wire tag and how to read and write a value. Fields that are `nothing` are not written. Encoders in `src/encodings.jl` are inverses of the decoders beside them (`encode_plain`, `encode_rle_bitpacked` — RLE-runs-only, always a valid form of the hybrid encoding). `src/filewriter.jl` assembles pages (length-prefixed RLE def levels + PLAIN values), column chunks, and the footer. All columns are written OPTIONAL, with null-count statistics for readers that use them.

Nested writing is one recursion over the column's element type. `_plan_node` builds the schema subtree — `NamedTuple` → group, `AbstractVector` → 3-level LIST, otherwise a primitive leaf, every node OPTIONAL — and gives each node the list of leaves below it. `_shred!` then walks each row (Dremel shredding): `def` goes up by one for every present optional node and once more on entering a list's items; an item's `rep` is its list's depth, except the first item, which inherits the enclosing one. A null or empty value is recorded in every leaf below the node where the path stopped. `_data_page` writes any leaf. `NamedTuple` types must be concrete, since member types are read from the type. For example `List<primitive>` has def 0 = null list, 1 = empty list, 2 = null element, 3 = value. `Vector{UInt8}` elements are byte strings, not lists. `null_count` follows pyarrow leaf by leaf (`_null_count`), for readers that derive nullability from it (ours did until the recursive reader; it now uses the decoded levels). pyarrow's rule is not written down anywhere; measured on pyarrow 23 across 27 leaf positions it is: every level entry without a value is counted, null and empty lists included, for a leaf that is itself a list's element, for a map's key and value, and for every string or binary leaf; a fixed-width leaf below a struct inside a list counts only the list's existing slots, so null and empty lists are left out; outside lists the two agree. (The string/binary case was missed when the rule was first measured on 18 leaves, none of which was a string under a list of structs; found 2026-10-04.) The test compares our statistics with pyarrow's for every shape, so a change in pyarrow's behaviour would show up there.

Because dispatch is on element type, the reader's containers (`Arrow.List`, `StructColumn`, `ListOfStructsColumn`, `FixedSizeListVector`, `ChainedVector` chunks) are written without special cases.

Arrow.jl declares a fixed-size list whose element type is exactly `UInt8` as fixed-size binary (`src/eltypes.jl` in Arrow.jl 2.8.1), which is not what a `FixedSizeList<UInt8>` column is. Both places that hand such a column to Arrow.jl therefore present its elements as nullable, which keeps it a list: `_concrete_view` for the `ARROW:schema` entry and `_fixed_size_child` for `Arrow.write`. pyarrow then sees `fixed_size_list<uint8>[N]`, with a nullable element field as in its own files.

A `FixedSizeListVector` is written as a plain LIST, as pyarrow does; the fixed size lives in the `ARROW:schema` key-value entry. `_arrow_schema_kv` gets that entry from Arrow.jl instead of building FlatBuffers by hand: it serializes a zero-row copy of the table and keeps the first IPC message, which is the schema. The entry is written only when the table has a FixedSizeList column, at top level or nested in structs, lists or maps (`_has_fsl`): Arrow.jl compiles its schema code for each new set of column types, which adds 5–20 s to a first `write_parquet` call (measured 2026-10-03), and no other type we write needs it. Tables with a FixedSizeList column still pay that once per session and table shape.

Narrow and unsigned integers are stored in INT32/INT64 with a converted-type annotation; `encode_plain` converts with `% Int32` / `% Int64`, the inverse of the reader's `% T`. `Date` is written as INT32 days since the Unix epoch with converted type DATE. Timestamps are described under Timestamp Design.

Current writer scope: flat, list, and struct columns nested to any depth (Int8–Int64, UInt8–UInt64, Float32/Float64, Bool, String, Date, DateTime, bytes + Missing unions), one row group. Each v1 data page body (levels + values) is compressed as a whole by `compress`, the inverse of `decompress` in `src/compression.jl`; Snappy is the default, as in pyarrow, whose codec names we follow (`:lz4` means LZ4_RAW; the deprecated Hadoop-framed LZ4 is not written).

Compression is a lookup (`CHUNK_CODECS`: Parquet codec → ChunkCodecs decoder and encoder) plus `encode` / `decode`. `decode` is given the page header's declared uncompressed size as `max_size`, so a corrupt file cannot force a larger allocation; a page that expands beyond its declared size is an error. ChunkCodecs releases native contexts itself, which is what the earlier zstd leak (unclosed TranscodingStream) got wrong. The Hadoop LZ4 framing stays our own code, since ChunkCodecs has only the block codec. ChunkCodecLibSnappy 1.0.0 does not declare `is_thread_safe` (it defaults to `false`); its codec is a stateless singleton over snappy's one-shot functions, and we share it across reader tasks. Next steps are in `tasks/todo.md`.

## Writer Encodings

`write_parquet(...; encoding)` picks the value encoding per leaf; levels are always RLE. Each encoder sits beside its decoder in `src/encodings.jl` as its inverse. `_plan_node` gives every leaf a user-facing `key`: the schema path without a list's structural `list`/`element` segments (`particles.list.element.pt` → `particles.pt`). The key is threaded through planning, not derived by dropping names, so a struct field that happens to be called `list` or `element` keeps its segment. `_leaf_encodings` resolves the keyword: a single name is best-effort (PLAIN where the type does not allow it); a `Dict` is strict (wrong type or unmatched key is an error) and the longest matching key wins. All columns are planned before the file is opened, so these errors leave no partial file. The page header's `encoding` and the column metadata's `encodings` state what was written.

Implemented: PLAIN, BYTE_STREAM_SPLIT (Float32/Float64: the K bytes of each value are the columns of a K×n matrix, and the encoding is its rows laid end to end), and DELTA_BINARY_PACKED for every leaf whose physical type is INT32 or INT64. `physical_ints` maps narrow and unsigned integers, dates and timestamps to that physical type, for PLAIN and delta alike. Blocks hold 128 deltas in 4 miniblocks of 32, as pyarrow writes them.

Delta arithmetic wraps around in the column's own width (so every delta fits in it), and `delta - min_delta` is taken as unsigned. The decoder mirrors that: deltas are unpacked as UInt64 (bit widths up to 64, through a UInt128 accumulator) and added with wrap-around, and an INT32 column is truncated to 32 bits at the end. Before E2 the decoder unpacked into UInt32 and converted with range checks, so it dropped pyarrow's INT32 columns with wrap-around deltas and INT64 columns with deltas wider than 32 bits.

DELTA_LENGTH_BYTE_ARRAY (strings and bytes) is the lengths, delta-packed as INT32, followed by the values' bytes back to back; it reuses the delta encoder.

Not written: dictionary encoding (read, but writing is deferred to v0.3; `:dictionary` is not an accepted name), and DELTA_BYTE_ARRAY, which the reader does not decode either.

## Timestamp Design

Julia's `DateTime` holds milliseconds and no zone, so it is exact only for a naive millisecond timestamp. The reader therefore uses a mixed rule (`timestamp_julia_type`): naive milliseconds → `DateTime`; everything else → `Arrow.Timestamp{U, TZ}` with `TZ` `:UTC` or `nothing`. `Arrow.Timestamp` is a bits type wrapping one `Int64`, so the decoded INT64 values are reinterpreted in place, with no per-value conversion.

`leaf_annotation(elem)` is the single place that decides what a leaf means: the parsed `logicalType` TIMESTAMP if present, else TIMESTAMP_MILLIS / TIMESTAMP_MICROS read as UTC-adjusted (the spec's meaning, and pyarrow's), else the plain converted type. Every assembly path (flat, list, struct member, list<struct>, FixedSizeList) passes that annotation where it used to pass the converted type, so nested timestamps need no special cases.

Thrift: `logicalType` is SchemaElement field 10, a union. `read_union` reads the one set member; only member 8 (TIMESTAMP: `isAdjustedToUTC` + a `TimeUnit` union of empty structs) is parsed, the rest is skipped. In the compact protocol a bool field's value is carried in the field header, so it is written with `writeBool`, not `write`.

The writer emits `logicalType` for `DateTime` (naive millis) and `Arrow.Timestamp`. As pyarrow does, it adds the converted type only where it means the same thing: UTC millis or micros. pyarrow then reports the same converted and logical types as for its own files.

## Testing and CI

`julia --project=. -e 'using Pkg; Pkg.test()'` runs everything. pyarrow is the reference: the tests have pyarrow write fixtures and read our files, through `uv` in the Python environment committed under `test/pyhelper` (pyarrow pinned in `pyproject.toml` and `uv.lock`). The Apache parquet-testing files are a lazy artifact declared in `test/Artifacts.toml`: the upstream repository's archive at a pinned commit, downloaded on the first test run and kept in the Julia depot. It is a test-only dependency (`Artifacts` and `LazyArtifacts` are in `[extras]`), so installing the package downloads nothing. It replaced a git submodule, which made the repository impossible to install with `Pkg.add(url=…)`: Pkg checks a package out of a bare clone and libgit2 refuses a tree that contains a submodule. The archive is the one GitHub generates for the commit; if its bytes ever change, the checksum in `test/Artifacts.toml` has to be updated (the tree hash stays valid).

Without `uv`, or when the artifact cannot be fetched, the tests that need them are skipped with a warning. With `PARQUET3_TEST_STRICT=1` a missing dependency is a failure instead; CI sets it, so a green run means the pyarrow cross-checks and the parquet-testing suite ran.

**Running tests.** The full suite takes about 12 minutes, nearly all of it Julia compiling code for each new table schema. While iterating, run only the groups that matter:

```
julia --project=. -e 'using Pkg; Pkg.test(test_args=["FixedSizeList", "Writer"])'
```

runs the top-level groups (`@group "name"` in `test/runtests.jl`) whose name contains one of the arguments, ignoring case, and logs which groups ran and which were skipped. An argument that matches no group is an error. A single group takes its own time plus about 20 s to load. Groups do not depend on each other. The rule: selective runs while iterating, the full suite (no arguments) before every commit. CI passes no arguments.

**Benchmark gate.** Reads of the waveform columns of `testdata/part-0.parquet` must not slow down. `julia --project=. -t4 test/benchmark_part0.jl` prints the best and median of 15 reads for all columns, each waveform column and `tracelist`.
1. Quick form, the default: run it once on the change and once on the baseline commit (`git archive <commit> | tar -x -C <dir>`, copy `Manifest.toml`, `--project=<dir>`), and compare the minima.
2. The minimum of one process varies by about ±15% on unchanged code (measured: `waveform_windowed` between 145 and 192 ms). So the quick form only shows a regression larger than about 20%. If the change is slower by more than the reference spread below, or touches the dense path, alternate the two sides three times and compare the ranges; a regression is a range that sits above the baseline's.
3. Reference (2026-10-04, 4 threads, Apple silicon, minima over three alternating rounds): all columns 175–213 ms, `waveform_windowed` 138–175 ms, `waveform_presummed` 113–138 ms, `tracelist` 3.9–4.4 ms. Layout checks complement it for the fixed-size types: `sizeof` of the dense view is 16 and of the column 24, and `@code_native` of dense indexing has no branch.

`.github/workflows/CI.yml` runs the suite on Julia 1.10 (the declared minimum) and the latest release, on Linux, with four threads. `CompatHelper.yml` opens a pull request when a dependency moves outside the `[compat]` bounds, which matters for the tight Arrow bound. Files that exist only on the author's machine (`testdata/part-0.parquet`) are used by the corpus test when present and are not needed for a green run.

## Known Limitations

Moved to [docs/limitations.md](docs/limitations.md), with the user-facing descriptions of reading and writing in [docs/reading.md](docs/reading.md) and [docs/writing.md](docs/writing.md).
