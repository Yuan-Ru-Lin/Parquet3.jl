# Writer support

## W1 — flat columns (DONE, awaiting review)
- [x] `write_thrift` + write field tables (src/metadata.jl)
- [x] `encode_plain` / `encode_rle_bitpacked` (src/encodings.jl)
- [x] `src/filewriter.jl`: `write_parquet` — schema, pages, footer; Tables dep added
- [x] Tests (27/27): round-trip all W1 types + missings/NaN/empty-string/bytes,
      0 rows, all-missing, error cases, footer re-parse, RLE unit, pyarrow cross-read
- [x] Full suite green; README + dev-note updated

## v0.2.0 release scope: struct reading (done) + writer with nested columns

Nested writing is driven by element type, so reader outputs round-trip:
`AbstractVector` elements → LIST, `NamedTuple` elements → group (struct).

- [x] N1 — `List<primitive>`: 3-level LIST schema, rep/def level generation, nulls at
      list and element level; round-trip + pyarrow cross-read (DONE, awaiting review;
      on branch `writer-w1`, rebased onto main)
- [x] N2 — struct of flat fields: one group, several leaves, def levels only (DONE,
      awaiting review; `_shred` now returns several leaves per column)
- [x] N3 — composition: struct{list}, struct-of-struct, list<struct>, list<list>
      (recursive shredder over the schema tree; N1/N2 become its base cases) (DONE, awaiting
      review; `_plan_node` + `_shred!` replace the per-shape shredders)
- [x] N4 — read→write round-trip of reader containers (`Arrow.List`, `StructColumn`,
      `ListOfStructsColumn`, `FixedSizeListVector` written as plain LIST) (DONE, awaiting
      review; no writer change needed, tests only; single- and multi-RG sources;
      pyarrow confirms equal values and, except FSL, equal types)
- [x] Int8/Int16 and UInt8/16/32/64 writing (DONE, awaiting review): INT32/INT64 plus
      converted type; pyarrow restores the exact types.
- [x] Date/DateTime writing (DONE, awaiting review).
- [x] Timestamps through `logicalType` (DONE, awaiting review; decided for v0.2.0 on
      2026-10-03 via the planning session). Read: naive ms → `DateTime`, everything else →
      `Arrow.Timestamp{U,TZ}`; converted-type-only files read as UTC. Write: `DateTime` →
      naive TIMESTAMP(MILLIS); `Arrow.Timestamp` keeps unit and UTC flag. Only the TIMESTAMP
      member of the union is parsed/written.
- [x] Reader fix found by the timestamp tests: `list<struct>` across row groups with a null
      list in only some of them gave mismatched chunk types (`col.field` threw). List
      nullability no longer trusts member `null_count` (pyarrow excludes null lists there).
- [x] `null_count` matches pyarrow for every leaf position (DONE, awaiting review; found
      2026-10-03, fixed 2026-10-04). Rule measured on 18 leaves: a list's own element leaf
      counts all entries without a value; a leaf under a struct inside a list counts only
      existing slots. Files from our writer and from pyarrow now read back with identical
      element types. No reader change.
- [ ] Release: commit, clean untracked files, bump to 0.2.0, tag, push

- [x] N1.5 (DONE, awaiting review: `_arrow_schema_kv`; our reader and pyarrow both restore
      fixed_size_list; pyarrow cannot write an FSL with null rows to Parquet, so that case
      is untested) — FixedSizeList fidelity (REQUIRED for v0.2.0, decided 2026-10-03): written as a
      plain LIST plus `ARROW:schema` key-value metadata so it reads back as
      `FixedSizeListVector`. Spike first: get the schema message from Arrow.jl (serialize
      the table schema, take the first IPC message) rather than hand-building FlatBuffers;
      confirm our reader and pyarrow both restore fixed_size_list.

- [x] N1.6 — FixedSizeList inside structs (DONE, awaiting review). Found 2026-10-03: in
      `testdata/part-0.parquet`, `waveform_windowed.values` is `fixed_size_list<int32>[1400]`
      and `waveform_presummed.values` is `[1024]`, both struct members.
      - reader: `_collect_fsl!` walks struct children; member assembled as `FixedSizeListVector`
      - writer: `_has_fsl` triggers `ARROW:schema` at any depth
      - verified on part-0: read → write → pyarrow reports equal types and values (12355 rows)
      - not covered: FSL inside a list or list<struct> (still read as variable-length)
- [ ] Follow-up (if it bothers users): first-write latency for tables with a FixedSizeList
      column (5–20 s, Arrow.jl compilation in `_arrow_schema_kv`). Needs a schema path that
      avoids Arrow.jl's generic writer.

- [x] Compression (DONE, awaiting review, decided for v0.2.0 on 2026-10-03):
      `write_parquet(...; compression)` with snappy (default, as pyarrow), gzip, zstd, lz4,
      uncompressed; pyarrow reads all four.

Deferred (edge case, decided 2026-10-03): infer struct member types from rows when the
`NamedTuple` eltype is not concrete (`[(a = missing,), (a = 2,)]`); currently a clear error.

## Codecs (v0.2.0, decided 2026-10-04 via the planning session)
- [x] Replace CodecZlib/CodecZstd/CodecLz4/Snappy/TranscodingStreams with ChunkCodecs.jl
      (DONE, awaiting review): `src/compression.jl` 117 → 64 lines; declared page size bounds
      the output; leak check from 6ab762e still flat for every codec.
- [x] Brotli read and write (`compression = :brotli`), checked both ways against pyarrow.

## Release blockers from the structure review (2026-10-04, via the planning session)
- [x] A — page headers over 1024 bytes dropped the column; headers of any size now parse
- [x] B — Project.toml: julia 1.10 (suite run on 1.10.11, 1.11.9, 1.12.5), ArrowTypes dep, Tables extra removed
- [x] C — `read_parquet` throws `ColumnReadError` instead of warn-and-skip; the parquet-testing
      columns that throw are listed in dev-note Known Limitations and pinned by a test
- [x] D — Arrow compat restricted to the 2.8 series (`~2.8.1`)
- [x] E — dead code deleted (199 source lines); none of it was exported
- [ ] Found by C, not fixed (were silently dropped before): empty v2 data page,
      `dictionary_page_offset = 0`, PLAIN fixed-length byte arrays, deprecated LZ4 codec (both
      framings), `rle_boolean_encoding`, >2 GB string column. Not implemented: DELTA_BYTE_ARRAY,
      BYTE_STREAM_SPLIT beyond float/double, RLE booleans.

## Writer encodings (v0.2.0, decided 2026-10-04 via the planning session)
Goal: write every value encoding the reader decodes. One step at a time, each reported for approval.
- [x] E1 — BYTE_STREAM_SPLIT for Float32/Float64, the `encoding` keyword (one name for the
      whole table, or a `Dict` keyed by user path), and path resolution (DONE, awaiting review)
- [x] E2 — DELTA_BINARY_PACKED for Int32/Int64 (DONE, awaiting review). Narrow/unsigned ints,
      Date, DateTime and Arrow.Timestamp fell out via `physical_ints`. Also fixed the decoder:
      it dropped pyarrow INT32 columns with wrap-around deltas and INT64 columns with deltas
      wider than 32 bits, and failed on zero values and on zigzag values beyond ±2^62.
- [x] E3 — DELTA_LENGTH_BYTE_ARRAY for strings and bytes; reuses E2 for lengths (DONE, awaiting review)
- E4 — Dictionary encoding on write: deferred to v0.3 (decided 2026-10-04); see the v0.3 list.
Out of scope: DELTA_BYTE_ARRAY, BYTE_STREAM_SPLIT for ints/FLBA, data page v2, multiple pages.

## Deferred to v0.3
- Multiple row groups on write; min/max statistics
- Other logical types (LIST-only annotation, TIME, INT96)
- E4 — Dictionary encoding on write (RLE_DICTIONARY). `:dictionary` is not an accepted
  `encoding` name today, and the bit-packed run encoder is not written. Needs: dictionary
  page, index page, `dictionary_page_offset`, bit-packed runs in `encode_rle_bitpacked`;
  no size-based fallback to PLAIN (record as a limitation).
- Candidate: derive element nullability from the definition levels actually decoded
  instead of the `null_count` statistic, so element types do not depend on which writer
  produced the file.

## Arrow.write of nested columns (do with writer work)
- [ ] `Arrow.write` does not see `NestedColumn` (StructColumn / ListOfStructsColumn) as an
      Arrow array, so it re-encodes the column row by row (`Arrow.ToStruct` → `Arrow.ToList`)
      instead of reusing the wrapped `Arrow.Struct` / `Arrow.List` buffers. Hand Arrow the
      wrapped array directly; decide what to do for multi-RG `ChainedVector` chunks.
- [ ] Same path throws for a nullable struct with a list member once a struct row is null:
      `struct<a: int64, v: list<int64>>`, rows `[{a:1, v:[1]}, None]` →
      `MethodError: Cannot convert SubArray{…Vector…} to SubArray{…Arrow.Primitive…}`.
      Reproduced. Likely disappears once the row-by-row path is bypassed; re-check after.
- [x] dev-note "verified by round-trip" claim is false for the struct-with-list shape; fix
      the claim and add a round-trip test with a null struct row. (Claim corrected, failure
      listed under Known Limitations, `@test_broken` pins it; the bug itself is still open.)

---

# Struct (Parquet group) column support

## Part 1 — flat structs (DONE)
- [x] Predicate + `column_names` (src/filereader.jl); specs partition + assembly (src/api.jl); tests

## Part 2 — struct containing list fields (DONE, awaiting review)
- [x] `struct_member_kind` / `_single_leaf_chain`; `is_flat_struct_group` → `is_struct_group` (src/filereader.jl)
- [x] `_to_arrow_nested` gains `record_null_def` threshold (default preserves old top-level behavior)
- [x] `_read_struct_column` / `_assemble_struct_chunk` handle :flat and :list members;
      `_record_defs` for structs with no flat member
- [x] Tests: 4-layer null attribution (struct/list/empty/element), all-list struct, unit test
- [x] Verified on real data `part-0.parquet` (12355 rows, waveform structs) — values match pyarrow exactly
- [x] Full suite green; README + dev-note updated

## Part 2.5 — StructColumn wrapper (DONE)
- [x] `StructColumn <: AbstractVector`: row access delegates to Arrow.Struct/ChainedVector;
      `getproperty` gives named child columns, chunk-chained for multi-RG
- [x] `Arrow.getmetadata` delegation; `Arrow.write` round-trip verified on part-0 data
- [x] Tests updated (Struct Columns 51/51); README + dev-note

## Part 3 — list<struct> + struct-of-struct (DONE, awaiting review)
- [x] `list_of_structs_parts` detection (3-level standard + 2-level legacy, all-flat element)
- [x] `_read_los_column` / `_assemble_los_chunk`: shared offsets/validity from first member,
      per-member child arrays at element granularity → Arrow.List over Arrow.Struct
- [x] `ListOfStructsColumn` wrapper: rows = lazy NamedTuple vectors; `col.pt` = ragged
      per-field list sharing offsets, chunk-chained
- [x] Struct-of-struct: recursive `_plan_struct` / `_assemble_struct_chunk`; validity for all
      nesting levels sliced from one leftmost-leaf `record_defs` vector; named access composes
      (`tbl.event.vertex.x` via `_wrap_struct`)
- [x] Name-collision guard: repeated leaf gets bare top name only if sole leaf under top;
      unsupported shapes (list<struct{list}>, maps) fall back to distinct dotted columns
- [x] Tests (Struct Columns 95/95) + part-0 regression + docs

## Review fixes (Parts 1–2 review, 2026-10-03) — CURRENT FOCUS
Reviewer confirmed def/rep-level arithmetic, eltype stability across row groups, name
handling, and logical types on members. Bugs below are in priority order; one at a time.

### A. Whole struct column dropped (leaf-level cause, struct amplifies: one bad member loses all)
- [x] R1 Narrow/unsigned int member (fixed: `values .% T` in `convert_primitive_values`). `assemble_flat_column` leaves null slots `undef`, then
      `convert_primitive_values` runs a checked `T.(values)` over them
      (src/api.jl:696-697, src/pagereader.jl:186, reached from api.jl:322).
      - `struct<x: int8>` rows `[{x:-5}, None, {x:None}]` → `InexactError`, flaky
      - any `uint32` ≥ 2^31 / `uint64` ≥ 2^63, even without nulls → deterministic
        (`convert(UInt32, -294967296)`; needs reinterpret, not convert)
- [x] R2 (fixed: null element slot left uninitialized, no placeholder value) `list<string>` / binary / large_string / FLBA / decimal member with a null element
      (src/api.jl:660, placeholder push ~:645, in `_to_arrow_nested`).
      - `struct<x: list<string>>` rows `[{x:['hé', None]}, None, {x:None}]`

### B. Wrong or missing result
- [x] R3 (fixed: per-level validity from def thresholds) Null inner list in a `list<list<T>>` member reads as empty (src/api.jl:670;
      intermediate levels hard-coded all-valid).
      - `[[1,2], [], None, [None,3]]` → `[[1,2], [], [], [None,3]]`
- [x] R8 (found while fixing R3) In `list<list<T>>`, a row after an empty or null outer list
      read the wrong inner lists: `[[[1,2],[3]], [], [[4]]]` → third row `[[]]`. The loop pushed
      an inner offset per record even when the record had no inner list. Fixed by rewriting
      the `_to_arrow_nested` loop to push a start offset when a list opens.
- [x] R4 (fixed: `@warn` listing unmatched names; member projection not implemented) `columns=["s.a"]` / `["s.b.c"]` silently returns no columns, no warning
      (src/api.jl:123-127). Old flattened reader returned the dotted column.
- [x] R5 (fixed: `_empty_pages` gives typed empty columns) Zero-row file: `column_names` lists columns but `read_parquet` returns none
      (src/api.jl:507, no row groups). All column kinds, not only structs.

### C. Suspected, not reproduced
- [ ] R6 (left open: no file reproduces it; documented under Known Limitations) LIST group annotated only with `logicalType` (no `converted_type`) would be read
      as a struct with one member `list` (src/filereader.jl:53-61; `logicalType` never parsed).
- [x] R7 Row group with zero rows: confirmed (current pyarrow writes one empty row group for an
      empty table) and fixed together with R5.

### D. Tests to add (alongside the fix they cover)
- [x] narrow + unsigned int members with nulls (R1)
- [x] `list<string>` member with null element (R2); null/empty lists at every level (R3, R8)
- [x] required struct and required members; all-null struct; zero-row file (R5)
- [x] `write_statistics=False` with multiple row groups; multi-RG struct with a list member
- [x] `columns=["s.a"]` (R4)

### E. Docs / tidy (last)
- [x] dev-note says validity comes from "a flat member if any"; code uses member 1 whatever its kind
- [x] this file names `_struct_nulls_at_records`; the code is `_record_defs`
- [x] `is_struct_group` docstring omits nested structs
- [ ] `_plan_struct` fills dummy `leaf`/`path`/`thresholds` for `:struct` members
- [ ] `_page_defs` re-walks levels `assemble_flat_column` already scanned
- [x] Dead code deleted (2026-10-04): `assemble_column`, `assemble_nested_column`, `assemble_nested`,
      `_assemble_nested_rep1`, `_assemble_nested_general`, `convert_fixed_size_list`, `find_column`,
      `parse_page_header`. Their four level-array unit tests now drive the live `_to_arrow_nested`.
- [x] `snulls` loop is just `record_defs .< own_def`

## Part 4 — one recursive reader (plan revision 2; go-ahead relayed 2026-10-04; branch `recursive-reader`)

Requested 2026-10-04 via the planning session; revised the same day for two questions from
the user (member selection through `columns=`; nullability from decoded levels). Goal:
replace the three shape-specific read paths (leaf/list `_assemble_to_arrow`, struct
`_read_struct_column`, list<struct> `_read_los_column`) and the flattened fallback with one
recursion that is the inverse of the writer's `_plan_node` / `_shred!`:
schema tree → plan tree → prune to the selection → recursive assembly.

### Design
Node kinds and what each contributes to the Arrow array:
- **leaf** — values and element validity (`def == max_def`). One slot per level entry with
  `def >=` the def level of the nearest enclosing list item (every entry, outside lists).
- **list** — offsets from repetition levels and validity from definition levels
  (`def >= own_def`); wraps its one child. Covers the standard 3-level LIST, the legacy
  2-level form, a bare repeated field, and MAP (a list of `key_value` structs).
- **struct** — validity only (`def >= own_def`); wraps its children.
- **fixed-size list** — a list node that `ARROW:schema` declares fixed-size with a primitive
  element, at top level or as a struct member (as today). Keeps today's dense fast path.

One leaf's levels serve every ancestor: all leaves under a node carry identical structure
above that node (the Dremel invariant), so each list/struct takes its offsets and validity
from its leftmost leaf, in the single pass that leaf already makes. Every other leaf only
scatters its values into slots. This generalises today's `_to_arrow_nested` and the struct
path's `record_defs` slicing from "record level only" to any depth.

Assembly is two stages, which is what makes the two revisions below cheap:
1. **Buffers**, per row group in parallel: for each node, its raw buffers (values, offsets,
   validity bitmap) and a null count. Nothing here depends on whether a type admits `Missing`.
2. **Wrap**, after all row groups are done: build the Arrow arrays and the `ChainedVector`,
   choosing each node's element type from the joined null counts. This stage only wraps
   buffers; it copies no data.

### Revision 1 — selecting members through `columns=` (agreed: it is pruning)
Leaves are stored independently, so a selection prunes the plan tree: keep the selected
leaves and their ancestors, then assemble as usual. Structure comes from the leftmost
*remaining* leaf, which the Dremel invariant makes equivalent. Unselected leaves are never
decoded, so `columns=["wf.t0"]` does not touch the waveform values.
- Semantics follow pyarrow: `columns=["wf.values"]` returns column `wf` as a struct with only
  `values`; `columns=["particles.pt"]` returns `particles` as a list of structs with only
  `pt`; a key naming a struct or list selects everything under it; overlapping keys union.
- Keys are the short dotted paths the writer's `encoding` keyword uses: no `list`/`element`
  segments, and for a MAP no `key_value` segment (`"m.key"`, `"m.value"`).
- Decided by the user 2026-10-04: keys are the short dotted paths only. No alias for full
  Parquet paths (`particles.list.element.pt`), and no bare leaf names (ambiguous).
- This replaces today's matching by Parquet path or by bare leaf name. An unmatched key is
  an error naming the key(s) (decided by the user 2026-10-04; replaces today's warning once
  the new reader becomes the default at R7).
- Nothing makes this harder than pruning. It also shrinks breaking change 5 below: dotted
  selection keeps working, and returns a pruned nested column instead of a flat one.

### Revision 2 — nullability from decoded levels (agreed: simpler, and lower risk)
Each node's type admits `Missing` if and only if a null actually occurs at that node in the
data read. No rule about `null_count` is written at all; `_column_has_nulls`,
`_group_nullable`, `_flat_descendant_nullables` and the conservative multi-row-group cases
are deleted. My answers to the three points raised:
- (a) Types across row groups: neither "decode everything, then assemble" nor "assemble,
  then promote by re-wrapping" is needed. With the two-stage assembly above, the buffers
  stage returns a null count per node, the counts are joined across row groups, and the wrap
  stage builds every chunk with the agreed type. No extra memory and no re-wrap.
- (b) The dense fixed-size-list path already scans the definition levels to decide it can be
  used (`_fsl_no_nulls`), and every other path already counts nulls while building validity
  (`v.nc`). The flags are by-products that exist today; statistics are only used to make
  chunks agree. So no new scan. The benchmark still gates R3.
- (c) Zero rows: no levels, so no nulls observed, so no `Missing` anywhere: an OPTIONAL
  int64 column in a zero-row file reads with `eltype` `Int64`.
- The writer keeps matching pyarrow's `null_count`, for other readers.

Deliberate change: element types get tighter, never looser. Cases that lose a `Missing`
they do not need:
- a list column with an empty or null list but no null elements (pyarrow counts those in
  the leaf's `null_count`, so today both the list and its elements are `Union{Missing, …}`);
- a struct whose members each contain nulls while the struct itself is never null;
- `list<struct>` across several row groups (today conservatively nullable);
- any multi-row-group file without statistics (today conservatively nullable).
Property to know: a column's element type depends on the data read, so two files with the
same schema can differ in `Missing`. That is already true today through statistics.

Why I judge this simpler, not riskier: it removes the one risk I was least sure about
(reproducing three hand-tuned rules exactly), replaces a writer-dependent heuristic with a
fact, and the oracle becomes checkable from the result alone. The price is that "nothing
changes" becomes "nothing changes except `Missing` where no null exists", which you would
be accepting as a user-visible change in v0.2.0.

### Public result per shape
| Shape | Result | Change |
|---|---|---|
| primitive, string | `Arrow.Primitive` / `Arrow.BoolVector` / string list | none |
| `list<…>` of primitives or lists | `Arrow.List` | none |
| struct | `StructColumn` | none |
| `list<struct>` | `ListOfStructsColumn` | members may now be lists/structs |
| `list<struct{list}>`, `list<struct{struct}>`, `list<list<struct>>` | `ListOfStructsColumn`; `col.f` is that field through every list level, sharing offsets | new (was flattened) |
| struct with a `list<struct>` member | `StructColumn`; that member is a `ListOfStructsColumn`, so `tbl.s.hits.x` composes | new (was flattened) |
| MAP | `ListOfStructsColumn` with fields `key`, `value` (Arrow's own layout for maps); not a `Dict` | new (was flattened) |
"None" means structure and values; `Missing` in element types follows revision 2. The two
wrapper names stay (no API break). Internally they are already one type (`NestedColumn`);
the rule becomes "named field access wherever the element, through any number of list
levels, is a struct".

Fallback: once every list/struct shape assembles, the flattened dotted columns are removed.
What stays unsupported is leaf-level (encodings, the Known Limitations list) plus
FixedSizeList inside a list (still read as variable-length). A schema the planner cannot
classify throws `ColumnReadError`. `column_names` returns the top-level field names.

Own file: yes, `src/reader.jl` for the plan tree, pruning and assembly; `api.jl` keeps the
public API and the wrappers. Not a general split of `api.jl`.

### Harness oracle (revised)
Old path against new path, for every file in the corpus:
- column names, values and structure equal;
- element types equal after stripping `Missing` at every level;
- nullability checked by its own invariant, on the new result alone: at every node, the
  type admits `Missing` if and only if a missing occurs there.
  Refined at R2: a struct member counts as missing wherever its struct is missing, because
  `col.member` returns the member for every row and must show a missing for those rows
  (pyarrow's `flatten` semantics). So a slot is null at a node when `def < node.def_level`,
  for any reason. Counting only "the member itself is null" would give a tighter row type
  but leave `col.member` returning undefined values under null structs.
The cases where the new types are tighter than the old are collected and listed in the
report, so the deliberate change is visible, not inferred.

### Staging — each step ends with a report and waits for approval
Old paths stay the default until R6; the new reader runs beside them behind an internal
entry point.
- [x] R0 (DONE, awaiting review) — regression harness (oracle above) and baseline, no reader change.
      `test/reader_harness.jl`; 80 files locally (26 pyarrow, 2 writer, 51 parquet-testing,
      part-0 local only), 79 in the committed suite. Old reader vs itself: 0 differences;
      its types are loose in 26 files (the preview of what tightens). Corpus: every
      pyarrow fixture in the suite, the parquet-testing files that read, `part-0.parquet`,
      and a writer-produced shape matrix.
      Baseline benchmark (min of 7, 8 threads, 2026-10-04, commit 8614d23):
      all columns 182.6 ms / 1317 MiB; `waveform_windowed` 181.2 ms / 712 MiB;
      `waveform_presummed` 146.3 ms / 539 MiB; `tracelist` 5.0 ms / 12 MiB.
- [x] R1 (DONE, awaiting review) — `src/reader.jl`: `plan_read_tree`, `prune_read_plan` (unmatched key
      throws). Not used by `read_parquet` yet. All 64 parquet-testing schemas plan, and agree
      with the current reader's leaves, levels and list thresholds.
      Original wording: plan tree from the schema (kinds, levels, user paths) and the pruning function.
      Pure functions with unit tests, including legacy 2-level lists, MAP, bare repeated
      fields, and selections (leaf, group, overlapping, unmatched). No assembly.
- [x] R2 (DONE, awaiting review) — `_read_buffers` (stage 1), `_wrap_buffers` (stage 2),
      `_read_parquet_recursive` (internal entry point). 64 corpus files, 345 list-free
      columns: 0 differences, 0 loose nodes; types tighten in 8 files. Flat columns of
      part-0 (local): old 13.7 ms, new 13.9 ms.
      Original wording: two-stage assembly for leaves and structs without lists; nullability from
      levels. Harness on flat columns, structs, struct-of-struct, zero-row files,
      multi-row-group files with nulls in only some groups.
- [x] R3 (DONE, awaiting review) — lists at any depth, struct members that are lists,
      FixedSizeList at top level and as struct member (dense path kept). 74 corpus files,
      423 columns: 0 loose nodes; 1 difference, an old-reader bug (an empty required list
      read as `missing`; the new reader and pyarrow give `[]`).
      Benchmark on part-0 (local, best of 22 interleaved, 8 threads), old → new:
      all columns 199.8 → 196.5 ms; `waveform_windowed` 154.8 → 142.3 ms;
      `waveform_presummed` 108.2 → 110.1 ms; `tracelist` 4.6 → 3.8 ms. No regression.
      `waveform_windowed.t0` alone, pruned: 4.4 ms.
      Deviation from the plan text: structure is computed per node from the leftmost leaf's
      levels (one pass per list or struct), not in one pass for all ancestors. Simpler, and
      the benchmark did not ask for more.
- [x] R4 (DONE) — `list<struct>` with flat members: `_wrap_nested` and a `_member_list` that
      projects a field through any number of list levels (src/api.jl). 79 corpus files, 441
      columns, every column the current reader assembles: no new differences, no loose
      nodes, and `col.field` equal to the current reader's for every named field.
- [x] R5 (DONE, awaiting review) — the new shapes and MAP. No assembly code was needed beyond
      R3/R4: the recursion and `_wrap_nested` already cover them. One planner fix: a MAP
      whose `key_value` has only a key reads as a list of keys, as pyarrow does.
      Verified: pyarrow fixture with `list<struct{list}>`, `list<struct{struct}>`,
      `struct{list<struct>}`, `list<list<struct>>`, map, map of maps, map in a struct (one and
      several row groups): pyarrow finds equal values in our rewrite. Writer round-trip for
      five shapes. Ten parquet-testing nested files equal to pyarrow's values. All 80 corpus
      files read in full with no loose nodes.
      Moved to R7: rewriting the tests that pin the flattened fallback. They test
      `read_parquet`, which keeps the old paths until the switch.
- [x] R6 (DONE) — member selection end to end. 18 selections on the new-shapes fixture
      (members inside lists, structs, lists of lists, maps, several row groups): pyarrow's
      pruned read of the same leaves equals our result in every case. A struct or
      list<struct> with an undecodable member reads when that member is not selected.
      Benchmark (local): `waveform_windowed.t0` alone 4.4 ms / 2 MiB against 142 ms / 701 MiB
      for the whole struct.
      Finding: "as in pyarrow" holds for `pq.ParquetFile(...).read(columns=<leaf paths>)`,
      which returns the pruned nested column. `pq.read_table(columns=["s.a"])` behaves
      differently: it returns a top-level column `a`, and cannot select inside lists.
- [x] R7 (DONE, awaiting review; NOT merged into writer-w1) — `read_parquet` uses the
      recursive reader. Deleted: the three old paths, the flattened fallback, the shape
      classification helpers in `src/filereader.jl`, and statistics-based nullability.
      Reader source (api.jl + filereader.jl + pagereader.jl + reader.jl): 1403 → 1202 lines.
      Tests: the old-versus-new comparisons are retired with the old paths; the fallback
      and unmatched-selection tests are rewritten; the four level-array unit tests now
      drive `_list_structure` / `_scatter_leaf`.
      Final benchmark on part-0 (local; three alternating process pairs, best of 15 each,
      median of the three minima), writer-w1 → recursive-reader: all columns 198.0 → 196.5 ms;
      `waveform_windowed` 165.8 → 166.5 ms; `waveform_presummed` 136.2 → 137.6 ms;
      `tracelist` 5.1 → 4.5 ms. `waveform_windowed.t0` alone: 4.5 ms.
      Task C read failures re-run: none went away. All 13 files fail at the same leaves;
      they are leaf-level (encodings, codecs, page parsing), which this rewrite did not touch.
      (Correction: the parquet-testing directory has 64 `.parquet` files, not 128; the
      earlier "13 of 128" counted two tests per file.)
- R8 (PROPOSED 2026-10-04, revised the same day; awaiting the user; not started) — maps with
      Arrow's convention for what a map is, and our own zero-copy view for how it is shown
      (the FixedSizeListVector / FixedSizeView precedent).
      - Read: a MAP column keeps the columnar layout the reader already builds. `col.key` and
        `col.value` stay as today. `col[i]` returns a lightweight `MapView{K,V} <:
        AbstractDict{K,V}` over that row's keys and values: iteration in storage order
        (duplicates and order preserved), `length`, `keys`, `values`, lookup by linear scan.
        No allocation proportional to the row. `Dict(col[i])` for a real Dict. A key-only
        map stays a list of keys.
      - Write: an element type `<: AbstractDict` is written as a Parquet MAP (group annotated
        MAP, repeated `key_value`, required key, optional value), for `Vector{Dict}` and for a
        map column from `read_parquet` alike, nested or not. A vector of `(key, value)`
        NamedTuples stays a list of structs.
      - Check: whether `Arrow.write` serialises it as an Arrow map through `MapKind` without
        extra code.
      - Tests: pyarrow-written map / map of maps / map in struct / map in list; pyarrow reads
        ours as map type with equal contents; read → write → read; `Arrow.write`; `col[i]`
        does not allocate per entry; duplicate keys and order survive.
      Points against, to weigh before starting:
      - `AbstractDict` expects `get`, `haskey`, `iterate`, `length` at least; a linear-scan
        `get` is O(entries), fine for small maps, surprising for large ones.
      - With duplicate keys, `Dict(col[i])` and `col[i][k]` have to pick one (first or last);
        that choice must be documented.
      - The row type changes from "vector of (key, value) NamedTuples" (R5) to a dict view,
        so R5's map behaviour is not final; nothing is released yet, so no user is affected.
      - `columns=["m.key"]` returns a map without values; that stays a list of keys.
      - A null key is invalid in Parquet; the reader has to decide what a file containing one
        reads as.
      - Whether it is a flag on `NestedColumn` or a small new wrapper: likely a third `kind`
        on `NestedColumn`, since field access is already there.

### Constraints carried from the brief
- Structure and values of everything that assembles today are unchanged.
- Per-row-group parallelism, `ChainedVector` composition with stable element types, existing
  zero-copy, FixedSizeList restoration, logical conversions at every depth, legacy lists,
  zero-row files.

### Risks
1. **Element types tighten** (revision 2). Deliberate and listed, but user-visible: code
   that relied on a `Union{Missing, …}` element type for a column without nulls will see a
   plain type.
2. **Performance.** The dense FixedSizeList path and per-leaf parallelism must survive.
   The recursion has to stay at node level, with typed loops per leaf.
3. **Level arithmetic at depth, and pruning.** Mitigated by the harness, by the writer as
   an independent implementation of the same arithmetic, and by pyarrow (including its
   `columns=` results).
4. **New public surface.** Field access through several list levels, MAP-as-list and
   dotted selection are behaviour users will rely on.
5. **Breaking change.** Shapes that read today as several flattened dotted columns will read
   as one nested column. Selecting them by dotted path still works, but returns the pruned
   nested column and uses user paths, not Parquet paths.

### Size and what I would cut
Still the largest single task so far: it replaces roughly 600 lines of `src/api.jl` and
`src/filereader.jl` with an estimated 350–450. Revision 2 makes it somewhat smaller and
removes its least predictable part; revision 1 adds one pure function and one test step.
To keep v0.2.0 shippable throughout, I would do it on its own branch off `writer-w1`.
Cut: FixedSizeList inside lists; any `Dict` type for maps.

## Review (Part 2)
- `waveform_windowed` / `waveform_presummed` now read as `Arrow.Struct` with fields
  (t0, dt, values); values is `Arrow.List` (zero-copy SubArray views on access).
- Fallback unchanged for unsupported shapes (nested struct, list<struct>, maps).
- Pre-existing, unrelated: byte_stream_split_extended int32/float16/flba5/decimal columns
  cannot be read (BSS decode only supports FLOAT/DOUBLE); since 2026-10-04 that is an error, not a skip.
