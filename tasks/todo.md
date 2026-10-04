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

## Part 4 (future, if needed) — full closure: list<struct{list}>, list<struct{struct}>, maps

## Review (Part 2)
- `waveform_windowed` / `waveform_presummed` now read as `Arrow.Struct` with fields
  (t0, dt, values); values is `Arrow.List` (zero-copy SubArray views on access).
- Fallback unchanged for unsupported shapes (nested struct, list<struct>, maps).
- Pre-existing, unrelated: byte_stream_split_extended int32/float16/flba5/decimal columns
  cannot be read (BSS decode only supports FLOAT/DOUBLE); since 2026-10-04 that is an error, not a skip.
