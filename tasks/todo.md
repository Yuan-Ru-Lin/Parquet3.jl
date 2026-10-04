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
- [ ] Still rejected by the writer though the reader returns them: Date/DateTime
      (deferred to v0.3).
- [ ] Release: commit, clean untracked files, bump to 0.2.0, tag, push

- [x] N1.5 (DONE, awaiting review: `_arrow_schema_kv`; our reader and pyarrow both restore
      fixed_size_list; pyarrow cannot write an FSL with null rows to Parquet, so that case
      is untested) — FixedSizeList fidelity (REQUIRED for v0.2.0, decided 2026-10-03): written as a
      plain LIST plus `ARROW:schema` key-value metadata so it reads back as
      `FixedSizeListVector`. Spike first: get the schema message from Arrow.jl (serialize
      the table schema, take the first IPC message) rather than hand-building FlatBuffers;
      confirm our reader and pyarrow both restore fixed_size_list.

- [ ] Follow-up (if it bothers users): first-write latency for tables with a FixedSizeList
      column (5–20 s, Arrow.jl compilation in `_arrow_schema_kv`). Needs a schema path that
      avoids Arrow.jl's generic writer.

Open: compression — in v0.2.0 or v0.3?

Deferred (edge case, decided 2026-10-03): infer struct member types from rows when the
`NamedTuple` eltype is not concrete (`[(a = missing,), (a = 2,)]`); currently a clear error.

## Deferred to v0.3 — multi-RG, Date/DateTime, min/max stats

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
- [ ] `assemble_column` / `assemble_nested*` in src/pagereader.jl are reached only from unit
      tests, not the read path; `_assemble_nested_general` likely has the R8 offset bug. Delete or fix.
- [x] `snulls` loop is just `record_defs .< own_def`

## Part 4 (future, if needed) — full closure: list<struct{list}>, list<struct{struct}>, maps

## Review (Part 2)
- `waveform_windowed` / `waveform_presummed` now read as `Arrow.Struct` with fields
  (t0, dt, values); values is `Arrow.List` (zero-copy SubArray views on access).
- Fallback unchanged for unsupported shapes (nested struct, list<struct>, maps).
- Pre-existing, unrelated: byte_stream_split_extended int32/float16/flba5/decimal columns
  warn-and-skip (BSS decode only supports FLOAT/DOUBLE).
