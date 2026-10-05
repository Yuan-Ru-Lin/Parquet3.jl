# Parquet3.jl — open work

Everything planned for v0.2.0 is implemented and committed locally on `writer-w1`. The
plans and reports for that work are in `tasks/history.md`.

## Release of v0.2.0 (waiting on the user)
- [ ] Push `writer-w1` (nothing has been pushed; the CI workflows have never run)
- [ ] First CI run green on Julia 1.10 and latest
- [ ] Merge to `main`, bump the version to 0.2.0, write the changelog from the list below, tag

## Null elements in a fixed-size list (audit finding 2; design note, 2026-10-04)
Decision (the user, via the planning session): follow Arrow. An Arrow fixed-size list has a
validity bitmap on the list and a second one on its child; `FixedSizeListVector` has only
the first, so a null element read as 0.

- **One view type and one vector type** (the user, after a first version with two extra
  types): the element null bits are a field whose type is a parameter,
  `FixedSizeView{N, E, T, B}` and `FixedSizeListVector{N, T, ET, B}`. `B === Nothing` for a
  column without a null element: a field of type `Nothing` takes no space and the null
  check is resolved by dispatch, so the dense layout (16-byte view, 24-byte column) and the
  machine code for indexing are what they were. `B === BitVector` otherwise, with
  `E === Union{Missing, T}` over the same flat `Vector{T}`.
  Four parameters on the view because Julia cannot derive a field type or a supertype from
  another parameter: `E` (element type, for `AbstractVector{E}`), `T` (storage) and `B` all
  have to be named. `FixedSizeView{N, E}` is the short form.
  A view is (storage, offset); a shape for the v0.3 2-D idea can be added later.
- **Stage 1** (per row group): the scatter path records the positions of null elements, only
  when the levels show one. The dense path cannot meet one (every level is at its maximum).
- **Stage 2** (the wrap, across row groups): if any row group recorded a null element, every
  chunk of the column has the bitmap (all false for a row group without one); otherwise
  none has. So chunks agree on the type.
- **Arrow.write**: `Arrow.FixedSizeList` over an `Arrow.Primitive` child carrying the element
  validity; buffers reused.
- **write_parquet**: the element type is a vector of `Union{Missing,T}`, which the writer
  already writes as a LIST with nullable elements; `_has_fsl` learns the new view type so
  `ARROW:schema` declares the fixed size.
- [x] Types, reader, Arrow.write, writer; tests against pyarrow at each position, one and
      several row groups; benchmark gate; docs.

## Breaking changes to list in the v0.2.0 changelog (collected as decided)
- Binary (raw-bytes) columns read with element type `Base.CodeUnits{UInt8, String}` instead
  of `Vector{UInt8}`. Only raw-bytes columns are affected; code that mutates the bytes or
  dispatches on `Vector{UInt8}` needs `Vector(x)`.
- Element types admit `Missing` only where a null occurs (see the R7 report for the cases).
- Shapes that read as flattened dotted columns are one nested column; maps are `MapColumn`.
- `columns=` takes short dotted paths; an unmatched key is an `ArgumentError`.
- A column that cannot be read throws `ColumnReadError` instead of being skipped.
- An empty list that cannot be null reads as `[]`, not `missing`.
- UTC and sub-millisecond timestamps read as `Arrow.Timestamp`, not `DateTime`.
- A missing file is a `SystemError`; nothing is created at the path.
- Also for the changelog (not a break): pyarrow cannot read a file in which a fixed-size
  list row is null; `write_parquet` still writes it faithfully.
- A null element in a fixed-size list reads as `missing`; it used to read as a silent 0.
- `FixedSizeView` and `FixedSizeListVector` have more type parameters
  (`FixedSizeView{N, E, T, B}`, `FixedSizeListVector{N, T, ET, B}`). `FixedSizeView{N, T}`
  and `FixedSizeListVector{N, T}` still match with `isa` / `<:` but are no longer concrete
  types. What stops working: `eltype(col) == FixedSizeView{N, T}` (now false; use `<:`) and
  any dispatch or field typed on the two-parameter name expecting a concrete type. What
  still works: `x isa FixedSizeView{N, T}`, `col isa FixedSizeListVector{N, T}`, `<:`, the
  constructor `FixedSizeView{N, T}(parent, offset)`, and writing a column typed with it.
- A fixed-size list of strings or binary reads as a variable-length list; without nulls it
  used to read as a `FixedSizeListVector` of strings.

## Deferred to v0.3 (refreshed 2026-10-04)
- Multiple row groups and multiple pages on write; min/max statistics
- E4 — Dictionary encoding on write (RLE_DICTIONARY). `:dictionary` is not an accepted
  `encoding` name today, and the bit-packed run encoder is not written. Needs: dictionary
  page, index page, `dictionary_page_offset`, bit-packed runs in `encode_rle_bitpacked`;
  no size-based fallback to PLAIN (record as a limitation).
- Read gaps: DELTA_BYTE_ARRAY; BYTE_STREAM_SPLIT beyond FLOAT/DOUBLE; the deprecated LZ4
  codec; string data over 2 GB in one chunk (64-bit offsets)
- Other logical types: LIST-only annotation without a converted type, TIME, INT96 timestamps,
  DECIMAL (read as the raw unscaled bytes today), Float16 (two raw bytes), duration (Int64)
- First-write latency for tables with a FixedSizeList (build the ARROW:schema message
  without Arrow.jl's generic writer)
- Infer struct member types for loosely typed `NamedTuple` / `Dict` literals
- 2-D fixed-size lists (`fixed_size_list<fixed_size_list<T>[M]>[N]`), an edge case that must
  not slow the 1-D waveform path. Design recorded 2026-10-04 (the user's suggestion): treat
  it as ONE flat fixed-size list of N×M values with a shape, and present each row as a
  zero-copy M×N view (a reshaped view, or a shape on the view type). `FixedSizeListVector`'s
  storage and stride logic stay as they are; `FixedSizeView` is NOT made generic over its
  parent. Arrow's canonical "fixed shape tensor" extension type (a flat fixed-size list plus
  a shape in metadata) is the interchange form to look at. Applies only when inner lists
  are never null; otherwise today's behaviour (inner fixed, outer variable).
Done since this list was first written, no longer v0.3: nullability from decoded levels
(shipped with the recursive reader); every nested shape and maps; member selection.


## Test speed (closed 2026-10-04)
- Slimming the two slow groups: closed without changes; the measurements are the result.
  The suite takes about 12 min and is compile-bound. "Arrow.write of what read_parquet
  returns" (3 min 10 s): in its corpus loop, 145 s, Arrow.jl compiling its writer per
  schema takes 74 s, the comparison 36 s, reading 22 s. "Writer" (2 min 04 s): the
  read → write round-trip of two table types takes 33 s, the other 18 sub-groups 0.1–12 s.
  The heavy parts already use one many-column table per schema. A non-specialised
  comparison helper was slower (part-0: 3 s → 45 s). `-O0` cut the loop from 145 s to 80 s
  but was ruled out, as was dropping cases.
- [x] Selective runs instead: `Pkg.test(test_args=[…])`, see dev-note "Running tests".
      Working rule: selective runs while iterating, the full suite before every commit.
- [x] Benchmark gate lightened and written down: dev-note "Benchmark gate",
      `test/benchmark_part0.jl`.

## Upstream to Arrow.jl (not tied to a Parquet3 version)
Parquet3 builds its own Arrow array types, which means implementing Arrow.jl's internal
interface for them. The user's position (2026-10-04): that is a sign the types belong
upstream, where a change to the interface would be made together with them. For v0.2.0 the
overloads stay, contained by the `Arrow = "~2.8.1"` pin. The fix is upstreaming, not removal.
What Parquet3 carries only because Arrow.jl lacks it, and what would go once it lands there:
1. View-based fixed-size lists. `Arrow.FixedSizeList` materialises an `NTuple` per access.
   Would delete: `FixedSizeListVector` / `FixedSizeView` (and the element-null variants),
   their `_arrow_native` conversion and the `ArrowKind` declarations. quinnj welcomed this on
   the v0.1 announcement thread (discourse.julialang.org/t/136295, post 2).
2. Zero-copy map rows. `Arrow.Map` builds a `Dict` per access. Would delete: `MapView`,
   `MapVector` and their conversion.
3. Named, columnar access to struct members, also through list levels. `Arrow.Struct`
   stores members positionally with no access by name. Would delete: `NestedColumn`
   (`StructColumn`, `ListOfStructsColumn`, `MapColumn`, `ListColumn`), `_member_list`.
4. A public way to build Arrow arrays from existing buffers and to tell `Arrow.write` "this
   column already is an Arrow array". Would delete: the calls to the internal positional
   constructors, the `Arrow.arrowvector` method, the `.arrays` property on `NestedColumn`,
   and the tight compat pin.

5. A fixed-size list of `UInt8` that is not fixed-size binary. Arrow.jl turns any
   fixed-size list with element type exactly `UInt8` into `FixedSizeBinary`. Would delete:
   the two workarounds that present the elements as nullable (`_concrete_view` in the
   writer, `_fixed_size_child` for `Arrow.write`).

## After v0.2.0: structure (auditor, 2026-10-05, undecided)
Suggestions from the auditor's reading of e1bfcc1. The user's ruling: they should not keep blocking v0.2; whether and when to do any of them is undecided. Nothing here is started.
1. A seeded random round-trip test in the suite (the auditor's strongest recommendation): about 50 random nested tables, write_parquet → read_parquet, write_parquet → pyarrow, pyarrow rewrite → read_parquet, values compared through a canonical form. The v2-page bug passed the example tests and was found by such a test within minutes. Cost to watch: each new table schema with a fixed-size list pays the 5–20 s Arrow schema compilation, so keep those few. The auditor has a ~100-line script; ask for it when this is taken up.
2. One file for fixed-size lists. The logic is in four places: `assemble_fsl_direct`, `_assemble_fsl_dense`, `_assemble_fsl_slots` in src/reader.jl; the types and Arrow conversion in src/arrays.jl; `_has_fsl` / `_schema_eltype` / `_concrete_view` in src/filewriter.jl; the UInt8 special case in both arrays.jl and filewriter.jl. The auditor's last three findings were all in this code.
3. Inside the reader's fixed-size path: `RawNode` carries element null bits as a fake child node, detected by `!isempty(chunk.children)`; an explicit field would be clearer. `assemble_fsl_direct` looks like the `slot_rep == 0` case of `_assemble_fsl_slots` and keeps parameters from the old reader (`def_thresholds`, `nullable`); possibly mergeable, effect on the waveform path's speed unchecked.
4. Split src/reader.jl at its seams (planning and pruning vs assembly), and move the Arrow.write bridge at the end of src/arrays.jl (`_arrow_native`, `_concat`) to its own file, so what must be rechecked on each Arrow release is in one place.
5. Split test/runtests.jl by its existing group names, one file per group, `Pkg.test` as the only entry point. (Note from implementation: the groups are already named and independent since f1ee2b4, each run alone once, so this would be a move-only change.)
6. Leftovers, each checked against the code on 2026-10-05 and accurate as stated: `get_leaf_columns` is used only by tests (once in src, its definition; twice in test/); `own_def_level` / `own_rep_level` on `SchemaNode` are set in filereader.jl and never read; `ReadContext` is a one-field named tuple passed through four functions; `assemble_flat_column` and `collect_page_data` are assembly code living in src/pagereader.jl and called only from src/reader.jl.
The auditor advised leaving two things as they are: the symbol-tagged node kinds with if-chains, and the hand-written Thrift field tables.

## Small clean-ups carried over
- [ ] `_read_leaf` scans a flat column's levels twice when its parent needs them:
      `assemble_flat_column` and then `_page_defs` (src/reader.jl). Minor; measure first.

## Dropped from the old list
One clean-up item from the Parts 1–2 review (`_plan_struct` dummy fields) named code that
the recursive reader removed.
