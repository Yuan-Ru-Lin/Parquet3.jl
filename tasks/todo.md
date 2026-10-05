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

- **The dense types do not change.** `FixedSizeView{N,T}` and `FixedSizeListVector{N,T,ET}`
  keep their fields, layout and indexing. The waveform path never sees the new types.
- **Two new types beside them** (src/arrays.jl):
  `NullableFixedSizeListVector{N,T,ET}`: the same flat `Vector{T}`, the list-level nulls,
  plus `element_nulls::BitVector`, one bit per element of the flat vector. No copy of the
  values, no `Vector{Union{Missing,T}}`.
  `NullableFixedSizeView{N,T} <: AbstractVector{Union{Missing,T}}`: the flat vector, the
  element bitmap and an offset; `v[j]` is `missing` when the bit is set.
  A view is (storage, offset); a shape for the v0.3 2-D idea can be added to a view type
  later without touching this.
- **Stage 1** (per row group): the scatter path records the positions of null elements, only
  when the levels show one. The dense path cannot meet one (every level is at its maximum).
- **Stage 2** (the wrap, across row groups): if any row group recorded a null element, every
  chunk of the column is a `NullableFixedSizeListVector` (chunks without one get an all-false
  bitmap); otherwise every chunk is a `FixedSizeListVector`, as today. So chunks agree.
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
- A null element in a fixed-size list reads as `missing` (the column is then a
  `NullableFixedSizeListVector`); it used to read as a silent 0.
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

## Small clean-ups carried over
- [ ] `_read_leaf` scans a flat column's levels twice when its parent needs them:
      `assemble_flat_column` and then `_page_defs` (src/reader.jl). Minor; measure first.

## Dropped from the old list
One clean-up item from the Parts 1–2 review (`_plan_struct` dummy fields) named code that
the recursive reader removed.
