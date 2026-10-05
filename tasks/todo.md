# Parquet3.jl — open work

Everything planned for v0.2.0 is implemented and committed locally on `writer-w1`. The
plans and reports for that work are in `tasks/history.md`.

## Release of v0.2.0 (waiting on the user)
- [ ] Push `writer-w1` (nothing has been pushed; the CI workflows have never run)
- [ ] First CI run green on Julia 1.10 and latest
- [ ] Merge to `main`, bump the version to 0.2.0, write the changelog from the list below, tag

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


## Small clean-ups carried over
- [ ] `_read_leaf` scans a flat column's levels twice when its parent needs them:
      `assemble_flat_column` and then `_page_defs` (src/reader.jl). Minor; measure first.

## Dropped from the old list
One clean-up item from the Parts 1–2 review (`_plan_struct` dummy fields) named code that
the recursive reader removed.
