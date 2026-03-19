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

## Known Limitations

- Read-only. No write support.
- Without `ARROW:schema` metadata, `FixedSizeList` columns are read as regular variable-length lists since Parquet's schema does not encode the list size.
- LZ4 Hadoop framing (used by older Spark/Hadoop writers) is implemented but not tested end-to-end — only the standard LZ4 raw/frame format is covered by the test suite.
- `open_parquet` / `read_parquet` on a non-existent path gives "File too small" instead of "File not found" (Mmap.mmap silently creates an empty file). Needs a guard in the public API.
