# Known limitations

What Parquet3.jl does not do yet, or does differently from what one might expect. See also [reading.md](reading.md) and [writing.md](writing.md).

## Reading

- A column that cannot be read is an error: `read_parquet` throws `ColumnReadError`, naming the column and keeping the original exception as `cause`; nothing is skipped silently. The other columns, and the other members of the same struct or list, can be read with `columns=`. Columns in the Apache parquet-testing files that currently throw (6 of the 64 files; pinned by the test "every file reads fully or is a known gap"):
  - Not implemented: DELTA_BYTE_ARRAY encoding (`delta_byte_array`, `delta_encoding_optional_column`, `delta_encoding_required_column`: string columns); BYTE_STREAM_SPLIT for fixed-length byte arrays (`byte_stream_split_extended`: float16, fixed-length, decimal); FLOAT, DOUBLE, INT32 and INT64 are decoded.
  - A limit: more than 2 GB of string data in one column chunk overflows the 32-bit offsets (`large_string_map.brotli`: the map's keys; `columns=["arr.value"]` reads).
  - A malformed file, which pyarrow rejects too ("Unexpected end of stream"): `fixed_length_byte_array` declares its column required but its pages contain nulls (page 0 announces 100 values and holds 91), so the nulls cannot be located. It exists to test the page index. PLAIN fixed-length byte arrays themselves are read: `fixed_length_decimal`, `fixed_length_decimal_legacy`, and the `flba5_plain`, `float16_plain` and `decimal_plain` columns of `byte_stream_split_extended`.
  - Fixed for v0.2.0 (2026-10-04): `dictionary_page_offset = 0` (`dict-page-offset-zero`); an empty v2 data section (`datapage_v2_empty_datapage.snappy`); v2 pages that store repetition levels for a non-repeated column, and RLE-encoded booleans (`rle_boolean_encoding`, `datapage_v2.snappy`).
- Of the `logicalType` union only the TIMESTAMP member is parsed; everything else still relies on `converted_type`. A LIST group carrying only `logicalType` would be read as a struct with a single member `list` (not observed in practice; pyarrow writes both). INT96 timestamps and TIME are not converted.
- Types that come back as stored, without conversion:
  - DECIMAL: the unscaled integer. pyarrow stores it as a fixed-length byte array, which reads as a `Vector{UInt8}` of the big-endian two's-complement value (4 bytes for `decimal128(9, 2)`, 13 for `decimal128(30, 2)`); the scale is in the schema only. A decimal stored as INT32 or INT64 reads as that integer.
  - Float16: a two-byte `Vector{UInt8}`, little-endian (`reinterpret(Float16, bytes)[1]`).
  - Other fixed-length byte arrays (UUID, interval): `Vector{UInt8}`.
  - Duration: `Int64` in the file's unit (Parquet has no duration type; the unit is in `ARROW:schema`). TIME: `Int32` or `Int64` in the file's unit. INT96: `Int96`.
- A `FixedSizeList` of primitives is restored wherever `ARROW:schema` declares it: at top level, in structs, in lists and as a map value, and it keeps its size through `write_parquet` and `Arrow.write`. Two cases are not restored; their values are correct, but they read as variable-length lists and are written back as such:
  - the outer level of a fixed-size list of fixed-size lists (`fixed_size_list<fixed_size_list<T>[M]>[N]` reads, and is written back, as `list<fixed_size_list<T>[M]>`);
  - a fixed-size list whose elements are not fixed-width. The rule (`_fixed_width_leaf`): the element's Julia type must be a bits type, which covers integers, floats, `Bool`, `Date`, `DateTime`, `Arrow.Timestamp` and INT96. Strings, binary, fixed-length byte arrays (so decimals and Float16, see below), structs and lists are not.
- A file whose stored lists do not all have the fixed size it declares is malformed, and reading that column is an error ("a list declared fixed_size_list[N] in ARROW:schema …"). Every list is checked, with or without nulls, at top level and inside lists. pyarrow rejects such a file as well.
- A file with a null map key (invalid in Parquet) is not rejected: the key type then admits `Missing` and the entry iterates as `missing => value`. Untested, since pyarrow does not write such files.
- Selecting only a map's keys or only its values (`columns=["m.key"]`) returns a list of one-member structs, not a map.
- Without `ARROW:schema` metadata, `FixedSizeList` columns are read as regular variable-length lists since Parquet's schema does not encode the list size.

## Writing

- The first `write_parquet` call for each new table schema that contains a FixedSizeList (top-level, or nested in structs or lists) takes 5–20 s. The `ARROW:schema` entry is produced by Arrow.jl's generic writer, which Julia compiles per table type. Tables without a FixedSizeList skip that path, and later writes of the same schema in the same session are fast. Hand-building the schema message would avoid it; that was decided against for v0.2.0.
- Parquet stores only a UTC flag for timestamps, not a time zone name. An `Arrow.Timestamp` with a named zone is written as UTC-adjusted and reads back as `:UTC`. pyarrow shows it as `tz=UTC` too, unless the file also has an `ARROW:schema` entry (i.e. a FixedSizeList is present), in which case pyarrow restores the zone name from there. The instants are the same either way.
- Writing back a type that is returned as stored keeps the values and loses the annotation, since the writer only sees bytes or integers: decimals, Float16 and fixed-length binary are written by `write_parquet` as plain binary (pyarrow then sees `binary`, and on re-read the element type is `Base.CodeUnits{UInt8, String}` instead of `Vector{UInt8}`) and by `Arrow.write` as a list of `UInt8`; times and durations are written as plain `Int32`/`Int64`. INT96 columns cannot be written at all (`write_parquet` rejects the element type; Arrow has no 96-bit integer). Checked against pyarrow for decimal128(9,2), Float16, binary(4), time32, duration and INT96.

## Arrow.write

- `Arrow.write` cannot write INT96 columns (Arrow has no 96-bit integer); leave them out with `columns=`.
- A multi-row-group table is split into Arrow record batches through each column's `arrays` property. A struct column with a member named `arrays` shadows it (`tbl.s.arrays` is the member). `Arrow.write` of a multi-row-group table then fails when a plain column comes before that struct column, because Arrow.jl splits the table by the first column's chunks and asks every other column for `arrays`. It works when the struct column is first, or written on its own: the table is then one partition and the chunks are joined. The same holds for a list of such structs and for a struct whose `arrays` member is a list.
- `Arrow.write` of a plain vector of `FixedSizeView{N, UInt8}` rows (for example `collect(col)`, or such rows inside collected lists or maps) produces Arrow fixed-size binary: a plain vector does not go through `_arrow_native`, so Arrow.jl applies its own `UInt8` rule. The column as `read_parquet` returns it, `write_parquet` of collected rows, and a `UInt8` fixed-size list with null elements are not affected.

## pyarrow interop

- pyarrow reads the values of a `map<K, fixed_size_list>` as variable-length lists even from its own files; this package restores the fixed size from `ARROW:schema`.
- pyarrow cannot read a Parquet file in which a fixed-size list is null ("Expected all lists to be of size=N but index i had size=0"), whoever wrote the file, and cannot write one either; this is a limit of pyarrow's Parquet reader, since a null list has no values in Parquet. `write_parquet` keeps the file faithful (decided 2026-10-04): it writes such columns (a null waveform, say) with the fixed size declared, and `read_parquet` reads them back, but pyarrow rejects the file. The same holds when the null is on an ancestor: a null struct that has a fixed-size list member (pyarrow can write that file, cannot read it back, and cannot read our rewrite of it either, which has identical levels). This is the one known exception to "pyarrow reads what we write". Null *elements* inside a fixed-size list are fine in both directions.
