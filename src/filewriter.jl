# Parquet file writer (W1: flat columns, PLAIN encoding, uncompressed, one row group)

const CREATED_BY = "Parquet3.jl"

"""Map a Julia element type to (ParquetType, ConvertedType or nothing)."""
function writer_parquet_type(::Type{T}) where T
    T === Union{} && error("write_parquet: column eltype is Missing; use a typed " *
                           "column such as Union{Missing, Int64}[missing, ...]")
    T === Int32   && return (INT32, nothing)
    T === Int64   && return (INT64, nothing)
    T === Float32 && return (FLOAT, nothing)
    T === Float64 && return (DOUBLE, nothing)
    T === Bool    && return (BOOLEAN, nothing)
    T <: AbstractString && return (BYTE_ARRAY, CT_UTF8)
    T === Vector{UInt8} && return (BYTE_ARRAY, nothing)
    error("write_parquet: unsupported column eltype $T " *
          "(supported: Int32, Int64, Float32, Float64, Bool, String, Vector{UInt8}, and Missing unions)")
end

"""
    write_parquet(path::String, tbl) -> path

Write a Tables.jl-compatible table to a Parquet file. Supported column eltypes:
Int32, Int64, Float32, Float64, Bool, String, Vector{UInt8}, and their `Missing`
unions. Columns are written as OPTIONAL fields with PLAIN encoding, uncompressed,
in a single row group.
"""
function write_parquet(path::String, tbl)
    cols = Tables.columns(tbl)
    names = collect(Symbol, Tables.columnnames(cols))
    isempty(names) && error("write_parquet: table has no columns")
    vectors = [Tables.getcolumn(cols, name) for name in names]
    nrows = length(vectors[1])
    all(v -> length(v) == nrows, vectors) || error("write_parquet: ragged columns")

    open(path, "w") do io
        write(io, PARQUET_MAGIC)

        schema = [SchemaElement(name = "schema", num_children = Int32(length(names)))]
        chunks = ColumnChunk[]
        total_bytes = 0

        for (name, col) in zip(names, vectors)
            ptype, ctype = writer_parquet_type(Base.nonmissingtype(eltype(col)))
            push!(schema, SchemaElement(type = ptype, repetition_type = OPTIONAL,
                                        name = String(name), converted_type = ctype))

            offset = position(io)
            page, null_count = _flat_data_page(col)
            write(io, page)
            total_bytes += length(page)

            meta = ColumnMetaData(
                type = ptype,
                encodings = [PLAIN, RLE],
                path_in_schema = [String(name)],
                codec = UNCOMPRESSED,
                num_values = Int64(nrows),
                total_uncompressed_size = Int64(length(page)),
                total_compressed_size = Int64(length(page)),
                data_page_offset = Int64(offset),
                statistics = Statistics(null_count = Int64(null_count)))
            push!(chunks, ColumnChunk(file_offset = Int64(offset), meta_data = meta))
        end

        rg = RowGroup(columns = chunks, total_byte_size = Int64(total_bytes), num_rows = Int64(nrows))
        fmeta = FileMetaData(version = Int32(1), schema = schema, num_rows = Int64(nrows),
                             row_groups = [rg], created_by = CREATED_BY)

        footer = serialize_thrift(fmeta, FILE_METADATA_W)
        write(io, footer)
        write(io, htol(UInt32(length(footer))))
        write(io, PARQUET_MAGIC)
    end
    path
end

"""
Build one DataPage (v1) for a flat OPTIONAL column: thrift PageHeader followed by
length-prefixed RLE def levels (0 = null, 1 = present) and PLAIN-encoded values.
Returns `(page_bytes, null_count)`.
"""
function _flat_data_page(col::AbstractVector)
    n = length(col)
    def_levels = Int[ismissing(v) ? 0 : 1 for v in col]
    values = collect(skipmissing(col))

    rle = encode_rle_bitpacked(def_levels, 1)
    body = IOBuffer()
    write(body, htol(UInt32(length(rle))))
    write(body, rle)
    write(body, encode_plain(values))
    data = take!(body)

    header = PageHeader(
        type = DATA_PAGE,
        uncompressed_page_size = Int32(length(data)),
        compressed_page_size = Int32(length(data)),
        data_page_header = DataPageHeader(
            num_values = Int32(n), encoding = PLAIN,
            definition_level_encoding = RLE, repetition_level_encoding = RLE))

    (vcat(serialize_thrift(header, PAGE_HEADER_W), data), n - length(values))
end
