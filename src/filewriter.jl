# Parquet file writer (flat, List<primitive>, and flat-struct columns; PLAIN, uncompressed, one row group)

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
          "(supported: Int32, Int64, Float32, Float64, Bool, String, Vector{UInt8}, " *
          "vectors or NamedTuples of those, and Missing unions)")
end

# Vector{UInt8} is a byte string; any other vector element type is a list
_is_list_type(::Type{T}) where T = T !== Union{} && T <: AbstractVector && T !== Vector{UInt8}

"""
Shred one column into its schema elements and leaves. Each leaf is one column chunk:
its path, repetition/definition levels (`rep === nothing` when not repeated), and
non-null values.
"""
function _shred(name::String, col::AbstractVector)
    T = Base.nonmissingtype(eltype(col))
    T !== Union{} && T <: NamedTuple && return _shred_struct(name, col, T)
    _is_list_type(T) ? _shred_list(name, col, Base.nonmissingtype(eltype(T))) : _shred_flat(name, col, T)
end

function _shred_flat(name::String, col::AbstractVector, ::Type{T}) where T
    ptype, ctype = writer_parquet_type(T)
    (elements = [SchemaElement(type = ptype, repetition_type = OPTIONAL, name = name, converted_type = ctype)],
     leaves = [(path = [name], ptype = ptype, max_rep = 0, max_def = 1,
                rep = nothing, def = Int[ismissing(v) ? 0 : 1 for v in col],
                values = collect(skipmissing(col)))])
end

"""
List<primitive> in the standard 3-level layout
`optional group name (LIST) { repeated group list { optional T element } }`.
Definition levels: 0 = null list, 1 = empty list, 2 = null element, 3 = value.
"""
function _shred_list(name::String, col::AbstractVector, ::Type{E}) where E
    _is_list_type(E) && error("write_parquet: nested lists are not yet supported (column $name)")
    ptype, ctype = writer_parquet_type(E)
    rep, def, values = Int[], Int[], E[]
    for list in col
        if ismissing(list) || isempty(list)
            push!(rep, 0)
            push!(def, ismissing(list) ? 0 : 1)
            continue
        end
        for (j, v) in enumerate(list)
            push!(rep, j == 1 ? 0 : 1)
            push!(def, ismissing(v) ? 2 : 3)
            ismissing(v) || push!(values, v)
        end
    end
    (elements = [SchemaElement(repetition_type = OPTIONAL, name = name, num_children = Int32(1), converted_type = CT_LIST),
                 SchemaElement(repetition_type = REPEATED, name = "list", num_children = Int32(1)),
                 SchemaElement(type = ptype, repetition_type = OPTIONAL, name = "element", converted_type = ctype)],
     leaves = [(path = [name, "list", "element"], ptype = ptype, max_rep = 1, max_def = 3,
                rep = rep, def = def, values = values)])
end

"""
Struct of flat fields: `optional group name { optional T1 f1; optional T2 f2; ... }`,
one leaf per member. Definition levels: 0 = null struct, 1 = null member, 2 = value.
"""
function _shred_struct(name::String, col::AbstractVector, ::Type{NT}) where NT <: NamedTuple
    isconcretetype(NT) && fieldcount(NT) > 0 ||
        error("write_parquet: struct column $name needs a concrete, non-empty NamedTuple eltype, got $NT " *
              "(rows of differing field types need a typed vector, e.g. @NamedTuple{a::Union{Missing, Int64}}[...])")
    members = map(fieldnames(NT), fieldtypes(NT)) do f, FT
        T = Base.nonmissingtype(FT)
        (_is_list_type(T) || (T !== Union{} && T <: NamedTuple)) &&
            error("write_parquet: nested members are not yet supported (column $name, field $f)")
        ptype, ctype = writer_parquet_type(T)
        member = [ismissing(row) ? missing : row[f] for row in col]
        (element = SchemaElement(type = ptype, repetition_type = OPTIONAL, name = String(f), converted_type = ctype),
         leaf = (path = [name, String(f)], ptype = ptype, max_rep = 0, max_def = 2, rep = nothing,
                 def = Int[ismissing(row) ? 0 : ismissing(v) ? 1 : 2 for (row, v) in zip(col, member)],
                 values = collect(T, skipmissing(member))))
    end
    group = SchemaElement(repetition_type = OPTIONAL, name = name, num_children = Int32(length(members)))
    (elements = [group; [m.element for m in members]], leaves = [m.leaf for m in members])
end

"""
    write_parquet(path::String, tbl) -> path

Write a Tables.jl-compatible table to a Parquet file. Supported column eltypes:
Int32, Int64, Float32, Float64, Bool, String, Vector{UInt8}, vectors of those
(written as LIST), NamedTuples of those (written as a struct group), and `Missing`
unions at either level. Columns are written as OPTIONAL fields with PLAIN encoding,
uncompressed, in a single row group.
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
            shredded = _shred(String(name), col)
            append!(schema, shredded.elements)

            for leaf in shredded.leaves
                offset = position(io)
                page = _data_page(leaf)
                write(io, page)
                total_bytes += length(page)

                meta = ColumnMetaData(
                    type = leaf.ptype,
                    encodings = [PLAIN, RLE],
                    path_in_schema = leaf.path,
                    codec = UNCOMPRESSED,
                    num_values = Int64(length(leaf.def)),
                    total_uncompressed_size = Int64(length(page)),
                    total_compressed_size = Int64(length(page)),
                    data_page_offset = Int64(offset),
                    # Every level entry without a value, as pyarrow counts it
                    statistics = Statistics(null_count = Int64(length(leaf.def) - length(leaf.values))))
                push!(chunks, ColumnChunk(file_offset = Int64(offset), meta_data = meta))
            end
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
Build one DataPage (v1) for a shredded leaf: thrift PageHeader followed by the
length-prefixed RLE repetition levels (repeated columns only) and definition
levels, then the PLAIN-encoded values.
"""
function _data_page(leaf)
    body = IOBuffer()
    for (levels, max_level) in ((leaf.rep, leaf.max_rep), (leaf.def, leaf.max_def))
        max_level == 0 && continue
        rle = encode_rle_bitpacked(levels, ndigits(max_level, base = 2))
        write(body, htol(UInt32(length(rle))))
        write(body, rle)
    end
    write(body, encode_plain(leaf.values))
    data = take!(body)

    header = PageHeader(
        type = DATA_PAGE,
        uncompressed_page_size = Int32(length(data)),
        compressed_page_size = Int32(length(data)),
        data_page_header = DataPageHeader(
            num_values = Int32(length(leaf.def)), encoding = PLAIN,
            definition_level_encoding = RLE, repetition_level_encoding = RLE))

    vcat(serialize_thrift(header, PAGE_HEADER_W), data)
end
