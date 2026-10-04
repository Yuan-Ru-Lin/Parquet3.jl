# Parquet file writer (flat, list, and struct columns, arbitrarily nested; PLAIN, uncompressed, one row group)

const CREATED_BY = "Parquet3.jl"

"""Map a Julia element type to (ParquetType, ConvertedType or nothing)."""
function writer_parquet_type(::Type{T}) where T
    T === Union{} && error("write_parquet: column eltype is Missing; use a typed " *
                           "column such as Union{Missing, Int64}[missing, ...]")
    T === Int32   && return (INT32, nothing)
    T === Int64   && return (INT64, nothing)
    # Narrow and unsigned integers: stored in INT32/INT64, annotated with a converted type
    T === Int8    && return (INT32, CT_INT_8)
    T === Int16   && return (INT32, CT_INT_16)
    T === UInt8   && return (INT32, CT_UINT_8)
    T === UInt16  && return (INT32, CT_UINT_16)
    T === UInt32  && return (INT32, CT_UINT_32)
    T === UInt64  && return (INT64, CT_UINT_64)
    T === Float32 && return (FLOAT, nothing)
    T === Float64 && return (DOUBLE, nothing)
    T === Bool    && return (BOOLEAN, nothing)
    T <: AbstractString && return (BYTE_ARRAY, CT_UTF8)
    T === Vector{UInt8} && return (BYTE_ARRAY, nothing)
    error("write_parquet: unsupported column eltype $T " *
          "(supported: signed and unsigned integers up to 64 bits, Float32, Float64, Bool, String, Vector{UInt8}, " *
          "vectors or NamedTuples of those, and Missing unions)")
end

# Vector{UInt8} is a byte string; any other vector element type is a list
_is_list_type(::Type{T}) where T = T !== Union{} && T <: AbstractVector && T !== Vector{UInt8}
_is_struct_type(::Type{T}) where T = T !== Union{} && T <: NamedTuple

"""
Plan the schema subtree for a value of type `FT` named `name`, driven by element type:
`NamedTuple` → group (struct), `AbstractVector` → standard 3-level LIST, anything else →
primitive leaf. Every node is OPTIONAL. `path`, `max_rep`, `max_def` describe the parent.

Returns a node with its `kind`, depth-first `elements` (schema), and all `leaves` below
it. A leaf is one column chunk: path, max levels, and the rep/def levels and non-null
values that `_shred!` appends to.
"""
function _plan_node(name::String, ::Type{FT}, path::Vector{String}, max_rep::Int, max_def::Int) where FT
    T = Base.nonmissingtype(FT)
    path = [path; name]
    max_def += 1
    if _is_struct_type(T)
        isconcretetype(T) && fieldcount(T) > 0 ||
            error("write_parquet: struct $(join(path, '.')) needs a concrete, non-empty NamedTuple type, got $T " *
                  "(rows of differing field types need a typed vector, e.g. @NamedTuple{a::Union{Missing, Int64}}[...])")
        children = [_plan_node(String(f), ft, path, max_rep, max_def) for (f, ft) in zip(fieldnames(T), fieldtypes(T))]
        group = SchemaElement(repetition_type = OPTIONAL, name = name, num_children = Int32(length(children)))
        (kind = :struct, rep_level = max_rep, children = children,
         elements = [group; reduce(vcat, [c.elements for c in children])],
         leaves = reduce(vcat, [c.leaves for c in children]))
    elseif _is_list_type(T)
        # optional group name (LIST) { repeated group list { optional <element> } }
        child = _plan_node("element", eltype(T), [path; "list"], max_rep + 1, max_def + 1)
        (kind = :list, rep_level = max_rep + 1, children = [child],
         elements = [SchemaElement(repetition_type = OPTIONAL, name = name, num_children = Int32(1), converted_type = CT_LIST);
                     SchemaElement(repetition_type = REPEATED, name = "list", num_children = Int32(1));
                     child.elements],
         leaves = child.leaves)
    else
        ptype, ctype = writer_parquet_type(T)
        leaf = (path = path, ptype = ptype, max_rep = max_rep, max_def = max_def,
                rep = Int[], def = Int[], values = T[])
        (kind = :leaf, rep_level = max_rep, children = (),
         elements = [SchemaElement(type = ptype, repetition_type = OPTIONAL, name = name, converted_type = ctype)],
         leaves = [leaf])
    end
end

"""
Shred value `v` into the leaves under `node` (Dremel). `rep` is the repetition level of
this value; `def` counts the optional/repeated ancestors known to be present. A null
or empty value is recorded in every leaf below, at the level where the path stopped.
"""
function _shred!(node, v, rep::Int, def::Int)
    ismissing(v) && return _shred_stop!(node, rep, def)
    def += 1
    if node.kind == :leaf
        leaf = only(node.leaves)
        push!(leaf.rep, rep); push!(leaf.def, def); push!(leaf.values, v)
    elseif node.kind == :struct
        foreach((child, field) -> _shred!(child, field, rep, def), node.children, values(v))
    elseif isempty(v)
        _shred_stop!(node, rep, def)
    else
        # First item inherits rep; later items continue this list
        child = only(node.children)
        for (j, item) in enumerate(v)
            _shred!(child, item, j == 1 ? rep : node.rep_level, def + 1)
        end
    end
    nothing
end

_shred_stop!(node, rep::Int, def::Int) =
    foreach(leaf -> (push!(leaf.rep, rep); push!(leaf.def, def)), node.leaves)

"""
The table's Arrow schema as an `ARROW:schema` key-value entry (base64 of an IPC schema
message), which lets Arrow-based readers restore types Parquet's own schema cannot
express, such as FixedSizeList. Arrow.jl derives the schema from the column element
types, so a zero-row copy of the table is enough; its first stream message is the schema.

Returns `nothing` unless a column is a FixedSizeList: Arrow.jl compiles its schema code
per table type (seconds on a first call), and no other type we write needs the entry.
"""
function _arrow_schema_kv(names::Vector{Symbol}, vectors::Vector)
    any(v -> Base.nonmissingtype(eltype(v)) <: FixedSizeView, vectors) || return nothing
    empties = NamedTuple{Tuple(names)}(Tuple(eltype(v)[] for v in vectors))
    buf = take!(Arrow.tobuffer(empties))
    # Encapsulated message: 0xFFFFFFFF continuation, Int32 metadata length, metadata
    len = ltoh(reinterpret(Int32, buf[5:8])[1])
    [KeyValue(key = "ARROW:schema", value = Base64.base64encode(buf[1:8 + len]))]
end

"""
    write_parquet(path::String, tbl) -> path

Write a Tables.jl-compatible table to a Parquet file. Supported column eltypes:
Int8–Int64, UInt8–UInt64, Float32, Float64, Bool, String, Vector{UInt8}; vectors (written as
LIST) and NamedTuples (written as a struct group) of supported types, nested to any
depth; and `Missing` unions at every level. Columns are written as OPTIONAL fields
with PLAIN encoding, uncompressed, in a single row group.
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
            node = _plan_node(String(name), eltype(col), String[], 0, 0)
            foreach(v -> _shred!(node, v, 0, 0), col)
            append!(schema, node.elements)

            for leaf in node.leaves
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
                             row_groups = [rg], created_by = CREATED_BY,
                             key_value_metadata = _arrow_schema_kv(names, vectors))

        footer = serialize_thrift(fmeta, FILE_METADATA_W)
        write(io, footer)
        write(io, htol(UInt32(length(footer))))
        write(io, PARQUET_MAGIC)
    end
    path
end

"""
Build one DataPage (v1) for a shredded leaf: thrift PageHeader followed by the
length-prefixed RLE repetition levels (only under a list) and definition levels,
then the PLAIN-encoded values.
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
