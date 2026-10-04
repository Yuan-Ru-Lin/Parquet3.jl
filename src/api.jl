# High-level API for reading Parquet files

# ── FixedSizeList types ──────────────────────────────────────────────────────

"""Lightweight zero-copy view into a flat array, carrying list size N in the type."""
struct FixedSizeView{N, T} <: AbstractVector{T}
    parent::Vector{T}
    offset::Int  # 0-based; element j is at parent[offset + j]
end

Base.size(::FixedSizeView{N}) where N = (N,)
Base.IndexStyle(::Type{<:FixedSizeView}) = Base.IndexLinear()
@Base.propagate_inbounds function Base.getindex(v::FixedSizeView{N,T}, i::Int) where {N,T}
    @boundscheck checkbounds(v, i)
    @inbounds v.parent[v.offset + i]
end

# Tell Arrow.write this is a FixedSizeList element
ArrowTypes.ArrowKind(::Type{FixedSizeView{N,T}}) where {N,T} = ArrowTypes.FixedSizeListKind{N,T}()

"""Fixed-size list column: flat child array with fixed stride N, plus record-level nulls."""
struct FixedSizeListVector{N, T, ET} <: AbstractVector{ET}
    data::Vector{T}
    nulls::BitVector    # true = record is null
    len::Int
end

Base.size(v::FixedSizeListVector) = (v.len,)
Base.IndexStyle(::Type{<:FixedSizeListVector}) = Base.IndexLinear()
@Base.propagate_inbounds function Base.getindex(v::FixedSizeListVector{N,T,ET}, i::Int) where {N,T,ET}
    @boundscheck checkbounds(v, i)
    v.nulls[i] && return missing
    FixedSizeView{N,T}(v.data, (i - 1) * N)
end

# ── Nested column containers ─────────────────────────────────────────────────

"""
Column wrapper adding named field access to nested Arrow containers, which store
fields positionally. `col[i]` delegates row access to the wrapped array;
`col.fieldname` yields the field's full child column — zero-copy for a single row
group, chained across per-row-group chunks otherwise.
"""
struct NestedColumn{kind, T, fnames, D<:AbstractVector{T}} <: AbstractVector{T}
    _data::D    # Arrow.Struct / Arrow.List-over-Struct, or ChainedVector of chunks
end

"""Struct column: rows are lazy `NamedTuple`s; `col.fieldname` is the child column."""
const StructColumn{T, fnames, D} = NestedColumn{:struct, T, fnames, D}

"""List-of-structs column: rows are lazy vectors of `NamedTuple`s; `col.fieldname`
is a ragged per-field list sharing the parent's offsets and validity."""
const ListOfStructsColumn{T, fnames, D} = NestedColumn{:list_of_structs, T, fnames, D}

StructColumn(data::AbstractVector{T}, fnames::Tuple{Vararg{Symbol}}) where T =
    NestedColumn{:struct, T, fnames, typeof(data)}(data)
ListOfStructsColumn(data::AbstractVector{T}, fnames::Tuple{Vararg{Symbol}}) where T =
    NestedColumn{:list_of_structs, T, fnames, typeof(data)}(data)

Base.size(c::NestedColumn) = size(getfield(c, :_data))
Base.IndexStyle(::Type{<:NestedColumn}) = Base.IndexLinear()
@Base.propagate_inbounds Base.getindex(c::NestedColumn, i::Int) = getfield(c, :_data)[i]
Arrow.getmetadata(c::NestedColumn) = Arrow.getmetadata(_first_chunk(getfield(c, :_data)))

Base.propertynames(::NestedColumn{kind, T, fnames}) where {kind, T, fnames} = fnames
function Base.getproperty(c::NestedColumn{kind, T, fnames}, name::Symbol) where {kind, T, fnames}
    j = findfirst(==(name), fnames)
    j === nothing && return getfield(c, name)
    _child_column(getfield(c, :_data), j)
end

"""Project field `j` out of a nested container: struct → child column, list-over-struct → ragged list."""
_child_column(s::Arrow.Struct, j::Int) = _wrap_struct(s.data[j])
_child_column(l::Arrow.List, j::Int) = _member_list(l, j)
function _child_column(cv::ChainedVector, j::Int)
    first(cv.arrays) isa Arrow.Struct ?
        _wrap_struct(ChainedVector([s.data[j] for s in cv.arrays])) :
        ChainedVector([_member_list(l, j) for l in cv.arrays])
end

"""Ragged view of one struct member: an Arrow.List over the member's child array, sharing offsets/validity."""
function _member_list(l::Arrow.List, j::Int)
    child = l.data.data[j]
    _make_list(child, l.validity, l.offsets.offsets, length(l), Missing <: eltype(l))
end

"""Wrap struct-valued children in StructColumn so named access composes (`tbl.a.b.c`)."""
function _wrap_struct(v::AbstractVector)
    v isa ChainedVector && isempty(v.arrays) && return v
    s = _first_chunk(v)
    s isa Arrow.Struct || return v
    StructColumn(v, _struct_fnames(typeof(s)))
end
_struct_fnames(::Type{<:Arrow.Struct{T, S, fnames}}) where {T, S, fnames} = fnames

_first_chunk(v::AbstractVector) = v
_first_chunk(cv::ChainedVector) = first(cv.arrays)

"""
    read_parquet(path::String; columns=nothing) -> Arrow.Table

Read a Parquet file and return an Arrow.Table (Tables.jl-compatible).
"""
function read_parquet(path::String; columns::Union{Vector{String}, Nothing}=nothing)
    pf = open_parquet(path)
    try
        return read_parquet(pf; columns=columns)
    finally
        close(pf)
    end
end

function read_parquet(pf::ParquetFile; columns::Union{Vector{String}, Nothing}=nothing)
    schema_tree = build_schema_tree(pf.metadata.schema)
    leaf_columns = get_leaf_columns(schema_tree)
    (; schema, fsl, field_meta) = parse_arrow_schema(pf.metadata.key_value_metadata)

    struct_nodes = _struct_top_nodes(schema_tree)
    # Counted before any column filtering so names don't depend on the selection
    leaf_count = _leaf_counts(leaf_columns)

    if columns !== nothing
        # A struct is selected as a whole by its top-level name; other leaves by
        # dotted path, top-level name, or leaf name.
        selects(name, path) = haskey(struct_nodes, path[1]) ? name == path[1] :
            name in (join(path, "."), path[1], path[end])
        unmatched = filter(c -> !any(((path, _),) -> selects(c, path), leaf_columns), columns)
        isempty(unmatched) ||
            @warn "read_parquet: requested columns not found (struct members are selected via their struct, e.g. \"s\" then `tbl.s.a`)" unmatched
        leaf_columns = filter(((path, _),) -> any(c -> selects(c, path), columns), leaf_columns)
    end

    # One spec per output column, in schema order: a (path, node) leaf pair, or the
    # group SchemaNode for a struct assembled from all its member leaves.
    specs = Union{Tuple{Vector{String}, SchemaNode}, SchemaNode}[]
    emitted_structs = Set{String}()
    for (path, node) in leaf_columns
        top = path[1]
        if haskey(struct_nodes, top)
            top in emitted_structs && continue
            push!(emitted_structs, top)
            push!(specs, struct_nodes[top])
        else
            push!(specs, (path, node))
        end
    end

    row_groups = pf.metadata.row_groups

    # Per-column parallelism: each column reads its own row groups from the shared mmap'd data.
    # NUMA optimization opportunity: with ThreadPinning.jl, per-RG decompression/decoding tasks
    # could be pinned to NUMA-local threads, keeping data close to where it's consumed downstream.
    tasks = map(specs) do spec
        Threads.@spawn begin
            if spec isa SchemaNode
                return is_struct_group(spec) ?
                    _read_struct_column(pf.data, row_groups, spec, field_meta, schema_tree, fsl) :
                    _read_los_column(pf.data, row_groups, spec, field_meta)
            end
            path, node = spec
            top_name = path[1]
            name = _leaf_column_name(path, node, leaf_count)
            nullable = _column_has_nulls(row_groups, path, node)

            column = _read_column_chunks(row_groups) do rg
                pages = _read_pages_for_rg(pf.data, rg, path, node)
                _assemble_to_arrow(pages, node, schema_tree, path, fsl, field_meta, top_name; nullable)
            end
            column === nothing && return nothing
            (Symbol(name), column)
        end
    end

    # Collect results in column order
    col_names = Symbol[]
    col_vectors = AbstractVector[]
    for (i, task) in enumerate(tasks)
        result = try
            fetch(task)
        catch e
            spec = specs[i]
            col_name = spec isa SchemaNode ? spec.element.name : join(spec[1], ".")
            @warn "Failed to read column $col_name" exception=(e, catch_backtrace())
            nothing
        end
        result === nothing && continue
        push!(col_names, result[1])
        push!(col_vectors, result[2])
    end

    col_types = Type[eltype(v) for v in col_vectors]
    lookup = Dict{Symbol,AbstractVector}(zip(col_names, col_vectors))
    meta = _parse_kv_metadata(pf.metadata.key_value_metadata)
    schema_ref = schema !== nothing ? Ref(schema) : Ref{Arrow.Meta.Schema}()
    Arrow.Table(col_names, col_types, col_vectors, lookup,
        schema_ref,
        Ref{Union{Nothing,Base.ImmutableDict{String,String}}}(meta))
end

"""
    _column_has_nulls(row_groups, path, node) -> Bool

Check column chunk statistics across all row groups to determine if a column
actually contains nulls. Falls back to schema-based conservative check
(`multi_rg && max_def > 0`) when statistics are unavailable.
"""
function _column_has_nulls(row_groups::Vector{RowGroup}, path::Vector{String}, node::SchemaNode)
    node.max_def_level == 0 && return false
    for rg in row_groups
        idx = findfirst(c -> c.meta_data !== nothing && c.meta_data.path_in_schema == path, rg.columns)
        idx === nothing && continue
        stats = rg.columns[idx].meta_data.statistics
        if stats === nothing || stats.null_count === nothing
            # Statistics unavailable — fall back to conservative multi-RG check.
            # For single-RG files, actual null presence drives the type via any(nulls).
            return length(row_groups) > 1
        end
        stats.null_count > 0 && return true
    end
    false
end

"""Read and decode all pages from one row group's column chunk."""
function _read_pages_for_rg(data::Vector{UInt8}, rg::RowGroup, column_path::Vector{String}, node::SchemaNode)
    idx = findfirst(c -> c.meta_data !== nothing && c.meta_data.path_in_schema == column_path, rg.columns)
    idx === nothing && error("Column chunk not found: $(join(column_path, "."))")
    type_length = Int(something(node.element.type_length, 0))
    reader = ColumnReader(data, rg.columns[idx].meta_data, node, type_length)
    pages = read_all_pages(reader)
    isempty(pages) ? _empty_pages(node) : pages
end

# Zero-row file with no row groups at all
_read_pages_for_rg(::Vector{UInt8}, ::Nothing, ::Vector{String}, node::SchemaNode) = _empty_pages(node)

"""
One empty page of the leaf's physical type, standing in for a column chunk without
pages (zero-row row group or file) so the usual assembly yields typed empty columns.
"""
function _empty_pages(node::SchemaNode)
    type_length = Int(something(node.element.type_length, 0))
    values = decode_plain(node.element.type, UInt8[], 0, type_length)
    [DecodedPage(values, node.max_def_level > 0 ? Int[] : nothing,
                 node.max_rep_level > 0 ? Int[] : nothing, 0)]
end

# ── Struct (group) column assembly ───────────────────────────────────────────
#
# Parquet stores no struct data: a group is pure schema nesting over independently
# stored member leaves. Assembly = read each leaf as usual, then wrap the child
# arrays in Arrow.Struct (columnar: tuple of children + validity bitmap).
# Struct-level nulls are recovered from raw definition levels: a row's struct is
# null iff def < the group's own def level (leaf assembly alone can't distinguish
# "struct is null" from "struct present, field null").

"""
Build a recursive assembly plan for a struct group. Each member entry carries its
kind (:flat, :list, or :struct), leaf node and full schema path, per-column nullable
flag, list thresholds, and — for :struct — a nested plan.
"""
function _plan_struct(gnode::SchemaNode, base_path::Vector{String},
                      row_groups::Vector{RowGroup}, schema_tree::SchemaNode, fsl)
    members = map(gnode.children) do c
        kind = struct_member_kind(c)
        if kind == :struct
            (name = Symbol(c.element.name), kind = kind, leaf = c, path = String[],
             nullable = false, thresholds = Int[], null_def = 0, fsl_size = 0,
             plan = _plan_struct(c, [base_path; c.element.name], row_groups, schema_tree, fsl))
        else
            leaf, names = _single_leaf_chain(c)
            path = [base_path; names]
            (name = Symbol(c.element.name), kind = kind, leaf = leaf, path = path,
             nullable = _column_has_nulls(row_groups, path, leaf),
             thresholds = compute_def_thresholds(schema_tree, path),
             # Min def level for a list member to be present (below it: null).
             # A REQUIRED or REPEATED member field can never itself be null.
             null_def = c.element.repetition_type == OPTIONAL ? c.own_def_level : 0,
             # List size if ARROW:schema declares this member a FixedSizeList, else 0
             fsl_size = get(fsl, join([base_path; c.element.name], "."), 0),
             plan = nothing)
        end
    end

    (fnames = Tuple(m.name for m in members), members = members,
     own_def = gnode.own_def_level,
     nullable = _group_nullable(gnode.own_def_level, _flat_descendant_nullables(members), row_groups))
end

function _flat_descendant_nullables(members)
    out = Bool[]
    for m in members
        m.kind == :flat && push!(out, m.nullable)
        m.kind == :struct && append!(out, _flat_descendant_nullables(m.plan.members))
    end
    out
end

"""Read a struct group column: one Arrow.Struct chunk per row group."""
function _read_struct_column(data::Vector{UInt8}, row_groups::Vector{RowGroup},
                             gnode::SchemaNode, field_meta, schema_tree::SchemaNode, fsl)
    gname = gnode.element.name
    plan = _plan_struct(gnode, [gname], row_groups, schema_tree, fsl)
    meta = get(field_meta, gname, nothing)
    column = _read_column_chunks(rg -> first(_assemble_struct_chunk(data, rg, plan, meta)), row_groups)
    column === nothing && return nothing
    (Symbol(gname), StructColumn(column, plan.fnames))
end

"""
Assemble one row group's member leaves into an Arrow.Struct chunk, decoding members
in parallel and recursing into nested struct members. Returns `(struct_chunk,
record_defs)` where `record_defs` holds the leftmost leaf's def level at each
record — one leaf's def levels encode the nullness of every ancestor group, so each
nesting level extracts its own validity from the same vector by comparing against
its own def level. `record_defs` is only materialized when this level needs it
(`plan.own_def > 0`) or the caller asked for it (`want_defs`).
"""
function _assemble_struct_chunk(data::Vector{UInt8}, rg::Union{RowGroup, Nothing}, plan, meta; want_defs::Bool=false)
    need_defs = want_defs || plan.own_def > 0
    results = fetch.([Threads.@spawn _assemble_member(data, rg, m, need_defs && j == 1)
                      for (j, m) in enumerate(plan.members)])

    children = Tuple(first(r) for r in results)
    record_defs = results[1][2]
    n = length(first(children))
    snulls = plan.own_def > 0 ? record_defs .< plan.own_def : falses(n)
    (_make_struct(children, plan.fnames, snulls, plan.nullable, meta), record_defs)
end

"""Assemble one struct member's child column; optionally also return its record-level def levels."""
function _assemble_member(data::Vector{UInt8}, rg::Union{RowGroup, Nothing}, m, want_defs::Bool)
    m.kind == :struct && return _assemble_struct_chunk(data, rg, m.plan, nothing; want_defs)
    pages = _read_pages_for_rg(data, rg, m.path, m.leaf)
    elem = m.leaf.element
    if m.kind == :flat
        values, nulls = assemble_flat_column(pages, m.leaf.max_def_level)
        converted = convert_primitive_values(values, elem.type, elem.converted_type)
        child = _build_leaf_array(converted, nulls, elem.type, elem.converted_type; nullable=m.nullable)
        (child, want_defs ? _page_defs(pages, m.leaf.max_def_level) : nothing)
    elseif m.fsl_size > 0  # :list declared FixedSizeList — child is a FixedSizeListVector
        max_def = m.leaf.max_def_level
        if _fsl_no_nulls(pages, max_def)
            # Dense fast path; every record is fully defined, so its def level is max_def
            child = _assemble_fsl_dense(pages, elem.type, elem, m.fsl_size; nullable=m.nullable)
            return (child, want_defs ? fill(max_def, length(child)) : nothing)
        end
        all_rep, all_def, raw = collect_page_data(pages, max_def)
        converted = convert_primitive_values(raw, elem.type, elem.converted_type)
        child = assemble_fsl_direct(all_rep, all_def, converted, max_def, m.fsl_size, elem, m.thresholds;
                                    nullable=m.nullable, record_null_def=m.null_def)
        (child, want_defs ? _record_defs(all_rep, all_def) : nothing)
    else  # :list — child column is a regular Arrow.List
        all_rep, all_def, raw = collect_page_data(pages, m.leaf.max_def_level)
        converted = convert_primitive_values(raw, elem.type, elem.converted_type)
        child = _to_arrow_nested(all_rep, all_def, converted, m.leaf.max_def_level,
                                 m.leaf.max_rep_level, m.thresholds, elem.type, elem.converted_type;
                                 nullable=m.nullable, record_null_def=m.null_def)
        (child, want_defs ? _record_defs(all_rep, all_def) : nothing)
    end
end

"""Concatenate def levels across pages (pages without def levels are all-present)."""
function _page_defs(pages::Vector{<:DecodedPage}, max_def::Int)
    total = sum(p.num_values for p in pages; init=0)
    out = Vector{Int}(undef, total)
    pos = 1
    for p in pages
        if p.def_levels === nothing
            fill!(@view(out[pos:pos+p.num_values-1]), max_def)
        else
            copyto!(out, pos, p.def_levels, 1, p.num_values)
        end
        pos += p.num_values
    end
    out
end

"""
Read a List<Struct> column: one Arrow.List-over-Arrow.Struct chunk per row group.

Every member leaf carries an identical copy of the list structure in its rep/def
levels (Dremel guarantees this), so offsets and list/element validity are derived
once from the first member; the remaining members only contribute child arrays.
Def-level layers for `optional g (LIST) { repeated list { optional elem { optional f }}}`:
def < 1 list null, < 2 empty list, < 3 element null, < 4 field null, 4 value.
"""
function _read_los_column(data::Vector{UInt8}, row_groups::Vector{RowGroup},
                          gnode::SchemaNode, field_meta)
    parts = list_of_structs_parts(gnode)
    gname = gnode.element.name
    members = map(parts.elem.children) do c
        path = [parts.prefix; c.element.name]
        (leaf = c, path = path, nullable = _column_has_nulls(row_groups, path, c))
    end
    member_nullable = [m.nullable for m in members]
    elem_optional = parts.elem !== parts.rep && parts.elem.element.repetition_type == OPTIONAL

    # A null list (or null element) nulls every member leaf at that position, so
    # they are only possible when every member column contains nulls (cf. structs).
    plan = (members = members,
            entry_def = parts.rep.own_def_level,              # slot exists at/above this
            elem_null_def = elem_optional ? parts.elem.own_def_level : 0,
            list_null_def = gnode.own_def_level,
            list_nullable = _group_nullable(gnode.own_def_level, member_nullable, row_groups),
            elem_nullable = elem_optional &&
                _group_nullable(parts.elem.own_def_level, member_nullable, row_groups),
            fnames = Tuple(Symbol(c.element.name) for c in parts.elem.children),
            meta = get(field_meta, gname, nothing))

    column = _read_column_chunks(rg -> _assemble_los_chunk(data, rg, plan), row_groups)
    column === nothing && return nothing
    (Symbol(gname), ListOfStructsColumn(column, plan.fnames))
end

"""Assemble one row group's member leaves into an Arrow.List{Arrow.Struct} chunk."""
function _assemble_los_chunk(data::Vector{UInt8}, rg::Union{RowGroup, Nothing}, plan)
    # Decode all member column chunks in parallel
    fetched = fetch.([Threads.@spawn begin
                          pages = _read_pages_for_rg(data, rg, m.path, m.leaf)
                          collect_page_data(pages, m.leaf.max_def_level)
                      end for m in plan.members])

    # Offsets and list/element validity from the first member's levels
    rep1, def1, _ = fetched[1]
    n = count(==(0), rep1)
    nslots = count(>=(plan.entry_def), def1)
    offsets = Vector{Int32}(undef, n + 1)
    list_nulls = falses(n)
    elem_nulls = falses(nslots)
    rec = 0
    slot = 0
    @inbounds for i in eachindex(rep1)
        d = def1[i]
        if rep1[i] == 0
            rec += 1
            offsets[rec] = slot
            d < plan.list_null_def && (list_nulls[rec] = true)
        end
        if d >= plan.entry_def
            slot += 1
            plan.elem_null_def > 0 && d < plan.elem_null_def && (elem_nulls[slot] = true)
        end
    end
    offsets[n + 1] = slot

    # Member child arrays at element (slot) granularity
    children = map(plan.members, fetched) do m, (_, defj, valsj)
        converted = convert_primitive_values(valsj, m.leaf.element.type, m.leaf.element.converted_type)
        mvals, mnulls = _scatter_member(defj, converted, plan.entry_def, m.leaf.max_def_level, nslots)
        _build_leaf_array(mvals, mnulls, m.leaf.element.type, m.leaf.element.converted_type;
                          nullable=m.nullable)
    end

    elem_struct = _make_struct(Tuple(children), plan.fnames, elem_nulls, plan.elem_nullable, nothing)
    lv = _validity(list_nulls)
    _make_list(elem_struct, lv, offsets, n, lv.nc > 0 || plan.list_nullable, plan.meta)
end

"""Function barrier: scatter one member's values into element-slot granularity (type-stable inner loop)."""
function _scatter_member(defs, values::AbstractVector{T}, entry_def::Int, max_def::Int, nslots::Int) where T
    mvals = Vector{T}(undef, nslots)
    mnulls = falses(nslots)
    s = 0
    v = 0
    @inbounds for d in defs
        d >= entry_def || continue
        s += 1
        if d == max_def
            v += 1
            mvals[s] = values[v]
        else
            mnulls[s] = true
        end
    end
    (mvals, mnulls)
end

"""Def levels at record starts (rep == 0) of a repeated member's level streams."""
function _record_defs(all_rep, all_def)
    out = Vector{Int}(undef, count(==(0), all_rep))
    rec = 0
    @inbounds for i in eachindex(all_rep)
        all_rep[i] == 0 || continue
        rec += 1
        out[rec] = all_def[i]
    end
    out
end

"""Assemble one row group's decoded pages into an Arrow array chunk."""
function _assemble_to_arrow(pages::Vector{<:DecodedPage}, node::SchemaNode, schema_tree::SchemaNode,
                            column_path::Vector{String}, fsl_info, field_meta, top_name::String; nullable::Bool=false)
    is_nested = node.max_rep_level > 0
    ptype = node.element.type
    max_def = node.max_def_level
    meta = get(field_meta, top_name, nothing)

    if is_nested
        # Fast path: FSL columns with no nulls — bypass collect_page_data entirely
        if haskey(fsl_info, top_name) && _fsl_no_nulls(pages, max_def)
            return _assemble_fsl_dense(pages, ptype, node.element, fsl_info[top_name]; nullable)
        end

        all_rep, all_def, raw = collect_page_data(pages, max_def)
        converted = convert_primitive_values(raw, ptype, node.element.converted_type)
        def_thresholds = compute_def_thresholds(schema_tree, column_path)

        if haskey(fsl_info, top_name)
            assemble_fsl_direct(all_rep, all_def, converted, max_def, fsl_info[top_name],
                                node.element, def_thresholds; nullable)
        else
            _to_arrow_nested(all_rep, all_def, converted, max_def, node.max_rep_level,
                             def_thresholds, ptype, node.element.converted_type; nullable, meta)
        end
    else
        # All flat columns: bool, bytes, numeric
        values, nulls = assemble_flat_column(pages, max_def)
        converted = convert_primitive_values(values, ptype, node.element.converted_type)
        _build_leaf_array(converted, nulls, ptype, node.element.converted_type; nullable, meta)
    end
end

"""Invert a Parquet nulls BitVector into an Arrow ValidityBitmap."""
function _validity(nulls::BitVector)
    nc = count(nulls)
    nc == 0 && return Arrow.ValidityBitmap(UInt8[], 1, length(nulls), 0)
    bytes = Vector{UInt8}(reinterpret(UInt8, .~nulls.chunks))
    Arrow.ValidityBitmap(bytes, 1, length(nulls), nc)
end

"""
Assemble one chunk per row group in parallel; chain multiple chunks. With no row
groups, assemble a single empty chunk (`rg === nothing`).
"""
function _read_column_chunks(assemble::Function, row_groups::Vector{RowGroup})
    isempty(row_groups) && return assemble(nothing)
    chunks = fetch.([Threads.@spawn assemble(rg) for rg in row_groups])
    length(chunks) == 1 ? only(chunks) : ChainedVector(chunks)
end

"""
Whether a group level (struct, list, or list element) can be null, decided from
member column statistics: a null at the group level nulls every flat leaf below it,
so any zero-null member column rules it out. With no flat-descendant statistics the
call is conservative for multi-row-group files (single-RG eltypes follow the data).
"""
_group_nullable(own_def::Int, flat_nullables::Vector{Bool}, row_groups::Vector{RowGroup}) =
    own_def > 0 && (isempty(flat_nullables) ? length(row_groups) > 1 : all(flat_nullables))

"""Build an Arrow.Struct from child columns and group-null bits, widening eltype when nullable."""
function _make_struct(children::Tuple, fnames, nulls::BitVector, nullable::Bool, meta)
    NT = NamedTuple{fnames, Tuple{map(eltype, children)...}}
    v = _validity(nulls)
    ET = (v.nc > 0 || nullable) ? Union{Missing, NT} : NT
    Arrow.Struct{ET, typeof(children), fnames}(v, children, length(nulls), meta)
end

"""Build an Arrow.List over `child` from 0-based offsets, widening eltype when `withmissing`."""
function _make_list(child::AbstractVector, v::Arrow.ValidityBitmap, offsets::Vector{Int32},
                    n::Integer, withmissing::Bool, meta=nothing)
    ST = SubArray{eltype(child), 1, typeof(child), Tuple{UnitRange{Int64}}, true}
    ET = withmissing ? Union{Missing, ST} : ST
    Arrow.List{ET, Int32, typeof(child)}(UInt8[], v, Arrow.Offsets(UInt8[], offsets), child, Int(n), meta)
end

"""Build the appropriate Arrow leaf array from flat leaf values and nulls."""
function _build_leaf_array(leaf_values::AbstractVector{T}, leaf_nulls::BitVector, ptype, ctype;
                           nullable::Bool=false, meta=nothing) where T
    v = _validity(leaf_nulls)
    has_nulls = v.nc > 0 || nullable
    n = length(leaf_nulls)

    if ptype == BYTE_ARRAY
        # String/bytes: build Arrow.List with flat byte data + offsets
        is_utf8 = ctype == CT_UTF8
        flat = UInt8[]
        offsets = Int32[0]
        for i in 1:n
            if !leaf_nulls[i]
                append!(flat, leaf_values[i])
            end
            push!(offsets, Int32(length(flat)))
        end
        BT = is_utf8 ? String : Vector{UInt8}
        ET = has_nulls ? Union{Missing,BT} : BT
        return Arrow.List{ET, Int32, Vector{UInt8}}(UInt8[], v, Arrow.Offsets(UInt8[], offsets), flat, n, meta)
    elseif ptype == BOOLEAN
        bytes = zeros(UInt8, cld(n, 8))
        for i in 1:n
            if !leaf_nulls[i] && leaf_values[i]
                bytes[((i-1) >> 3) + 1] |= UInt8(1) << ((i-1) & 7)
            end
        end
        ET = has_nulls ? Union{Missing,Bool} : Bool
        return Arrow.BoolVector{ET}(bytes, 1, v, Int64(n), meta)
    else
        ET = has_nulls ? Union{Missing,T} : T
        return Arrow.Primitive(ET, UInt8[], v, leaf_values, n, meta)
    end
end

"""Build Arrow.List directly from rep/def levels — single pass, no intermediate Vector{Vector{T}}."""
function _to_arrow_nested(all_rep, all_def, values::AbstractVector{T}, max_def, max_rep,
                          def_thresholds, ptype, ctype; nullable::Bool=false, meta=nothing,
                          record_null_def::Int = max_def > 0 ? 1 : 0) where T
    # Innermost threshold: min def_level for a leaf element to exist
    inner_threshold = length(def_thresholds) >= max_rep ? def_thresholds[max_rep] : max_rep

    # Per nesting depth k (1 = outermost list): start offsets into the level's child
    # array, the running child count, and nulls of the level-k lists (k = 1: records)
    offsets = [Int32[] for _ in 1:max_rep]
    child_count = zeros(Int32, max_rep)
    level_nulls = [BitVector() for _ in 1:max_rep]

    # Flat leaf data
    leaf_values = T[]
    leaf_nulls = BitVector()
    value_idx = 1

    for i in eachindex(all_rep)
        rep = all_rep[i]
        def = all_def[i]

        # rep = r continues the level-r list, so new lists open at levels r+1..max_rep,
        # each as an item of its parent — as long as that parent item exists.
        for k in (rep + 1):max_rep
            if k == 1
                # Null record: for top-level list columns def == 0; for a list member
                # inside a struct, any def below the list group's own def level.
                push!(level_nulls[1], def < record_null_def)
            else
                def >= def_thresholds[k - 1] || break
                child_count[k - 1] += 1
                # The level-k list group sits one def level below its repeated node
                push!(level_nulls[k], def < def_thresholds[k] - 1)
            end
            push!(offsets[k], child_count[k])
        end

        # Leaf handling
        if def >= inner_threshold
            if def == max_def
                push!(leaf_values, values[value_idx])
                value_idx += 1
            else
                # Null element: slot is never read, leave it uninitialized
                resize!(leaf_values, length(leaf_values) + 1)
            end
            push!(leaf_nulls, def < max_def)
            child_count[max_rep] += 1
        end
    end

    for k in 1:max_rep
        push!(offsets[k], child_count[k])
    end

    # Build bottom-up: leaf array → wrap with List at each level
    child = _build_leaf_array(leaf_values, leaf_nulls, ptype, ctype; nullable)

    for k in max_rep:-1:1
        n = length(offsets[k]) - 1
        v = _validity(level_nulls[k])
        # Field metadata belongs to the top level only
        child = _make_list(child, v, offsets[k], n, v.nc > 0 || nullable, k == 1 ? meta : nothing)
    end

    child
end

"""Parse Parquet key-value metadata into Arrow-compatible ImmutableDict."""
function _parse_kv_metadata(kv::Union{Vector{KeyValue}, Nothing})
    kv === nothing && return nothing
    isempty(kv) && return nothing
    d = Base.ImmutableDict(kv[1].key => something(kv[1].value, ""))
    for i in 2:length(kv)
        d = Base.ImmutableDict(d, kv[i].key => something(kv[i].value, ""))
    end
    d
end

"""Convert primitive values based on Parquet and converted types."""
function convert_primitive_values(values, ptype, ctype)
    if ctype == CT_DATE && ptype == INT32
        [Date(1970, 1, 1) + Day(v) for v in values]
    elseif ctype == CT_TIMESTAMP_MILLIS && ptype == INT64
        [DateTime(1970, 1, 1) + Millisecond(v) for v in values]
    elseif ctype == CT_TIMESTAMP_MICROS && ptype == INT64
        [DateTime(1970, 1, 1) + Microsecond(v) for v in values]
    elseif (T = _converted_int_type(ctype)) !== nothing && T !== eltype(values)
        # Bit-truncating, not checked: unsigned values are stored in signed physical
        # types, and null slots hold arbitrary bits
        values .% T
    else
        values
    end
end

"""Map ConvertedType integer annotations to Julia types. Returns nothing if no conversion needed."""
function _converted_int_type(ctype)
    ctype === nothing && return nothing
    ctype == CT_INT_8   && return Int8
    ctype == CT_INT_16  && return Int16
    ctype == CT_INT_32  && return Int32
    ctype == CT_INT_64  && return Int64
    ctype == CT_UINT_8  && return UInt8
    ctype == CT_UINT_16 && return UInt16
    ctype == CT_UINT_32 && return UInt32
    ctype == CT_UINT_64 && return UInt64
    nothing
end

"""Convert nested values into a FixedSizeListVector (flat child array + record-level nulls)."""
function convert_fixed_size_list(values, nulls::BitVector, elem::SchemaElement, list_size::Int; nullable::Bool=false)
    T = element_julia_type(elem.type, elem.converted_type)
    nrows = length(nulls)
    data = Vector{T}(undef, list_size * nrows)
    val_idx = 0
    @inbounds for row in 1:nrows
        base = (row - 1) * list_size
        if nulls[row]
            for j in 1:list_size; data[base + j] = zero(T); end
        else
            val_idx += 1
            inner = values[val_idx]
            for j in 1:list_size
                v = inner[j]
                data[base + j] = v === missing ? zero(T) : T(v)
            end
        end
    end
    has_nulls = any(nulls) || nullable
    ET = has_nulls ? Union{Missing, FixedSizeView{list_size, T}} : FixedSizeView{list_size, T}
    FixedSizeListVector{list_size, T, ET}(data, nulls, nrows)
end

"""
Direct FSL assembly: scatter values from rep/def levels into a flat buffer
in a single pass, bypassing intermediate Vector{Vector{T}} creation.
"""
function assemble_fsl_direct(all_rep, all_def, values::AbstractVector{V},
                             max_def::Int, list_size::Int, elem::SchemaElement,
                             def_thresholds::Vector{Int}; nullable::Bool=false,
                             record_null_def::Int = max_def > 0 ? 1 : 0) where V
    T = element_julia_type(elem.type, elem.converted_type)
    num_records = count(==(0), all_rep)
    data = Vector{T}(undef, list_size * num_records)
    nulls = falses(num_records)

    inner_threshold = length(def_thresholds) >= 1 ? def_thresholds[1] : 1

    record_idx = 0
    value_idx = 1
    slot_idx = 0  # position within current record's list

    @inbounds for i in eachindex(all_rep)
        rep = all_rep[i]
        def = all_def[i]

        if rep == 0
            # New record
            record_idx += 1
            slot_idx = 0
            base = (record_idx - 1) * list_size

            # Null record: def == 0 for a top-level column; inside a struct, any def
            # below the list member's own def level (struct null or list null)
            if def < record_null_def
                nulls[record_idx] = true
                # Zero-fill the null record's slots
                for j in 1:list_size
                    data[base + j] = zero(T)
                end
                continue
            end
        end

        base = (record_idx - 1) * list_size

        if def == max_def
            slot_idx += 1
            if slot_idx <= list_size
                data[base + slot_idx] = T(values[value_idx])
            end
            value_idx += 1
        elseif def >= inner_threshold
            # Null leaf value
            slot_idx += 1
            if slot_idx <= list_size
                data[base + slot_idx] = zero(T)
            end
        end
    end

    has_nulls = any(nulls) || nullable
    ET = has_nulls ? Union{Missing, FixedSizeView{list_size, T}} : FixedSizeView{list_size, T}
    FixedSizeListVector{list_size, T, ET}(data, nulls, num_records)
end

"""Check if all pages in an FSL column have no nulls (all defs at max or no def levels)."""
function _fsl_no_nulls(pages::Vector{<:DecodedPage}, max_def::Int)
    for page in pages
        if page.def_levels !== nothing
            all(==(max_def), page.def_levels) || return false
        end
    end
    true
end

"""
Dense FSL assembly: when there are no nulls, page values are already contiguous.
Copy them directly into the flat FSL buffer — no rep/def level processing needed.
"""
function _assemble_fsl_dense(pages::Vector{<:DecodedPage}, ptype, elem::SchemaElement,
                             list_size::Int; nullable::Bool=false)
    T = element_julia_type(ptype, elem.converted_type)
    # Count records from rep levels
    num_records = sum(pages) do page
        page.rep_levels === nothing ? page.num_values : count(==(0), page.rep_levels)
    end

    data = Vector{T}(undef, list_size * num_records)
    nulls = falses(num_records)

    # Convert and copy page values in bulk using a function barrier for type stability
    _fsl_dense_copy!(data, pages, ptype, elem.converted_type)

    ET = nullable ? Union{Missing, FixedSizeView{list_size, T}} : FixedSizeView{list_size, T}
    FixedSizeListVector{list_size, T, ET}(data, nulls, num_records)
end

"""Type-stable inner loop: convert page values and copyto! into the flat buffer."""
function _fsl_dense_copy!(data::Vector{T}, pages::Vector{<:DecodedPage}, ptype, ctype) where T
    pos = 1
    for page in pages
        converted = convert_primitive_values(page.values, ptype, ctype)
        n = length(converted)
        if eltype(converted) === T
            copyto!(data, pos, converted, 1, n)
        else
            @inbounds for i in 1:n
                data[pos + i - 1] = T(converted[i])
            end
        end
        pos += n
    end
end


"""Get Julia type for a Parquet primitive type."""
function element_julia_type(ptype, ctype)
    if ctype == CT_UTF8 && ptype == BYTE_ARRAY
        String
    elseif ctype == CT_DATE && ptype == INT32
        Date
    elseif ctype in (CT_TIMESTAMP_MILLIS, CT_TIMESTAMP_MICROS) && ptype == INT64
        DateTime
    elseif (T = _converted_int_type(ctype)) !== nothing
        T
    elseif ptype == INT96
        Int96
    elseif ptype == BOOLEAN
        Bool
    elseif ptype == INT32
        Int32
    elseif ptype == INT64
        Int64
    elseif ptype == FLOAT
        Float32
    elseif ptype == DOUBLE
        Float64
    elseif ptype == BYTE_ARRAY
        Vector{UInt8}
    else
        Any
    end
end

"""
    schema_string(pf::ParquetFile) -> String

Get a human-readable schema representation.
"""
function schema_string(pf::ParquetFile)::String
    lines = String[]

    function format_element(elem::SchemaElement, indent::Int)
        parts = String[]
        elem.repetition_type !== nothing && push!(parts, string(elem.repetition_type))
        elem.type !== nothing && push!(parts, string(elem.type))
        (elem.num_children !== nothing && elem.num_children > 0) && push!(parts, "group")
        push!(parts, elem.name)
        elem.converted_type !== nothing && push!(parts, "($(elem.converted_type))")
        "  "^indent * join(parts, " ")
    end

    function traverse(schema, idx, indent)
        idx > length(schema) && return idx
        elem = schema[idx]
        push!(lines, format_element(elem, indent))

        next_idx = idx + 1
        if elem.num_children !== nothing
            for _ in 1:elem.num_children
                next_idx = traverse(schema, next_idx, indent + 1)
            end
        end
        next_idx
    end

    traverse(pf.metadata.schema, 1, 0)
    join(lines, "\n")
end
