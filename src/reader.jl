# Recursive reader (Part 4): the read plan — the inverse of the writer's `_plan_node`.
# Schema tree → plan tree → prune to the selected columns. Assembly follows in later steps;
# nothing here is used by `read_parquet` yet.

"""
One node of the read plan. Three kinds, mirroring the writer:

- `:leaf`   — a primitive column chunk; contributes values and element validity.
- `:list`   — one child (its element); contributes offsets and validity. Covers the
  standard 3-level LIST, the legacy 2-level forms, a bare repeated field, and MAP
  (a list of key/value structs).
- `:struct` — its members; contributes validity only.

`key` is the path a user types (`"wf.values"`, `"particles.pt"`, `"m.key"`): it has no
segment for a list's structural `list`/`element` groups or a map's `key_value` group, the
same convention as the writer's `encoding` keyword. `path` is the Parquet path of the
schema node the kind was read from (for a leaf, its column path).

Levels, all cumulative definition/repetition levels as in the schema tree: the node is
non-null when `def >= def_level`; `rep_level` counts the enclosing lists, a list included;
for a list, an item exists when `def >= item_def`.
"""
struct ReadNode
    kind::Symbol
    name::String
    key::String
    path::Vector{String}
    def_level::Int
    rep_level::Int
    item_def::Int
    schema::SchemaNode
    children::Vector{ReadNode}
end

_is_repeated(node::SchemaNode) = node.element.repetition_type == REPEATED
_is_map(node::SchemaNode) = node.element.converted_type in (CT_MAP, CT_MAP_KEY_VALUE)

"""Plan the top-level columns of a schema tree."""
plan_read_tree(root::SchemaNode) = [_plan_read(child, String[], "") for child in root.children]

"""
Plan `node` as a field named after it under `parent_path` / `parent_key`. A repeated node
is a list of itself; anything else is planned by `_plan_value`.
"""
function _plan_read(node::SchemaNode, parent_path::Vector{String}, parent_key::String)
    name = node.element.name
    path = [parent_path; name]
    key = isempty(parent_key) ? name : string(parent_key, ".", name)
    _is_repeated(node) || return _plan_value(node, name, key, path)
    # A repeated field outside a LIST/MAP wrapper: a list that cannot be null, whose
    # elements are the node itself, taken as required.
    ReadNode(:list, name, key, path, node.max_def_level - 1, node.max_rep_level, node.max_def_level,
             node, [_plan_value(node, name, key, path)])
end

"""
Plan `node` as a value: a leaf, a list (LIST or MAP group), or a struct. Also used for the
item of a repeated node, which is present exactly when the item exists: its
`max_def_level` is then the item's level, so the same rule gives a node that is never null.
"""
function _plan_value(node::SchemaNode, name::String, key::String, path::Vector{String})
    isempty(node.children) && return ReadNode(:leaf, name, key, path, node.max_def_level,
                                              node.max_rep_level, 0, node, ReadNode[])
    wrapped = node.element.converted_type == CT_LIST || _is_map(node)
    if wrapped && length(node.children) == 1 && _is_repeated(only(node.children))
        rep = only(node.children)
        return ReadNode(:list, name, key, path, node.max_def_level, rep.max_rep_level, rep.max_def_level,
                        node, [_plan_element(node, rep, name, key, [path; rep.element.name])])
    end
    # Any other group is a struct. That includes a group annotated LIST or MAP without the
    # single repeated child the annotation requires: its children are read as they are.
    ReadNode(:struct, name, key, path, node.max_def_level, node.max_rep_level, 0, node,
             [_plan_read(child, path, key) for child in node.children])
end

"""
The element of LIST/MAP group `list` with repeated child `rep` (at `path`), following the
format's backward-compatibility rules: the repeated node is itself the element when it is
a primitive, has several fields, or carries a legacy name (`array`, `<list>_tuple`), and
always for a map; otherwise its single child is the element (the standard 3-level layout).
Structural groups add nothing to the user key.
"""
function _plan_element(list::SchemaNode, rep::SchemaNode, name::String, key::String, path::Vector{String})
    legacy = isempty(rep.children) || length(rep.children) > 1 || _is_map(list) ||
             rep.element.name in ("array", list.element.name * "_tuple")
    legacy && return _plan_value(rep, name, key, path)
    element = only(rep.children)
    path = [path; element.element.name]
    _is_repeated(element) ?
        ReadNode(:list, name, key, path, element.max_def_level - 1, element.max_rep_level, element.max_def_level,
                 element, [_plan_value(element, name, key, path)]) :
        _plan_value(element, name, key, path)
end

"""All leaves under `node`, in schema order."""
read_leaves(node::ReadNode) = node.kind == :leaf ? [node] : reduce(vcat, map(read_leaves, node.children); init = ReadNode[])

"""
Compact description of a plan node for tests and debugging, e.g.
`wf: struct@1{t0: leaf@2, values: list@2/3<leaf@4>}` (`@def_level`, lists `@def_level/item_def`).
"""
function read_plan_string(node::ReadNode; top::Bool = true)
    body = node.kind == :leaf ? "leaf@$(node.def_level)" :
           node.kind == :list ? "list@$(node.def_level)/$(node.item_def)<$(read_plan_string(only(node.children); top = false))>" :
           "struct@$(node.def_level){" * join(("$(c.name): $(read_plan_string(c; top = false))" for c in node.children), ", ") * "}"
    top ? "$(node.name): $body" : body
end

"""
Prune the plan to the selected `columns` (user keys). A key naming a struct or list keeps
everything under it; a key naming a member keeps that member and its ancestors, so the
column comes back with only the selected parts, as in pyarrow. Leaves that are not
selected are dropped from the plan and are never decoded. Columns stay in schema order.
A key that matches nothing is an error.
"""
function prune_read_plan(nodes::Vector{ReadNode}, columns::AbstractVector{<:AbstractString})
    leaf_keys = [leaf.key for node in nodes for leaf in read_leaves(node)]
    unmatched = filter(c -> !any(k -> _key_covers(String(c), k), leaf_keys), columns)
    isempty(unmatched) ||
        throw(ArgumentError("read_parquet: no column matches $(join(repr.(unmatched), ", ")) " *
                            "(top-level columns: $(join((n.name for n in nodes), ", ")); " *
                            "members are selected by dotted path, e.g. \"wf.values\")"))
    ReadNode[p for p in (_prune(node, columns) for node in nodes) if p !== nothing]
end

function _prune(node::ReadNode, columns)
    any(c -> _key_covers(String(c), node.key), columns) && return node
    children = ReadNode[p for p in (_prune(child, columns) for child in node.children) if p !== nothing]
    isempty(children) ? nothing :
        ReadNode(node.kind, node.name, node.key, node.path, node.def_level, node.rep_level, node.item_def, node.schema, children)
end

# ── Assembly ──────────────────────────────────────────────────────────────
#
# Two stages. Stage 1 runs per row group, in parallel, and produces each node's raw
# buffers; nothing in it depends on whether a type admits `Missing`. Stage 2 runs once all
# row groups are in: it joins what was observed and wraps the buffers in Arrow arrays, so
# every chunk of a column gets the same type. A node's type admits `Missing` exactly when
# a null was decoded at that node, in any row group; statistics are not consulted.
#
# A slot is null at a node when `def < node.def_level`, whatever the reason: the node
# itself is null, or an ancestor is. A struct member is therefore missing wherever its
# struct is, which is what `col.member` shows for those rows.
#
# A column whose elements are structs, directly or through list levels, is returned in a
# wrapper with named field access (`_wrap_nested` in api.jl).

"""Buffers of one plan node for one row group (stage 1)."""
struct RawNode
    values::Any                 # leaf: values, one per slot; fixed-size list: its flat vector; otherwise `nothing`
    nulls::BitVector            # per slot: null at this node
    offsets::Vector{Int32}      # list: start of each slot's items in the child, plus the end; otherwise empty
    children::Vector{RawNode}
end

"""
The definition and repetition levels of a node's leftmost leaf. All leaves under a node
carry the same structure above it, so one leaf's levels give the offsets and validity of
every ancestor. `rep === nothing` means one entry per row: the leaf is outside every list
(or is a fixed-size list, which reports one entry per row for itself).
"""
struct Levels
    rep::Union{Vector{Int}, Nothing}
    def::Vector{Int}
end

# Per-file context for stage 1
const ReadContext = @NamedTuple{data::Vector{UInt8}, fsl::Dict{String, Int}}

"""
Stage 1 for `node` in row group `rg` (`nothing` for a file without row groups).

A node has one slot per item of the nearest enclosing list, or one per row outside lists.
`slot_rep` and `slot_def` describe that list: a level entry starts a slot when
`rep <= slot_rep && def >= slot_def` (both 0 outside lists, where every row is a slot).

Returns the node's buffers and, when `want_levels`, its leftmost leaf's levels.
"""
function _read_buffers(ctx::ReadContext, rg::Union{RowGroup, Nothing}, node::ReadNode,
                       slot_rep::Int, slot_def::Int, want_levels::Bool)
    if node.kind == :leaf
        return _read_leaf(ctx, rg, node, slot_rep, slot_def, want_levels)
    elseif node.kind == :struct
        # A struct that cannot be null at its slots (def_level == slot_def) needs no levels of its own
        need = want_levels || node.def_level > slot_def
        results = fetch.([Threads.@spawn _read_buffers(ctx, rg, child, slot_rep, slot_def, need && j == 1)
                          for (j, child) in enumerate(node.children)])
        children = RawNode[first(r) for r in results]
        levels = results[1][2]
        nulls = need ? _slot_nulls(levels, slot_rep, slot_def, node.def_level) : falses(length(first(children).nulls))
        return (RawNode(nothing, nulls, Int32[], children), levels)
    end
    # A list that ARROW:schema declares fixed-size, with a primitive element and outside other lists
    child = only(node.children)
    if slot_rep == 0 && child.kind == :leaf && haskey(ctx.fsl, node.key)
        return _read_fixed_size_list(ctx, rg, node, child, ctx.fsl[node.key], want_levels)
    end
    raw_child, levels = _read_buffers(ctx, rg, child, node.rep_level, node.item_def, true)
    offsets, nulls = _list_structure(levels, slot_rep, slot_def, node)
    (RawNode(nothing, nulls, offsets, RawNode[raw_child]), levels)
end

function _read_leaf(ctx::ReadContext, rg, node::ReadNode, slot_rep::Int, slot_def::Int, want_levels::Bool)
    pages = _read_pages_for_rg(ctx.data, rg, node.path, node.schema)
    elem = node.schema.element
    if node.rep_level == 0
        values, nulls = assemble_flat_column(pages, node.def_level)
        converted = convert_primitive_values(values, elem.type, leaf_annotation(elem))
        return (RawNode(converted, nulls, Int32[], RawNode[]),
                want_levels ? Levels(nothing, _page_defs(pages, node.def_level)) : nothing)
    end
    rep, def, raw = collect_page_data(pages, node.def_level)
    converted = convert_primitive_values(raw, elem.type, leaf_annotation(elem))
    values, nulls = _scatter_leaf(converted, def, slot_def, node.def_level)
    (RawNode(values, nulls, Int32[], RawNode[]), want_levels ? Levels(rep, def) : nothing)
end

"""
Place a list leaf's values in its slots. Every level entry belongs to this leaf's
innermost list, so an entry is a slot exactly when `def >= slot_def`; it holds a value
when `def == max_def`. Without null elements the values already are the slots.
"""
function _scatter_leaf(values::AbstractVector{T}, def::Vector{Int}, slot_def::Int, max_def::Int) where T
    nslots = count(>=(slot_def), def)
    nslots == length(values) && return (values, falses(nslots))
    out = Vector{T}(undef, nslots)
    nulls = falses(nslots)
    slot = value = 0
    @inbounds for d in def
        d >= slot_def || continue
        slot += 1
        if d == max_def
            out[slot] = values[value += 1]
        else
            nulls[slot] = true      # the slot's value is never read
        end
    end
    (out, nulls)
end

"""Null bits of a node at its slots: `def < def_level`."""
function _slot_nulls(levels::Levels, slot_rep::Int, slot_def::Int, def_level::Int)
    rep, def = levels.rep, levels.def
    rep === nothing && return def .< def_level
    nulls = BitVector()
    @inbounds for i in eachindex(def)
        rep[i] <= slot_rep && def[i] >= slot_def && push!(nulls, def[i] < def_level)
    end
    nulls
end

"""
Offsets and null bits of list `node` from its leftmost leaf's levels, in one pass: an
entry that starts a slot of the list records where its items begin; an entry with
`rep <= node.rep_level && def >= node.item_def` is one item.
"""
function _list_structure(levels::Levels, slot_rep::Int, slot_def::Int, node::ReadNode)
    rep, def = levels.rep, levels.def
    offsets, nulls = Int32[], BitVector()
    items = Int32(0)
    @inbounds for i in eachindex(def)
        r, d = rep[i], def[i]
        if r <= slot_rep && d >= slot_def
            push!(offsets, items)
            push!(nulls, d < node.def_level)
        end
        r <= node.rep_level && d >= node.item_def && (items += Int32(1))
    end
    push!(offsets, items)
    (offsets, nulls)
end

"""
A fixed-size list, read into one flat vector. Without nulls the page values are copied
straight in (the dense path); otherwise they are scattered by level. It reports one level
entry per row, since nothing above it needs to look inside.
"""
function _read_fixed_size_list(ctx::ReadContext, rg, node::ReadNode, leaf::ReadNode, size::Int, want_levels::Bool)
    pages = _read_pages_for_rg(ctx.data, rg, leaf.path, leaf.schema)
    elem = leaf.schema.element
    max_def = leaf.def_level
    if _fsl_no_nulls(pages, max_def)
        column = _assemble_fsl_dense(pages, elem.type, elem, size)
        defs = want_levels ? fill(max_def, length(column)) : nothing
    else
        rep, def, raw = collect_page_data(pages, max_def)
        converted = convert_primitive_values(raw, elem.type, leaf_annotation(elem))
        column = assemble_fsl_direct(rep, def, converted, max_def, size, elem, [node.item_def];
                                     record_null_def = node.def_level)
        defs = want_levels ? _record_defs(rep, def) : nothing
    end
    (RawNode(column, column.nulls, Int32[], RawNode[]), want_levels ? Levels(nothing, defs) : nothing)
end

"""
Stage 2: wrap the buffers of `node` — one `RawNode` per row group — in Arrow arrays, one
per row group and all of the same type.
"""
function _wrap_buffers(node::ReadNode, chunks::Vector{RawNode}, meta)
    nullable = any(chunk -> any(chunk.nulls), chunks)
    if node.kind == :leaf
        elem = node.schema.element
        annotation = leaf_annotation(elem)
        return [_build_leaf_array(chunk.values, chunk.nulls, elem.type, annotation; nullable, meta) for chunk in chunks]
    elseif node.kind == :struct
        members = [_wrap_buffers(child, RawNode[chunk.children[j] for chunk in chunks], nothing)
                   for (j, child) in enumerate(node.children)]
        fnames = Tuple(Symbol(child.name) for child in node.children)
        return [_make_struct(Tuple(member[i] for member in members), fnames, chunk.nulls, nullable, meta)
                for (i, chunk) in enumerate(chunks)]
    elseif first(chunks).values isa FixedSizeListVector
        return [_fixed_size_list(chunk.values, nullable) for chunk in chunks]
    end
    elements = _wrap_buffers(only(node.children), RawNode[only(chunk.children) for chunk in chunks], nothing)
    [_make_list(elements[i], _validity(chunk.nulls), chunk.offsets, length(chunk.nulls), nullable, meta)
     for (i, chunk) in enumerate(chunks)]
end

"""The same fixed-size list buffers with the element type the whole column agreed on."""
function _fixed_size_list(column::FixedSizeListVector{N, T}, nullable::Bool) where {N, T}
    ET = nullable ? Union{Missing, FixedSizeView{N, T}} : FixedSizeView{N, T}
    FixedSizeListVector{N, T, ET}(column.data, column.nulls, column.len)
end

"""Read one top-level column of the plan: stage 1 per row group, then stage 2."""
function _read_column(ctx::ReadContext, row_groups::Vector{RowGroup}, node::ReadNode, field_meta)
    rgs = isempty(row_groups) ? [nothing] : row_groups
    chunks = RawNode[first(fetch(task)) for task in [Threads.@spawn _read_buffers(ctx, rg, node, 0, 0, false) for rg in rgs]]
    arrays = _wrap_buffers(node, chunks, get(field_meta, node.name, nothing))
    _wrap_nested(length(arrays) == 1 ? only(arrays) : ChainedVector(arrays))
end

"""
The recursive reader's entry point. Internal until it replaces `read_parquet`'s current
paths (tasks/todo.md, Part 4, R7); until then it exists for the regression harness.
"""
function _read_parquet_recursive(pf::ParquetFile; columns::Union{AbstractVector{<:AbstractString}, Nothing} = nothing)
    (; schema, fsl, field_meta) = parse_arrow_schema(pf.metadata.key_value_metadata)
    plan = plan_read_tree(build_schema_tree(pf.metadata.schema))
    columns === nothing || (plan = prune_read_plan(plan, columns))

    ctx = (data = pf.data, fsl = fsl)
    tasks = [Threads.@spawn _read_column(ctx, pf.metadata.row_groups, node, field_meta) for node in plan]
    vectors = AbstractVector[try fetch(task) catch e; throw(ColumnReadError(node.name, _root_cause(e))) end
                             for (node, task) in zip(plan, tasks)]

    names = Symbol[Symbol(node.name) for node in plan]
    Arrow.Table(names, Type[eltype(v) for v in vectors], vectors, Dict{Symbol, AbstractVector}(zip(names, vectors)),
                schema !== nothing ? Ref(schema) : Ref{Arrow.Meta.Schema}(),
                Ref{Union{Nothing, Base.ImmutableDict{String, String}}}(_parse_kv_metadata(pf.metadata.key_value_metadata)))
end

function _read_parquet_recursive(path::String; kwargs...)
    pf = open_parquet(path)
    try
        return _read_parquet_recursive(pf; kwargs...)
    finally
        close(pf)
    end
end
