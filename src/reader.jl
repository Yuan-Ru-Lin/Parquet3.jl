# The reader: the inverse of the writer's `_plan_node` / `_shred!`.
# Schema tree → plan tree → prune to the selected columns → recursive assembly.

"""
    ColumnReadError(column, cause)

Thrown by `read_parquet` when a column cannot be read, for example because it uses an
encoding or type that is not supported yet. `cause` is the original exception. The other
columns can still be read by passing `columns=` without the failing one.
"""
struct ColumnReadError <: Exception
    column::String
    cause::Exception
end

function Base.showerror(io::IO, e::ColumnReadError)
    print(io, "ColumnReadError: failed to read column \"", e.column, "\". To read the other columns, ",
          "pass `columns=` without it. Caused by: ")
    showerror(io, e.cause)
end

# Column tasks nest: unwrap to the exception that actually failed
_root_cause(e) = e isa TaskFailedException ? _root_cause(e.task.exception) : e

"""
    read_parquet(path::String; columns=nothing) -> Arrow.Table

Read a Parquet file and return an Arrow.Table (Tables.jl-compatible).

`columns` selects what to read, by the dotted paths used to reach the data: `"id"` for a
column, `"wf.values"` for a struct member, `"particles.pt"` for a member of a list of
structs, `"m.key"` for a map's keys. A name selects everything under it; selecting a
member returns its column with only the selected parts. Leaves that are not selected are
not decoded. A name that matches nothing is an `ArgumentError`.

A column's element type admits `Missing` exactly where a null occurs in the data read.

A column that cannot be read throws a [`ColumnReadError`](@ref) naming it; nothing is
skipped silently.
"""
function read_parquet(path::String; columns::Union{AbstractVector{<:AbstractString}, Nothing}=nothing)
    pf = open_parquet(path)
    try
        return read_parquet(pf; columns=columns)
    finally
        close(pf)
    end
end

"""
One node of the read plan. Three kinds, mirroring the writer:

- `:leaf`   — a primitive column chunk; contributes values and element validity.
- `:list`   — one child (its element); contributes offsets and validity. Covers the
  standard 3-level LIST, the legacy 2-level forms and a bare repeated field.
- `:map`    — a list whose element is the key/value struct of a MAP group; assembled as
  a list and presented as a map.
- `:struct` — its members; contributes validity only.

`key` is the path a user types (`"wf.values"`, `"particles.pt"`, `"m.key"`): it has no
segment for a list's structural `list`/`element` groups or a map's `key_value` group, the
same convention as the writer's `encoding` keyword. `path` is the Parquet path of the
schema node the kind was read from (for a leaf, its column path).

Levels, all cumulative definition/repetition levels as in the schema tree: the node is
non-null when `def >= def_level`; `rep_level` counts the enclosing lists, a list included;
for a list, an item exists when `def >= item_def`. `fsl_size` is the list's fixed size
when `ARROW:schema` declares it a FixedSizeList, else 0.
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
    fsl_size::Int
end

ReadNode(kind, name, key, path, def_level, rep_level, item_def, schema, children) =
    ReadNode(kind, name, key, path, def_level, rep_level, item_def, schema, children, 0)

# The Arrow schema (from ARROW:schema) has the same tree shape as the read plan: a struct's
# members by name, a list's single element, a map's entries struct. The planner walks both
# together, so a FixedSizeList is found at any depth. Where the trees do not line up, the
# Arrow side is simply `nothing` from there down.
const ArrowField = Union{Arrow.Meta.Field, Nothing}

_arrow_children(field::ArrowField) = field === nothing || field.children === nothing ? () : field.children

"""The Arrow field of struct member `name`."""
function _arrow_member(field::ArrowField, name::String)
    field !== nothing && field.type isa Arrow.Meta.Struct || return nothing
    i = findfirst(child -> child.name == name, collect(_arrow_children(field)))
    i === nothing ? nothing : _arrow_children(field)[i]
end

"""The Arrow field of a list's element (for a map, its entries struct)."""
function _arrow_element(field::ArrowField)
    children = _arrow_children(field)
    field !== nothing && length(children) == 1 &&
        field.type isa Union{Arrow.Meta.List, Arrow.Meta.LargeList, Arrow.Meta.FixedSizeList, Arrow.Meta.Map} ?
        only(children) : nothing
end

_arrow_fsl_size(field::ArrowField) =
    field !== nothing && field.type isa Arrow.Meta.FixedSizeList ? Int(field.type.listSize) : 0

_is_repeated(node::SchemaNode) = node.element.repetition_type == REPEATED
_is_map(node::SchemaNode) = node.element.converted_type in (CT_MAP, CT_MAP_KEY_VALUE)

"""Plan the top-level columns of a schema tree."""
function plan_read_tree(root::SchemaNode, schema::Union{Arrow.Meta.Schema, Nothing} = nothing)
    fields = schema === nothing || schema.fields === nothing ? () : collect(schema.fields)
    top(name) = (i = findfirst(f -> f.name == name, fields); i === nothing ? nothing : fields[i])
    [_plan_read(child, String[], "", top(child.element.name)) for child in root.children]
end

"""
Plan `node` as a field named after it under `parent_path` / `parent_key`, with `field` its
Arrow counterpart if there is one. A repeated node is a list of itself; anything else is
planned by `_plan_value`.
"""
function _plan_read(node::SchemaNode, parent_path::Vector{String}, parent_key::String, field::ArrowField)
    name = node.element.name
    path = [parent_path; name]
    key = isempty(parent_key) ? name : string(parent_key, ".", name)
    _is_repeated(node) || return _plan_value(node, name, key, path, field)
    # A repeated field outside a LIST/MAP wrapper: a list that cannot be null, whose
    # elements are the node itself, taken as required.
    ReadNode(:list, name, key, path, node.max_def_level - 1, node.max_rep_level, node.max_def_level,
             node, [_plan_value(node, name, key, path, _arrow_element(field))], _arrow_fsl_size(field))
end

"""
Plan `node` as a value: a leaf, a list (LIST or MAP group), or a struct. Also used for the
item of a repeated node, which is present exactly when the item exists: its
`max_def_level` is then the item's level, so the same rule gives a node that is never null.
"""
function _plan_value(node::SchemaNode, name::String, key::String, path::Vector{String}, field::ArrowField)
    isempty(node.children) && return ReadNode(:leaf, name, key, path, node.max_def_level,
                                              node.max_rep_level, 0, node, ReadNode[])
    wrapped = node.element.converted_type == CT_LIST || _is_map(node)
    if wrapped && length(node.children) == 1 && _is_repeated(only(node.children))
        rep = only(node.children)
        # A map needs both a key and a value; a MAP group with only keys is a list of them
        kind = _is_map(node) && length(rep.children) == 2 ? :map : :list
        return ReadNode(kind, name, key, path, node.max_def_level, rep.max_rep_level, rep.max_def_level,
                        node, [_plan_element(node, rep, name, key, [path; rep.element.name], _arrow_element(field))],
                        _arrow_fsl_size(field))
    end
    # Any other group is a struct. That includes a group annotated LIST or MAP without the
    # single repeated child the annotation requires: its children are read as they are.
    ReadNode(:struct, name, key, path, node.max_def_level, node.max_rep_level, 0, node,
             [_plan_read(child, path, key, _arrow_member(field, child.element.name)) for child in node.children])
end

"""
The element of LIST/MAP group `list` with repeated child `rep` (at `path`), following the
format's backward-compatibility rules: the repeated node is itself the element when it is
a primitive, has several fields (a map's key and value, or a legacy struct element), or
carries a legacy name (`array`, `<list>_tuple`); otherwise its single child is the element
(the standard 3-level layout; also a map without values, which pyarrow reads as a list of
its keys). Structural groups add nothing to the user key. `field` is the Arrow field of the
element.
"""
function _plan_element(list::SchemaNode, rep::SchemaNode, name::String, key::String, path::Vector{String}, field::ArrowField)
    legacy = isempty(rep.children) || length(rep.children) > 1 ||
             rep.element.name in ("array", list.element.name * "_tuple")
    legacy && return _plan_value(rep, name, key, path, field)
    element = only(rep.children)
    path = [path; element.element.name]
    _is_repeated(element) ?
        ReadNode(:list, name, key, path, element.max_def_level - 1, element.max_rep_level, element.max_def_level,
                 element, [_plan_value(element, name, key, path, _arrow_element(field))], _arrow_fsl_size(field)) :
        _plan_value(element, name, key, path, field)
end

"""All leaves under `node`, in schema order."""
read_leaves(node::ReadNode) = node.kind == :leaf ? [node] : reduce(vcat, map(read_leaves, node.children); init = ReadNode[])

"""
Compact description of a plan node for tests and debugging, e.g.
`wf: struct@1{t0: leaf@2, values: list@2/3<leaf@4>}` (`@def_level`, lists `@def_level/item_def`).
"""
function read_plan_string(node::ReadNode; top::Bool = true)
    body = node.kind == :leaf ? "leaf@$(node.def_level)" :
           node.kind in (:list, :map) ? "$(node.kind)@$(node.def_level)/$(node.item_def)<$(read_plan_string(only(node.children); top = false))>" :
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
        ReadNode(node.kind, node.name, node.key, node.path, node.def_level, node.rep_level, node.item_def, node.schema, children, node.fsl_size)
end

# ── Levels and fixed-size list buffers ────────────────────────────────────

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

"""
Direct FSL assembly: scatter values from rep/def levels into a flat buffer
in a single pass, bypassing intermediate Vector{Vector{T}} creation.
"""
function assemble_fsl_direct(all_rep, all_def, values::AbstractVector{V},
                             max_def::Int, list_size::Int, elem::SchemaElement,
                             def_thresholds::Vector{Int}; nullable::Bool=false,
                             record_null_def::Int = max_def > 0 ? 1 : 0) where V
    T = element_julia_type(elem.type, leaf_annotation(elem))
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
    T = element_julia_type(ptype, leaf_annotation(elem))
    # Count records from rep levels
    num_records = sum(pages) do page
        page.rep_levels === nothing ? page.num_values : count(==(0), page.rep_levels)
    end

    data = Vector{T}(undef, list_size * num_records)
    nulls = falses(num_records)

    # Convert and copy page values in bulk using a function barrier for type stability
    _fsl_dense_copy!(data, pages, ptype, leaf_annotation(elem))

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
# wrapper with named field access (`_wrap_nested` in arrays.jl).

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
const ReadContext = @NamedTuple{data::Vector{UInt8}}

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
    if slot_rep == 0 && child.kind == :leaf && node.fsl_size > 0
        return _read_fixed_size_list(ctx, rg, node, child, node.fsl_size, want_levels)
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
    lists = [_make_list(elements[i], _validity(chunk.nulls), chunk.offsets, length(chunk.nulls), nullable, meta)
             for (i, chunk) in enumerate(chunks)]
    # A map is presented as one only while it has both its key and its value: a selection
    # of just one of them (`columns=["m.key"]`) leaves a list of structs with that member.
    node.kind == :map && length(only(node.children).children) == 2 ? map(MapVector, lists) : lists
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

function read_parquet(pf::ParquetFile; columns::Union{AbstractVector{<:AbstractString}, Nothing} = nothing)
    (; schema, field_meta) = parse_arrow_schema(pf.metadata.key_value_metadata)
    plan = plan_read_tree(build_schema_tree(pf.metadata.schema), schema)
    columns === nothing || (plan = prune_read_plan(plan, columns))

    ctx = (data = pf.data,)
    tasks = [Threads.@spawn _read_column(ctx, pf.metadata.row_groups, node, field_meta) for node in plan]
    vectors = AbstractVector[try fetch(task) catch e; throw(ColumnReadError(node.name, _root_cause(e))) end
                             for (node, task) in zip(plan, tasks)]

    names = Symbol[Symbol(node.name) for node in plan]
    Arrow.Table(names, Type[eltype(v) for v in vectors], vectors, Dict{Symbol, AbstractVector}(zip(names, vectors)),
                schema !== nothing ? Ref(schema) : Ref{Arrow.Meta.Schema}(),
                Ref{Union{Nothing, Base.ImmutableDict{String, String}}}(_parse_kv_metadata(pf.metadata.key_value_metadata)))
end
