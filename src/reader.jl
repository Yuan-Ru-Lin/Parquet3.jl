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
