# Parquet file reader

struct ParquetFile
    data::Vector{UInt8}   # Mmap.mmap'd file contents
    path::String
    metadata::FileMetaData
end

function read_footer(data::Vector{UInt8})::FileMetaData
    file_size = length(data)
    file_size < 12 && error("File too small")

    data[end-3:end] == PARQUET_MAGIC || error("Missing trailing magic")

    footer_len = ltoh(reinterpret(UInt32, data[end-7:end-4])[1])
    footer_len > file_size - 12 && error("Invalid footer length")

    data[1:4] == PARQUET_MAGIC || error("Missing leading magic")

    parse_file_metadata(data[end-7-footer_len:end-8])
end

function open_parquet(path::String)::ParquetFile
    data = Mmap.mmap(path)
    ParquetFile(data, path, read_footer(data))
end

Base.close(::ParquetFile) = nothing

num_rows(pf::ParquetFile) = pf.metadata.num_rows
num_row_groups(pf::ParquetFile) = length(pf.metadata.row_groups)
schema(pf::ParquetFile) = pf.metadata.schema
metadata(pf::ParquetFile) = pf.metadata

"""Follow a single-child chain down to its end; returns (end_node, path_names), or nothing if any node branches."""
function _single_leaf_chain(c::SchemaNode)
    node = c
    names = String[c.element.name]
    while length(node.children) == 1
        node = only(node.children)
        push!(names, node.element.name)
    end
    isempty(node.children) ? (node, names) : nothing
end

"""
    struct_member_kind(c::SchemaNode) -> Symbol

Classify a struct group's child: `:flat` (non-repeated leaf), `:list` (a LIST group
or repeated field whose subtree holds exactly one leaf), `:struct` (nested struct
group, recursively), or `:unsupported` (map, list-of-struct, ...).
"""
function struct_member_kind(c::SchemaNode)
    isempty(c.children) && c.max_rep_level == 0 && return :flat
    chain = _single_leaf_chain(c)
    if chain !== nothing && first(chain).max_rep_level > 0 &&
       (c.element.converted_type == CT_LIST || c.element.repetition_type == REPEATED)
        return :list
    end
    is_struct_group(c) && return :struct
    :unsupported
end

"""
    list_of_structs_parts(node::SchemaNode) -> NamedTuple or nothing

Detect a List<Struct> column: a LIST group whose repeated node holds a struct
element with all-flat members. Returns `(list, rep, elem, prefix)` where `prefix`
is the schema path down to (and including) the element node. Handles the standard
3-level layout (`list → element group`) and the legacy 2-level layout where the
repeated group itself carries the element fields.
"""
function list_of_structs_parts(node::SchemaNode)
    node.element.converted_type == CT_LIST || return nothing
    node.element.repetition_type == REPEATED && return nothing
    length(node.children) == 1 || return nothing
    r = only(node.children)
    r.element.repetition_type == REPEATED || return nothing

    if length(r.children) == 1 && !isempty(only(r.children).children)
        elem = only(r.children)                       # standard 3-level
        prefix = [node.element.name, r.element.name, elem.element.name]
    elseif length(r.children) > 1
        elem = r                                      # legacy 2-level
        prefix = [node.element.name, r.element.name]
    else
        return nothing                                # plain List<primitive>
    end
    all(c -> isempty(c.children) && c.max_rep_level == elem.max_rep_level, elem.children) ||
        return nothing
    (list = node, rep = r, elem = elem, prefix = prefix)
end

"""
    is_struct_group(node::SchemaNode) -> Bool

A struct group assemblable into `Arrow.Struct`: a non-repeated, non-LIST/MAP group
whose children are all flat leaves, single-leaf list fields, or nested struct groups.
"""
function is_struct_group(node::SchemaNode)
    isempty(node.children) && return false
    node.element.repetition_type == REPEATED && return false
    node.element.converted_type in (CT_LIST, CT_MAP, CT_MAP_KEY_VALUE) && return false
    all(c -> struct_member_kind(c) != :unsupported, node.children)
end

"""Top-level group nodes assembled as single columns (structs and List<Struct>), by name."""
_struct_top_nodes(tree::SchemaNode) =
    Dict(c.element.name => c for c in tree.children
         if is_struct_group(c) || list_of_structs_parts(c) !== nothing)

"""Leaves per top-level field. A repeated leaf may only claim the bare top-level name
when it is the sole leaf there — otherwise unsupported shapes (list<struct{...}>,
maps) would silently collide."""
function _leaf_counts(leaves)
    counts = Dict{String, Int}()
    for (path, _) in leaves
        counts[path[1]] = get(counts, path[1], 0) + 1
    end
    counts
end

"""Output column name for a leaf outside assembled struct groups."""
_leaf_column_name(path::Vector{String}, node::SchemaNode, leaf_count) =
    (node.max_rep_level > 0 && leaf_count[path[1]] == 1) ? path[1] : join(path, ".")

function column_names(pf::ParquetFile)::Vector{String}
    tree = build_schema_tree(pf.metadata.schema)
    struct_tops = keys(_struct_top_nodes(tree))
    leaves = get_leaf_columns(tree)
    leaf_count = _leaf_counts(leaves)
    seen = Set{String}()
    names = String[]
    for (path, node) in leaves
        name = path[1] in struct_tops ? path[1] : _leaf_column_name(path, node, leaf_count)
        name in seen && continue
        push!(seen, name)
        push!(names, name)
    end
    names
end

function build_schema_tree(schema::Vector{SchemaElement})::SchemaNode
    isempty(schema) && error("Empty schema")

    function build(idx, def, rep)
        elem = schema[idx]

        # Calculate this node's contribution to def/rep levels
        adds_def = elem.repetition_type in (OPTIONAL, REPEATED) ? 1 : 0
        adds_rep = elem.repetition_type == REPEATED ? 1 : 0

        new_def = def + adds_def
        new_rep = rep + adds_rep

        children = SchemaNode[]
        next = idx + 1

        if elem.num_children !== nothing && elem.num_children > 0
            for _ in 1:elem.num_children
                child, next = build(next, new_def, new_rep)
                push!(children, child)
            end
        end

        node = SchemaNode(
            element = elem,
            children = children,
            max_def_level = new_def,
            max_rep_level = new_rep,
            own_def_level = adds_def > 0 ? new_def : 0,
            own_rep_level = adds_rep > 0 ? new_rep : 0
        )
        (node, next)
    end

    first(build(1, 0, 0))
end

"""
    compute_def_thresholds(root::SchemaNode, path::Vector{String}) -> Vector{Int}

Compute the definition level threshold for each repetition level.
`thresholds[i]` is the minimum def_level at which rep_level `i` has a defined element.
Used by nested column assembly to distinguish "empty inner list" from "null leaf value".
"""
function compute_def_thresholds(root::SchemaNode, path::Vector{String})
    thresholds = Int[]
    cum_def = 0
    node = root
    for name in path
        idx = findfirst(c -> c.element.name == name, node.children)
        idx === nothing && break
        child = node.children[idx]
        rt = child.element.repetition_type
        cum_def += (rt == OPTIONAL || rt == REPEATED) ? 1 : 0
        rt == REPEATED && push!(thresholds, cum_def)
        node = child
    end
    thresholds
end

function get_leaf_columns(root::SchemaNode)
    result = Tuple{Vector{String}, SchemaNode}[]

    function traverse(node, path)
        current = vcat(path, [node.element.name])
        if isempty(node.children)
            push!(result, (current, node))
        else
            for child in node.children
                traverse(child, current)
            end
        end
    end

    # Start from root's children, skipping root schema name
    for child in root.children
        traverse(child, String[])
    end
    result
end
