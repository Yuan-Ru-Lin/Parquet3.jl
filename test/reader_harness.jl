# Regression harness for the recursive reader (tasks/todo.md, Part 4).
#
# The oracle compares two readers' results for one file:
#   - column names, container kinds, values and structure must be equal;
#   - element types must be equal after stripping `Missing` at every level;
# and checks nullability on a result by itself: a type should admit `Missing` exactly
# where a missing occurs (`loose_nodes` lists the places where it admits one needlessly).

"""Element type `T` with `Missing` removed at every level, as a comparable description."""
function harness_shape(::Type{T}) where T
    S = Base.nonmissingtype(T)
    S === Union{} && return :missing
    S <: NamedTuple && return (:struct, fieldnames(S), map(harness_shape, fieldtypes(S)))
    S <: Parquet3.FixedSizeView && return (:fixed_size_list, S.parameters[1], harness_shape(eltype(S)))
    S <: AbstractVector && return (:list, harness_shape(eltype(S)))
    S
end

"""What kind of column container `col` is, ignoring element types."""
harness_container(col) =
    col isa Parquet3.NestedColumn ? (typeof(col).parameters[1], propertynames(col)) :
    col isa Parquet3.FixedSizeListVector ? :fixed_size_list :
    col isa Parquet3.ChainedVector ? (:chained, harness_container(first(col.arrays))) : :array

"""Value equality through lists and structs, without materialising copies."""
harness_same(a, b) =
    (a === missing || b === missing) ? (a === missing && b === missing) :
    (a isa AbstractVector{<:Number} && b isa AbstractVector{<:Number}) ? isequal(a, b) :
    a isa AbstractVector ? (b isa AbstractVector && length(a) == length(b) && all(harness_same(x, y) for (x, y) in zip(a, b))) :
    a isa NamedTuple ? (b isa NamedTuple && keys(a) == keys(b) && all(harness_same(x, y) for (x, y) in zip(values(a), values(b)))) :
    isequal(a, b)

"""Differences between two readers' tables for the same file; empty when they agree."""
function reader_differences(old, new)
    names_old, names_new = collect(Tables.columnnames(old)), collect(Tables.columnnames(new))
    names_old == names_new || return ["column names differ: $names_old vs $names_new"]
    diffs = String[]
    for name in names_old
        a, b = Tables.getcolumn(old, name), Tables.getcolumn(new, name)
        harness_container(a) == harness_container(b) ||
            push!(diffs, "$name: container $(harness_container(a)) vs $(harness_container(b))")
        harness_shape(eltype(a)) == harness_shape(eltype(b)) ||
            push!(diffs, "$name: element type $(eltype(a)) vs $(eltype(b))")
        harness_same(a, b) || push!(diffs, "$name: values differ")
    end
    diffs
end

"""
Paths in `table` whose type admits `Missing` although no missing occurs there
(`"col"`, `"col.field"`, `"col[]"` for list elements). The recursive reader must return
none; the statistics-based reader returns some, and those are where types will tighten.

A struct member counts as missing wherever its struct is missing: `col.field` returns the
member for every row, and shows a missing for those rows. List elements exist only inside
lists that are present.
"""
function loose_nodes(table)
    out = String[]
    for name in Tables.columnnames(table)
        col = Tables.getcolumn(table, name)
        _loose_nodes!(out, String(name), eltype(col), col)
    end
    out
end

function _loose_nodes!(out, path, ::Type{T}, vals) where T
    S = Base.nonmissingtype(T)
    admits = Missing <: T
    nested = S !== Union{} && (S <: NamedTuple || (S <: AbstractVector && eltype(S) !== UInt8))
    admits || nested || return            # a plain leaf type that cannot hold a missing
    admits && !any(ismissing, vals) && push!(out, path)
    if S !== Union{} && S <: NamedTuple
        for (i, f) in enumerate(fieldnames(S))
            _loose_nodes!(out, "$path.$f", fieldtype(S, i), (v === missing ? missing : v[i] for v in vals))
        end
    elseif nested
        _loose_nodes!(out, path * "[]", eltype(S), Iterators.flatten(admits ? skipmissing(vals) : vals))
    end
end

# ── Corpus ────────────────────────────────────────────────────────────────

const HARNESS_PYARROW_CORPUS = """
import pyarrow as pa, pyarrow.parquet as pq, sys
out = sys.argv[1] if len(sys.argv) > 1 else '.'
n = 12
i64, i32, f32 = pa.int64(), pa.int32(), pa.float32()
wf = pa.struct([('t0', pa.float64()), ('dt', f32), ('values', pa.list_(i32))])
wfx = pa.struct([('t0', f32), ('values', pa.list_(i32, 4))])
ev = pa.struct([('id', i64), ('vertex', pa.struct([('x', f32), ('tag', pa.string())]))])
pt = pa.struct([('pt', f32), ('q', i32)])
def nulls(vals, every): return [None if i % every == every - 1 else v for i, v in enumerate(vals)]
tables = {
    'flat': pa.table({
        'i32': pa.array(nulls(range(n), 4), type=i32), 'i64': pa.array(range(n), type=i64),
        'u16': pa.array(nulls(range(n), 5), type=pa.uint16()), 'i8': pa.array(range(n), type=pa.int8()),
        'f32': pa.array(nulls([0.5 * i for i in range(n)], 3), type=f32), 'f64': pa.array([0.25 * i for i in range(n)]),
        'flag': pa.array(nulls([i % 2 == 0 for i in range(n)], 6)), 'name': pa.array(nulls(['n%d' % i for i in range(n)], 4)),
        'blob': pa.array(nulls([b'b%d' % i for i in range(n)], 7), type=pa.binary()),
        'day': pa.array(nulls(range(n), 5), type=pa.date32()),
        'ts_ms': pa.array(nulls(range(n), 4), type=pa.timestamp('ms')), 'ts_us': pa.array(range(n), type=pa.timestamp('us', tz='UTC')),
        'allnull': pa.array([None] * n, type=i32),
    }),
    'lists': pa.table({
        'hits': pa.array([None if i % 5 == 4 else [] if i % 5 == 3 else [None if j == 1 else j for j in range(i % 4)] for i in range(n)], type=pa.list_(i32)),
        'clean': pa.array([[i, i + 1] for i in range(n)], type=pa.list_(i64)),
        'empties': pa.array([[] if i % 3 == 0 else [i] for i in range(n)], type=pa.list_(i64)),
        'words': pa.array([None if i % 6 == 5 else ['w%d' % j if j != 2 else None for j in range(i % 4)] for i in range(n)], type=pa.list_(pa.string())),
        'll': pa.array([None if i % 6 == 5 else [None if j == 1 else [k for k in range(j)] for j in range(i % 4)] for i in range(n)], type=pa.list_(pa.list_(i64))),
        'lll': pa.array([[[[i, None], []], []] if i % 2 else [] for i in range(n)], type=pa.list_(pa.list_(pa.list_(i64)))),
        'ltime': pa.array([[i, None] for i in range(n)], type=pa.list_(pa.timestamp('us'))),
    }),
    'structs': pa.table({
        'wf': pa.array([None if i % 6 == 5 else {'t0': None if i % 4 == 3 else 0.5 * i, 'dt': 0.1,
                        'values': None if i % 5 == 4 else [None if j == 2 else j for j in range(i % 4)]} for i in range(n)], type=wf),
        'ev': pa.array([{'id': i, 'vertex': {'x': 0.5 * i, 'tag': 'v%d' % i}} for i in range(n)], type=ev),
        'evn': pa.array([None if i % 4 == 3 else {'id': i, 'vertex': None if i % 3 == 2 else {'x': None if i % 2 else 1.5, 'tag': 't'}} for i in range(n)], type=ev),
        'onlylist': pa.array([{'v': [i], 'w': []} for i in range(n)], type=pa.struct([('v', pa.list_(i64)), ('w', pa.list_(i64))])),
        'membernulls': pa.array([{'a': None if i % 2 else i, 'b': None if i % 3 else 'x'} for i in range(n)], type=pa.struct([('a', i64), ('b', pa.string())])),
    }),
    'list_of_structs': pa.table({
        'parts': pa.array([None if i % 6 == 5 else [] if i % 6 == 4 else [{'pt': 1.5 * j, 'q': None if j == 1 else j} for j in range(i % 4)] for i in range(n)], type=pa.list_(pt)),
        'clean': pa.array([[{'pt': 1.0, 'q': i}] for i in range(n)], type=pa.list_(pt)),
        'nullelem': pa.array([[{'pt': 1.0, 'q': 1}, None] for i in range(n)], type=pa.list_(pt)),
    }),
    'fixed_size': pa.table({
        'fsl': pa.FixedSizeListArray.from_arrays(pa.array(range(n * 3), type=i32), 3),
        'wfx': pa.array([{'t0': 0.5 * i, 'values': [i, i + 1, i + 2, i + 3]} for i in range(n)], type=wfx),
        'id': pa.array(range(n), type=i64),
    }),
    'zero_rows': pa.schema([('id', i64), ('name', pa.string()), ('hits', pa.list_(i32)), ('wf', wf), ('parts', pa.list_(pt)),
                            ('ll', pa.list_(pa.list_(i64)))]).empty_table(),
}
variants = {'': {}, '_rg5': {'row_group_size': 5}, '_rg5_nostats': {'row_group_size': 5, 'write_statistics': False},
            '_v2_dict': {'data_page_version': '2.0', 'use_dictionary': True}, '_plain_zstd': {'use_dictionary': False, 'compression': 'zstd'}}
for name, table in tables.items():
    for suffix, kwargs in variants.items():
        if name == 'zero_rows' and suffix: continue
        # v2 data pages RLE-encode booleans, which the reader does not decode yet (Known Limitations)
        if suffix == '_v2_dict' and 'flag' in table.column_names: table = table.drop(['flag'])
        pq.write_table(table, '%s/py_%s%s.parquet' % (out, name, suffix), **kwargs)
print('SUCCESS')
"""

"""Shapes written by our own writer that the reader assembles today."""
function harness_writer_tables()
    M = Missing
    PT = @NamedTuple{pt::Float32, q::Union{M, Int32}}
    (
        nested = (
            wf    = Union{M, @NamedTuple{t0::Float64, values::Union{M, Vector{Union{M, Int32}}}}}[
                        (t0 = 0.5, values = [1, missing]), missing, (t0 = 1.5, values = missing), (t0 = 2.5, values = [])],
            ev    = [(id = i, vertex = (x = 0.1i, tag = "v$i")) for i in 1:4],
            parts = Union{M, Vector{PT}}[[(pt = 1f0, q = 1), (pt = 2f0, q = missing)], PT[], missing, [(pt = 3f0, q = -1)]],
            ll    = [[[1, 2], [3]], Vector{Int}[], [[4]], [Int[], [5]]],
            oll   = Union{M, Vector{Union{M, Vector{Union{M, Int64}}}}}[[[1, missing], missing, []], missing, [], [[2]]],
            ints  = Int8[1, 2, 3, 4], name = ["a", missing, "c", ""],
        ),
        clean = (id = collect(1:6), x = [0.5i for i in 1:6], hits = [[i, i + 1] for i in 1:6],
                 pos = [(x = 1.0i, y = 2.0i) for i in 1:6], parts = [[(pt = 1f0, q = Int32(i))] for i in 1:6]),
    )
end

"""
Build the corpus in `dir` and return `(label, path)` pairs: pyarrow fixtures (several
shapes × writer options), files from our own writer, every parquet-testing file that
reads fully, and — only when present locally, it is not in the repository — part-0.parquet.
"""
function harness_corpus(dir::String)
    corpus = Tuple{String, String}[]
    script = joinpath(dir, "corpus.py")
    write(script, HARNESS_PYARROW_CORPUS)
    if _run_pyarrow("import sys; sys.argv = ['', '$(dir)']; exec(open('$(script)').read())") == "SUCCESS"
        append!(corpus, [("pyarrow:" * f, joinpath(dir, f)) for f in sort(filter(startswith("py_"), readdir(dir)))])
    else
        @warn "Reader harness: pyarrow corpus not built (uv/pyarrow not available)"
    end
    for (name, tbl) in pairs(harness_writer_tables())
        path = joinpath(dir, "writer_$name.parquet")
        write_parquet(path, tbl)
        push!(corpus, ("writer:$name", path))
    end
    if HAS_PARQUET_TESTING
        for f in sort(filter(endswith(".parquet"), readdir(PARQUET_TESTING_DIR)))
            haskey(PARQUET_TESTING_KNOWN_GAPS, f) || push!(corpus, ("parquet-testing:" * f, joinpath(PARQUET_TESTING_DIR, f)))
        end
    end
    local_file = joinpath(@__DIR__, "..", "testdata", "part-0.parquet")
    isfile(local_file) && push!(corpus, ("local:part-0.parquet", local_file))
    corpus
end

"""
Run `read_new` against `read_old` over the corpus. Returns, per file, the differences the
oracle found and the loose nodes of each reader's result.
"""
function run_reader_harness(read_old, read_new, corpus)
    map(corpus) do (label, path)
        old, new = read_old(path), read_new(path)
        (label = label, differences = reader_differences(old, new), loose_old = loose_nodes(old), loose_new = loose_nodes(new))
    end
end

"""
Top-level columns of `path` the recursive reader assembles at step R3: any nesting of
lists and structs, as long as no struct sits inside a list (that comes with R4/R5).
"""
function harness_r3_columns(path::String)
    pf = open_parquet(path)
    plan = Parquet3.plan_read_tree(Parquet3.build_schema_tree(pf.metadata.schema))
    close(pf)
    struct_in_list(node, in_list = false) = (node.kind == :struct && in_list) ||
        any(c -> struct_in_list(c, in_list || node.kind == :list), node.children)
    [node.name for node in plan if !struct_in_list(node)]
end
