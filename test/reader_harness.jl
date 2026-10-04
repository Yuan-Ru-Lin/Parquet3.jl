# Reader test support: a corpus of files, the nullability check, and pyarrow comparisons.
#
# While the recursive reader was built (tasks/todo.md, Part 4) this also held the oracle
# that compared it with the reader it replaced; that comparison ended with the old paths.

"""
Paths in `table` whose type admits `Missing` although no missing occurs there
(`"col"`, `"col.field"`, `"col[]"` for list elements). The reader must return none: it
derives nullability from the levels it decodes.

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

"""Nested shapes written by our own writer."""
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


# pyarrow compares its own reading of each original with its reading of our rewrite of it.
# Maps come back from pyarrow as (key, value) tuples and from our rewrite as key/value
# structs, so both are normalised to dicts. Arguments: original=rewrite pairs.
const HARNESS_COMPARE_VALUES = """
import pyarrow.parquet as pq, sys
def norm(v):
    if isinstance(v, tuple): return {'key': norm(v[0]), 'value': norm(v[1])}
    if isinstance(v, list): return [norm(x) for x in v]
    if isinstance(v, dict): return {k: norm(x) for k, x in v.items()}
    return v
def compare(orig, ours):
    a, b = pq.read_table(orig), pq.read_table(ours)
    if a.column_names != b.column_names: return 'column names differ'
    bad = [n for n in a.column_names if norm(a.column(n).to_pylist()) != norm(b.column(n).to_pylist())]
    return 'equal' if not bad else 'DIFFER in ' + ', '.join(bad)
print(';'.join(compare(*pair.split('=')) for pair in ARGS))
"""

"""Run `HARNESS_COMPARE_VALUES` over `(original, rewrite)` path pairs; one verdict per pair, or `nothing` without pyarrow."""
function harness_pyarrow_compare(pairs)
    args = join(("'$(a)=$(b)'" for (a, b) in pairs), ", ")
    result = _run_pyarrow("ARGS = [$args]\n" * HARNESS_COMPARE_VALUES)
    result === nothing ? nothing : split(result, ';')
end

# Shapes the old reader flattens, written by pyarrow: structs holding lists inside lists,
# structs holding structs inside lists, a struct with a list<struct> member, lists of
# lists of structs, and maps (plain, nested, and as a struct member).
const HARNESS_NEW_SHAPES = """
import pyarrow as pa, pyarrow.parquet as pq
i32, i64, f32, s = pa.int32(), pa.int64(), pa.float32(), pa.string()
hit = pa.struct([('x', f32), ('adc', pa.list_(i32))])
trk = pa.struct([('id', i32), ('vertex', pa.struct([('x', f32), ('tag', s)]))])
n = 9
table = pa.table({
    'lsl': pa.array([None if i % 5 == 4 else [] if i % 5 == 3 else [None if j == 2 else {'x': 0.5 * j, 'adc': None if j == 1 else [None if k == 1 else k for k in range(j + i % 3)]} for j in range(i % 4)] for i in range(n)], type=pa.list_(hit)),
    'lss': pa.array([[{'id': j, 'vertex': None if j == 1 else {'x': 1.5 * j, 'tag': None if i % 2 else 't%d' % j}} for j in range(i % 3)] for i in range(n)], type=pa.list_(trk)),
    'sls': pa.array([None if i % 4 == 3 else {'run': i, 'hits': None if i % 3 == 2 else [{'x': 0.25 * j, 'adc': [j]} for j in range(i % 3)]} for i in range(n)], type=pa.struct([('run', i64), ('hits', pa.list_(hit))])),
    'lls': pa.array([[[{'id': k, 'vertex': {'x': 1.0, 'tag': 'a'}} for k in range(j)] for j in range(i % 3)] if i % 4 != 3 else None for i in range(n)], type=pa.list_(pa.list_(trk))),
    'm':   pa.array([None if i % 4 == 3 else [('k%d' % j, None if j == 1 else j * i) for j in range(i % 3)] for i in range(n)], type=pa.map_(s, i32)),
    'mm':  pa.array([[('a', [('x', i), ('y', None)]), ('b', None), ('c', [])] if i % 2 else [] for i in range(n)], type=pa.map_(s, pa.map_(s, i64))),
    'sm':  pa.array([{'tags': [('t', 1.5)], 'n': i} for i in range(n)], type=pa.struct([('tags', pa.map_(s, pa.float64())), ('n', i32)])),
    'id':  pa.array(range(n), type=i64),
})
"""

# pyarrow's file-level reader takes Parquet leaf paths and returns the nested column pruned
# to them, which is the selection semantics of `columns=` here. (`pq.read_table` differs: it
# returns a selected struct member as a top-level column and cannot select inside lists.)
# Arguments: the original file, then rewrite=leaf.path,leaf.path,... per selection.
const HARNESS_COMPARE_SELECTIONS = """
import pyarrow.parquet as pq
def norm(v):
    if isinstance(v, tuple): return {'key': norm(v[0]), 'value': norm(v[1])}
    if isinstance(v, list): return [norm(x) for x in v]
    if isinstance(v, dict): return {k: norm(x) for k, x in v.items()}
    return v
pf = pq.ParquetFile(ARGS[0])
def compare(spec):
    ours, leaves = spec.split('=')
    a, b = pf.read(columns=leaves.split(',')), pq.read_table(ours)
    if a.column_names != b.column_names: return 'column names differ: %s vs %s' % (a.column_names, b.column_names)
    bad = [n for n in a.column_names if norm(a.column(n).to_pylist()) != norm(b.column(n).to_pylist())]
    return 'equal' if not bad else 'DIFFER in ' + ', '.join(bad)
print(';'.join(compare(spec) for spec in ARGS[1:]))
"""

"""
For each selection (a vector of user keys), read `path` with `read`, write the result to
`dir`, and have pyarrow compare it with its own pruned read of the same leaves. Returns
one verdict per selection, or `nothing` without pyarrow.
"""
function harness_compare_selections(read, path::String, selections, dir::String)
    pf = open_parquet(path)
    plan = Parquet3.plan_read_tree(Parquet3.build_schema_tree(pf.metadata.schema))
    close(pf)
    specs = map(enumerate(selections)) do (i, keys)
        out = joinpath(dir, "selection_$i.parquet")
        write_parquet(out, read(path; columns = keys))
        leaves = [join(leaf.path, ".") for node in Parquet3.prune_read_plan(plan, keys) for leaf in Parquet3.read_leaves(node)]
        "'$(out)=$(join(leaves, ","))'"
    end
    result = _run_pyarrow("ARGS = ['$(path)', $(join(specs, ", "))]\n" * HARNESS_COMPARE_SELECTIONS)
    result === nothing ? nothing : split(result, ';')
end
