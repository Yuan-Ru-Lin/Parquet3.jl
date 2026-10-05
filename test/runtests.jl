using Test
using Parquet3
using Arrow
using Tables
using Dates

# Selective runs: `Pkg.test(test_args=["FixedSizeList", "Writer"])` runs only the groups
# whose name contains one of the arguments (case-insensitive); without arguments every
# group runs. A group is a top-level `@group "name" begin … end`, which is a `@testset`
# when selected. Groups do not depend on each other; shared helpers are at top level.
const GROUP_FILTERS = lowercase.(ARGS)
const GROUPS_RUN, GROUPS_SKIPPED = String[], String[]
group_selected(name) = isempty(GROUP_FILTERS) || any(f -> occursin(f, lowercase(name)), GROUP_FILTERS)

macro group(name, body)
    esc(quote
        if group_selected($name)
            push!(GROUPS_RUN, $name)
            @testset $name $body
        else
            push!(GROUPS_SKIPPED, $name)
        end
    end)
end

@group "Parquet3.jl" begin

    @testset "Plain Encoding" begin
        data = collect(reinterpret(UInt8, Int32[1, 2, 3, 4, 5]))
        @test collect(Parquet3.decode_plain(Parquet3.INT32, data, 5)) == Int32[1, 2, 3, 4, 5]

        data = collect(reinterpret(UInt8, Float64[1.5, 2.5, 3.5]))
        @test collect(Parquet3.decode_plain(Parquet3.DOUBLE, data, 3)) == Float64[1.5, 2.5, 3.5]
    end

    @testset "RLE Decoding" begin
        data = UInt8[6, 5]  # RLE: 3 repetitions of value 5
        @test Parquet3.decode_rle_bitpacked(data, 3, 8) == UInt32[5, 5, 5]
    end

    # Level arrays straight into the reader's list assembly. `list_node(k)` is the k-th of
    # nested required lists with optional elements: it is non-null from def k-1 and has an
    # item from def k; its slots are the items of list k-1 (or the rows, for k = 1).
    list_node(k) = Parquet3.ReadNode(:list, "l", "l", ["l"], k - 1, k, k,
                                     Parquet3.SchemaNode(element = Parquet3.SchemaElement()), Parquet3.ReadNode[])
    structure(rep, def, k) = Parquet3._list_structure(Parquet3.Levels(rep, def), k - 1, k - 1, list_node(k))

    @testset "Nested Column Assembly" begin
        # [[1, 2], [3], [4, 5, 6]]: rep 0 starts a record, 1 continues its list; def 2 = value
        rep, def = [0, 1, 0, 0, 1, 1], [2, 2, 2, 2, 2, 2]
        @test structure(rep, def, 1) == (Int32[0, 2, 3, 6], falses(3))
        values, nulls = Parquet3._scatter_leaf(Int32[1, 2, 3, 4, 5, 6], def, 1, 2)
        @test values == [1, 2, 3, 4, 5, 6] && !any(nulls)
    end

    @testset "Nested Column with Nulls" begin
        # [[1, null, 2], [3]]: def 1 = null element, which takes a slot but no value
        rep, def = [0, 1, 1, 0], [2, 1, 2, 2]
        @test structure(rep, def, 1) == (Int32[0, 3, 4], falses(2))
        values, nulls = Parquet3._scatter_leaf(Int32[1, 2, 3], def, 1, 2)
        @test values[[1, 3, 4]] == [1, 2, 3] && nulls == [false, true, false, false]
    end

    @testset "Deeply Nested (List<List<T>>)" begin
        # [[[1, 2], [3]], [[4, 5]]]: rep 2 continues the inner list, 1 starts a new inner list
        rep, def = [0, 2, 1, 0, 2], [3, 3, 3, 3, 3]
        @test structure(rep, def, 1) == (Int32[0, 2, 3], falses(2))          # rows → inner lists
        @test structure(rep, def, 2) == (Int32[0, 2, 3, 5], falses(3))       # inner lists → values
    end

    @testset "Triple Nested (List<List<List<T>>>)" begin
        # [[[[1, 2]]]]: rep 3 continues the innermost list
        rep, def = [0, 3], [4, 4]
        @test [first(structure(rep, def, k)) for k in 1:3] == [Int32[0, 1], Int32[0, 1], Int32[0, 2]]
    end

    @testset "Null and empty lists" begin
        # optional list of optional elements: [[7], null, [], [null]] → def 3 value, 0 null list, 1 empty, 2 null element
        node = Parquet3.ReadNode(:list, "l", "l", ["l"], 1, 1, 2, Parquet3.SchemaNode(element = Parquet3.SchemaElement()), Parquet3.ReadNode[])
        offsets, nulls = Parquet3._list_structure(Parquet3.Levels([0, 0, 0, 0], [3, 0, 1, 2]), 0, 0, node)
        @test offsets == Int32[0, 1, 1, 1, 2] && nulls == [false, true, false, false]
    end

    @testset "Struct null attribution" begin
        # optional group person { optional field }: max_def = 2, group's own def level = 1
        # def: 2 = fully present, 1 = struct present but field null, 0 = struct itself null
        page = Parquet3.DecodedPage(Int32[10, 20], [2, 1, 0, 2], nothing, 4)
        defs = Parquet3._page_defs([page], 2)
        @test defs == [2, 1, 0, 2]
        @test (defs .< 1) == [false, false, true, false]   # struct validity at own def = 1

        # Pages without def levels (all required) are all-present
        dense = Parquet3.DecodedPage(Int32[1, 2, 3], nothing, nothing, 3)
        @test Parquet3._page_defs([dense], 2) == [2, 2, 2]

        # From a repeated (list) member: only record starts (rep == 0) count.
        # Records: [v, v], struct-null, [], list-null  (struct def = 1)
        rep = [0, 1, 0, 0, 0]
        def = [4, 4, 0, 2, 1]
        @test Parquet3._record_defs(rep, def) == [4, 0, 2, 1]
        @test (Parquet3._record_defs(rep, def) .< 1) == [false, true, false, false]
    end

    @testset "An encoding on a type it is not defined for is an error" begin
        P = Parquet3
        data = zeros(UInt8, 64)
        # never decoded as some other type (an INT64 BYTE_STREAM_SPLIT column once came back as Float64)
        @test_throws "BYTE_STREAM_SPLIT" P.decode_values(data, 4, P.FIXED_LEN_BYTE_ARRAY, P.BYTE_STREAM_SPLIT, 4, nothing)
        @test_throws "DELTA_BINARY_PACKED" P.decode_values(data, 4, P.DOUBLE, P.DELTA_BINARY_PACKED, 0, nothing)
        @test_throws "DELTA_LENGTH_BYTE_ARRAY" P.decode_values(data, 4, P.INT32, P.DELTA_LENGTH_BYTE_ARRAY, 0, nothing)
        @test_throws "Unsupported encoding: RLE for INT32" P.decode_values(data, 4, P.INT32, P.RLE, 0, nothing)
        @test_throws "Unsupported encoding" P.decode_values(data, 4, P.BYTE_ARRAY, P.DELTA_BYTE_ARRAY, 0, nothing)
        @test_throws "Unsupported level encoding" P.read_levels(data, 4, 1, P.PLAIN)
        @test eltype(P.decode_values(data, 4, P.INT64, P.BYTE_STREAM_SPLIT, 0, nothing)) == Int64
        @test_throws "is not written" P._encode_values([1.5], P.RLE_DICTIONARY)
    end

    @testset "Snappy Decompression" begin
        compressed = UInt8[0x05, 0x10, 0x68, 0x65, 0x6c, 0x6c, 0x6f]
        @test String(Parquet3.decompress(compressed, Parquet3.SNAPPY, 5)) == "hello"
        # The declared size bounds the output: a page claiming less than it holds is rejected
        @test_throws Exception Parquet3.decompress(compressed, Parquet3.SNAPPY, 4)
    end

end

# pyarrow is the reference the tests compare against. It runs through `uv` in the Python
# environment committed under test/pyhelper (pyarrow pinned, uv.lock).
const PYHELPER_DIR = joinpath(@__DIR__, "pyhelper")

# With PARQUET3_TEST_STRICT set (CI sets it), a missing test dependency is a failure, not
# a skip: `uv`/pyarrow for the cross-checks, and the parquet-testing submodule. Without
# it, as on a machine that has neither, those tests are skipped with a warning.
const TEST_STRICT = lowercase(get(ENV, "PARQUET3_TEST_STRICT", "")) in ("1", "true", "yes")

"""
Run a Python script in the test environment and return what it prints. Returns `nothing`
when `uv` is not installed, unless `PARQUET3_TEST_STRICT` is set, in which case that is an
error. A script that fails is always an error.
"""
function _run_pyarrow(script::String)
    uv = Sys.which("uv")
    if uv === nothing
        TEST_STRICT && error("PARQUET3_TEST_STRICT is set but `uv` was not found: the pyarrow cross-checks cannot run")
        return nothing
    end
    strip(read(Cmd(`$uv run --frozen python -c $script`; dir=PYHELPER_DIR), String))
end

@testset "Test dependencies" begin
    version = _run_pyarrow("import pyarrow; print(pyarrow.__version__)")
    if version === nothing
        @warn "uv not found: every pyarrow cross-check in this run is skipped (set PARQUET3_TEST_STRICT=1 to make that a failure)"
    else
        @test version == "23.0.0"       # the version pinned in test/pyhelper and measured against
    end
    @info "Test run" strict = TEST_STRICT pyarrow = version threads = Threads.nthreads() julia = VERSION
end

# Corpus, nullability check and pyarrow comparisons shared by the reader and writer tests
include("reader_harness.jl")

"""Generate a parquet file via pyarrow, run tests, then clean up."""
function _with_pyarrow_file(test_fn::Function, label::String, filename::String, pyscript::String)
    test_file = joinpath(@__DIR__, filename)
    result = _run_pyarrow("write_kwargs = {}\n" * pyscript * "\npq.write_table(table, '$(test_file)', **write_kwargs)\nprint('SUCCESS')")
    if result == "SUCCESS"
        @info "Testing $label"
        try
            test_fn(read_parquet(test_file))
        finally
            rm(test_file, force=true)
        end
    else
        @warn "Skipping $label: uv/pyarrow not available"
    end
end

@group "Real Parquet File" begin
    _with_pyarrow_file("real parquet file", "test.parquet", """
import pyarrow as pa, pyarrow.parquet as pq
table = pa.table({
    'id': [1, 2, 3, 4, 5],
    'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
    'value': [1.5, 2.5, 3.5, 4.5, 5.5],
})""") do tbl
        @test tbl isa Arrow.Table
        @test length(Tables.columns(tbl)) == 3
        @test collect(tbl.id) == [1, 2, 3, 4, 5]
        @test collect(tbl.name) == ["Alice", "Bob", "Charlie", "David", "Eve"]
        @test collect(tbl.value) == [1.5, 2.5, 3.5, 4.5, 5.5]
    end
end

@group "Missing file" begin
    path = joinpath(mktempdir(), "nope.parquet")
    @test_throws "No such file or directory" read_parquet(path)
    @test_throws SystemError open_parquet(path)
    @test !ispath(path)        # and nothing is created there
end

@group "column_names matches read_parquet keys" begin
    _with_pyarrow_file("column_names consistency", "test_colnames.parquet", """
import pyarrow as pa, pyarrow.parquet as pq
table = pa.table({
    'id': [1, 2, 3],
    'name': ['a', 'b', 'c'],
    'tags': [['x', 'y'], ['z'], ['w']],
    'scores': [[1, 2], [3, 4], [5, 6]],
})""") do tbl
        pf = open_parquet(joinpath(@__DIR__, "test_colnames.parquet"))
        expected = collect(string.(Tables.columnnames(tbl)))
        @test column_names(pf) == expected
        close(pf)
    end
end

@group "Nested Data (Lists)" begin
    _with_pyarrow_file("nested data (lists)", "test_nested.parquet", """
import pyarrow as pa, pyarrow.parquet as pq
table = pa.table({
    'id': [1, 2, 3],
    'tags': [['a', 'b'], ['c'], ['d', 'e', 'f']],
    'scores': [[1, 2, 3], [4, 5], [6]],
})""") do tbl
        @test tbl isa Arrow.Table
        @test collect(tbl.id) == [1, 2, 3]

        @test :tags in Tables.columnnames(tbl)
        @test :scores in Tables.columnnames(tbl)

        tags = tbl.tags
        @test length(tags) == 3
        @test collect(skipmissing(tags[1])) == ["a", "b"]
        @test collect(skipmissing(tags[2])) == ["c"]
        @test collect(skipmissing(tags[3])) == ["d", "e", "f"]

        scores = tbl.scores
        @test length(scores) == 3
        @test collect(skipmissing(scores[1])) == [1, 2, 3]
        @test collect(skipmissing(scores[2])) == [4, 5]
        @test collect(skipmissing(scores[3])) == [6]
    end
end

@group "Deeply Nested Data (List<List<Int>>)" begin
    _with_pyarrow_file("deeply nested data (List<List<Int>>)", "test_deep_nested.parquet", """
import pyarrow as pa, pyarrow.parquet as pq
data = [
    [[1, 2], [3]],
    [[4, 5, 6]],
    [[7], [8, 9]],
]
table = pa.table({
    'id': [1, 2, 3],
    'nested': data,
})""") do tbl
        @test tbl isa Arrow.Table
        @test collect(tbl.id) == [1, 2, 3]

        @test :nested in Tables.columnnames(tbl)

        nested = tbl.nested
        @test length(nested) == 3

        @test length(nested[1]) == 2
        @test collect(skipmissing(nested[1][1])) == [1, 2]
        @test collect(skipmissing(nested[1][2])) == [3]

        @test length(nested[2]) == 1
        @test collect(skipmissing(nested[2][1])) == [4, 5, 6]

        @test length(nested[3]) == 2
        @test collect(skipmissing(nested[3][1])) == [7]
        @test collect(skipmissing(nested[3][2])) == [8, 9]
    end
end

@group "Compression Codecs" begin
    pyscript = """
import pyarrow as pa, pyarrow.parquet as pq
table = pa.table({
    'id': [1, 2, 3, 4, 5],
    'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
    'value': [1.5, 2.5, 3.5, 4.5, 5.5],
})"""

    function check_table(tbl)
        @test tbl isa Arrow.Table
        @test collect(tbl.id) == [1, 2, 3, 4, 5]
        @test collect(tbl.name) == ["Alice", "Bob", "Charlie", "David", "Eve"]
        @test collect(tbl.value) == [1.5, 2.5, 3.5, 4.5, 5.5]
    end

    for codec in ["none", "snappy", "gzip", "brotli", "zstd", "lz4"]
        @testset "$codec" begin
            _with_pyarrow_file("compression=$codec", "test_$codec.parquet",
                pyscript * "\nwrite_kwargs = {'compression': '$codec'}") do tbl
                check_table(tbl)
            end
        end
    end
end

@group "Multi-RowGroup with ChainedVector" begin
    _with_pyarrow_file("multi-rowgroup flat", "test_multi_rg.parquet", """
import pyarrow as pa, pyarrow.parquet as pq
table = pa.table({
    'id': list(range(100)),
    'name': [f'name_{i}' for i in range(100)],
    'value': [float(i) * 1.1 for i in range(100)],
})
write_kwargs = {'row_group_size': 10}""") do tbl
        @test tbl isa Arrow.Table
        @test length(tbl.id) == 100
        @test collect(tbl.id) == collect(0:99)
        @test tbl.name[1] == "name_0"
        @test tbl.name[100] == "name_99"
        @test tbl.value[1] ≈ 0.0
        @test tbl.value[100] ≈ 99.0 * 1.1
    end

    _with_pyarrow_file("multi-rowgroup nested", "test_multi_rg_nested.parquet", """
import pyarrow as pa, pyarrow.parquet as pq
table = pa.table({
    'id': list(range(30)),
    'tags': [[f't{i}_{j}' for j in range(i % 3 + 1)] for i in range(30)],
})
write_kwargs = {'row_group_size': 5}""") do tbl
        @test tbl isa Arrow.Table
        @test length(tbl.id) == 30
        @test collect(tbl.id) == collect(0:29)
        tags = tbl.tags
        @test length(tags) == 30
        @test collect(skipmissing(tags[1])) == ["t0_0"]
        @test collect(skipmissing(tags[3])) == ["t2_0", "t2_1", "t2_2"]
    end
end

@group "Field-level metadata from ARROW:schema" begin
    _with_pyarrow_file("field-level metadata", "test_field_meta.parquet", """
import pyarrow as pa, pyarrow.parquet as pq
schema = pa.schema([
    pa.field('x', pa.float64(), metadata={'units': 's', 'codec': 'raw'}),
    pa.field('y', pa.int32(), metadata={'datatype': 'array<1>{real}'}),
    pa.field('plain', pa.utf8()),
])
table = pa.table({'x': [1.0, 2.0], 'y': pa.array([10, 20], type=pa.int32()), 'plain': ['a', 'b']}, schema=schema)""") do tbl
        @test tbl isa Arrow.Table
        mx = Arrow.getmetadata(tbl.x)
        @test mx !== nothing
        @test mx["units"] == "s"
        @test mx["codec"] == "raw"

        my = Arrow.getmetadata(tbl.y)
        @test my !== nothing
        @test my["datatype"] == "array<1>{real}"

        # Column without field metadata should return nothing
        @test Arrow.getmetadata(tbl.plain) === nothing
    end
end

@group "Narrow and unsigned integers" begin
    _with_pyarrow_file("narrow/unsigned ints", "test_small_ints.parquet", """
import pyarrow as pa, pyarrow.parquet as pq
st = pa.struct([('i8', pa.int8()), ('u32', pa.uint32()), ('l', pa.list_(pa.uint64()))])
table = pa.table({
    'i8':  pa.array([-5, None, 127], type=pa.int8()),
    'i16': pa.array([None, -32768, 7], type=pa.int16()),
    'u8':  pa.array([255, None, 0], type=pa.uint8()),
    'u16': pa.array([65535, 1, None], type=pa.uint16()),
    'u32': pa.array([2**32 - 1, None, 2**31], type=pa.uint32()),
    'u64': pa.array([2**64 - 1, 2**63, None], type=pa.uint64()),
    's':   pa.array([{'i8': -5, 'u32': 2**32 - 1, 'l': [2**64 - 1, None]}, None,
                     {'i8': None, 'u32': None, 'l': None}], type=st),
})""") do tbl
        @test isequal(tbl.i8,  Union{Missing, Int8}[-5, missing, 127])
        @test isequal(tbl.i16, Union{Missing, Int16}[missing, -32768, 7])
        @test isequal(tbl.u8,  Union{Missing, UInt8}[255, missing, 0])
        @test isequal(tbl.u16, Union{Missing, UInt16}[65535, 1, missing])
        @test isequal(tbl.u32, Union{Missing, UInt32}[typemax(UInt32), missing, 2^31])
        @test isequal(tbl.u64, Union{Missing, UInt64}[typemax(UInt64), UInt64(2)^63, missing])

        @test :s in propertynames(tbl)
        @test isequal(tbl.s.i8,  Union{Missing, Int8}[-5, missing, missing])
        @test isequal(tbl.s.u32, Union{Missing, UInt32}[typemax(UInt32), missing, missing])
        @test isequal(tbl.s.l[1], Union{Missing, UInt64}[typemax(UInt64), missing])
        @test ismissing(tbl.s[2]) && ismissing(tbl.s.l[3])
    end
end

@group "Null and empty lists at every level" begin
    # v1 data pages, then v2 pages with plain values (whose headers count nulls differently)
    for kwargs in ("{}", "{'data_page_version': '2.0', 'use_dictionary': False}")
    _with_pyarrow_file("nested list nulls ($kwargs)", "test_list_nulls.parquet", """
import pyarrow as pa, pyarrow.parquet as pq
st = pa.struct([('x', pa.list_(pa.string())), ('vv', pa.list_(pa.list_(pa.int64())))])
table = pa.table({
    'll': pa.array([[[1, 2], [3]], [], [[4]], None, [[5], None, [], [None, 6]]],
                   type=pa.list_(pa.list_(pa.int64()))),
    'ls': pa.array([['a', None], None, [], ['b'], [None]], type=pa.list_(pa.string())),
    's':  pa.array([{'x': ['hé', None], 'vv': [[1, 2], [], None, [None, 3]]}, None,
                    {'x': None, 'vv': None}, {'x': [], 'vv': []}, {'x': [None], 'vv': [None]}], type=st),
})
write_kwargs = $kwargs""") do tbl
        # Arrow lists → plain nested vectors, keeping missings at every level
        plain(x) = x isa AbstractVector ? Any[plain(v) for v in x] : x

        # Rows after an empty/null outer list must stay aligned with their inner lists
        @test isequal(plain(tbl.ll), Any[Any[Any[1, 2], Any[3]], Any[], Any[Any[4]], missing,
                                         Any[Any[5], missing, Any[], Any[missing, 6]]])
        @test isequal(plain(tbl.ls), Any[Any["a", missing], missing, Any[], Any["b"], Any[missing]])

        @test :s in propertynames(tbl)
        @test ismissing(tbl.s[2])
        @test isequal(plain(tbl.s.x), Any[Any["hé", missing], missing, missing, Any[], Any[missing]])
        @test isequal(plain(tbl.s.vv), Any[Any[Any[1, 2], Any[], missing, Any[missing, 3]], missing,
                                           missing, Any[], Any[missing]])
    end
    end

    # A v2 page for a leaf under a struct under a list: pyarrow's header does not count the
    # entry of an empty or null list as a null, so the number of stored values has to come
    # from the levels. (With dictionary pages the over-count happens to be harmless.)
    plain(x) = x isa AbstractDict ? Dict(k => plain(v) for (k, v) in x) : x isa AbstractVector ? Any[plain(v) for v in x] : x isa NamedTuple ? map(plain, x) : x
    _with_pyarrow_file("v2 pages: empty and null lists above a struct", "test_v2_struct_lists.parquet", """
import pyarrow as pa, pyarrow.parquet as pq
st = pa.struct([('a', pa.int64())])
table = pa.table({
    'empty': pa.array([[{'a': 1}], [], [{'a': 2}]], pa.list_(st)),
    'null':  pa.array([[{'a': 1}], None, [{'a': 2}]], pa.list_(st)),
    'lls':   pa.array([[[{'a': 1}]], [], [[], [{'a': 2}]]], pa.list_(pa.list_(st))),
    'ms':    pa.array([[('k', {'a': 1})], [], [('j', {'a': 2})]], pa.map_(pa.string(), st)),
})
write_kwargs = {'data_page_version': '2.0', 'use_dictionary': False, 'compression': 'none'}""") do tbl
        @test plain(tbl.empty) == Any[Any[(a = 1,)], Any[], Any[(a = 2,)]]
        @test isequal(plain(tbl.null), Any[Any[(a = 1,)], missing, Any[(a = 2,)]])
        @test plain(tbl.lls) == Any[Any[Any[(a = 1,)]], Any[], Any[Any[], Any[(a = 2,)]]]
        @test plain(tbl.ms) == Any[Dict("k" => (a = 1,)), Dict(), Dict("j" => (a = 2,))]
    end
end

@group "Zero-row file" begin
    _with_pyarrow_file("zero rows", "test_zero_rows.parquet", """
import pyarrow as pa, pyarrow.parquet as pq
schema = pa.schema([
    ('id', pa.int64()), ('name', pa.string()), ('flag', pa.bool_()), ('d', pa.date32()),
    ('u', pa.uint16()), ('l', pa.list_(pa.int32())), ('ll', pa.list_(pa.list_(pa.float64()))),
    ('s', pa.struct([('a', pa.int64()), ('v', pa.list_(pa.int64())), ('n', pa.struct([('x', pa.float32())]))])),
    ('los', pa.list_(pa.struct([('pt', pa.float32()), ('q', pa.int8())]))),
])
table = schema.empty_table()""") do tbl
        names = [:id, :name, :flag, :d, :u, :l, :ll, :s, :los]
        @test collect(propertynames(tbl)) == names
        @test all(n -> length(getproperty(tbl, n)) == 0, names)
        @test nonmissingtype(eltype(tbl.id)) == Int64
        @test nonmissingtype(eltype(tbl.name)) == String
        @test nonmissingtype(eltype(tbl.flag)) == Bool
        @test nonmissingtype(eltype(tbl.u)) == UInt16
        @test tbl.s isa Parquet3.StructColumn && isempty(tbl.s.a) && isempty(tbl.s.n.x)
        @test tbl.los isa Parquet3.ListOfStructsColumn && isempty(tbl.los.pt)
    end
end

@group "Column selection" begin
    _with_pyarrow_file("column selection", "test_select.parquet", """
import pyarrow as pa, pyarrow.parquet as pq
table = pa.table({'id': [1, 2], 's': pa.array([{'a': 1, 'b': 'x'}, {'a': 2, 'b': 'y'}], type=pa.struct([('a', pa.int64()), ('b', pa.string())]))})""") do _
        path = joinpath(@__DIR__, "test_select.parquet")
        @test collect(propertynames(read_parquet(path; columns=["s"]))) == [:s]
        @test collect(propertynames(read_parquet(path; columns=["s", "id"]))) == [:id, :s]     # schema order
        # A member is selected by its dotted path; the struct comes back with only that member
        tbl = read_parquet(path; columns=["id", "s.a"])
        @test collect(propertynames(tbl)) == [:id, :s] && propertynames(tbl.s) == (:a,) && tbl.s.a == [1, 2]
        # A name that matches nothing is an error naming it
        @test_throws "no column matches \"typo\"" read_parquet(path; columns=["id", "s.a", "typo"])
        @test_throws ArgumentError read_parquet(path; columns=["a"])       # a bare member name is not a column
    end
end

@group "Struct edge cases" begin
    plain(x) = x isa AbstractVector ? Any[plain(v) for v in x] : x

    _with_pyarrow_file("required struct and members", "test_struct_required.parquet", """
import pyarrow as pa, pyarrow.parquet as pq
req = pa.struct([pa.field('a', pa.int64(), nullable=False), pa.field('b', pa.int64()),
                 pa.field('v', pa.list_(pa.int64()), nullable=False)])
opt = pa.struct([pa.field('a', pa.int64(), nullable=False),
                 pa.field('v', pa.list_(pa.int64()), nullable=False)])
schema = pa.schema([pa.field('req', req, nullable=False), pa.field('opt', opt), pa.field('allnull', opt)])
table = pa.table({
    'req': pa.array([{'a': 1, 'b': None, 'v': []}, {'a': 2, 'b': 20, 'v': [None, 5]}], type=req),
    'opt': pa.array([None, {'a': 3, 'v': [7]}], type=opt),
    'allnull': pa.array([None, None], type=opt),
}, schema=schema)""") do tbl
        @test !(Missing <: eltype(tbl.req))
        @test tbl.req.a == [1, 2]
        @test isequal(tbl.req.b, [missing, 20])
        @test isequal(plain(tbl.req.v), Any[Any[], Any[missing, 5]])
        @test ismissing(tbl.opt[1]) && tbl.opt[2].a == 3 && tbl.opt[2].v == [7]
        @test all(ismissing, tbl.allnull) && length(tbl.allnull) == 2
    end

    # Nulls only in the last row group; struct null, member null, and empty list all present
    rows = """
import pyarrow as pa, pyarrow.parquet as pq
t = pa.struct([('a', pa.int64()), ('v', pa.list_(pa.int64()))])
rows = [{'a': i, 'v': [i, i + 1]} for i in range(20)] + [None, {'a': None, 'v': None}, {'a': 22, 'v': []}]
table = pa.table({'s': pa.array(rows, type=t)})
"""
    for (label, kwargs) in (("multi-RG struct with list member", "{'row_group_size': 10}"),
                            ("multi-RG struct without statistics", "{'row_group_size': 10, 'write_statistics': False}"))
        _with_pyarrow_file(label, "test_struct_multirg.parquet", rows * "write_kwargs = $kwargs") do tbl
            @test length(tbl.s) == 23
            @test isequal(tbl.s.a, [0:19; missing; missing; 22])
            @test all(i -> tbl.s[i].v == [i - 1, i], 1:20)
            @test ismissing(tbl.s[21]) && !ismissing(tbl.s[22]) && ismissing(tbl.s[22].v)
            @test isempty(tbl.s[23].v) && ismissing(tbl.s.v[21]) && ismissing(tbl.s.v[22])
        end
    end
end

@group "Page headers larger than 1 KB" begin
    # pyarrow stores min/max statistics in the page header, so long values make long headers
    _with_pyarrow_file("long page headers", "test_long_header.parquet", """
import pyarrow as pa, pyarrow.parquet as pq
st = pa.struct([('label', pa.string()), ('n', pa.int32())])
table = pa.table({
    's':    ['a' * 1500, 'b' * 1500, 'c' * 1500],
    'huge': ['x' * 100000, 'y' * 100000, None],
    'l':    pa.array([['p' * 3000, 'q'], [], ['r' * 3000]], type=pa.list_(pa.string())),
    'st':   pa.array([{'label': 'm' * 5000, 'n': 1}, None, {'label': 'z' * 5000, 'n': 3}], type=st),
    'id':   [1, 2, 3],
})""") do tbl
        @test collect(propertynames(tbl)) == [:s, :huge, :l, :st, :id]
        @test collect(tbl.s) == ["a"^1500, "b"^1500, "c"^1500]
        @test isequal(collect(tbl.huge), ["x"^100000, "y"^100000, missing])
        @test collect.(tbl.l) == [["p"^3000, "q"], String[], ["r"^3000]]
        @test tbl.st[1] == (label = "m"^5000, n = Int32(1)) && ismissing(tbl.st[2]) && tbl.st.label[3] == "z"^5000
        @test collect(tbl.id) == [1, 2, 3]
    end
end

@group "Struct Columns" begin
    _with_pyarrow_file("flat struct with nulls", "test_struct.parquet", """
import pyarrow as pa, pyarrow.parquet as pq
person = pa.array([
    {'name': 'Alice', 'age': 30},
    {'name': None, 'age': 25},
    None,
    {'name': 'Dana', 'age': None},
], type=pa.struct([pa.field('name', pa.string()), pa.field('age', pa.int64())]))
table = pa.table({'id': [1, 2, 3, 4], 'person': person})""") do tbl
        @test tbl isa Arrow.Table
        @test collect(tbl.id) == [1, 2, 3, 4]
        @test :person in Tables.columnnames(tbl)

        p = tbl.person
        @test p isa Parquet3.StructColumn
        @test length(p) == 4
        @test propertynames(p) == (:name, :age)
        @test p.name[1] == "Alice"                    # named child-column access
        @test isequal(collect(p.age), [30, 25, missing, missing])
        @test p[1] == (name = "Alice", age = 30)
        @test p[2].name === missing
        @test p[2].age == 25
        @test p[3] === missing            # struct-level null, not a struct of missings
        @test p[4].name == "Dana"
        @test p[4].age === missing

        # column_names reports the struct as one column
        pf = open_parquet(joinpath(@__DIR__, "test_struct.parquet"))
        @test column_names(pf) == ["id", "person"]
        close(pf)

        # Column selection by struct name
        sel = read_parquet(joinpath(@__DIR__, "test_struct.parquet"); columns=["person"])
        @test collect(Tables.columnnames(sel)) == [:person]
        @test sel.person[1] == (name = "Alice", age = 30)
    end

    _with_pyarrow_file("struct without nulls", "test_struct_dense.parquet", """
import pyarrow as pa, pyarrow.parquet as pq
point = pa.array([{'x': i, 'y': float(i) * 0.5} for i in range(6)],
                 type=pa.struct([pa.field('x', pa.int64()), pa.field('y', pa.float64())]))
table = pa.table({'point': point})""") do tbl
        p = tbl.point
        @test length(p) == 6
        # No nulls anywhere: eltype should not include Missing
        @test !(Missing <: eltype(p))
        @test p[3] == (x = 2, y = 1.0)
        @test [v.x for v in p] == collect(0:5)
    end

    _with_pyarrow_file("struct with list field", "test_struct_list.parquet", """
import pyarrow as pa, pyarrow.parquet as pq
wf = pa.array([
    {'t0': 0.0, 'dt': 1.0, 'values': [1, 2, 3]},
    {'t0': 0.5, 'dt': 1.0, 'values': []},
    None,
    {'t0': 1.5, 'dt': None, 'values': None},
    {'t0': 2.0, 'dt': 2.0, 'values': [4, None, 5]},
], type=pa.struct([pa.field('t0', pa.float32()), pa.field('dt', pa.float32()),
                   pa.field('values', pa.list_(pa.int32()))]))
table = pa.table({'id': [1, 2, 3, 4, 5], 'wf': wf})""") do tbl
        wf = tbl.wf
        @test wf isa Parquet3.StructColumn
        @test length(wf) == 5
        @test wf.t0 isa AbstractVector            # full child columns by name
        @test collect(skipmissing(wf.t0)) == Float32[0.0, 0.5, 1.5, 2.0]
        @test wf.values isa Arrow.List
        @test length(wf.values) == 5
        @test wf[1].t0 == 0.0f0
        @test collect(skipmissing(wf[1].values)) == Int32[1, 2, 3]
        @test wf[2].values !== missing        # empty list, not a null list
        @test isempty(wf[2].values)
        @test wf[3] === missing               # struct-level null
        @test wf[4].dt === missing
        @test wf[4].values === missing        # list-level null (struct present)
        @test wf[5].values[2] === missing     # element-level null
        @test collect(skipmissing(wf[5].values)) == Int32[4, 5]
    end

    # No flat member: struct validity must come from the list member's rep/def levels
    _with_pyarrow_file("struct with only list fields", "test_struct_only_lists.parquet", """
import pyarrow as pa, pyarrow.parquet as pq
s = pa.array([{'a': [1, 2], 'b': [7]}, None, {'a': [], 'b': None}],
             type=pa.struct([pa.field('a', pa.list_(pa.int64())), pa.field('b', pa.list_(pa.int64()))]))
table = pa.table({'s': s})""") do tbl
        s = tbl.s
        @test length(s) == 3
        @test collect(skipmissing(s[1].a)) == [1, 2]
        @test collect(skipmissing(s[1].b)) == [7]
        @test s[2] === missing
        @test s[3].a !== missing && isempty(s[3].a)
        @test s[3].b === missing
    end

    _with_pyarrow_file("nested struct (struct-of-struct)", "test_struct_nested.parquet", """
import pyarrow as pa, pyarrow.parquet as pq
event = pa.array([
    {'vertex': {'x': 1.0, 'y': 2.0, 'z': 3.0}, 'energy': 10},
    {'vertex': None, 'energy': 20},
    None,
    {'vertex': {'x': 4.0, 'y': None, 'z': 6.0}, 'energy': None},
], type=pa.struct([
    pa.field('vertex', pa.struct([('x', pa.float64()), ('y', pa.float64()), ('z', pa.float64())])),
    pa.field('energy', pa.int64()),
]))
table = pa.table({'event': event})""") do tbl
        e = tbl.event
        @test e isa Parquet3.StructColumn
        @test length(e) == 4
        @test e[1].vertex == (x = 1.0, y = 2.0, z = 3.0)   # row access recurses
        @test e[1].energy == 10
        @test e[2].vertex === missing                       # inner-struct null
        @test e[2].energy == 20
        @test e[3] === missing                              # outer-struct null
        @test e[4].vertex.y === missing
        @test e[4].vertex.x == 4.0

        # Named access composes through nesting levels
        v = e.vertex
        @test v isa Parquet3.StructColumn
        @test v[2] === missing && v[3] === missing          # outer null propagates
        @test collect(skipmissing(v.x)) == [1.0, 4.0]
        @test isequal(collect(e.energy), [10, 20, missing, missing])
    end

    _with_pyarrow_file("nested struct containing list", "test_struct_nested_list.parquet", """
import pyarrow as pa, pyarrow.parquet as pq
s = pa.array([
    {'meta': {'tag': 'a', 'ids': [1, 2]}, 'n': 1},
    {'meta': {'tag': 'b', 'ids': []}, 'n': 2},
], type=pa.struct([
    pa.field('meta', pa.struct([('tag', pa.string()), ('ids', pa.list_(pa.int64()))])),
    pa.field('n', pa.int32()),
]))
table = pa.table({'s': s})""") do tbl
        s = tbl.s
        @test s[1].meta.tag == "a"
        @test collect(skipmissing(s[1].meta.ids)) == [1, 2]
        @test isempty(s[2].meta.ids)
        @test s.meta.ids isa Arrow.List                     # named access to depth-2 list
        @test collect(skipmissing(s.meta.ids[1])) == [1, 2]
        @test isequal(collect(s.n), Int32[1, 2])
    end

    _with_pyarrow_file("list of structs", "test_los.parquet", """
import pyarrow as pa, pyarrow.parquet as pq
particles = pa.array([
    [{'pt': 1.0, 'eta': 0.5, 'charge': 1}, {'pt': 2.0, 'eta': None, 'charge': -1}],
    [],
    None,
    [{'pt': 3.0, 'eta': 1.5, 'charge': 0}, None, {'pt': 4.0, 'eta': -1.0, 'charge': 1}],
], type=pa.list_(pa.struct([pa.field('pt', pa.float64()), pa.field('eta', pa.float64()),
                            pa.field('charge', pa.int32())])))
table = pa.table({'id': [1, 2, 3, 4], 'particles': particles})""") do tbl
        ps = tbl.particles
        @test ps isa Parquet3.ListOfStructsColumn
        @test length(ps) == 4
        @test propertynames(ps) == (:pt, :eta, :charge)

        @test length(ps[1]) == 2
        @test ps[1][1] == (pt = 1.0, eta = 0.5, charge = Int32(1))
        @test ps[1][2].eta === missing            # field-level null
        @test ps[1][2].pt == 2.0
        @test ps[2] !== missing && isempty(ps[2]) # empty list
        @test ps[3] === missing                   # list-level null
        @test ps[4][2] === missing                # element-level null
        @test ps[4][3].pt == 4.0

        # Named ragged access: one field across all records, sharing offsets
        pts = ps.pt
        @test pts isa Arrow.List
        @test collect(skipmissing(pts[1])) == [1.0, 2.0]
        @test pts[3] === missing
        @test isequal(collect(pts[4]), [3.0, missing, 4.0])
    end

    _with_pyarrow_file("multi-rowgroup list of structs", "test_los_multi_rg.parquet", """
import pyarrow as pa, pyarrow.parquet as pq
n = 30
data = [[{'a': i * 10 + k, 'b': f's{i}_{k}'} for k in range(i % 4)] for i in range(n)]
particles = pa.array(data, type=pa.list_(pa.struct([pa.field('a', pa.int64()),
                                                    pa.field('b', pa.string())])))
table = pa.table({'particles': particles})
write_kwargs = {'row_group_size': 7}""") do tbl
        ps = tbl.particles
        @test length(ps) == 30
        @test isempty(ps[1])                      # i = 0: 0 elements
        @test length(ps[4]) == 3                  # i = 3: 3 elements
        @test ps[4][2].a == 31 && ps[4][2].b == "s3_1"
        @test ps[30][1].a == 290                  # i = 29, crosses chunks
        @test length(ps.a) == 30                  # chunk-chained field access
        @test collect(skipmissing(ps.a[4])) == [30, 31, 32]
        @test sum(length, ps.a) == sum(i % 4 for i in 0:29)
    end

    # list<struct{list}> is one column with named access to both members
    _with_pyarrow_file("list<struct{list}>", "test_los_fallback.parquet", """
import pyarrow as pa, pyarrow.parquet as pq
particles = pa.array(
    [[{'pt': 1.0, 'trace': [1, 2]}], [{'pt': 2.0, 'trace': [3]}, {'pt': 3.0, 'trace': []}]],
    type=pa.list_(pa.struct([pa.field('pt', pa.float64()),
                             pa.field('trace', pa.list_(pa.int32()))])))
table = pa.table({'particles': particles})""") do tbl
        @test collect(Tables.columnnames(tbl)) == [:particles]
        @test tbl.particles isa Parquet3.ListOfStructsColumn && propertynames(tbl.particles) == (:pt, :trace)
        @test collect(tbl.particles.pt[2]) == [2.0, 3.0]
        @test collect.(tbl.particles.trace[2]) == [[3], Int32[]] && tbl.particles[1][1].trace == [1, 2]
    end

    _with_pyarrow_file("multi-rowgroup struct", "test_struct_multi_rg.parquet", """
import pyarrow as pa, pyarrow.parquet as pq
n = 40
person = pa.array(
    [{'x': i, 'y': float(i)} if i % 5 else None for i in range(n)],
    type=pa.struct([pa.field('x', pa.int64()), pa.field('y', pa.float64())]))
table = pa.table({'person': person})
write_kwargs = {'row_group_size': 10}""") do tbl
        p = tbl.person
        @test length(p) == 40
        @test p[1] === missing            # i = 0
        @test p[6] === missing            # i = 5
        @test p[2].x == 1
        @test p[13].y == 12.0             # crosses into second row group
        @test count(ismissing, p) == 8
        # Named child access chains across row-group chunks
        @test length(p.x) == 40
        @test p.x[2] == 1 && p.x[13] == 12
        @test isequal(collect(skipmissing(p.y)), [float(i) for i in 0:39 if i % 5 != 0])
    end
end

@group "Writer (W1)" begin
    wfile(name) = joinpath(@__DIR__, name)

    @testset "flat round-trip, all supported types" begin
        tbl = (
            i32  = Int32[1, -2, 3, typemax(Int32)],
            i64  = [10, -20, 30, typemin(Int64)],
            f32  = Float32[1.5, -2.5, Inf32, 0.0],
            f64  = [0.1, -0.2, NaN, 4.0e100],
            flag = [true, false, true, false],
            str  = ["alice", "", "déjà vu", "z"],
            byt  = [UInt8[1, 2], UInt8[], UInt8[0xff], UInt8[0x00]],
            oi   = [1, missing, 3, missing],
            os   = [missing, "x", missing, "z"],
            ob   = [true, missing, missing, false],
        )
        f = wfile("test_w1_roundtrip.parquet")
        try
            write_parquet(f, tbl)
            t = read_parquet(f)
            @test collect(Tables.columnnames(t)) == collect(keys(tbl))
            for k in keys(tbl)
                @test isequal(collect(Tables.getcolumn(t, k)), collect(tbl[k]))
            end
        finally
            rm(f, force=true)
        end
    end

    @testset "edge cases" begin
        f = wfile("test_w1_edge.parquet")
        try
            # 0 rows
            write_parquet(f, (a = Int32[], b = String[]))
            t = read_parquet(f)
            @test length(t.a) == 0 && length(t.b) == 0

            # all-missing typed column
            write_parquet(f, (x = Union{Missing, Int64}[missing, missing, missing],))
            t = read_parquet(f)
            @test all(ismissing, t.x) && length(t.x) == 3

            # errors
            @test_throws Exception write_parquet(f, (bad = Union{Missing, Missing}[missing],))
            @test_throws Exception write_parquet(f, (bad = [1im, 2im],))
        finally
            rm(f, force=true)
        end
    end

    @testset "footer is re-parseable metadata" begin
        f = wfile("test_w1_meta.parquet")
        try
            write_parquet(f, (a = [1, 2, missing], b = ["x", "y", "z"]))
            pf = open_parquet(f)
            @test num_rows(pf) == 3
            @test num_row_groups(pf) == 1
            @test column_names(pf) == ["a", "b"]
            # null_count statistics present (drives reader's eltype decisions)
            st = pf.metadata.row_groups[1].columns[1].meta_data.statistics
            @test st !== nothing && st.null_count == 1
            close(pf)
        finally
            rm(f, force=true)
        end
    end

    @testset "pyarrow reads our files" begin
        f = wfile("test_w1_pyarrow.parquet")
        try
            write_parquet(f, (a = Int32[1, 2, 3], b = ["x", "y", "z"], c = [1.5, missing, 3.5]))
            result = _run_pyarrow("""
import pyarrow.parquet as pq
t = pq.read_table('$(f)')
print(t.column('a').to_pylist())
print(t.column('b').to_pylist())
print(t.column('c').to_pylist())""")
            if result !== nothing
                lines = split(result, '\n')
                @test lines[1] == "[1, 2, 3]"
                @test lines[2] == "['x', 'y', 'z']"
                @test lines[3] == "[1.5, None, 3.5]"
            else
                @warn "Skipping pyarrow cross-check of written file: uv/pyarrow not available"
            end
        finally
            rm(f, force=true)
        end
    end

    @testset "List<primitive> round-trip (N1)" begin
        plain(x) = x isa AbstractVector ? Any[plain(v) for v in x] : x
        tbl = (
            li  = [Int32[1, 2], Int32[], Int32[3]],
            lf  = [[1.5, NaN], [2.5], Float64[]],
            ls  = [["a", ""], String[], ["déjà vu"]],
            lb  = [[true, false], [true], Bool[]],
            # nulls at list and element level
            oli = [[1, missing, 3], missing, Union{Missing, Int64}[]],
            ols = [missing, ["x", missing], [missing]],
            id  = [1, 2, 3],
        )
        f = wfile("test_n1_lists.parquet")
        try
            write_parquet(f, tbl)
            t = read_parquet(f)
            @test collect(Tables.columnnames(t)) == collect(keys(tbl))
            for k in keys(tbl)
                @test isequal(plain(Tables.getcolumn(t, k)), plain(tbl[k]))
            end

            # zero rows, all-null, and all-empty list columns
            write_parquet(f, (a = Vector{Int32}[],))
            @test length(read_parquet(f).a) == 0
            write_parquet(f, (b = Union{Missing, Vector{Int64}}[missing, missing], c = [Float32[], Float32[]]))
            t = read_parquet(f)
            @test all(ismissing, t.b) && length(t.b) == 2
            @test all(isempty, t.c) && length(t.c) == 2

            write_parquet(f, (l = [Int32[1, 2], missing, Int32[], [missing, Int32(5)]], s = [["x"], ["y", "z"], String[], missing]))
            result = _run_pyarrow("""
import pyarrow.parquet as pq
t = pq.read_table('$(f)')
print(t.schema.field('l').type)
print(t.column('l').to_pylist())
print(t.column('s').to_pylist())""")
            if result !== nothing
                lines = split(result, '\n')
                @test lines[1] == "list<element: int32>"
                @test lines[2] == "[[1, 2], None, [], [None, 5]]"
                @test lines[3] == "[['x'], ['y', 'z'], [], None]"
            else
                @warn "Skipping pyarrow cross-check of written lists: uv/pyarrow not available"
            end
        finally
            rm(f, force=true)
        end
    end

    @testset "Struct of flat fields round-trip (N2)" begin
        P = @NamedTuple{x::Float64, n::Union{Missing, Int32}, tag::String, ok::Bool}
        tbl = (
            # no nulls anywhere
            pos = [(x = 1.0, y = 2.0), (x = 3.0, y = NaN)],
            # null structs, null members, and a struct whose members are all null
            s   = Union{Missing, P}[(x = 1.5, n = Int32(1), tag = "a", ok = true), missing],
            m   = @NamedTuple{a::Union{Missing, Int64}, b::Union{Missing, String}}[
                      (a = missing, b = "x"), (a = 2, b = missing), ],
            id  = [1, 2],
        )
        f = wfile("test_n2_structs.parquet")
        try
            write_parquet(f, tbl)
            t = read_parquet(f)
            @test collect(Tables.columnnames(t)) == collect(keys(tbl))
            for k in keys(tbl)
                @test isequal(collect(Tables.getcolumn(t, k)), collect(tbl[k]))
            end
            @test t.s isa Parquet3.StructColumn
            @test t.pos.x == [1.0, 3.0] && isequal(t.s.tag, ["a", missing])
            @test !(Missing <: eltype(t.pos))

            # zero rows and all-null struct columns
            write_parquet(f, (s = P[],))
            @test length(read_parquet(f).s) == 0
            write_parquet(f, (s = Union{Missing, P}[missing, missing], id = [1, 2]))
            @test all(ismissing, read_parquet(f).s)

            # untyped rows are rejected
            @test_throws Exception write_parquet(f, (bad = Any[(a = 1,)],))
            @test_throws Exception write_parquet(f, (bad = [(a = missing,), (a = 2,)],))

            write_parquet(f, (s = Union{Missing, @NamedTuple{a::Union{Missing, Int32}, b::String}}[
                                  (a = Int32(1), b = "x"), missing, (a = missing, b = "z")],))
            result = _run_pyarrow("""
import pyarrow.parquet as pq
t = pq.read_table('$(f)')
print(t.schema.field('s').type)
print(t.column('s').to_pylist())""")
            if result !== nothing
                lines = split(result, '\n')
                @test lines[1] == "struct<a: int32, b: string>"
                @test lines[2] == "[{'a': 1, 'b': 'x'}, None, {'a': None, 'b': 'z'}]"
            else
                @warn "Skipping pyarrow cross-check of written structs: uv/pyarrow not available"
            end
        finally
            rm(f, force=true)
        end
    end

    @testset "Nested composition round-trip (N3)" begin
        plain(x) = x isa AbstractVector ? Any[plain(v) for v in x] : x isa NamedTuple ? map(plain, x) : x
        WF = @NamedTuple{t0::Float64, values::Union{Missing, Vector{Union{Missing, Int32}}}}
        PT = @NamedTuple{pt::Float32, q::Union{Missing, Int32}}
        OLL = Union{Missing, Vector{Union{Missing, Vector{Union{Missing, Int64}}}}}
        tbl = (
            # struct{list}: null struct, null list, empty list, null element
            wf    = Union{Missing, WF}[(t0 = 0.5, values = [1, missing]), missing,
                                       (t0 = 1.5, values = missing), (t0 = 2.5, values = [])],
            # struct-of-struct
            ev    = [(id = i, vertex = (x = 0.1i, tag = "v$i")) for i in 1:4],
            # list<struct>
            parts = Union{Missing, Vector{PT}}[[(pt = 1f0, q = 1), (pt = 2f0, q = missing)], PT[], missing, [(pt = 3f0, q = -1)]],
            # list<list>, with a row after an empty outer list
            ll    = [[[1, 2], [3]], Vector{Int}[], [[4]], [Int[], [5]]],
            oll   = OLL[[[1, missing], missing, []], missing, [], [[2]]],
            lll   = [[[[1.5], Float64[]]], [[[2.5, 3.5]], Vector{Float64}[]], Vector{Vector{Float64}}[], [[[4.5]]]],
        )
        f = wfile("test_n3_nested.parquet")
        try
            write_parquet(f, tbl)
            t = read_parquet(f)
            @test collect(Tables.columnnames(t)) == collect(keys(tbl))
            for k in keys(tbl)
                @test isequal(plain(Tables.getcolumn(t, k)), plain(tbl[k]))
            end
            @test t.wf isa Parquet3.StructColumn && t.parts isa Parquet3.ListOfStructsColumn
            @test t.ev.vertex.tag == ["v1", "v2", "v3", "v4"]

            # Shapes our reader does not assemble yet: check them with pyarrow
            E = @NamedTuple{a::Int32, v::Vector{Int32}}
            write_parquet(f, (
                los = [[(a = Int32(1), v = Int32[1, 2]), (a = Int32(2), v = Int32[])], E[], [(a = Int32(3), v = Int32[3])]],
                sl  = Union{Missing, @NamedTuple{hits::Vector{@NamedTuple{x::Int32}}}}[
                          (hits = [(x = Int32(1),), (x = Int32(2),)],), missing, (hits = [],)],
            ))
            result = _run_pyarrow("""
import pyarrow.parquet as pq
t = pq.read_table('$(f)')
print(t.schema.field('los').type)
print(t.column('los').to_pylist())
print(t.schema.field('sl').type)
print(t.column('sl').to_pylist())""")
            if result !== nothing
                lines = split(result, '\n')
                @test lines[1] == "list<element: struct<a: int32, v: list<element: int32>>>"
                @test lines[2] == "[[{'a': 1, 'v': [1, 2]}, {'a': 2, 'v': []}], [], [{'a': 3, 'v': [3]}]]"
                @test lines[3] == "struct<hits: list<element: struct<x: int32>>>"
                @test lines[4] == "[{'hits': [{'x': 1}, {'x': 2}]}, None, {'hits': []}]"
            else
                @warn "Skipping pyarrow cross-check of nested writes: uv/pyarrow not available"
            end
        finally
            rm(f, force=true)
        end
    end

    @testset "Read → write round-trip of reader containers (N4)" begin
        plain(x) = x isa AbstractVector ? Any[plain(v) for v in x] : x isa NamedTuple ? map(plain, x) : x
        src, out = wfile("test_n4_src.parquet"), wfile("test_n4_out.parquet")
        pytable = """
import pyarrow as pa, pyarrow.parquet as pq
wf = pa.struct([('t0', pa.float64()), ('values', pa.list_(pa.int32()))])
ev = pa.struct([('id', pa.int64()), ('vertex', pa.struct([('x', pa.float32()), ('tag', pa.string())]))])
pt = pa.struct([('pt', pa.float32()), ('q', pa.int32())])
table = pa.table({
    'i32': pa.array([1, None, 3, 4, 5, 6], type=pa.int32()),
    'f64': pa.array([0.1, 0.2, 0.3, 0.4, 0.5, 0.6], type=pa.float64()),
    'flag': pa.array([True, False, None, True, False, True]),
    'name': pa.array(['a', None, 'déjà', '', 'e', 'f']),
    'blob': pa.array([b'ab', b'', None, b'c', b'x', b'y'], type=pa.binary()),
    'hits': pa.array([[1, 2], [], None, [None, 4], [5], [6, 7, 8]], type=pa.list_(pa.int32())),
    'words': pa.array([['a', None], None, [], ['b'], ['c', 'd'], ['e']], type=pa.list_(pa.string())),
    'll': pa.array([[[1, 2], [3]], [], [[4]], None, [[5], None, [], [None, 6]], [[7]]], type=pa.list_(pa.list_(pa.int64()))),
    'wf': pa.array([{'t0': 0.5, 'values': [1, None]}, None, {'t0': 1.5, 'values': None}, {'t0': 2.5, 'values': []},
                    {'t0': None, 'values': [9]}, {'t0': 3.5, 'values': [1, 2, 3]}], type=wf),
    'ev': pa.array([{'id': i, 'vertex': {'x': 0.5 * i, 'tag': 'v%d' % i}} for i in range(6)], type=ev),
    'parts': pa.array([[{'pt': 1.0, 'q': 1}, {'pt': 2.0, 'q': None}], [], None, [{'pt': 3.0, 'q': -1}],
                       [{'pt': 4.0, 'q': 1}], []], type=pa.list_(pt)),
    'fsl': pa.FixedSizeListArray.from_arrays(pa.array(range(18), type=pa.int32()), 3),
    'fslf': pa.FixedSizeListArray.from_arrays(pa.array([0.5 * i for i in range(12)], type=pa.float64()), 2),
})
"""
        # Single row group, then several (columns arrive as ChainedVector)
        for kwargs in ("{}", "{'row_group_size': 4}")
            _with_pyarrow_file("reader containers ($kwargs)", "test_n4_src.parquet", pytable * "write_kwargs = $kwargs") do t
                try
                    write_parquet(out, t)
                    back = read_parquet(out)
                    @test collect(Tables.columnnames(back)) == collect(Tables.columnnames(t))
                    for k in Tables.columnnames(t)
                        @test isequal(plain(Tables.getcolumn(back, k)), plain(Tables.getcolumn(t, k)))
                    end
                    @test back.wf isa Parquet3.StructColumn && back.parts isa Parquet3.ListOfStructsColumn
                    # FixedSizeList survives via the ARROW:schema metadata we write (N1.5)
                    @test back.fsl isa Parquet3.FixedSizeListVector{3, Int32}
                    @test back.fslf isa Parquet3.FixedSizeListVector{2, Float64}

                    # Arrow IPC round-trip of nested reader columns, including a struct with a
                    # list member and a null struct row (the fuller test is "Arrow.write" below)
                    arrow_rt(col) = (io = IOBuffer(); Arrow.write(io, (c = col,)); seekstart(io);
                                     isequal(plain(Arrow.Table(io).c), plain(col)))
                    @test arrow_rt(t.ev) && arrow_rt(t.parts)
                    @test arrow_rt(t.wf)

                    # ARROW:schema is written only when a FixedSizeList column needs it
                    has_schema(path) = (pf = open_parquet(path); kv = pf.metadata.key_value_metadata; close(pf);
                                        kv !== nothing && any(e -> e.key == "ARROW:schema", kv))
                    @test has_schema(out)
                    write_parquet(out, (hits = t.hits, wf = t.wf))
                    @test !has_schema(out)
                    write_parquet(out, t)

                    # pyarrow sees the same values and types as in its own file
                    result = _run_pyarrow("""
import pyarrow.parquet as pq
a, b = pq.read_table('$(src)'), pq.read_table('$(out)')
print([n for n in a.column_names if a.column(n).to_pylist() != b.column(n).to_pylist()])
print([n for n in a.column_names if a.schema.field(n).type != b.schema.field(n).type])
print(b.schema.field('fsl').type, '|', b.schema.field('fslf').type)""")
                    lines = split(result, '\n')
                    @test lines[1] == "[]"
                    @test lines[2] == "[]"
                    @test lines[3] == "fixed_size_list<element: int32>[3] | fixed_size_list<element: double>[2]"
                finally
                    rm(out, force=true)
                end
            end
        end
    end

    @testset "Narrow and unsigned integers round-trip" begin
        tbl = (
            i8  = Int8[-128, 0, 127],
            i16 = [typemin(Int16), missing, typemax(Int16)],
            u8  = UInt8[0, 128, 255],
            u16 = [missing, 0x0001, typemax(UInt16)],
            u32 = UInt32[0, 2^31, typemax(UInt32)],
            u64 = [typemax(UInt64), UInt64(2)^63, missing],
            l   = [Int8[-1, 1], Int8[], Int8[127]],
            s   = [(a = Int16(-7), b = 0xff), (a = Int16(7), b = 0x00), (a = Int16(0), b = 0x80)],
        )
        f = wfile("test_small_ints_w.parquet")
        try
            write_parquet(f, tbl)
            t = read_parquet(f)
            for k in (:i8, :i16, :u8, :u16, :u32, :u64, :s)
                col = Tables.getcolumn(t, k)
                @test isequal(collect(col), collect(tbl[k]))
                @test nonmissingtype(eltype(col)) == nonmissingtype(eltype(tbl[k]))
            end
            # the empty list makes the element type nullable on read (null_count counts it)
            @test collect.(t.l) == tbl.l && nonmissingtype(eltype(first(t.l))) == Int8

            result = _run_pyarrow("""
import pyarrow.parquet as pq
t = pq.read_table('$(f)')
print(' '.join(str(t.schema.field(n).type) for n in ['i8', 'i16', 'u8', 'u16', 'u32', 'u64']))
print(t.column('i8').to_pylist(), t.column('u32').to_pylist(), t.column('u64').to_pylist())
print(t.schema.field('l').type, t.schema.field('s').type)""")
            if result !== nothing
                lines = split(result, '\n')
                @test lines[1] == "int8 int16 uint8 uint16 uint32 uint64"
                @test lines[2] == "[-128, 0, 127] [0, 2147483648, 4294967295] [18446744073709551615, 9223372036854775808, None]"
                @test lines[3] == "list<element: int8> struct<a: int16, b: uint8>"
            else
                @warn "Skipping pyarrow cross-check of narrow ints: uv/pyarrow not available"
            end
        finally
            rm(f, force=true)
        end
    end

    @testset "FixedSizeList inside a struct (N1.6)" begin
        plain(x) = x isa AbstractVector ? Any[plain(v) for v in x] : x isa NamedTuple ? map(plain, x) : x
        src, out = wfile("test_n16_src.parquet"), wfile("test_n16_out.parquet")
        pytable = """
import pyarrow as pa, pyarrow.parquet as pq
wf = pa.struct([('t0', pa.float32()), ('values', pa.list_(pa.int32(), 3))])
table = pa.table({'wf': pa.array([{'t0': 0.5 * i, 'values': [i, i + 1, i + 2]} for i in range(6)], type=wf),
                  'id': list(range(6))})
"""
        for kwargs in ("{}", "{'row_group_size': 4}")
            _with_pyarrow_file("struct{fsl} ($kwargs)", "test_n16_src.parquet", pytable * "write_kwargs = $kwargs") do t
                try
                    @test eltype(t.wf.values) <: Parquet3.FixedSizeView{3, Int32}
                    @test t.wf[2].values == [1, 2, 3] && collect.(t.wf.values)[6] == [5, 6, 7]

                    write_parquet(out, t)
                    back = read_parquet(out)
                    @test back.wf.values isa Parquet3.FixedSizeListVector{3, Int32}
                    @test isequal(plain(back.wf), plain(t.wf))

                    result = _run_pyarrow("""
import pyarrow.parquet as pq
a, b = pq.read_table('$(src)'), pq.read_table('$(out)')
print(a.schema.field('wf').type == b.schema.field('wf').type, a.column('wf').to_pylist() == b.column('wf').to_pylist())
print(b.schema.field('wf').type)""")
                    lines = split(result, '\n')
                    @test lines[1] == "True True"
                    @test lines[2] == "struct<t0: float, values: fixed_size_list<element: int32>[3]>"
                finally
                    rm(out, force=true)
                end
            end
        end

        # Null struct rows: pyarrow cannot write these, so the fixture comes from our writer
        V = Parquet3.FixedSizeView{2, Float64}
        fsv(a, b) = V([a, b], 0)
        wf = Union{Missing, @NamedTuple{t0::Union{Missing, Float64}, values::V}}[
            (t0 = 0.5, values = fsv(1.0, 2.0)), missing, (t0 = missing, values = fsv(3.0, 4.0))]
        try
            write_parquet(out, (wf = wf,))
            back = read_parquet(out)
            @test back.wf.values isa Parquet3.FixedSizeListVector{2, Float64}
            @test isequal(plain(back.wf), plain(wf))
            @test ismissing(back.wf[2]) && ismissing(back.wf.values[2]) && ismissing(back.wf[3].t0)
        finally
            rm(out, force=true)
        end
    end

    @testset "Compression" begin
        plain(x) = x isa AbstractVector ? Any[plain(v) for v in x] : x isa NamedTuple ? map(plain, x) : x
        n = 2000
        tbl = (
            id   = collect(1:n),
            x    = [0.5 * (i % 7) for i in 1:n],
            name = [isodd(i) ? "event-$(i % 10)" : missing for i in 1:n],
            hits = [Int32[i % 5, 1, 2][1:(i % 4)] for i in 1:n],
            wf   = [(t0 = 0.1f0, values = fill(Int32(i % 3), 8)) for i in 1:n],
        )
        f = wfile("test_compression.parquet")
        try
            write_parquet(f, tbl; compression = :uncompressed)
            raw_size = filesize(f)
            for (codec, pyname) in ((:snappy, "SNAPPY"), (:gzip, "GZIP"), (:brotli, "BROTLI"), (:zstd, "ZSTD"), (:lz4, "LZ4"))
                write_parquet(f, tbl; compression = codec)
                @test filesize(f) < raw_size ÷ 2
                t = read_parquet(f)
                @test all(k -> isequal(plain(Tables.getcolumn(t, k)), plain(tbl[k])), keys(tbl))

                result = _run_pyarrow("""
import pyarrow.parquet as pq
pf = pq.ParquetFile('$(f)')
c = pf.metadata.row_group(0).column(0)
t = pf.read()
print(c.compression, c.total_compressed_size < c.total_uncompressed_size)
print(t.column('id').to_pylist() == list(range(1, $(n) + 1)), t.column('hits').to_pylist()[:4], t.column('name').null_count)""")
                if result !== nothing
                    lines = split(result, '\n')
                    @test lines[1] == "$pyname True"
                    @test lines[2] == "True [[1], [2, 1], [3, 1, 2], []] $(n ÷ 2)"
                else
                    @warn "Skipping pyarrow cross-check of $codec: uv/pyarrow not available"
                end
            end

            # default is snappy; strings accepted; zero-row tables compress too
            write_parquet(f, tbl)
            pf = open_parquet(f)
            @test pf.metadata.row_groups[1].columns[1].meta_data.codec == Parquet3.SNAPPY
            close(pf)
            write_parquet(f, (a = Int32[], l = Vector{Float64}[]); compression = "zstd")
            @test length(read_parquet(f).a) == 0
            @test_throws "unknown compression" write_parquet(f, tbl; compression = :lzo)
        finally
            rm(f, force=true)
        end
    end

    @testset "Date and DateTime round-trip" begin
        plain(x) = x isa AbstractVector ? Any[plain(v) for v in x] : x isa NamedTuple ? map(plain, x) : x
        tbl = (
            d  = [Date(2024, 2, 29), missing, Date(1969, 12, 31)],
            ts = [DateTime(2024, 2, 29, 13, 14, 15, 123), DateTime(1969, 12, 31, 23, 59, 59, 999), missing],
            ld = [[Date(2020, 1, 1), missing], Date[], [Date(1970, 1, 1)]],
            lt = [[DateTime(2020, 1, 1, 12)], missing, DateTime[]],
            s  = Union{Missing, @NamedTuple{when::DateTime, day::Union{Missing, Date}}}[
                     (when = DateTime(2000, 1, 1, 0, 0, 0, 1), day = Date(2000, 1, 1)), missing,
                     (when = DateTime(1970, 1, 1), day = missing)],
        )
        f = wfile("test_dates_w.parquet")
        try
            write_parquet(f, tbl)
            t = read_parquet(f)
            for k in keys(tbl)
                @test isequal(plain(Tables.getcolumn(t, k)), plain(tbl[k]))
            end
            @test nonmissingtype(eltype(t.d)) == Date && nonmissingtype(eltype(t.ts)) == DateTime

            result = _run_pyarrow("""
import pyarrow.parquet as pq
t = pq.read_table('$(f)')
print(' | '.join(str(t.schema.field(n).type) for n in t.column_names))
print(t.column('d').to_pylist())
print([None if v is None else v.isoformat() for v in t.column('ts').to_pylist()])
print(t.column('ld').to_pylist(), t.column('s').to_pylist()[1])""")
            if result !== nothing
                lines = split(result, '\n')
                # DateTime carries logicalType TIMESTAMP(isAdjustedToUTC=false), so pyarrow shows it naive
                @test lines[1] == "date32[day] | timestamp[ms] | list<element: date32[day]> | " *
                                  "list<element: timestamp[ms]> | struct<when: timestamp[ms], day: date32[day]>"
                @test lines[2] == "[datetime.date(2024, 2, 29), None, datetime.date(1969, 12, 31)]"
                @test lines[3] == "['2024-02-29T13:14:15.123000', '1969-12-31T23:59:59.999000', None]"
                @test lines[4] == "[[datetime.date(2020, 1, 1), None], [], [datetime.date(1970, 1, 1)]] None"
            else
                @warn "Skipping pyarrow cross-check of dates: uv/pyarrow not available"
            end
        finally
            rm(f, force=true)
        end
    end

    @testset "Timestamps via logicalType" begin
        plain(x) = x isa AbstractVector ? Any[plain(v) for v in x] : x isa NamedTuple ? map(plain, x) : x
        TS{U, TZ} = Arrow.Timestamp{U, TZ}
        MS, US, NS = Arrow.Meta.TimeUnit.MILLISECOND, Arrow.Meta.TimeUnit.MICROSECOND, Arrow.Meta.TimeUnit.NANOSECOND
        src, out = wfile("test_ts_src.parquet"), wfile("test_ts_out.parquet")
        pytable = """
import pyarrow as pa, pyarrow.parquet as pq
vals = {'ms': [1709212455123, None, -1, 4], 'us': [1709212455123456, None, -1, 4], 'ns': [1709212455123456789, None, -1, 4]}
cols = {}
for u in ('ms', 'us', 'ns'):
    cols[u + '_naive'] = pa.array(vals[u], type=pa.timestamp(u))
    cols[u + '_utc'] = pa.array(vals[u], type=pa.timestamp(u, tz='UTC'))
cols['berlin'] = pa.array([1500, None, 3, 4], type=pa.timestamp('us', tz='Europe/Berlin'))
cols['l'] = pa.array([[1500, None], None, [3], []], type=pa.list_(pa.timestamp('us')))
cols['s'] = pa.array([{'t': 1500, 'n': 7}, None, {'t': None, 'n': 9}, {'t': 1, 'n': 2}],
                     type=pa.struct([('t', pa.timestamp('ns', tz='UTC')), ('n', pa.timestamp('ms'))]))
cols['los'] = pa.array([[{'t': 1500}], [], None, [{'t': 2}, {'t': 3}]], type=pa.list_(pa.struct([('t', pa.timestamp('us'))])))
table = pa.table(cols)
"""
        # Single row group, then several: the element type must not depend on the chunk
        for kwargs in ("{}", "{'row_group_size': 2}")
            _with_pyarrow_file("timestamps ($kwargs)", "test_ts_src.parquet", pytable * "write_kwargs = $kwargs") do t
                try
                    # Mixed rule: naive milliseconds → DateTime, everything else → Arrow.Timestamp
                    @test nonmissingtype(eltype(t.ms_naive)) == DateTime
                    @test nonmissingtype(eltype(t.ms_utc))   == TS{MS, :UTC}
                    @test nonmissingtype(eltype(t.us_naive)) == TS{US, nothing}
                    @test nonmissingtype(eltype(t.us_utc))   == TS{US, :UTC}
                    @test nonmissingtype(eltype(t.ns_naive)) == TS{NS, nothing}
                    @test nonmissingtype(eltype(t.ns_utc))   == TS{NS, :UTC}
                    @test nonmissingtype(eltype(t.berlin))   == TS{US, :UTC}

                    # Sub-millisecond values survive exactly
                    @test t.ms_naive[1] == DateTime(2024, 2, 29, 13, 14, 15, 123) && ismissing(t.ms_naive[2])
                    @test t.us_naive[1].x == 1709212455123456 && t.us_utc[3].x == -1 && ismissing(t.us_naive[2])
                    @test t.ns_utc[1].x == 1709212455123456789 && t.ns_naive[4].x == 4

                    # Inside lists, structs and list<struct>
                    @test t.l[1][1] == TS{US, nothing}(1500) && ismissing(t.l[1][2]) && ismissing(t.l[2])
                    @test t.s[1].t == TS{NS, :UTC}(1500) && t.s[1].n == DateTime(1970, 1, 1, 0, 0, 0, 7) && ismissing(t.s[2])
                    @test t.los[1][1].t == TS{US, nothing}(1500) && t.los.t[4] == [TS{US, nothing}(2), TS{US, nothing}(3)]

                    # A read table writes back unchanged
                    write_parquet(out, t)
                    back = read_parquet(out)
                    for k in Tables.columnnames(t)
                        @test isequal(plain(Tables.getcolumn(back, k)), plain(Tables.getcolumn(t, k)))
                    end
                    @test all(k -> nonmissingtype(eltype(Tables.getcolumn(back, k))) == nonmissingtype(eltype(Tables.getcolumn(t, k))),
                              (:ms_naive, :ms_utc, :us_naive, :us_utc, :ns_naive, :ns_utc, :berlin))

                    # pyarrow sees the types and annotations it wrote itself. The one exception:
                    # Parquet only has a UTC flag, so a named time zone comes back as UTC.
                    result = _run_pyarrow("""
import pyarrow.parquet as pq
a, b = pq.read_table('$(src)'), pq.read_table('$(out)')
print([n for n in a.column_names if a.schema.field(n).type != b.schema.field(n).type], b.schema.field('berlin').type)
print(all(a.column(n).cast(b.schema.field(n).type).combine_chunks().equals(b.column(n).combine_chunks()) for n in a.column_names))
pa_, pb = pq.ParquetFile('$(src)').schema, pq.ParquetFile('$(out)').schema
print(all(pa_.column(i).converted_type == pb.column(i).converted_type and str(pa_.column(i).logical_type) == str(pb.column(i).logical_type) for i in range(len(pa_.names))))""")
                    lines = split(result, '\n')
                    @test lines[1] == "['berlin'] timestamp[us, tz=UTC]"
                    @test lines[2] == "True"
                    @test lines[3] == "True"
                finally
                    rm(out, force=true)
                end
            end
        end

        # Columns built in Julia, including one next to a FixedSizeList (ARROW:schema is then written)
        V = Parquet3.FixedSizeView{2, Int32}
        tbl = (
            dt     = [DateTime(2024, 2, 29, 13, 14, 15, 123), missing, DateTime(1969, 12, 31, 23, 59, 59, 999)],
            us_utc = [TS{US, :UTC}(1709212455123456), missing, TS{US, :UTC}(-1)],
            ns     = TS{NS, nothing}[TS{NS, nothing}(1), TS{NS, nothing}(2), TS{NS, nothing}(3)],
            zoned  = [TS{MS, Symbol("Europe/Berlin")}(5), TS{MS, Symbol("Europe/Berlin")}(6), TS{MS, Symbol("Europe/Berlin")}(7)],
        )
        try
            for (label, extra) in (("plain", (;)), ("with fsl", (f = [V(Int32[1, 2], 0), V(Int32[3, 4], 0), V(Int32[5, 6], 0)],)))
                write_parquet(out, merge(tbl, extra))
                t = read_parquet(out)
                @test isequal(collect(t.dt), collect(tbl.dt)) && isequal(collect(t.us_utc), collect(tbl.us_utc))
                @test collect(t.ns) == tbl.ns && [v.x for v in t.zoned] == [5, 6, 7]
                @test nonmissingtype(eltype(t.zoned)) == TS{MS, :UTC}

                result = _run_pyarrow("""
import pyarrow.parquet as pq
t = pq.read_table('$(out)')
print(' | '.join(str(t.schema.field(n).type) for n in ['dt', 'us_utc', 'ns', 'zoned']))
print(t.column('us_utc').cast('int64').to_pylist(), t.column('ns').cast('int64').to_pylist(), t.column('dt').cast('int64').to_pylist())""")
                lines = split(result, '\n')
                @test lines[1] == (label == "plain" ? "timestamp[ms] | timestamp[us, tz=UTC] | timestamp[ns] | timestamp[ms, tz=UTC]" :
                                                      "timestamp[ms] | timestamp[us, tz=UTC] | timestamp[ns] | timestamp[ms, tz=Europe/Berlin]")
                @test lines[2] == "[1709212455123456, None, -1] [1, 2, 3] [1709212455123, None, -1]"
            end
            # Parquet has no second-resolution timestamps
            @test_throws Exception write_parquet(out, (s = [TS{Arrow.Meta.TimeUnit.SECOND, nothing}(1)],))
        finally
            rm(out, force=true)
        end
    end

    @testset "Encodings: BYTE_STREAM_SPLIT (E1) and the encoding keyword" begin
        plain(x) = x isa AbstractVector ? Any[plain(v) for v in x] : x isa NamedTuple ? map(plain, x) : x

        # encoder is the inverse of the decoder
        for T in (Float32, Float64), vals in (T[1.5, -2.25, NaN, Inf, 0.0, floatmin(T)], T[], T[3.0])
            enc = Parquet3.encode_byte_stream_split(vals)
            dec = Parquet3.decode_byte_stream_split(T, enc, length(vals))
            @test length(enc) == sizeof(T) * length(vals) && isequal(collect(dec), vals)
        end

        n = 500
        tbl = (
            id   = collect(1:n),
            x    = [isodd(i) ? 0.25 * i : missing for i in 1:n],
            f    = Float32[sin(i) for i in 1:n],
            hits = [Float32[0.5f0 * j for j in 1:(i % 4)] for i in 1:n],
            wf   = [(t0 = 0.1 * i, dt = 0.5f0, n = Int32(i)) for i in 1:n],
            parts = [[(pt = 1.5f0 * i, q = Int32(1)) for _ in 1:(i % 3)] for i in 1:n],
        )
        f = wfile("test_e1_bss.parquet")
        encodings_of(path) = (pf = open_parquet(path);
            r = Dict(join(c.meta_data.path_in_schema, ".") => c.meta_data.encodings for c in pf.metadata.row_groups[1].columns);
            close(pf); r)
        roundtrips(path) = (t = read_parquet(path); all(k -> isequal(plain(Tables.getcolumn(t, k)), plain(tbl[k])), keys(tbl)))
        BSS, PL = Parquet3.BYTE_STREAM_SPLIT, Parquet3.PLAIN
        try
            # One name for the whole table: floats use it, everything else stays PLAIN
            for codec in (:uncompressed, :zstd)
                write_parquet(f, tbl; encoding = :byte_stream_split, compression = codec)
                @test roundtrips(f)
                e = encodings_of(f)
                @test all(k -> BSS in e[k] && !(PL in e[k]), ["x", "f", "hits.list.element", "wf.t0", "wf.dt", "parts.list.element.pt"])
                @test all(k -> PL in e[k] && !(BSS in e[k]), ["id", "wf.n", "parts.list.element.q"])
            end

            # Per column, keyed by the path a user would type; the most specific key wins
            write_parquet(f, tbl; encoding = Dict("x" => :byte_stream_split, "hits" => "BYTE_STREAM_SPLIT",
                                                  "wf.t0" => :byte_stream_split, "parts.pt" => :byte_stream_split,
                                                  "parts" => :plain, "id" => :plain))
            @test roundtrips(f)
            e = encodings_of(f)
            @test all(k -> BSS in e[k], ["x", "hits.list.element", "wf.t0", "parts.list.element.pt"])
            @test all(k -> !(BSS in e[k]), ["id", "f", "wf.dt", "wf.n", "parts.list.element.q"])

            result = _run_pyarrow("""
import pyarrow.parquet as pq
pf = pq.ParquetFile('$(f)')
rg = pf.metadata.row_group(0)
print(sorted(rg.column(i).path_in_schema for i in range(rg.num_columns) if 'BYTE_STREAM_SPLIT' in rg.column(i).encodings))
t = pf.read()
print(t.column('x').to_pylist()[:3], t.column('hits').to_pylist()[:4], t.column('wf').to_pylist()[0], t.column('parts').to_pylist()[1])""")
            if result !== nothing
                lines = split(result, '\n')
                @test lines[1] == "['hits.list.element', 'parts.list.element.pt', 'wf.t0', 'x']"
                @test lines[2] == "[0.25, None, 0.75] [[0.5], [0.5, 1.0], [0.5, 1.0, 1.5], []] {'t0': 0.1, 'dt': 0.5, 'n': 1} [{'pt': 3.0, 'q': 1}, {'pt': 3.0, 'q': 1}]"
            else
                @warn "Skipping pyarrow cross-check of BYTE_STREAM_SPLIT: uv/pyarrow not available"
            end

            # In a mapping nothing falls back silently
            @test_throws "not valid for column id" write_parquet(f, tbl; encoding = Dict("id" => :byte_stream_split))
            @test_throws "not valid for column wf.n" write_parquet(f, tbl; encoding = Dict("wf" => :byte_stream_split))
            @test_throws "match no column: nope, wf.values" write_parquet(f, tbl; encoding = Dict("nope" => :plain, "wf.values" => :plain))
            @test_throws "unknown encoding" write_parquet(f, tbl; encoding = :rle_magic)
        finally
            rm(f, force=true)
        end
    end

    @testset "Encodings: DELTA_BINARY_PACKED (E2)" begin
        plain(x) = x isa AbstractVector ? Any[plain(v) for v in x] : x isa NamedTuple ? map(plain, x) : x

        # encoder is the inverse of the decoder, including wrap-around deltas and block boundaries
        for T in (Int32, Int64), vals in (T[], T[7], T[1, 2, 3], fill(T(5), 129), T.(1:257), T.(cumsum(rand(-3:3, 300))),
                                          T[typemin(T), typemax(T), 0, -1, typemax(T), typemin(T)], rand(T, 1000))
            enc = Parquet3.encode_delta_binary_packed(vals)
            dec, pos = Parquet3.decode_delta_binary_packed(enc, length(vals))
            @test (T == Int32 ? dec .% Int32 : dec) == vals
            @test isempty(vals) || pos == length(enc) + 1
        end
        @test length(Parquet3.encode_delta_binary_packed(collect(1:1000))) < 100
        for w in (1, 7, 8, 13, 32, 33, 64), n in (1, 32)
            vals = rand(UInt64, n) .>> (64 - w)
            out = Vector{UInt64}(undef, n)
            Parquet3.unpack_bits!(out, 1, Parquet3.pack_bits(vals, w), n, w)
            @test out == vals
        end

        # The reader used to drop these pyarrow columns: 32-bit wrap-around and deltas wider than 32 bits
        _with_pyarrow_file("pyarrow delta extremes", "test_e2_py.parquet", """
import pyarrow as pa, pyarrow.parquet as pq
table = pa.table({
    'i32': pa.array([-2**31, 2**31 - 1, 0, None, -5, 2**31 - 1, -2**31], type=pa.int32()),
    'i64': pa.array([-2**63, 2**63 - 1, 0, None, 10**15, -10**15, 7], type=pa.int64()),
})
write_kwargs = {'use_dictionary': False, 'column_encoding': 'DELTA_BINARY_PACKED'}""") do t
            @test isequal(collect(t.i32), [typemin(Int32), typemax(Int32), 0, missing, -5, typemax(Int32), typemin(Int32)])
            @test isequal(collect(t.i64), [typemin(Int64), typemax(Int64), 0, missing, 10^15, -10^15, 7])
        end

        n = 400
        TS = Arrow.Timestamp{Arrow.Meta.TimeUnit.MICROSECOND, :UTC}
        tbl = (
            id   = collect(1:n),
            i32  = Int32[isodd(i) ? typemin(Int32) + i : typemax(Int32) - i for i in 1:n],
            oi   = [i % 7 == 0 ? missing : i^2 for i in 1:n],
            i8   = Int8[i % 100 for i in 1:n],
            u32  = UInt32[typemax(UInt32) - i for i in 1:n],
            u64  = UInt64[typemax(UInt64) - UInt64(i) for i in 1:n],
            day  = [Date(2024, 1, 1) + Day(i) for i in 1:n],
            dt   = [DateTime(2024, 1, 1) + Second(i) for i in 1:n],
            ts   = [TS(1_700_000_000_000_000 + 250i) for i in 1:n],
            x    = [0.5 * i for i in 1:n],
            name = ["n$i" for i in 1:n],
            hits = [Int32[j for j in 1:(i % 4)] for i in 1:n],
            wf   = [(t0 = 0.1 * i, n = Int32(i)) for i in 1:n],
        )
        ints = ["id", "i32", "oi", "i8", "u32", "u64", "day", "dt", "ts", "hits.list.element", "wf.n"]
        f = wfile("test_e2_delta.parquet")
        encodings_of(path) = (pf = open_parquet(path);
            r = Dict(join(c.meta_data.path_in_schema, ".") => c.meta_data.encodings for c in pf.metadata.row_groups[1].columns);
            close(pf); r)
        DBP = Parquet3.DELTA_BINARY_PACKED
        try
            # Whole table: every integer-backed leaf (narrow, unsigned, dates, timestamps) uses it
            for codec in (:uncompressed, :snappy)
                write_parquet(f, tbl; encoding = :delta_binary_packed, compression = codec)
                t = read_parquet(f)
                @test all(k -> isequal(plain(Tables.getcolumn(t, k)), plain(tbl[k])), keys(tbl))
                e = encodings_of(f)
                @test all(k -> DBP in e[k], ints) && all(k -> !(DBP in e[k]), ["x", "name", "wf.t0"])
            end
            plain_size = (write_parquet(f, (id = tbl.id,); compression = :uncompressed); filesize(f))
            @test (write_parquet(f, (id = tbl.id,); compression = :uncompressed, encoding = :delta_binary_packed); filesize(f)) < plain_size ÷ 10

            # Mixed per-column encodings, checked by pyarrow
            write_parquet(f, tbl; encoding = Dict("id" => :delta_binary_packed, "i32" => :delta_binary_packed, "u64" => :delta_binary_packed,
                                                  "ts" => :delta_binary_packed, "hits" => :delta_binary_packed, "wf.n" => :delta_binary_packed,
                                                  "x" => :byte_stream_split))
            result = _run_pyarrow("""
import pyarrow.parquet as pq
pf = pq.ParquetFile('$(f)')
rg = pf.metadata.row_group(0)
print(sorted(rg.column(i).path_in_schema for i in range(rg.num_columns) if 'DELTA_BINARY_PACKED' in rg.column(i).encodings))
t = pf.read()
print(t.column('id').to_pylist() == list(range(1, $(n) + 1)), t.column('i32').to_pylist()[:2], t.column('u64').to_pylist()[0], t.column('ts').cast('int64').to_pylist()[0])
print(t.column('hits').to_pylist()[:4], t.column('wf').to_pylist()[1], t.column('oi').to_pylist()[5:8], str(t.column('day')[0]), t.column('i8').to_pylist()[-1])""")
            if result !== nothing
                lines = split(result, '\n')
                @test lines[1] == "['hits.list.element', 'i32', 'id', 'ts', 'u64', 'wf.n']"
                @test lines[2] == "True [-2147483647, 2147483645] 18446744073709551614 1700000000000250"
                @test lines[3] == "[[1], [1, 2], [1, 2, 3], []] {'t0': 0.2, 'n': 2} [36, None, 64] 2024-01-02 0"
            else
                @warn "Skipping pyarrow cross-check of DELTA_BINARY_PACKED: uv/pyarrow not available"
            end

            write_parquet(f, (a = Int64[], b = Union{Missing, Int32}[]); encoding = :delta_binary_packed)
            @test length(read_parquet(f).a) == 0
            write_parquet(f, (b = Union{Missing, Int32}[missing, missing],); encoding = :delta_binary_packed)
            @test all(ismissing, read_parquet(f).b)
            @test_throws "not valid for column x" write_parquet(f, tbl; encoding = Dict("x" => :delta_binary_packed))
        finally
            rm(f, force=true)
        end
    end

    @testset "Encodings: DELTA_LENGTH_BYTE_ARRAY (E3)" begin
        plain(x) = x isa AbstractVector ? Any[plain(v) for v in x] : x isa NamedTuple ? map(plain, x) : x

        # encoder is the inverse of the decoder
        for vals in (String[], ["a"], ["", "héllo", "", "x"^300, "z"], ["s$i" for i in 1:200])
            enc = Parquet3.encode_delta_length_byte_array(vals)
            @test [String(copy(v)) for v in Parquet3.decode_delta_length_byte_array(enc, length(vals))] == vals
        end
        bytes = [UInt8[1, 2], UInt8[], UInt8[0xff]]
        @test [Vector{UInt8}(v) for v in Parquet3.decode_delta_length_byte_array(
                   Parquet3.encode_delta_length_byte_array(bytes), 3)] == bytes

        n = 300
        tbl = (
            id    = collect(1:n),
            name  = ["event-$(i % 13)" for i in 1:n],
            oname = [i % 5 == 0 ? missing : "é"^(i % 4) for i in 1:n],
            blob  = [UInt8[j % 256 for j in 1:(i % 6)] for i in 1:n],
            tags  = [["t$j" for j in 1:(i % 3)] for i in 1:n],
            wf    = [(label = "wf$i", t0 = 0.5 * i) for i in 1:n],
        )
        strs = ["name", "oname", "blob", "tags.list.element", "wf.label"]
        f = wfile("test_e3_dlba.parquet")
        encodings_of(path) = (pf = open_parquet(path);
            r = Dict(join(c.meta_data.path_in_schema, ".") => c.meta_data.encodings for c in pf.metadata.row_groups[1].columns);
            close(pf); r)
        DLBA = Parquet3.DELTA_LENGTH_BYTE_ARRAY
        try
            for codec in (:uncompressed, :gzip)
                write_parquet(f, tbl; encoding = :delta_length_byte_array, compression = codec)
                t = read_parquet(f)
                @test all(k -> isequal(plain(Tables.getcolumn(t, k)), plain(tbl[k])), keys(tbl))
                e = encodings_of(f)
                @test all(k -> DLBA in e[k], strs) && all(k -> !(DLBA in e[k]), ["id", "wf.t0"])
            end

            # All three encodings in one file, checked by pyarrow
            write_parquet(f, tbl; encoding = Dict("name" => :delta_length_byte_array, "blob" => :delta_length_byte_array,
                                                  "tags" => :delta_length_byte_array, "wf.label" => :delta_length_byte_array,
                                                  "wf.t0" => :byte_stream_split, "id" => :delta_binary_packed))
            result = _run_pyarrow("""
import pyarrow.parquet as pq
pf = pq.ParquetFile('$(f)')
rg = pf.metadata.row_group(0)
print(sorted(rg.column(i).path_in_schema for i in range(rg.num_columns) if 'DELTA_LENGTH_BYTE_ARRAY' in rg.column(i).encodings))
t = pf.read()
print(t.column('name').to_pylist()[:3], t.column('oname').to_pylist()[3:6], t.column('blob').to_pylist()[:3])
print(t.column('tags').to_pylist()[:3], t.column('wf').to_pylist()[0], t.column('id').to_pylist()[-1])""")
            if result !== nothing
                lines = split(result, '\n')
                @test lines[1] == "['blob', 'name', 'tags.list.element', 'wf.label']"
                @test lines[2] == "['event-1', 'event-2', 'event-3'] ['', None, 'éé'] [b'\\x01', b'\\x01\\x02', b'\\x01\\x02\\x03']"
                @test lines[3] == "[['t1'], ['t1', 't2'], []] {'label': 'wf1', 't0': 0.5} $(n)"
            else
                @warn "Skipping pyarrow cross-check of DELTA_LENGTH_BYTE_ARRAY: uv/pyarrow not available"
            end

            write_parquet(f, (s = String[], o = Union{Missing, String}[]); encoding = :delta_length_byte_array)
            @test length(read_parquet(f).s) == 0
            write_parquet(f, (o = Union{Missing, String}[missing, missing],); encoding = :delta_length_byte_array)
            @test all(ismissing, read_parquet(f).o)
            @test_throws "not valid for column id" write_parquet(f, tbl; encoding = Dict("id" => :delta_length_byte_array))
            # Dictionary encoding is on hold, so its name is not accepted
            @test_throws "unknown encoding" write_parquet(f, tbl; encoding = :dictionary)
        finally
            rm(f, force=true)
        end
    end

    @testset "null_count matches pyarrow for every leaf position" begin
        plain(x) = x isa AbstractVector ? Any[plain(v) for v in x] : x isa NamedTuple ? map(plain, x) : x
        M = Missing
        # Each nested shape, with a null and an empty at every level that can have one
        tbl = (
            flat = [1, missing, 3, missing],
            l    = Union{M, Vector{Union{M, Int}}}[[1, missing], missing, [], [2]],
            s    = Union{M, @NamedTuple{a::Union{M, Int}, b::Union{M, String}}}[
                       (a = 1, b = "x"), missing, (a = missing, b = "y"), (a = 2, b = missing)],
            los  = Union{M, Vector{Union{M, @NamedTuple{a::Union{M, Int}, b::Union{M, Int}}}}}[
                       [(a = 1, b = 1), missing, (a = missing, b = 2)], missing, [], [(a = 3, b = missing)]],
            sl   = Union{M, @NamedTuple{v::Union{M, Vector{Union{M, Int}}}, n::Union{M, Int}}}[
                       (v = [1, missing], n = 1), missing, (v = missing, n = 2), (v = [], n = missing)],
            ll   = Union{M, Vector{Union{M, Vector{Union{M, Int}}}}}[[[1, missing], missing, []], missing, [], [[2]]],
            ss   = Union{M, @NamedTuple{inner::Union{M, @NamedTuple{x::Union{M, Int}}}}}[
                       (inner = (x = 1,),), missing, (inner = missing,), (inner = (x = missing,),)],
            sls  = Union{M, @NamedTuple{hits::Union{M, Vector{Union{M, @NamedTuple{x::Union{M, Int}}}}}}}[
                       (hits = [(x = 1,), missing, (x = missing,)],), missing, (hits = missing,), (hits = [],)],
            lsl  = Union{M, Vector{Union{M, @NamedTuple{v::Union{M, Vector{Union{M, Int}}}}}}}[
                       [(v = [1, missing],), missing, (v = missing,), (v = [],)], missing, [], [(v = [2],)]],
            lls  = Union{M, Vector{Union{M, Vector{Union{M, @NamedTuple{x::Union{M, Int}}}}}}}[
                       [[(x = 1,), missing, (x = missing,)], missing, []], missing, [], [[(x = 2,)]]],
            # no nulls at all, and lists without nulls but with an empty one
            los_clean = [[(a = 1, b = 2)], [(a = 3, b = 4), (a = 5, b = 6)], [(a = 7, b = 8)], [(a = 9, b = 0)]],
            los_empty = [[(a = 1, b = 2)], @NamedTuple{a::Int, b::Int}[], [(a = 7, b = 8)], [(a = 9, b = 0)]],
            l_empty   = [[1], Int[], [2, 3], [4]],
            # string and binary leaves count differently from fixed-width ones under a struct in a list
            los_s = Union{M, Vector{Union{M, @NamedTuple{n::Union{M, Int32}, s::Union{M, String}, f::Union{M, Float64}, b::Union{M, Vector{UInt8}}}}}}[
                        [(n = 1, s = "x", f = 1.5, b = UInt8[1])], missing, [], [missing, (n = missing, s = missing, f = missing, b = missing)]],
            # maps: null map, empty map, null value; and a map of maps
            m    = Union{M, Dict{String, Union{M, Int}}}[Dict("a" => 1), missing, Dict{String, Union{M, Int}}(), Dict("b" => missing)],
            mm   = [Dict("o" => Dict("i" => 1.5)), Dict{String, Dict{String, Float64}}(), Dict("p" => Dict{String, Float64}()), Dict("q" => Dict("j" => 2.5))],
        )
        ours, theirs = wfile("test_nc_ours.parquet"), wfile("test_nc_pyarrow.parquet")
        try
            write_parquet(ours, tbl)
            # pyarrow rewrites the same data; its statistics are the reference
            result = _run_pyarrow("""
import pyarrow.parquet as pq
t = pq.read_table('$(ours)')
pq.write_table(t, '$(theirs)')
a, b = pq.ParquetFile('$(ours)').metadata.row_group(0), pq.ParquetFile('$(theirs)').metadata.row_group(0)
print(pq.read_table('$(theirs)').equals(t), a.num_columns)
print([a.column(i).path_in_schema for i in range(a.num_columns)
       if (a.column(i).num_values, a.column(i).statistics.null_count) != (b.column(i).num_values, b.column(i).statistics.null_count)])
print([a.column(i).statistics.null_count for i in range(a.num_columns)])""")
            if result !== nothing
                lines = split(result, '\n')
                @test lines[1] == "True 27"
                @test lines[2] == "[]"       # no leaf differs from pyarrow
                # pinned so a change in either side shows up: fixed-width los/sls/lls members count slots
                # only; string and binary members, and map keys and values, count every entry without a value
                @test lines[3] == "[2, 3, 2, 2, 2, 2, 4, 2, 5, 3, 2, 6, 2, 0, 0, 0, 0, 1, 2, 4, 2, 4, 2, 3, 1, 2, 2]"

                # Same data, same types, whichever writer produced the file
                a, b = read_parquet(ours), read_parquet(theirs)
                @test collect(Tables.columnnames(a)) == collect(Tables.columnnames(b))
                for k in Tables.columnnames(a)
                    @test eltype(Tables.getcolumn(a, k)) == eltype(Tables.getcolumn(b, k))
                    @test isequal(plain(Tables.getcolumn(a, k)), plain(Tables.getcolumn(b, k)))
                end
                @test !(Missing <: eltype(a.los_clean)) && !(Missing <: eltype(a.los_empty))
                @test eltype(first(a.los_empty)) == @NamedTuple{a::Int64, b::Int64}
            else
                @warn "Skipping null_count comparison: uv/pyarrow not available"
            end
        finally
            rm(ours, force=true); rm(theirs, force=true)
        end
    end

    @testset "Maps: Dict elements written as Parquet MAP, read back as MapView (R8)" begin
        P = Parquet3
        M = Missing
        f, out = wfile("test_r8_maps.parquet"), wfile("test_r8_maps_rw.parquet")
        D = Dict{String, Union{M, Int32}}
        tbl = (
            m   = Union{M, D}[D("a" => 1, "b" => missing), missing, D(), D("c" => 3)],
            mm  = [Dict("o" => Dict(1 => 1.5, 2 => 2.5)), Dict{String, Dict{Int, Float64}}(), Dict("p" => Dict{Int, Float64}()), Dict("q" => Dict(3 => 3.5))],
            sm  = [(tags = Dict("t" => "x"), n = i) for i in 1:4],
            lm  = [[Dict("k" => 1.5), Dict{String, Float64}()], Dict{String, Float64}[], [Dict("u" => 2.5, "v" => 3.5)], [Dict("w" => 4.5)]],
            ml  = [Dict("xs" => [1, 2], "ys" => Int[]), Dict{String, Vector{Int}}(), Dict("zs" => [3]), Dict{String, Vector{Int}}()],
            id  = collect(1:4),
        )
        try
            write_parquet(f, tbl)
            t = read_parquet(f)
            @test collect(propertynames(t)) == collect(keys(tbl))

            # Rows are zero-copy dictionary views that equal the dictionaries written
            @test t.m isa P.MapColumn && t.m[1] isa P.MapView{String, Union{M, Int32}}
            for k in (:m, :mm, :ml)
                @test all(isequal(a, b) for (a, b) in zip(getproperty(t, k), tbl[k]))
            end
            @test all(t.sm[i].tags == tbl.sm[i].tags && t.sm[i].n == i for i in 1:4)
            @test all(collect(a) == b for (a, b) in zip(t.lm, tbl.lm))
            @test ismissing(t.m[2]) && isempty(t.m[3]) && t.m[1]["a"] == 1 && ismissing(t.m[1]["b"])
            @test t.mm[1]["o"][2] == 2.5 && t.ml[1]["xs"] == [1, 2] && haskey(t.m[4], "c") && !haskey(t.m[4], "a")
            @test get(t.m[1], "zzz", 0) == 0 && length(t.m[1]) == 2 && Dict(t.m[4]) == Dict("c" => 3)
            @test isempty(loose_nodes(t))

            # The columnar view stays: all keys and all values, per row
            @test propertynames(t.m) == (:key, :value)
            @test sort(collect(t.m.key[1])) == ["a", "b"] && collect(t.m.key[4]) == ["c"] && ismissing(t.m.key[2])
            @test collect(t.m.value[4]) == [3] && collect.(t.lm.key[3]) |> only |> sort == ["u", "v"]
            @test collect.(t.mm.value.value[4]) == [[3.5]] && t.sm.tags isa P.MapColumn && collect(t.sm.tags.value[2]) == ["x"]

            # Index access does not allocate per entry: the same cost for 2 entries and for 2000
            big = Dict(string(i) => i for i in 1:2000)
            write_parquet(out, (small = [Dict("a" => 1, "b" => 2)], big = [big]))
            b = read_parquet(out)
            b.small[1]; b.big[1]
            @test (@allocated b.big[1]) == (@allocated b.small[1]) && length(b.big[1]) == 2000 && b.big[1]["1234"] == 1234

            # pyarrow reads our file as map types with the contents we wrote
            result = _run_pyarrow("""
import pyarrow.parquet as pq
t = pq.read_table('$(f)')
print(' | '.join(str(t.schema.field(n).type) for n in ['m', 'mm', 'sm', 'lm', 'ml']))
print([None if r is None else sorted(r, key=lambda kv: kv[0]) for r in t.column('m').to_pylist()])
print(t.column('lm').to_pylist()[0], t.column('ml').to_pylist()[2], t.column('sm').to_pylist()[0])""")
            if result !== nothing
                lines = split(result, '\n')
                @test lines[1] == "map<string, int32 ('m')> | map<string, map<int64, double ('value')> ('mm')> | " *
                                  "struct<tags: map<string, string ('tags')>, n: int64> | list<element: map<string, double ('element')>> | " *
                                  "map<string, list<element: int64> ('ml')>"
                @test lines[2] == "[[('a', 1), ('b', None)], None, [], [('c', 3)]]"
                @test lines[3] == "[[('k', 1.5)], []] [('zs', [3])] {'tags': [('t', 'x')], 'n': 1}"
            end

            # A map column from read_parquet writes back as a map, unchanged
            write_parquet(out, t)
            back = read_parquet(out)
            @test all(isequal(collect(getproperty(back, k)), collect(getproperty(t, k))) && eltype(getproperty(back, k)) == eltype(getproperty(t, k))
                      for k in (:m, :mm, :ml))

            # Arrow.write serialises it as an Arrow map (dictionaries on the Arrow side)
            io = IOBuffer(); Arrow.write(io, (m = t.m, id = t.id)); seekstart(io)
            a = Arrow.Table(io)
            @test a.m isa Arrow.Map && all(isequal(Dict(x), y) for (x, y) in zip(skipmissing(t.m), skipmissing(a.m))) && ismissing(a.m[2])

            # A vector of (key, value) NamedTuples is a list of structs, not a map
            write_parquet(out, (kv = [[(key = "a", value = 1)], @NamedTuple{key::String, value::Int}[]],))
            @test read_parquet(out).kv isa P.ListOfStructsColumn
            # Keys cannot be missing; value types must be concrete
            @test_throws "map key cannot be missing" write_parquet(out, (d = [Dict{Union{M, String}, Int}(missing => 1)],))
            @test_throws "concrete dictionary type" write_parquet(out, (d = [Dict("x" => missing, "y" => 1.5), Dict("z" => 2.5)],))
        finally
            rm(f, force=true); rm(out, force=true)
        end

        # Storage order and duplicate keys are kept; lookup and Dict() take the last entry, as the format specifies
        dup = P.MapView(["a", "b", "a"], [1, 2, 3])
        @test collect(dup) == ["a" => 1, "b" => 2, "a" => 3] && length(dup) == 3
        @test dup["a"] == 3 && Dict(dup) == Dict("a" => 3, "b" => 2) && collect(keys(dup)) == ["a", "b", "a"]
        @test sprint(show, dup) == "MapView(\"a\" => 1, \"b\" => 2, \"a\" => 3)"
    end

    @testset "RLE encoder round-trip (unit)" begin
        for levels in ([0, 0, 1, 1, 1, 0], zeros(Int, 100), ones(Int, 7), [1], Int[])
            enc = Parquet3.encode_rle_bitpacked(levels, 1)
            if !isempty(levels)
                dec = Parquet3.decode_rle_bitpacked(enc, length(levels), 1)
                @test Int.(dec) == levels
            else
                @test isempty(enc)
            end
        end
    end
end

# =============================================================================
# Apache parquet-testing suite
# =============================================================================

const PARQUET_TESTING_DIR = joinpath(@__DIR__, "parquet-testing", "data")
const HAS_PARQUET_TESTING = isdir(PARQUET_TESTING_DIR)

# file => columns that cannot be read yet (see Known Limitations in dev-note.md).
# read_parquet throws for these; every other column, and every other file, must read.
const PARQUET_TESTING_KNOWN_GAPS = Dict(
    "byte_stream_split_extended.gzip.parquet" => ["float16_byte_stream_split", "flba5_byte_stream_split", "decimal_byte_stream_split"],
    "delta_byte_array.parquet" => ["c_customer_id", "c_salutation", "c_first_name", "c_last_name", "c_preferred_cust_flag",
                                   "c_birth_country", "c_login", "c_email_address", "c_last_review_date"],
    "delta_encoding_optional_column.parquet" => ["c_customer_id", "c_salutation", "c_first_name", "c_last_name",
                                                 "c_preferred_cust_flag", "c_birth_country", "c_email_address", "c_last_review_date"],
    "delta_encoding_required_column.parquet" => ["c_customer_id:", "c_salutation:", "c_first_name:", "c_last_name:",
                                                 "c_preferred_cust_flag:", "c_birth_country:", "c_email_address:", "c_last_review_date:"],
    # malformed (a required column whose pages contain nulls); pyarrow rejects it as well
    "fixed_length_byte_array.parquet" => ["flba_field"],
    "hadoop_lz4_compressed.parquet" => ["c0", "c1", "v11"],
    "hadoop_lz4_compressed_larger.parquet" => ["a"],
    "large_string_map.brotli.parquet" => ["arr"],
    "non_hadoop_lz4_compressed.parquet" => ["c0", "c1", "v11"],
)

if HAS_PARQUET_TESTING
    @info "Running parquet-testing suite"

    @group "parquet-testing: Smoke (all files open)" begin
        files = filter(f -> endswith(f, ".parquet"), readdir(PARQUET_TESTING_DIR))
        for f in files
            @testset "$f" begin
                pf = open_parquet(joinpath(PARQUET_TESTING_DIR, f))
                @test num_rows(pf) >= 0
                @test length(schema(pf)) > 0
                close(pf)
            end
        end
    end

    @group "parquet-testing: Row counts" begin
        expected_rows = Dict(
            "alltypes_dictionary.parquet" => 2,
            "alltypes_plain.parquet" => 8,
            "alltypes_plain.snappy.parquet" => 2,
            "alltypes_tiny_pages.parquet" => 7300,
            "alltypes_tiny_pages_plain.parquet" => 7300,
            "binary.parquet" => 12,
            "binary_truncated_min_max.parquet" => 12,
            "byte_array_decimal.parquet" => 24,
            "byte_stream_split.zstd.parquet" => 300,
            "byte_stream_split_extended.gzip.parquet" => 200,
            "column_chunk_key_value_metadata.parquet" => 0,
            "concatenated_gzip_members.parquet" => 513,
            "data_index_bloom_encoding_stats.parquet" => 14,
            "datapage_v1-uncompressed-checksum.parquet" => 5120,
            "datapage_v1-snappy-compressed-checksum.parquet" => 5120,
            "datapage_v2.snappy.parquet" => 5,
            "delta_binary_packed.parquet" => 200,
            "delta_encoding_required_column.parquet" => 100,
            "delta_encoding_optional_column.parquet" => 100,
            "delta_length_byte_array.parquet" => 1000,
            "fixed_length_decimal.parquet" => 24,
            "fixed_length_decimal_legacy.parquet" => 24,
            "int32_decimal.parquet" => 24,
            "int32_with_null_pages.parquet" => 1000,
            "int64_decimal.parquet" => 24,
            "list_columns.parquet" => 3,
            "lz4_raw_compressed.parquet" => 4,
            "lz4_raw_compressed_larger.parquet" => 10000,
            "nan_in_stats.parquet" => 2,
            "nation.dict-malformed.parquet" => 25,
            "nested_lists.snappy.parquet" => 3,
            "null_list.parquet" => 1,
            "nulls.snappy.parquet" => 8,
            "old_list_structure.parquet" => 1,
            "overflow_i16_page_cnt.parquet" => 40000,
            "page_v2_empty_compressed.parquet" => 10,
            "plain-dict-uncompressed-checksum.parquet" => 1000,
            "rle-dict-snappy-checksum.parquet" => 1000,
            "single_nan.parquet" => 1,
            "sort_columns.parquet" => 6,
            "unknown-logical-type.parquet" => 3,
        )
        for (f, exp) in sort(collect(expected_rows))
            @testset "$f" begin
                pf = open_parquet(joinpath(PARQUET_TESTING_DIR, f))
                @test Int(num_rows(pf)) == exp
                close(pf)
            end
        end
    end

    @group "parquet-testing: Full read" begin

        @testset "alltypes_plain" begin
            t = read_parquet(joinpath(PARQUET_TESTING_DIR, "alltypes_plain.parquet"))
            @test length(Tables.columnnames(t)) == 11
            @test t.id == Int32[4, 5, 6, 7, 2, 3, 0, 1]
            @test t.bool_col == Bool[1, 0, 1, 0, 1, 0, 1, 0]
            @test eltype(t.float_col) == Float32
            @test t.float_col ≈ Float32[0, 1.1, 0, 1.1, 0, 1.1, 0, 1.1]
            @test t.double_col ≈ [0.0, 10.1, 0.0, 10.1, 0.0, 10.1, 0.0, 10.1]
        end

        @testset "alltypes_dictionary" begin
            t = read_parquet(joinpath(PARQUET_TESTING_DIR, "alltypes_dictionary.parquet"))
            @test length(Tables.columnnames(t)) == 11
            @test t.id == Int32[0, 1]
            @test t.bool_col == Bool[true, false]
        end

        @testset "alltypes_plain.snappy" begin
            t = read_parquet(joinpath(PARQUET_TESTING_DIR, "alltypes_plain.snappy.parquet"))
            @test length(Tables.columnnames(t)) == 11
            @test t.id == Int32[6, 7]
        end

        @testset "alltypes_tiny_pages" begin
            t = read_parquet(joinpath(PARQUET_TESTING_DIR, "alltypes_tiny_pages.parquet"))
            @test length(Tables.columnnames(t)) == 13
            @test length(t.id) == 7300
        end

        @testset "binary" begin
            t = read_parquet(joinpath(PARQUET_TESTING_DIR, "binary.parquet"))
            @test length(t.foo) == 12
        end

        @testset "binary_truncated_min_max" begin
            t = read_parquet(joinpath(PARQUET_TESTING_DIR, "binary_truncated_min_max.parquet"))
            @test length(Tables.columnnames(t)) == 6
            @test t.utf8_full_truncation[1] == "Blart Versenwald III"
            @test t.utf8_no_truncation[2] == "Al"
        end

        @testset "concatenated_gzip_members" begin
            t = read_parquet(joinpath(PARQUET_TESTING_DIR, "concatenated_gzip_members.parquet"))
            @test length(t.long_col) == 513
            @test t.long_col[1] == 1
            @test t.long_col[end] == 513
        end

        @testset "lz4_raw_compressed" begin
            t = read_parquet(joinpath(PARQUET_TESTING_DIR, "lz4_raw_compressed.parquet"))
            @test length(Tables.columnnames(t)) == 3
            @test t.c0 == [1593604800, 1593604800, 1593604801, 1593604801]
            @test t.v11 ≈ [42.0, 7.7, 42.125, 7.7]
        end

        @testset "lz4_raw_compressed_larger" begin
            t = read_parquet(joinpath(PARQUET_TESTING_DIR, "lz4_raw_compressed_larger.parquet"))
            @test length(Tables.getcolumn(t, first(Tables.columnnames(t)))) == 10000
        end

        @testset "nan_in_stats" begin
            t = read_parquet(joinpath(PARQUET_TESTING_DIR, "nan_in_stats.parquet"))
            @test t.x[1] == 1.0
            @test isnan(t.x[2])
        end

        @testset "single_nan" begin
            t = read_parquet(joinpath(PARQUET_TESTING_DIR, "single_nan.parquet"))
            @test t.mycol[1] === missing
        end

        @testset "nulls.snappy" begin
            t = read_parquet(joinpath(PARQUET_TESTING_DIR, "nulls.snappy.parquet"))
            # optional group b_struct { optional int32 b_c_int }: all 8 structs are
            # present (def = 1) with a null field — not struct-level nulls
            col = t.b_struct
            @test col isa Parquet3.StructColumn
            @test length(col) == 8
            @test all(x -> x.b_c_int === missing, col)
            @test all(ismissing, col.b_c_int)
        end

        @testset "sort_columns" begin
            t = read_parquet(joinpath(PARQUET_TESTING_DIR, "sort_columns.parquet"))
            @test t.a[2] == 2
            @test t.a[3] == 1
            @test t.b == ["a", "b", "c", "a", "b", "c"]
        end

        @testset "page_v2_empty_compressed" begin
            t = read_parquet(joinpath(PARQUET_TESTING_DIR, "page_v2_empty_compressed.parquet"))
            @test length(t.integer_column) == 10
            @test all(ismissing, t.integer_column)
        end

        @testset "nation.dict-malformed" begin
            t = read_parquet(joinpath(PARQUET_TESTING_DIR, "nation.dict-malformed.parquet"))
            @test length(Tables.columnnames(t)) == 4
            @test length(t.nation_key) == 25
            @test t.nation_key[1:3] == Int32[0, 1, 2]
        end

        @testset "byte_stream_split.zstd" begin
            t = read_parquet(joinpath(PARQUET_TESTING_DIR, "byte_stream_split.zstd.parquet"))
            @test length(Tables.columnnames(t)) == 2
            @test length(t.f32) == 300
            @test length(t.f64) == 300
            @test eltype(t.f32) == Float32
            @test eltype(t.f64) == Float64
        end

        @testset "datapage_v1 checksum files" begin
            for f in ["datapage_v1-uncompressed-checksum.parquet",
                       "datapage_v1-snappy-compressed-checksum.parquet",
                       "datapage_v1-corrupt-checksum.parquet"]
                t = read_parquet(joinpath(PARQUET_TESTING_DIR, f))
                @test length(Tables.columnnames(t)) == 2
                @test length(t.a) == 5120
            end
        end

        @testset "dict encoding files" begin
            for f in ["plain-dict-uncompressed-checksum.parquet",
                       "rle-dict-snappy-checksum.parquet",
                       "rle-dict-uncompressed-corrupt-checksum.parquet"]
                t = read_parquet(joinpath(PARQUET_TESTING_DIR, f))
                @test length(Tables.columnnames(t)) == 2
                @test length(Tables.getcolumn(t, first(Tables.columnnames(t)))) == 1000
            end
        end

        @testset "dict-page-offset-zero" begin
            # dictionary_page_offset = 0 means "no dictionary page", not "pages start at byte 0"
            t = read_parquet(joinpath(PARQUET_TESTING_DIR, "dict-page-offset-zero.parquet"))
            @test length(t.l_partkey) == 39 && all(==(1552), t.l_partkey)     # as pyarrow reads it
            # The mirror case: a zero-row chunk with only a dictionary page stores data_page_offset = 0
            t = read_parquet(joinpath(PARQUET_TESTING_DIR, "column_chunk_key_value_metadata.parquet"))
            @test collect(propertynames(t)) == [:column1, :column2] && length(t.column1) == 0
        end

        @testset "datapage_v2_empty_datapage.snappy" begin
            # One null value: the v2 page's data section is zero bytes, though flagged as compressed
            t = read_parquet(joinpath(PARQUET_TESTING_DIR, "datapage_v2_empty_datapage.snappy.parquet"))
            @test length(t.value) == 1 && ismissing(t.value[1]) && nonmissingtype(eltype(t.value)) == Float32   # pyarrow: [None], float
        end

        @testset "rle_boolean_encoding" begin
            # A v2 page that stores repetition levels for a column that is not repeated (they must be
            # skipped to find the data section), holding RLE-encoded booleans
            t = read_parquet(joinpath(PARQUET_TESTING_DIR, "rle_boolean_encoding.parquet"))
            col = t.datatype_boolean
            @test length(col) == 68 && count(ismissing, col) == 6
            @test isequal(collect(col[1:12]), [true, false, missing, true, true, false, false, true, true, true, false, false])
            result = _run_pyarrow("import pyarrow.parquet as pq; print(pq.read_table('$(joinpath(PARQUET_TESTING_DIR, "rle_boolean_encoding.parquet"))').column(0).to_pylist())")
            result === nothing || @test result == "[" * join((v === missing ? "None" : v ? "True" : "False" for v in col), ", ") * "]"
        end

        @testset "datapage_v2.snappy" begin
            # Its boolean column is RLE-encoded, as every boolean in a v2 page from Arrow-based writers
            t = read_parquet(joinpath(PARQUET_TESTING_DIR, "datapage_v2.snappy.parquet"))
            @test collect(propertynames(t)) == [:a, :b, :c, :d, :e]
            @test collect(t.d) == [true, true, true, false, true] && collect(t.b) == [1, 2, 3, 4, 5]
            @test isequal(collect(t.a), ["abc", "abc", "abc", missing, "abc"]) && collect(t.c) == [2.0, 3.0, 4.0, 5.0, 2.0]
            @test isequal([ismissing(l) ? missing : collect(l) for l in t.e], [[1, 2, 3], missing, missing, [1, 2, 3], [1, 2]])
        end

        @testset "PLAIN fixed-length byte arrays" begin
            # fixed_length_byte_array.parquet is malformed, so the encoding is checked on files that are not
            path = joinpath(PARQUET_TESTING_DIR, "byte_stream_split_extended.gzip.parquet")
            t = read_parquet(path; columns = ["flba5_plain"])
            @test length(t.flba5_plain) == 200 && all(v -> length(v) == 5, t.flba5_plain)
            result = _run_pyarrow("import pyarrow.parquet as pq; print(','.join(v.hex() for v in pq.read_table('$(path)', columns=['flba5_plain']).column(0).to_pylist()))")
            result === nothing || @test result == join((bytes2hex(collect(v)) for v in t.flba5_plain), ",")
            @test length(read_parquet(joinpath(PARQUET_TESTING_DIR, "fixed_length_decimal.parquet")).value) == 24
            @test_throws Parquet3.ColumnReadError read_parquet(joinpath(PARQUET_TESTING_DIR, "fixed_length_byte_array.parquet"))
        end

        @testset "overflow_i16_page_cnt" begin
            t = read_parquet(joinpath(PARQUET_TESTING_DIR, "overflow_i16_page_cnt.parquet"))
            @test length(Tables.getcolumn(t, first(Tables.columnnames(t)))) == 40000
        end

        @testset "int32_with_null_pages" begin
            t = read_parquet(joinpath(PARQUET_TESTING_DIR, "int32_with_null_pages.parquet"))
            col = Tables.getcolumn(t, first(Tables.columnnames(t)))
            @test length(col) == 1000
            @test any(ismissing, col)
        end
    end

    @group "parquet-testing: Nested types" begin

        @testset "nested_lists.snappy" begin
            t = read_parquet(joinpath(PARQUET_TESTING_DIR, "nested_lists.snappy.parquet"))
            @test length(Tables.columnnames(t)) == 2
            a = t.a
            @test length(a) == 3
            @test a[1][1][1] == ["a", "b"]
            @test a[1][1][2] == ["c"]
            @test t.b == Int32[1, 1, 1]
        end

        @testset "null_list" begin
            t = read_parquet(joinpath(PARQUET_TESTING_DIR, "null_list.parquet"))
            @test length(t.emptylist) == 1
            @test t.emptylist[1] == Int32[]
        end
    end

    @group "parquet-testing: Byte Stream Split cross-check" begin
        path = joinpath(PARQUET_TESTING_DIR, "byte_stream_split_extended.gzip.parquet")
        # BYTE_STREAM_SPLIT is decoded for FLOAT, DOUBLE, INT32 and INT64; the three fixed-length columns are an error
        err = try read_parquet(path); nothing catch e; e end
        @test err isa Parquet3.ColumnReadError && err.column == "float16_byte_stream_split"
        @test occursin("pass `columns=` without it", sprint(showerror, err))
        t = read_parquet(path; columns = ["float_plain", "float_byte_stream_split", "double_plain", "double_byte_stream_split",
                                           "int32_plain", "int32_byte_stream_split", "int64_plain", "int64_byte_stream_split"])
        @test Tables.getcolumn(t, :float_plain) ≈ Tables.getcolumn(t, :float_byte_stream_split)
        @test Tables.getcolumn(t, :double_plain) ≈ Tables.getcolumn(t, :double_byte_stream_split)
        # Integers: the same values and the same type as their PLAIN twins (an INT64 column used to come back as Float64)
        @test t.int32_byte_stream_split == t.int32_plain && eltype(t.int32_byte_stream_split) == eltype(t.int32_plain)
        @test t.int64_byte_stream_split == t.int64_plain && eltype(t.int64_byte_stream_split) == eltype(t.int64_plain)
    end

    @group "parquet-testing: every file reads fully or is a known gap" begin
        for f in filter(endswith(".parquet"), readdir(PARQUET_TESTING_DIR))
            path = joinpath(PARQUET_TESTING_DIR, f)
            failing = filter(column_names(open_parquet(path))) do name
                try read_parquet(path; columns = [name]); false catch e; e isa Parquet3.ColumnReadError || rethrow(); true end
            end
            @testset "$f" begin
                @test failing == get(PARQUET_TESTING_KNOWN_GAPS, f, String[])
                isempty(failing) ? @test(read_parquet(path) isa Arrow.Table) :
                                   @test_throws(Parquet3.ColumnReadError, read_parquet(path))
            end
        end
    end

elseif TEST_STRICT
    @testset "parquet-testing submodule" begin
        @test HAS_PARQUET_TESTING     # PARQUET3_TEST_STRICT is set: clone with --recurse-submodules
    end
else
    @warn "Skipping parquet-testing suite: submodule not found at $PARQUET_TESTING_DIR"
end

# =============================================================================
# Reader: plan, assembly, nested shapes, selection
# =============================================================================


@group "Reader corpus: every file reads in full, with exact nullability" begin
    @testset "loose_nodes" begin
        tight = (a = [1, 2], l = [[1], Int[]], s = [(x = 1,), (x = 2,)], o = [1, missing])
        @test isempty(loose_nodes(tight))
        loose = (a = Union{Missing, Int}[1, 2], l = Union{Missing, Vector{Union{Missing, Int}}}[[1], [2]],
                 s = Union{Missing, @NamedTuple{x::Union{Missing, Int}}}[(x = 1,), (x = 2,)],
                 m = Union{Missing, @NamedTuple{x::Union{Missing, Int}}}[(x = missing,), missing],
                 # a member is missing wherever its struct is, so `x` needs Missing here; `y`'s list elements do not
                 n = Union{Missing, @NamedTuple{x::Union{Missing, Int}, y::Union{Missing, Vector{Union{Missing, Int}}}}}[(x = 1, y = [1]), missing])
        @test loose_nodes(loose) == ["a", "l", "l[]", "s", "s.x", "n.y[]"]
    end

    # pyarrow fixtures (shapes × writer options), our writer's files, the parquet-testing
    # files without known gaps, and part-0.parquet where it exists locally
    mktempdir() do dir
        corpus = harness_corpus(dir)
        @test length(corpus) > 20
        @testset "$label" for (label, path) in corpus
            @test isempty(loose_nodes(read_parquet(path)))      # Missing only where a missing occurs
        end
    end
end

@group "Reader: plan and pruning" begin
    P = Parquet3
    plan_of(path) = (pf = open_parquet(path); tree = P.build_schema_tree(pf.metadata.schema); close(pf);
                     (P.plan_read_tree(tree), tree))
    strings(plan) = map(P.read_plan_string, plan)

    mktempdir() do dir
        path = joinpath(dir, "shapes.parquet")
        write_parquet(path, (
            id    = [1, 2],
            wf    = [(t0 = 0.5, values = Int32[1, 2]), (t0 = 1.5, values = Int32[])],
            ev    = [(id = 1, vertex = (x = 0.1, y = 0.2)), (id = 2, vertex = (x = 0.3, y = 0.4))],
            parts = [[(pt = 1f0, q = Int32(1))], [(pt = 2f0, q = Int32(-1))]],
            ll    = [[[1, 2], [3]], [[4]]],
            deep  = [[(a = 1, v = [1, 2]), (a = 2, v = Int[])], [(a = 3, v = [3])]],
        ))
        plan, _ = plan_of(path)
        @test strings(plan) == [
            "id: leaf@1",
            "wf: struct@1{t0: leaf@2, values: list@2/3<leaf@4>}",
            "ev: struct@1{id: leaf@2, vertex: struct@2{x: leaf@3, y: leaf@3}}",
            "parts: list@1/2<struct@3{pt: leaf@4, q: leaf@4}>",
            "ll: list@1/2<list@3/4<leaf@5>>",
            "deep: list@1/2<struct@3{a: leaf@4, v: list@4/5<leaf@6>}>",
        ]
        # User keys carry no list/element segments, as in the writer's `encoding` keyword
        @test [l.key for n in plan for l in P.read_leaves(n)] ==
              ["id", "wf.t0", "wf.values", "ev.id", "ev.vertex.x", "ev.vertex.y", "parts.pt", "parts.q", "ll", "deep.a", "deep.v"]
        @test [join(l.path, ".") for l in P.read_leaves(plan[4])] == ["parts.list.element.pt", "parts.list.element.q"]
        @test [n.rep_level for n in (plan[5], only(plan[5].children), only(only(plan[5].children).children))] == [1, 2, 2]

        # Pruning: a member keeps its ancestors; a group keeps everything; keys union; schema order
        pruned(keys) = strings(P.prune_read_plan(plan, keys))
        @test pruned(["wf.t0"]) == ["wf: struct@1{t0: leaf@2}"]
        @test pruned(["parts.pt", "id"]) == ["id: leaf@1", "parts: list@1/2<struct@3{pt: leaf@4}>"]
        @test pruned(["ev.vertex"]) == ["ev: struct@1{vertex: struct@2{x: leaf@3, y: leaf@3}}"]
        @test pruned(["ev.vertex.y", "ev.id"]) == ["ev: struct@1{id: leaf@2, vertex: struct@2{y: leaf@3}}"]
        @test pruned(["wf", "wf.t0"]) == ["wf: struct@1{t0: leaf@2, values: list@2/3<leaf@4>}"]
        @test pruned(["deep.v", "ll"]) == ["ll: list@1/2<list@3/4<leaf@5>>", "deep: list@1/2<struct@3{v: list@4/5<leaf@6>}>"]
        @test pruned([n.name for n in plan]) == strings(plan)
        # Unselected leaves are gone from the plan, so they cannot be decoded
        @test [l.key for n in P.prune_read_plan(plan, ["wf.t0", "parts.q"]) for l in P.read_leaves(n)] == ["wf.t0", "parts.q"]

        # A key matching nothing is an error naming it (Parquet paths and bare member names are not keys)
        @test_throws "no column matches \"nope\", \"wf.list\"" P.prune_read_plan(plan, ["id", "nope", "wf.list"])
        @test_throws "no column matches \"parts.list.element.pt\"" P.prune_read_plan(plan, ["parts.list.element.pt"])
        @test_throws "no column matches \"t0\"" P.prune_read_plan(plan, ["t0"])
        @test_throws ArgumentError P.prune_read_plan(plan, ["w"])      # a prefix of a name is not a match
    end

    if HAS_PARQUET_TESTING
        ptfile(f) = joinpath(PARQUET_TESTING_DIR, f)
        # Legacy and unusual layouts, per the format's backward-compatibility rules
        @test strings(first(plan_of(ptfile("old_list_structure.parquet")))) == ["a: list@0/1<list@1/2<leaf@2>>"]
        @test strings(first(plan_of(ptfile("repeated_primitive_no_list.parquet")))) == [
            "Int32_list: list@0/1<leaf@1>", "String_list: list@0/1<leaf@1>",
            "group_of_lists: struct@0{Int32_list_in_group: list@0/1<leaf@1>, String_list_in_group: list@0/1<leaf@1>}"]
        @test strings(first(plan_of(ptfile("repeated_no_annotation.parquet")))) == [
            "id: leaf@0", "phoneNumbers: struct@1{phone: list@1/2<struct@2{number: leaf@2, kind: leaf@3}>}"]
        @test strings(first(plan_of(ptfile("map_no_value.parquet")))) == [
            "my_map: map@0/1<struct@1{key: leaf@1, value: leaf@2}>", "my_map_no_v: list@0/1<leaf@1>",   # as pyarrow: a list of the keys
            "my_list: list@0/1<leaf@1>"]
        nested_maps, _ = plan_of(ptfile("nested_maps.snappy.parquet"))
        @test strings(nested_maps)[1] == "a: map@1/2<struct@2{key: leaf@2, value: map@3/4<struct@4{key: leaf@4, value: leaf@4}>}>"
        @test [l.key for l in P.read_leaves(nested_maps[1])] == ["a.key", "a.value.key", "a.value.value"]
        @test strings(first(plan_of(ptfile("nested_lists.snappy.parquet"))))[1] == "a: list@1/2<list@3/4<list@5/6<leaf@7>>>"

        # Every schema plans, and the plan agrees with the schema tree: the same leaves in the
        # same order with the same levels, each under as many lists as its repetition level.
        depths(node, n = 0) = node.kind == :leaf ? [(node, n)] :
            reduce(vcat, [depths(c, n + (node.kind in (:list, :map))) for c in node.children])
        for f in filter(endswith(".parquet"), readdir(PARQUET_TESTING_DIR))
            plan, tree = plan_of(ptfile(f))
            leaves, columns = [l for n in plan for l in P.read_leaves(n)], P.get_leaf_columns(tree)
            @testset "$f" begin
                @test [l.path for l in leaves] == first.(columns)
                @test all(l.def_level == c.max_def_level && l.rep_level == c.max_rep_level for (l, (_, c)) in zip(leaves, columns))
                @test all(leaf.rep_level == n for node in plan for (leaf, n) in depths(node))
            end
        end
    end
end


@group "Reader: leaves, structs, lists, fixed-size lists" begin
    if HAS_PARQUET_TESTING
        # An empty list that cannot be null (bare repeated field) is [], as pyarrow reads it; it used to read as missing
        path = joinpath(PARQUET_TESTING_DIR, "repeated_primitive_no_list.parquet")
        new = read_parquet(path; columns = ["Int32_list"])
        @test collect.(new.Int32_list) == [[0, 1, 2, 3], Int32[], [4], [5, 6, 7, 8]]
        result = _run_pyarrow("import pyarrow.parquet as pq; print(pq.read_table('$(path)').column('Int32_list').to_pylist())")
        result === nothing || @test result == "[[0, 1, 2, 3], [], [4], [5, 6, 7, 8]]"
    end

    # Lists, fixed-size lists and selection inside structs
    mktempdir() do dir
        path = joinpath(dir, "l.parquet")
        V = Parquet3.FixedSizeView{2, Int32}
        fsv(a, b) = V(Int32[a, b], 0)
        WF = @NamedTuple{t0::Float64, values::V, tags::Union{Missing, Vector{Union{Missing, String}}}}
        write_parquet(path, (
            wf   = Union{Missing, WF}[(t0 = 0.5, values = fsv(1, 2), tags = ["a", missing]), missing,
                                      (t0 = 1.5, values = fsv(3, 4), tags = missing), (t0 = 2.5, values = fsv(5, 6), tags = [])],
            fsl  = [fsv(1, 2), fsv(3, 4), fsv(5, 6), fsv(7, 8)],
            ll   = [[[1, 2], Int[]], Vector{Int}[], [[3]], [[4], [5, 6]]],
        ))
        t = read_parquet(path)
        @test t.fsl isa Parquet3.FixedSizeListVector{2, Int32} && collect.(t.fsl) == [[1, 2], [3, 4], [5, 6], [7, 8]]
        @test Missing <: eltype(t.wf.values) <: Union{Missing, V} && ismissing(t.wf.values[2]) && t.wf.values[3] == [3, 4]
        @test isequal(collect.(skipmissing(t.wf.tags)), [["a", missing], String[]]) && ismissing(t.wf[2]) && ismissing(t.wf[3].tags)
        @test collect(map(l -> collect.(l), t.ll)) == [[[1, 2], Int[]], Vector{Int}[], [[3]], [[4], [5, 6]]]
        @test !(Missing <: eltype(t.ll)) && !(Missing <: eltype(first(t.ll))) && eltype(first(first(t.ll))) == Int64
        # Selecting one member reads only that leaf; the struct's validity then comes from it
        sel = read_parquet(path; columns = ["wf.tags", "ll"])
        @test collect(propertynames(sel)) == [:wf, :ll] && propertynames(sel.wf) == (:tags,)
        @test ismissing(sel.wf[2]) && ismissing(sel.wf[3].tags) && isequal(collect(sel.wf[1].tags), ["a", missing])
        only_fsl = read_parquet(path; columns = ["wf.values"])
        @test only_fsl.wf.values isa Parquet3.FixedSizeListVector && ismissing(only_fsl.wf[2]) && only_fsl.wf[4].values == [5, 6]
    end

    # Member access and selection on a struct
    mktempdir() do dir
        path = joinpath(dir, "s.parquet")
        write_parquet(path, (id = [1, 2, 3], s = Union{Missing, @NamedTuple{a::Union{Missing, Int32}, b::String}}[
                                 (a = Int32(1), b = "x"), missing, (a = missing, b = "z")]))
        t = read_parquet(path)
        @test t.s isa Parquet3.StructColumn && isequal(t.s.a, [1, missing, missing]) && isequal(t.s.b, ["x", missing, "z"])
        @test eltype(t.id) == Int64 && ismissing(t.s[2]) && t.s[3].b == "z"
        only_b = read_parquet(path; columns = ["s.b"])
        @test collect(propertynames(only_b)) == [:s] && propertynames(only_b.s) == (:b,)
        @test isequal(collect(only_b.s), [(b = "x",), missing, (b = "z",)])
        @test_throws ArgumentError read_parquet(path; columns = ["s.c"])
    end
end

@group "Reader: nested shapes and maps" begin
    P = Parquet3
    plain(x) = x isa AbstractVector ? Any[plain(v) for v in x] : x isa NamedTuple ? map(plain, x) : x

    # pyarrow-written files, one row group and several
    mktempdir() do dir
        for (name, kwargs) in (("one.parquet", ""), ("chunks.parquet", ", row_group_size=4"), ("v2.parquet", ", data_page_version='2.0', use_dictionary=False"))
            path, out = joinpath(dir, name), joinpath(dir, "rw_" * name)
            _run_pyarrow(HARNESS_NEW_SHAPES * "pq.write_table(table, '$(path)'$(kwargs))\nprint('SUCCESS')") == "SUCCESS" ||
                (@warn "Skipping new-shape pyarrow fixtures: uv/pyarrow not available"; break)
            t = read_parquet(path)
            @test collect(propertynames(t)) == [:lsl, :lss, :sls, :lls, :m, :mm, :sm, :id]
            @test isempty(loose_nodes(t))

            # What each shape reads as
            @test t.lsl isa P.ListOfStructsColumn && propertynames(t.lsl) == (:x, :adc)
            @test t.lss isa P.ListOfStructsColumn && t.lls isa P.ListOfStructsColumn && propertynames(t.lls) == (:id, :vertex)
            @test t.sls isa P.StructColumn && t.sls.hits isa P.ListOfStructsColumn
            @test t.m isa P.MapColumn && propertynames(t.m) == (:key, :value) && t.sm.tags isa P.MapColumn && t.mm isa P.MapColumn

            # Rows
            @test isequal(plain(t.lsl[2]), Any[(x = 0.0f0, adc = Any[0])]) && ismissing(t.lsl[5]) && isempty(t.lsl[4])
            @test isequal(plain(t.lsl[3]), Any[(x = 0.0f0, adc = Any[0, missing]), (x = 0.5f0, adc = missing)])
            @test isequal(plain(t.sls[2]), (run = 1, hits = Any[(x = 0.0f0, adc = Any[0])])) && ismissing(t.sls[4]) && ismissing(t.sls[3].hits)
            @test t.m[3] isa P.MapView && isequal(collect(t.m[3]), ["k0" => 0, "k1" => missing]) && ismissing(t.m[4]) && isempty(t.m[1])
            @test t.mm[2]["a"]["x"] == 1 && ismissing(t.mm[2]["a"]["y"]) && ismissing(t.mm[2]["b"]) && isempty(t.mm[2]["c"])
            @test collect(keys(t.mm[2])) == ["a", "b", "c"] && t.sm[2].tags["t"] == 1.5

            # Named fields, through every list level and composed across structs
            @test plain(t.lsl.x[3]) == Any[0.0f0, 0.5f0] && isequal(plain(t.lsl.adc[3]), Any[Any[0, missing], missing])
            @test isequal(plain(t.lss.vertex.x[3]), Any[0.0f0, missing]) && isequal(plain(t.lss.vertex.tag[3]), Any["t0", missing])
            @test isequal(plain(t.sls.hits.x[1:4]), Any[Any[], Any[0.0f0], missing, missing])
            @test plain(t.lls.id[3]) == Any[Any[], Any[0]] && plain(t.lls.vertex.tag[3]) == Any[Any[], Any["a"]]
            @test plain(t.m.key[3]) == Any["k0", "k1"] && isequal(plain(t.m.value[3]), Any[0, missing])
            @test isequal(plain(t.mm.value.key[2]), Any[Any["x", "y"], missing, Any[]])
            @test plain(t.sm.tags.key[1]) == Any["t"] && plain(t.sm.tags.value[1]) == Any[1.5]

            # Selecting inside the new shapes
            sel = read_parquet(path; columns = ["lsl.adc", "m.key", "sls.hits.x"])
            @test collect(propertynames(sel)) == [:lsl, :sls, :m]
            @test propertynames(sel.lsl) == (:adc,) && propertynames(sel.m) == (:key,) && propertynames(sel.sls) == (:hits,)
            @test isequal(plain(sel.lsl.adc), plain(t.lsl.adc)) && isequal(plain(sel.m.key), plain(t.m.key))
            @test isequal(plain(sel.sls.hits.x), plain(t.sls.hits.x))

            # pyarrow reads our rewrite of what we read, and finds the values it wrote
            write_parquet(out, t)
            verdict = harness_pyarrow_compare([(path, out)])
            verdict === nothing || @test verdict == ["equal"]
        end
    end

    # Our own writer: write → read gives back the input, for the shapes the old reader flattens
    mktempdir() do dir
        M = Missing
        path = joinpath(dir, "w.parquet")
        tbl = (
            lsl = Union{M, Vector{Union{M, @NamedTuple{v::Union{M, Vector{Union{M, Int}}}}}}}[
                      [(v = [1, missing],), missing, (v = missing,), (v = [],)], missing, [], [(v = [2],)]],
            lss = [[(a = 1, p = (x = 1.5, y = "a")), (a = 2, p = (x = 2.5, y = "b"))], @NamedTuple{a::Int, p::@NamedTuple{x::Float64, y::String}}[],
                   [(a = 3, p = (x = 3.5, y = "c"))], [(a = 4, p = (x = 4.5, y = "d"))]],
            sls = Union{M, @NamedTuple{hits::Union{M, Vector{Union{M, @NamedTuple{x::Union{M, Int}}}}}}}[
                      (hits = [(x = 1,), missing, (x = missing,)],), missing, (hits = missing,), (hits = [],)],
            lls = Union{M, Vector{Union{M, Vector{Union{M, @NamedTuple{x::Union{M, Int}}}}}}}[
                      [[(x = 1,), missing, (x = missing,)], missing, []], missing, [], [[(x = 2,)]]],
            deep = [[(tracks = [(hits = [1, 2], q = Int32(1)), (hits = Int[], q = Int32(-1))],)], @NamedTuple{tracks::Vector{@NamedTuple{hits::Vector{Int}, q::Int32}}}[],
                    [(tracks = @NamedTuple{hits::Vector{Int}, q::Int32}[],)], [(tracks = [(hits = [3], q = Int32(1))],)]],
        )
        write_parquet(path, tbl)
        t = read_parquet(path)
        @test collect(propertynames(t)) == collect(keys(tbl))
        for k in keys(tbl)
            @test isequal(plain(getproperty(t, k)), plain(tbl[k]))
        end
        @test isempty(loose_nodes(t))
        @test !(Missing <: eltype(t.lss)) && !(Missing <: eltype(t.deep))      # no nulls written, none in the types
        @test plain(t.deep.tracks.hits[1]) == Any[Any[Any[1, 2], Any[]]] && plain(t.deep.tracks.q[4]) == Any[Any[1]]
    end

    if HAS_PARQUET_TESTING
        mktempdir() do dir
            # Nested files from other writers: legacy list layouts, maps, bare repeated fields
            files = ["nested_maps.snappy", "nullable.impala", "nonnullable.impala", "repeated_no_annotation", "map_no_value",
                     "nested_lists.snappy", "list_columns", "null_list", "old_list_structure", "repeated_primitive_no_list"]
            pairs = map(files) do f
                orig, out = joinpath(PARQUET_TESTING_DIR, f * ".parquet"), joinpath(dir, f * ".parquet")
                write_parquet(out, read_parquet(orig))
                (orig, out)
            end
            verdicts = harness_pyarrow_compare(pairs)
            verdicts === nothing || @testset "$f" for (f, v) in zip(files, verdicts)
                @test v == "equal"
            end
            # pyarrow rejects this file's schema; it reads here as a map of strings
            t = read_parquet(joinpath(PARQUET_TESTING_DIR, "incorrect_map_schema.parquet"))
            @test plain(t.my_map.key) == Any[Any["parent", "name"]] && plain(t.my_map.value) == Any[Any["another", "report"]]
        end
    end
end

@group "Reader: member selection" begin
    P = Parquet3
    plain(x) = x isa AbstractVector ? Any[plain(v) for v in x] : x isa NamedTuple ? map(plain, x) : x
    mktempdir() do dir
        path = joinpath(dir, "shapes.parquet")
        if _run_pyarrow(HARNESS_NEW_SHAPES * "pq.write_table(table, '$(path)', row_group_size=4)\nprint('SUCCESS')") == "SUCCESS"
            # Each selection, against pyarrow's own pruned read of the same leaves
            selections = [["id"], ["lsl"], ["lsl.adc"], ["lsl.x", "id"], ["lss.vertex.tag"], ["lss.vertex", "lss.id"], ["sls.run"],
                          ["sls.hits"], ["sls.hits.adc"], ["lls.vertex.x"], ["lls.id", "lsl.x"], ["m"], ["m.key"], ["m.value"],
                          ["mm.value.value"], ["mm.key", "mm.value.key"], ["sm.tags.value", "sm.n"], ["sm.tags"]]
            verdicts = harness_compare_selections(read_parquet, path, selections, dir)
            @testset "columns = $sel" for (sel, verdict) in zip(selections, verdicts)
                @test verdict == "equal"
            end

            # A selection is the full column with the other members left out
            full = read_parquet(path)
            sel = read_parquet(path; columns = ["lss.vertex.tag", "sls.run", "m.value"])
            @test isequal(plain(sel.lss.vertex.tag), plain(full.lss.vertex.tag)) && propertynames(sel.lss.vertex) == (:tag,)
            @test isequal(plain(sel.sls.run), plain(full.sls.run)) && isequal(plain(sel.m.value), plain(full.m.value))
            @test ismissing(sel.sls[4]) && ismissing(sel.m[4])      # validity comes from the remaining leaf
            @test isempty(loose_nodes(sel))
        else
            @warn "Skipping selection fixtures: uv/pyarrow not available"
        end

        # Unselected leaves are not decoded: `s.b` uses an encoding the reader cannot decode,
        # so reading `s` fails, while its other member and the other columns read fine.
        path = joinpath(dir, "undecodable.parquet")
        if _run_pyarrow("""
import pyarrow as pa, pyarrow.parquet as pq
st = pa.struct([('a', pa.int32()), ('b', pa.string())])
table = pa.table({'id': [1, 2, 3], 's': pa.array([{'a': 1, 'b': 'x'}, None, {'a': 3, 'b': 'z'}], type=st),
                  'l': pa.array([[{'a': 1, 'b': 'p'}], [], None], type=pa.list_(st))})
pq.write_table(table, '$(path)', use_dictionary=False,
               column_encoding={'id': 'PLAIN', 's.a': 'PLAIN', 's.b': 'DELTA_BYTE_ARRAY', 'l.list.element.a': 'PLAIN', 'l.list.element.b': 'DELTA_BYTE_ARRAY'})
print('SUCCESS')""") == "SUCCESS"
            err = try read_parquet(path); nothing catch e; e end
            @test err isa P.ColumnReadError && err.column == "s" && occursin("DELTA_BYTE_ARRAY", sprint(showerror, err))
            @test_throws P.ColumnReadError read_parquet(path; columns = ["s.b"])
            t = read_parquet(path; columns = ["id", "s.a", "l.a"])
            @test collect(t.id) == [1, 2, 3] && isequal(collect(t.s.a), [1, missing, 3]) && ismissing(t.s[2])
            @test isequal(plain(t.l), Any[Any[(a = 1,)], Any[], missing])
        end
    end
end

@group "Arrow.write of what read_parquet returns" begin
    P = Parquet3
    # Compare through lists, structs, maps and tuples (Arrow's fixed-size list rows), reading
    # dates and timestamps as their raw counts so that nothing is converted lossily on the way.
    canon(v::Date) = Dates.value(v - Date(1970, 1, 1))
    canon(v::DateTime) = Dates.value(v - DateTime(1970, 1, 1))
    canon(v::Union{Arrow.Timestamp, Arrow.Date}) = v.x
    canon(v) = v
    same(a, b) = (a === missing || b === missing) ? (a === missing && b === missing) :
        a isa AbstractDict ? (b isa AbstractDict && length(a) == length(b) && all(haskey(b, k) && same(v, b[k]) for (k, v) in a)) :
        (a isa AbstractVector || a isa Tuple) ? (length(a) == length(b) && all(same(x, y) for (x, y) in zip(a, b))) :
        a isa NamedTuple ? all(same(x, y) for (x, y) in zip(values(a), values(b))) : isequal(canon(a), canon(b))
    function roundtrip(tbl)
        io = IOBuffer(); Arrow.write(io, tbl); seekstart(io)
        Arrow.Table(io; convert = false)
    end
    equal_tables(t, a) = all(same(Tables.getcolumn(t, k), Tables.getcolumn(a, k)) for k in Tables.columnnames(t))

    # Every corpus file, as a whole table. Arrow has no 96-bit integer, so INT96 columns are
    # left out (Known Limitations).
    mktempdir() do dir
        @testset "$label" for (label, path) in harness_corpus(dir)
            pf = open_parquet(path)
            writable = [name for (name, element) in zip(column_names(pf), P.build_schema_tree(pf.metadata.schema).children)
                        if !any(leaf -> leaf.element.type == P.INT96, last.(P.get_leaf_columns(P.SchemaNode(element = element.element, children = [element]))))]
            close(pf)
            t = read_parquet(path; columns = writable)
            @test equal_tables(t, roundtrip(t))
        end
    end

    # Each wrapper, with a null at each level, from one row group and from several
    mktempdir() do dir
        path, out = joinpath(dir, "n.parquet"), joinpath(dir, "n.arrow")
        M = Missing
        V = P.FixedSizeView{2, Int32}
        fsv(a, b) = V(Int32[a, b], 0)
        WF = @NamedTuple{t0::Union{M, Float64}, values::Union{M, Vector{Union{M, Int32}}}, fixed::V, blob::Union{M, Vector{UInt8}}}
        PT = @NamedTuple{pt::Union{M, Float32}, tags::Union{M, Vector{String}}}
        n = 12
        tbl = (
            id   = collect(1:n),
            # a struct with a fixed-size list (never null: pyarrow cannot write a fixed-size list under a null parent) ...
            wf   = WF[(t0 = i % 3 == 0 ? missing : 0.5i, values = i % 5 == 0 ? missing : [i, missing][1:(i % 3)],
                       fixed = fsv(i, -i), blob = isodd(i) ? UInt8[i] : missing) for i in 1:n],
            # ... and a nullable struct with a list member, the shape Arrow.write used to throw on
            wn   = Union{M, @NamedTuple{a::Union{M, Int}, v::Union{M, Vector{Union{M, Int32}}}}}[
                       i % 4 == 0 ? missing : (a = isodd(i) ? i : missing, v = i % 5 == 0 ? missing : [i, missing][1:(i % 3)]) for i in 1:n],
            ev   = @NamedTuple{run::Int, vertex::Union{M, @NamedTuple{x::Float64, tag::Union{M, String}}}}[
                       (run = i, vertex = i % 3 == 0 ? missing : (x = 0.1i, tag = isodd(i) ? "v$i" : missing)) for i in 1:n],
            parts = Union{M, Vector{Union{M, PT}}}[i % 5 == 0 ? missing : Union{M, PT}[j == 2 ? missing : (pt = j == 3 ? missing : 1f0 * j, tags = j == 1 ? missing : ["a", "b"][1:(i % 3)])
                                                                           for j in 1:(i % 4)] for i in 1:n],
            m    = Union{M, Dict{String, Union{M, Int}}}[i % 4 == 0 ? missing : Dict{String, Union{M, Int}}("k$j" => (j == 2 ? missing : j) for j in 1:(i % 3)) for i in 1:n],
            lm   = [[Dict("a" => 1.5i), Dict{String, Float64}()][1:(i % 3)] for i in 1:n],
            ll   = Union{M, Vector{Union{M, Vector{Int}}}}[i % 6 == 0 ? missing : Union{M, Vector{Int}}[j == 2 ? missing : collect(1:j) for j in 1:(i % 4)] for i in 1:n],
            fsl  = [fsv(i, 2i) for i in 1:n],
            blob = [isodd(i) ? UInt8[i, i + 1] : missing for i in 1:n],
            ts   = [Arrow.Timestamp{Arrow.Meta.TimeUnit.MICROSECOND, :UTC}(1_700_000_000_000_000 + i) for i in 1:n],
        )
        write_parquet(path, tbl)
        single = read_parquet(path)
        # the same data in three row groups, rewritten by pyarrow
        result = _run_pyarrow("""
import pyarrow as pa, pyarrow.parquet as pq
pq.write_table(pq.read_table('$(path)'), '$(joinpath(dir, "multi.parquet"))', row_group_size=4)
print(pq.ParquetFile('$(joinpath(dir, "multi.parquet"))').metadata.num_row_groups)""")
        tables = result === nothing ? [("one row group", single)] :
                 [("one row group", single), ("three row groups", read_parquet(joinpath(dir, "multi.parquet")))]
        result === nothing || @test result == "3"

        @testset "$label" for (label, t) in tables
            a = roundtrip(t)
            @test equal_tables(t, a) && equal_tables(tbl, a)

            # What each column is on the Arrow side
            chunk(col) = col isa P.ChainedVector ? first(col.arrays) : col
            @test chunk(a.wf) isa Arrow.Struct && chunk(a.wn) isa Arrow.Struct && chunk(a.ev) isa Arrow.Struct && chunk(a.parts) isa Arrow.List
            @test chunk(a.m) isa Arrow.Map && chunk(a.fsl) isa Arrow.FixedSizeList && chunk(a.ll) isa Arrow.List
            @test eltype(a.blob) == Union{M, Base.CodeUnits}                 # Arrow's binary

            # Each column on its own as well (a chunked column is then joined into one array)
            @test all(same(Tables.getcolumn(t, k), Tables.getcolumn(roundtrip(NamedTuple{(k,)}((Tables.getcolumn(t, k),))), k))
                      for k in Tables.columnnames(t))

            # pyarrow reads the Arrow file and finds what it reads from the Parquet file
            Arrow.write(out, t)
            verdict = _run_pyarrow("""
import pyarrow as pa, pyarrow.parquet as pq
a, b = pq.read_table('$(path)'), pa.ipc.open_file('$(out)').read_all()
print([n for n in a.column_names if a.column(n).to_pylist() != b.column(n).to_pylist()])
print(pa.types.is_struct(b.schema.field('wf').type), pa.types.is_map(b.schema.field('m').type), pa.types.is_fixed_size_list(b.schema.field('fsl').type),
      pa.types.is_fixed_size_list(b.schema.field('wf').type.field('fixed').type), pa.types.is_binary(b.schema.field('blob').type), b.schema.field('ts').type)""")
            if verdict !== nothing
                @test split(verdict, '\n') == ["[]", "True True True True True timestamp[us, tz=UTC]"]
            end
        end

        # Buffers are handed over, not copied: the Arrow arrays hold the reader's own vectors
        native = P._arrow_native
        @test native(single.wf).data[1].data === getfield(single.wf, :_data).data[1].data            # struct member values
        @test native(single.wf).data[3].data.data === getfield(single.wf, :_data).data[3].data       # fixed-size list buffer in a struct
        @test native(single.fsl).data.data === single.fsl.data                                        # top-level fixed-size list buffer
        @test native(single.parts).offsets === getfield(single.parts, :_data).offsets                 # list offsets
        @test native(single.m).data.data[1] === getfield(single.m, :_data).entries.data.data[1]       # map keys
        @test native(single.id) === single.id

        # A fixed-size list under a null struct (one row group only; pyarrow cannot produce the file)
        nulled = Union{M, @NamedTuple{t0::Float64, fixed::V}}[(t0 = 0.5, fixed = fsv(1, 2)), missing, (t0 = 1.5, fixed = fsv(3, 4))]
        write_parquet(path, (s = nulled, id = [1, 2, 3]))
        t = read_parquet(path)
        @test equal_tables(t, roundtrip(t)) && ismissing(roundtrip(t).s[2]) && roundtrip(t).s[3].fixed == (3, 4)

        # Several row groups become several Arrow record batches when the table is written
        if length(tables) == 2
            multi = last(last(tables))
            io = IOBuffer(); Arrow.write(io, multi); seekstart(io)
            back = Arrow.Table(io; convert = false)
            @test back.wf isa P.ChainedVector && length(back.wf.arrays) == 3 && length(back.id.arrays) == 3
        end
    end
end

@group "FixedSizeList inside lists" begin
    P = Parquet3
    M = Missing
    plain(x) = x isa AbstractDict ? Dict(k => plain(v) for (k, v) in x) : x isa Union{AbstractVector, Tuple} ? Any[plain(v) for v in x] :
               x isa NamedTuple ? map(plain, x) : x
    # Through Arrow.write and back; Arrow.jl gives a fixed-size list row as a tuple
    function arrow_roundtrip(t)
        io = IOBuffer(); Arrow.write(io, t); seekstart(io)
        Arrow.Table(io; convert = false)
    end
    arrow_equal(t, a) = all(isequal(plain(Tables.getcolumn(t, k)), plain(Tables.getcolumn(a, k))) for k in Tables.columnnames(t))
    # The array under a column, through wrappers and row-group chunks, and what is inside it
    inner(c) = c isa P.NestedColumn ? inner(getfield(c, :_data)) : c isa P.ChainedVector ? inner(first(c.arrays)) : c
    fixed(a) = a isa P.FixedSizeListVector

    mktempdir() do dir
        for (name, kwargs) in (("one.parquet", ""), ("chunks.parquet", ", row_group_size=4"), ("v2.parquet", ", data_page_version='2.0', use_dictionary=False"))
            path = joinpath(dir, name)
            _run_pyarrow(HARNESS_NESTED_FIXED_SIZE * "pq.write_table(table, '$(path)'$(kwargs))\nprint('SUCCESS')") == "SUCCESS" ||
                (@warn "Skipping nested fixed-size list fixtures: uv/pyarrow not available"; break)
            t = read_parquet(path)

            # Restored wherever ARROW:schema declares it
            @test fixed(inner(t.lf).data) && fixed(inner(t.dense).data) && fixed(inner(t.top))
            @test fixed(inner(t.ls).data.data[2]) && t.ls isa P.ListOfStructsColumn        # list<struct<…, values: fsl>>
            @test fixed(inner(t.sl).data[1].data) && t.sl isa P.StructColumn                # struct with a list<fsl> member
            @test fixed(inner(t.llf).data.data)                                             # list<list<fsl>>
            @test fixed(inner(t.mf).entries.data.data[2]) && t.mf isa P.MapColumn           # map<string, fsl>
            # One fixed-size list inside another: the inner level is fixed, the outer reads as a list
            @test fixed(inner(t.ff).data) && inner(t.ff) isa Arrow.List && eltype(inner(t.ff).data) <: P.FixedSizeView{2, Int32}

            # Rows are views of FixedSizeViews: no copy, and the fixed size is in the type
            @test t.lf[1] isa SubArray && t.lf[1][2] isa P.FixedSizeView{3, Int32} && t.lf[1][2] == [4, 5, 6]
            @test isempty(t.lf[2]) && ismissing(t.lf[3]) && length(t.lf[5]) == 3
            @test t.ls[4][2].values == [7, 8, 9] && ismissing(t.ls[4][2].t0) && plain(t.ls.values[4]) == Any[Any[4, 5, 6], Any[7, 8, 9]]
            @test plain(t.sl.hits[4]) == Any[Any[4, 5, 6], Any[7, 8, 9]] && ismissing(t.sl[5]) && ismissing(t.sl.hits[3]) && isempty(t.sl[2].hits)
            @test isequal(plain(t.llf[5]), Any[missing, Any[Any[0, 1, 2]]]) && plain(t.llf[1]) == Any[Any[Any[1, 2, 3]], Any[]]
            @test t.mf[4]["c"] == [7, 8, 9] && t.mf[4]["c"] isa P.FixedSizeView{3, Int32} && ismissing(t.mf[3])
            @test plain(t.ff[2]) == Any[Any[1, 2], Any[3, 4], Any[5, 6]] && plain(t.dense[6]) == Any[Any[5, 5, 5], Any[6, 6, 6]]
            @test isempty(loose_nodes(t))

            # Arrow.write takes every shape, with the fixed sizes in the Arrow schema
            @test t.lf isa P.ListColumn && t.llf isa P.ListColumn && t.ls.values isa P.ListColumn
            a = arrow_roundtrip(t)
            @test arrow_equal(t, a)
            @test eltype(a.top) == NTuple{3, Int32} && eltype(eltype(a.dense)) == NTuple{3, Int32}
            @test nonmissingtype(eltype(nonmissingtype(eltype(a.lf)))) == NTuple{3, Int32}

            # pyarrow reads the same values from the file it wrote. (pyarrow itself gives the
            # map's values back as variable-length lists; the sizes here come from ARROW:schema.)
            out = joinpath(dir, "rw_" * name)
            write_parquet(out, t)
            back = read_parquet(out)
            @test all(isequal(plain(getproperty(back, k)), plain(getproperty(t, k))) && eltype(getproperty(back, k)) == eltype(getproperty(t, k))
                      for k in propertynames(t))
            verdict = _run_pyarrow("""
import pyarrow.parquet as pq
a, b = pq.read_table('$(path)'), pq.read_table('$(out)')
print([n for n in a.column_names if a.column(n).to_pylist() != b.column(n).to_pylist()])
print([n for n in a.column_names if a.schema.field(n).type != b.schema.field(n).type], b.schema.field('ff').type)
print(b.schema.field('lf').type, '|', b.schema.field('llf').type)""")
            lines = split(verdict, '\n')
            @test lines[1] == "[]"
            # everything keeps its type except the outer level of the list-in-list, which is a known gap
            @test lines[2] == "['ff'] list<element: fixed_size_list<element: int32>[2]>"
            @test lines[3] == "list<element: fixed_size_list<element: int32>[3]> | list<element: list<element: fixed_size_list<element: int32>[3]>>"
        end
    end

    # Null fixed-size lists below a list, next to empty and null lists (from our writer)
    mktempdir() do dir
        path = joinpath(dir, "n.parquet")
        V = P.FixedSizeView{2, Int32}
        fsv(a, b) = V(Int32[a, b], 0)
        S = @NamedTuple{t0::Float64, v::Union{M, V}}
        tbl = (
            lf = Union{M, Vector{Union{M, V}}}[[fsv(1, 2), missing, fsv(3, 4)], missing, [], [missing], [fsv(5, 6)]],
            ls = Vector{S}[[(t0 = 0.5, v = fsv(1, 2))], S[], [(t0 = 1.5, v = missing), (t0 = 2.5, v = fsv(3, 4))], [(t0 = 3.5, v = fsv(5, 6))], [(t0 = 4.5, v = missing)]],
            ll = [[[fsv(1, 2)], V[]], Vector{V}[], [[fsv(3, 4), fsv(5, 6)]], [[fsv(7, 8)]], [V[], [fsv(9, 0)]]],
        )
        write_parquet(path, tbl)
        t = read_parquet(path)
        @test all(isequal(plain(getproperty(t, k)), plain(tbl[k])) for k in keys(tbl))
        @test fixed(inner(t.lf).data) && Missing <: eltype(inner(t.lf).data) <: Union{M, V} && ismissing(t.lf[1][2]) && t.lf[1][3] == [3, 4]
        @test fixed(inner(t.ll).data.data) && eltype(inner(t.ll).data.data) <: V      # no nulls seen, none in the type
        @test arrow_equal(t, arrow_roundtrip(t))
        @test isempty(loose_nodes(t))
    end
end

@group "FixedSizeList element types" begin
    P = Parquet3
    # A fixed-size list is restored when its element is fixed-width; of anything else it reads as a list
    fixed_kinds = (:bool, :date, :tsms, :tsus, :i8, :u16, :u64, :f32, :f64, :dur, :time)
    list_kinds = (:str, :bin, :flba, :dec9, :dec30, :f16)
    be(i, nbytes) = reverse!(collect(reinterpret(UInt8, [Int128(100i)])))[end - nbytes + 1:end]   # decimal i.00, big-endian
    expected = Dict{Symbol, Any}(
        :str => i -> "s$i", :bin => i -> fill(UInt8('b'), i), :bool => isodd, :date => i -> Date(2020, 1, i),
        :tsms => i -> DateTime(2020, 1, i), :tsus => i -> Dates.value(DateTime(2020, 1, i) - DateTime(1970)) * 1000,
        :i8 => Int8, :u16 => UInt16, :u64 => UInt64, :f32 => Float32, :f64 => Float64,
        :flba => i -> Vector{UInt8}(lpad(i, 3, '0')), :dec9 => i -> be(i, 4), :dec30 => i -> be(i, 13),
        :f16 => i -> collect(reinterpret(UInt8, [Float16(i)])), :dur => Int64, :time => i -> Int32(1000i))
    plain(x) = x isa Arrow.Timestamp ? x.x : x isa Union{Base.CodeUnits, AbstractVector{UInt8}} ? Vector{UInt8}(x) :
               x isa AbstractVector ? Any[plain(v) for v in x] : x isa NamedTuple ? map(plain, x) : x
    inner(c) = c isa P.NestedColumn ? inner(getfield(c, :_data)) : c isa P.ChainedVector ? inner(first(c.arrays)) : c
    fixed(a) = a isa P.FixedSizeListVector

    mktempdir() do dir
        if _run_pyarrow("ARGS = ['$(dir)']\n" * HARNESS_FIXED_SIZE_ELEMENTS) != "SUCCESS"
            @warn "Skipping fixed-size list element fixtures: uv/pyarrow not available"
            return
        end
        tables = Dict(f => read_parquet(joinpath(dir, f * ".parquet")) for f in ("plain", "other", "int96"))
        @testset "$kind" for kind in (fixed_kinds..., list_kinds...)
            t = tables[haskey(tables["plain"], Symbol(:top_, kind)) ? "plain" : "other"]
            top, st, li = (getproperty(t, Symbol(pos, kind)) for pos in (:top_, :st_, :li_))
            rows = [Any[plain(expected[kind](i)), plain(expected[kind](i + 1))] for i in (1, 3, 5)]
            @test plain(top) == Any[rows..., rows[1]]
            @test plain(st.v) == Any[rows..., rows[1]] && st.a == 0:3
            @test isequal(plain(li), Any[rows[1:2], Any[], rows[3:3], missing])
            @test (fixed(inner(top)), fixed(inner(st.v)), fixed(inner(li).data)) == ntuple(_ -> kind in fixed_kinds, 3)
        end
        @test all(isempty ∘ loose_nodes, values(tables))
        # A null string inside what the file declares a fixed-size list; INT96 is fixed-width
        @test isequal(plain(tables["other"].strn), Any[Any["a", "b"], Any["c", missing], Any[missing, missing], Any["e", "f"]])
        @test fixed(inner(tables["int96"].top_i96)) && length(tables["int96"].top_i96) == 4

        # write → read gives the same values and types; pyarrow sees the same values, and the
        # same types except for lists of strings and bytes, which are written as plain lists
        out = joinpath(dir, "rewrite.parquet")
        t = tables["plain"]
        write_parquet(out, t)
        back = read_parquet(out)
        @test all(isequal(plain(getproperty(back, k)), plain(getproperty(t, k))) && eltype(getproperty(back, k)) == eltype(getproperty(t, k))
                  for k in propertynames(t))
        @test harness_pyarrow_compare([(joinpath(dir, "plain.parquet"), out)]) == ["equal"]
        # Arrow.write, whose Bool, Date and DateTime arrays are not stored as ours are
        io = IOBuffer(); Arrow.write(io, t); seekstart(io)
        a = Arrow.Table(io; convert = false)
        canon(x) = x isa Arrow.Date ? Date(1970) + Day(x.x) : x isa Arrow.Timestamp{Arrow.Flatbuf.TimeUnit.MILLISECOND, nothing} ? DateTime(1970) + Millisecond(x.x) :
                   x isa Arrow.Timestamp ? x.x : x isa Union{AbstractVector, Tuple} && !(x isa Union{Base.CodeUnits, AbstractVector{UInt8}}) ? Any[canon(v) for v in x] :
                   x isa NamedTuple ? map(canon, x) : plain(x)
        @test all(isequal(canon(Tables.getcolumn(a, k)), canon(Tables.getcolumn(t, k))) for k in Tables.columnnames(t))
        changed = _run_pyarrow("""
import pyarrow.parquet as pq
a, b = pq.read_schema('$(joinpath(dir, "plain.parquet"))'), pq.read_schema('$(out)')
print(','.join(sorted(n for n in a.names if a.field(n).type != b.field(n).type)))""")
        @test changed == join(sort(["$(pos)_$(kind)" for pos in (:top, :st, :li) for kind in (:str, :bin)]), ',')
    end
end

@group "FixedSizeList with a null element" begin
    P = Parquet3
    # Arrow.jl gives a fixed-size list row as a tuple and, unconverted, a date as its day count
    plain(x) = x isa AbstractDict ? Dict(k => plain(v) for (k, v) in x) : x isa Union{AbstractVector, Tuple} ? Any[plain(v) for v in x] :
               x isa NamedTuple ? map(plain, x) : x isa Arrow.Date ? Date(1970) + Day(x.x) : x
    inner(c) = c isa P.NestedColumn ? inner(getfield(c, :_data)) : c isa P.ChainedVector ? inner(first(c.arrays)) : c
    chunks_are(T, c) = all(chunk -> chunk isa T, P._chunks(c isa P.NestedColumn ? getfield(c, :_data) : c))
    function arrow_roundtrip(t)
        io = IOBuffer(); Arrow.write(io, t); seekstart(io)
        Arrow.Table(io; convert = false)
    end
    row(i) = Any[i, i == 5 ? missing : i + 1, i + 2]
    n = 8

    mktempdir() do dir
        for (name, kwargs) in (("one.parquet", ""), ("chunks.parquet", ", row_group_size=2"), ("v2.parquet", ", data_page_version='2.0', use_dictionary=False"))
            path = joinpath(dir, name)
            _run_pyarrow(HARNESS_FIXED_SIZE_NULL_ELEMENT * "pq.write_table(table, '$(path)'$(kwargs))\nprint('SUCCESS')") == "SUCCESS" ||
                (@warn "Skipping null-element fixtures: uv/pyarrow not available"; break)
            t = read_parquet(path)

            # The null element is `missing`, as pyarrow shows it, wherever the fixed-size list sits
            @test isequal(plain(t.top), Any[row(i) for i in 0:n-1])
            @test isequal(plain(t.st.v), Any[row(i) for i in 0:n-1]) && t.st.a == 0:n-1
            @test isequal(plain(t.li), Any[i == 2 ? missing : fill(row(i), i % 3) for i in 0:n-1])
            @test isequal(plain(t.mp), Any[isodd(i) ? Dict("k$i" => row(i)) : Dict() for i in 0:n-1])
            @test isequal(plain(t.dt), Any[Any[Date(2020, 1, i + 1), i == 5 ? missing : Date(2021, 1, i + 1)] for i in 0:n-1])
            @test isequal(plain(t.bo), Any[Any[true, i == 5 ? missing : false] for i in 0:n-1])
            # Still a fixed-size list over one flat vector, of the type with element nulls in
            # every row group, though only one row group has a null
            @test all(c -> chunks_are(P.FixedSizeListVector{3, Int32, <:Any, BitVector}, c) || chunks_are(P.FixedSizeListVector{2, <:Any, <:Any, BitVector}, c), (t.top, t.st.v, t.dt, t.bo))
            @test inner(t.li).data isa P.FixedSizeListVector{3, Int32, <:Any, BitVector} && t.li isa P.ListColumn
            @test eltype(t.top) == P.FixedSizeView{3, Union{Missing, Int32}, Int32, BitVector} && eltype(eltype(t.top)) == Union{Missing, Int32}
            @test t.top[6] isa P.FixedSizeView{3, Union{Missing, Int32}} && ismissing(t.top[6][2]) && t.top[6][3] === Int32(7)
            @test inner(t.top).data isa Vector{Int32} && t.top[6].parent === P._chunks(t.top)[end - (name == "chunks.parquet" ? 1 : 0)].data
            # A column without a null element keeps the plain types
            @test chunks_are(P.FixedSizeListVector{3, Int32, <:Any, Nothing}, t.ok) && eltype(t.ok) == P.FixedSizeView{3, Int32, Int32, Nothing}
            @test isempty(loose_nodes(t))
            @test isequal(plain(read_parquet(path; columns = ["top"]).top), plain(t.top))

            # Arrow.write: a fixed-size list whose child carries the nulls
            a = arrow_roundtrip(t)
            @test all(isequal(plain(Tables.getcolumn(a, k)), plain(Tables.getcolumn(t, k))) for k in Tables.columnnames(t))
            @test eltype(a.top) == NTuple{3, Union{Missing, Int32}} && eltype(a.ok) == NTuple{3, Int32}

            # write_parquet: the same values and types back, and pyarrow sees the nulls in fixed-size lists
            out = joinpath(dir, "rw_" * name)
            write_parquet(out, t)
            back = read_parquet(out)
            @test all(isequal(plain(getproperty(back, k)), plain(getproperty(t, k))) && eltype(getproperty(back, k)) == eltype(getproperty(t, k))
                      for k in propertynames(t))
            @test harness_pyarrow_compare([(path, out)]) == ["equal"]
            @test _run_pyarrow("""
import pyarrow.parquet as pq
a, b = pq.read_table('$(path)'), pq.read_table('$(out)')
print([n for n in a.column_names if a.schema.field(n).type != b.schema.field(n).type], b.schema.field('top').type, b.column('top').to_pylist()[5])""") ==
                  "[] fixed_size_list<element: int32>[3] [5, None, 7]"
        end
    end

    # A null list and a null element are independent (from our writer: pyarrow can neither write nor read a null fixed-size list)
    mktempdir() do dir
        path = joinpath(dir, "n.parquet")
        V = P.FixedSizeView{2, Union{Missing, Int32}, Int32, BitVector}
        nv(a, b) = P.FixedSizeView{2, Union{Missing, Int32}}(Int32[coalesce(a, 0), coalesce(b, 0)], BitVector([ismissing(a), ismissing(b)]), 0)
        tbl = (top = Union{Missing, V}[nv(1, 2), missing, nv(missing, 4), nv(5, missing), nv(missing, missing)],
               li  = Union{Missing, Vector{Union{Missing, V}}}[[nv(1, missing), missing], missing, [], [nv(3, 4)], [missing, nv(missing, 6)]])
        write_parquet(path, tbl)
        t = read_parquet(path)
        @test all(isequal(plain(getproperty(t, k)), plain(tbl[k])) for k in keys(tbl))
        @test inner(t.top) isa P.FixedSizeListVector{2, Int32, <:Any, BitVector} && eltype(t.top) == Union{Missing, V}
        @test inner(t.li).data isa P.FixedSizeListVector{2, Int32, <:Any, BitVector} && eltype(inner(t.li).data) == Union{Missing, V}
        @test all(isequal(plain(Tables.getcolumn(arrow_roundtrip(t), k)), plain(tbl[k])) for k in keys(tbl))

        # Null elements alone, pyarrow reads from our file. (A null fixed-size list it cannot
        # read from Parquet, whoever wrote the file: Known Limitations.)
        write_parquet(path, (c = V[nv(1, 2), nv(missing, 4), nv(5, missing)],))
        @test _run_pyarrow("import pyarrow.parquet as pq\nt = pq.read_table('$(path)')\nprint(t.schema.field('c').type, t.column('c').to_pylist())") in
              (nothing, "fixed_size_list<element: int32>[2] [[1, 2], [None, 4], [5, None]]")
    end
end

@group "Types returned as stored (decimal, Float16, duration)" begin
    mktempdir() do dir
        path = joinpath(dir, "stored.parquet")
        script = """
import pyarrow as pa, pyarrow.parquet as pq, decimal
D = decimal.Decimal
vals = [D('1.25'), None, D('-3.00')]
ints = pa.table({'d9': pa.array(vals, pa.decimal128(9, 2)), 'd18': pa.array(vals, pa.decimal128(18, 2))})
pq.write_table(ints, '$(path)', store_decimal_as_integer=True)
rest = pa.table({'d9': pa.array(vals, pa.decimal128(9, 2)), 'd30': pa.array(vals, pa.decimal128(30, 2)),
                 'f16': pa.array([1.5, None, -2.0], pa.float32()).cast(pa.float16()),
                 'dur': pa.array([1500, None, -2], pa.duration('ms'))})
pq.write_table(rest, '$(joinpath(dir, "bytes.parquet"))')
print('SUCCESS')"""
        if _run_pyarrow(script) != "SUCCESS"
            @warn "Skipping stored-type fixtures: uv/pyarrow not available"
            return
        end
        # A decimal stored as an integer is that integer, unscaled
        t = read_parquet(path)
        @test isequal(collect(t.d9), [Int32(125), missing, Int32(-300)]) && isequal(collect(t.d18), [125, missing, -300])
        @test eltype(t.d9) == Union{Missing, Int32} && eltype(t.d18) == Union{Missing, Int64}
        # As pyarrow stores it by default, the unscaled value in big-endian two's-complement bytes
        b = read_parquet(joinpath(dir, "bytes.parquet"))
        unscaled(bytes) = foldl((acc, x) -> (acc << 8) | x, bytes; init = (bytes[1] & 0x80 == 0 ? Int128(0) : Int128(-1)))
        @test length(b.d9[1]) == 4 && length(b.d30[1]) == 13 && ismissing(b.d9[2])
        @test unscaled.(b.d9[[1, 3]]) == [125, -300] && unscaled.(b.d30[[1, 3]]) == [125, -300]
        # Float16 as its two bytes, a duration as its count
        @test reinterpret(Float16, Vector{UInt8}(b.f16[1]))[1] == 1.5 && reinterpret(Float16, Vector{UInt8}(b.f16[3]))[1] == -2.0 && ismissing(b.f16[2])
        @test isequal(collect(b.dur), [1500, missing, -2]) && eltype(b.dur) == Union{Missing, Int64}
    end
end

# What this run covered. An argument that selects nothing is an error: a selective run
# must never pass by running no tests.
let unmatched = [f for f in GROUP_FILTERS if !any(name -> occursin(f, lowercase(name)), vcat(GROUPS_RUN, GROUPS_SKIPPED))]
    if isempty(GROUP_FILTERS)
        @info "Full run: $(length(GROUPS_RUN)) groups"
    else
        @info "Selective run" arguments = ARGS ran = GROUPS_RUN skipped = length(GROUPS_SKIPPED)
        @info "Skipped groups:\n  " * join(GROUPS_SKIPPED, "\n  ")
    end
    isempty(unmatched) || error("No test group matches $(join(repr.(unmatched), ", ")). Groups:\n  " * join(vcat(GROUPS_RUN, GROUPS_SKIPPED), "\n  "))
end
