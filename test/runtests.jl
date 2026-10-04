using Test
using Parquet3
using Arrow
using Tables

@testset "Parquet3.jl" begin

    @testset "Plain Encoding" begin
        data = collect(reinterpret(UInt8, Int32[1, 2, 3, 4, 5]))
        @test collect(Parquet3.decode_plain_int32(data, 5)) == Int32[1, 2, 3, 4, 5]

        data = collect(reinterpret(UInt8, Float64[1.5, 2.5, 3.5]))
        @test collect(Parquet3.decode_plain_float64(data, 3)) == Float64[1.5, 2.5, 3.5]
    end

    @testset "RLE Decoding" begin
        data = UInt8[6, 5]  # RLE: 3 repetitions of value 5
        @test Parquet3.decode_rle_bitpacked(data, 3, 8) == UInt32[5, 5, 5]
    end

    @testset "Nested Column Assembly" begin
        # Simulate list column: [[1, 2], [3], [4, 5, 6]]
        # rep_levels: [0, 1, 0, 0, 1, 1] - 0 starts new record, 1 continues
        # def_levels: [2, 2, 2, 2, 2, 2] - all non-null (max_def = 2)
        page = Parquet3.DecodedPage(
            Int32[1, 2, 3, 4, 5, 6],
            [2, 2, 2, 2, 2, 2],   # def_levels
            [0, 1, 0, 0, 1, 1],   # rep_levels
            6
        )

        values, nulls = Parquet3.assemble_nested_column([page], 2, 1)

        @test length(values) == 3
        @test collect(skipmissing(values[1])) == [1, 2]
        @test collect(skipmissing(values[2])) == [3]
        @test collect(skipmissing(values[3])) == [4, 5, 6]
        @test !any(nulls)
    end

    @testset "Nested Column with Nulls" begin
        # Simulate: [[1, null, 2], [3]]
        # rep_levels: [0, 1, 1, 0]
        # def_levels: [2, 1, 2, 2] - def=1 means null element
        page = Parquet3.DecodedPage(
            Int32[1, 2, 3],
            [2, 1, 2, 2],   # def_levels
            [0, 1, 1, 0],   # rep_levels
            4
        )

        values, nulls = Parquet3.assemble_nested_column([page], 2, 1)

        @test length(values) == 2
        @test values[1][1] == 1
        @test values[1][2] === missing
        @test values[1][3] == 2
        @test collect(skipmissing(values[2])) == [3]
    end

    @testset "Deeply Nested (List<List<T>>)" begin
        # Simulate: [[[1, 2], [3]], [[4, 5]]]
        # Two records, each with a list of lists
        # Record 0: outer = [[1,2], [3]]
        #   - outer[0] = [1, 2]
        #   - outer[1] = [3]
        # Record 1: outer = [[4, 5]]
        #   - outer[0] = [4, 5]
        #
        # Values: [1, 2, 3, 4, 5]
        # rep_levels: [0, 2, 1, 0, 2]
        #   - 0: new record, new outer, new inner → 1
        #   - 2: continue inner → 2
        #   - 1: new outer, new inner → 3
        #   - 0: new record, new outer, new inner → 4
        #   - 2: continue inner → 5
        # def_levels: [3, 3, 3, 3, 3] - all defined (max_def = 3 for list.list.element)
        page = Parquet3.DecodedPage(
            Int32[1, 2, 3, 4, 5],
            [3, 3, 3, 3, 3],   # def_levels
            [0, 2, 1, 0, 2],   # rep_levels
            5
        )

        values, nulls = Parquet3.assemble_nested(
            [0, 2, 1, 0, 2],
            [3, 3, 3, 3, 3],
            Int32[1, 2, 3, 4, 5],
            3, 2, Int32
        )

        @test length(values) == 2

        # Record 0: [[1, 2], [3]]
        @test length(values[1]) == 2
        @test values[1][1] == [1, 2]
        @test values[1][2] == [3]

        # Record 1: [[4, 5]]
        @test length(values[2]) == 1
        @test values[2][1] == [4, 5]
    end

    @testset "Triple Nested (List<List<List<T>>>)" begin
        # Simulate: [[[[1, 2]]]]
        # Single record with deeply nested structure
        # rep_levels: [0, 3] - 0 starts everything, 3 continues innermost
        # def_levels: [4, 4] - all defined
        values, nulls = Parquet3.assemble_nested(
            [0, 3],
            [4, 4],
            Int32[1, 2],
            4, 3, Int32
        )

        @test length(values) == 1
        @test length(values[1]) == 1      # outer list has 1 element
        @test length(values[1][1]) == 1   # middle list has 1 element
        @test values[1][1][1] == [1, 2]   # inner list is [1, 2]
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

    @testset "Snappy Decompression" begin
        compressed = UInt8[0x05, 0x10, 0x68, 0x65, 0x6c, 0x6c, 0x6f]
        @test String(Parquet3.decompress_snappy(compressed, 5)) == "hello"
    end

end

function _run_pyarrow(script::String)
    pyhelper = joinpath(@__DIR__, "..", "..", "pyhelper")
    isdir(pyhelper) || return nothing
    uv = Sys.which("uv")
    uv === nothing && return nothing
    strip(read(Cmd(`$uv run python -c $script`; dir=pyhelper), String))
end

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

@testset "Real Parquet File" begin
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

@testset "column_names matches read_parquet keys" begin
    _with_pyarrow_file("column_names consistency", "test_colnames.parquet", """
import pyarrow as pa, pyarrow.parquet as pq
table = pa.table({
    'id': [1, 2, 3],
    'name': ['a', 'b', 'c'],
    'tags': [['x', 'y'], ['z'], ['w']],
    'scores': [[1, 2], [3, 4], [5, 6]],
})""") do tbl
        pf = open_parquet("test_colnames.parquet")
        expected = collect(string.(Tables.columnnames(tbl)))
        @test column_names(pf) == expected
        close(pf)
    end
end

@testset "Nested Data (Lists)" begin
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

@testset "Deeply Nested Data (List<List<Int>>)" begin
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

@testset "Compression Codecs" begin
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

    for codec in ["none", "snappy", "gzip", "zstd", "lz4"]
        @testset "$codec" begin
            _with_pyarrow_file("compression=$codec", "test_$codec.parquet",
                pyscript * "\nwrite_kwargs = {'compression': '$codec'}") do tbl
                check_table(tbl)
            end
        end
    end
end

@testset "Multi-RowGroup with ChainedVector" begin
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

@testset "Field-level metadata from ARROW:schema" begin
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

@testset "Narrow and unsigned integers" begin
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

@testset "Null and empty lists at every level" begin
    _with_pyarrow_file("nested list nulls", "test_list_nulls.parquet", """
import pyarrow as pa, pyarrow.parquet as pq
st = pa.struct([('x', pa.list_(pa.string())), ('vv', pa.list_(pa.list_(pa.int64())))])
table = pa.table({
    'll': pa.array([[[1, 2], [3]], [], [[4]], None, [[5], None, [], [None, 6]]],
                   type=pa.list_(pa.list_(pa.int64()))),
    'ls': pa.array([['a', None], None, [], ['b'], [None]], type=pa.list_(pa.string())),
    's':  pa.array([{'x': ['hé', None], 'vv': [[1, 2], [], None, [None, 3]]}, None,
                    {'x': None, 'vv': None}, {'x': [], 'vv': []}, {'x': [None], 'vv': [None]}], type=st),
})""") do tbl
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

@testset "Zero-row file" begin
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

@testset "Unmatched column selection warns" begin
    _with_pyarrow_file("column selection", "test_select.parquet", """
import pyarrow as pa, pyarrow.parquet as pq
table = pa.table({'id': [1, 2], 's': pa.array([{'a': 1}, {'a': 2}], type=pa.struct([('a', pa.int64())]))})""") do _
        path = joinpath(@__DIR__, "test_select.parquet")
        @test collect(propertynames(read_parquet(path; columns=["s"]))) == [:s]
        @test_logs min_level=Base.CoreLogging.Warn read_parquet(path; columns=["id", "s"])
        tbl = @test_logs (:warn, r"not found") min_level=Base.CoreLogging.Warn read_parquet(path; columns=["id", "s.a", "typo"])
        @test collect(propertynames(tbl)) == [:id]
    end
end

@testset "Struct edge cases" begin
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

@testset "Struct Columns" begin
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

    # list<struct{list}> is not yet assembled — must fall back to distinct dotted
    # columns instead of silently colliding on the top-level name
    _with_pyarrow_file("unsupported list<struct{list}> fallback", "test_los_fallback.parquet", """
import pyarrow as pa, pyarrow.parquet as pq
particles = pa.array(
    [[{'pt': 1.0, 'trace': [1, 2]}], [{'pt': 2.0, 'trace': [3]}, {'pt': 3.0, 'trace': []}]],
    type=pa.list_(pa.struct([pa.field('pt', pa.float64()),
                             pa.field('trace', pa.list_(pa.int32()))])))
table = pa.table({'particles': particles})""") do tbl
        names = collect(Tables.columnnames(tbl))
        @test length(names) == length(unique(names)) == 2   # no collision, both leaves present
        pt_col = Tables.getcolumn(tbl, only(filter(n -> contains(string(n), "pt"), names)))
        @test collect(skipmissing(pt_col[2])) == [2.0, 3.0]
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

@testset "Writer (W1)" begin
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

                    # Arrow IPC round-trip of nested reader columns. A struct with a list member
                    # and a null struct row is a known Arrow.write failure (see dev-note.md).
                    arrow_rt(col) = (io = IOBuffer(); Arrow.write(io, (c = col,)); seekstart(io);
                                     isequal(plain(Arrow.Table(io).c), plain(col)))
                    @test arrow_rt(t.ev) && arrow_rt(t.parts)
                    @test_broken try arrow_rt(t.wf) catch; false end

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

if HAS_PARQUET_TESTING
    @info "Running parquet-testing suite"

    @testset "parquet-testing: Smoke (all files open)" begin
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

    @testset "parquet-testing: Row counts" begin
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

    @testset "parquet-testing: Full read" begin

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

    @testset "parquet-testing: Nested types" begin

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

    @testset "parquet-testing: Byte Stream Split cross-check" begin
        t = read_parquet(joinpath(PARQUET_TESTING_DIR, "byte_stream_split_extended.gzip.parquet"))
        cn = Tables.columnnames(t)
        for base in [:float, :double]
            plain_name = Symbol("$(base)_plain")
            bss_name = Symbol("$(base)_byte_stream_split")
            if plain_name in cn && bss_name in cn
                @test Tables.getcolumn(t, plain_name) ≈ Tables.getcolumn(t, bss_name)
            end
        end
    end

else
    @warn "Skipping parquet-testing suite: submodule not found at $PARQUET_TESTING_DIR"
end
