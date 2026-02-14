using Test
using Parquet3
using Tables

@testset "Parquet3.jl" begin

    @testset "Thrift Decoder" begin
        d = Parquet3.ThriftDecoder(UInt8[0x96, 0x01])
        @test Parquet3.read_varint(d) == 150

        d = Parquet3.ThriftDecoder(UInt8[0x01])
        @test Parquet3.read_zigzag(d) == -1

        d = Parquet3.ThriftDecoder(UInt8[0x02])
        @test Parquet3.read_zigzag(d) == 1
    end

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

        values, nulls = Parquet3.assemble_deep_nested(
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
        values, nulls = Parquet3.assemble_deep_nested(
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
    result = _run_pyarrow(pyscript * "\npq.write_table(table, '$(test_file)')\nprint('SUCCESS')")
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
        @test tbl isa ParquetTable
        @test length(Tables.columns(tbl)) == 3
        @test collect(tbl.id) == [1, 2, 3, 4, 5]
        @test collect(tbl.name) == ["Alice", "Bob", "Charlie", "David", "Eve"]
        @test collect(tbl.value) == [1.5, 2.5, 3.5, 4.5, 5.5]
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
        @test tbl isa ParquetTable
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
        @test tbl isa ParquetTable
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
