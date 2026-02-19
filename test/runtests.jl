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
            col = Tables.getcolumn(t, first(Tables.columnnames(t)))
            @test length(col) == 8
            @test all(ismissing, col)
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
