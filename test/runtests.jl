using Test
using Parquet3
using Arrow
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
        @test Parquet3.decode_plain_int32(data, 5) == Int32[1, 2, 3, 4, 5]

        data = collect(reinterpret(UInt8, Float64[1.5, 2.5, 3.5]))
        @test Parquet3.decode_plain_double(data, 3) == Float64[1.5, 2.5, 3.5]
    end

    @testset "RLE Decoding" begin
        # RLE: 3 repetitions of value 5
        data = UInt8[6, 5]
        @test Parquet3.decode_rle_bitpacked(data, 3, 8) == UInt32[5, 5, 5]
    end

    @testset "Snappy Decompression" begin
        compressed = UInt8[0x05, 0x10, 0x68, 0x65, 0x6c, 0x6c, 0x6f]
        @test String(Parquet3.decompress_snappy(compressed, 5)) == "hello"
    end

end

# Test with real Parquet file
@testset "Real Parquet File" begin
    test_file = joinpath(@__DIR__, "test.parquet")

    python = something(Sys.which("python3"), Sys.which("python"), nothing)
    if python !== nothing
        script = """
import sys
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    table = pa.table({
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
        'value': [1.5, 2.5, 3.5, 4.5, 5.5],
    })
    pq.write_table(table, '$(test_file)')
    print('SUCCESS')
except ImportError:
    print('NO_PYARROW')
"""
        result = strip(read(pipeline(`$python -c $script`), String))

        if result == "SUCCESS"
            @info "Testing with pyarrow-generated file"

            # Read as Arrow Table
            tbl = read_parquet(test_file)

            @test tbl isa Arrow.Table
            @test length(Tables.columns(tbl)) == 3
            @test collect(tbl.id) == [1, 2, 3, 4, 5]
            @test collect(tbl.name) == ["Alice", "Bob", "Charlie", "David", "Eve"]
            @test collect(tbl.value) == [1.5, 2.5, 3.5, 4.5, 5.5]

            rm(test_file, force=true)
            @info "All Arrow Table tests passed"
        else
            @warn "Skipping: $result"
        end
    else
        @warn "Skipping: Python not available"
    end
end
