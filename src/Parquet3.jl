module Parquet3

using Mmap
using Dates
using Arrow
using ArrowTypes: ArrowTypes
using SentinelArrays: ChainedVector
using ArraysOfArrays: nestedview, VectorOfVectors
using BitIntegers: @define_integers
using Tables
using Thrift: TCompactProtocol, TMemoryTransport, TType,
    readStructBegin, readStructEnd, readFieldBegin, readFieldEnd,
    readListBegin, readListEnd, skip,
    writeStructBegin, writeStructEnd, writeFieldBegin, writeFieldEnd,
    writeFieldStop, writeListBegin, writeListEnd, writeBool

@define_integers 96

include("types.jl")
include("metadata.jl")
include("encodings.jl")
include("compression.jl")
include("filereader.jl")
include("pagereader.jl")
include("arrow_schema.jl")
include("filewriter.jl")
include("api.jl")

export
    read_parquet,
    write_parquet,
    open_parquet,
    num_rows,
    num_row_groups,
    schema,
    column_names,
    schema_string,
    metadata,
    ParquetFile

end # module Parquet3
