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

# In dependency order: each file uses only what the files above it define.
include("types.jl")         # enums, metadata structs, schema nodes
include("metadata.jl")      # Thrift field tables: reading and writing metadata
include("typemap.jl")       # Parquet type ↔ Julia type, both directions
include("encodings.jl")     # value and level encodings, each decoder with its encoder
include("compression.jl")   # page compression
include("filereader.jl")    # opening a file, footer, schema tree
include("pagereader.jl")    # pages of a column chunk
include("arrow_schema.jl")  # ARROW:schema metadata
include("arrays.jl")        # the array types returned, and their builders
include("reader.jl")        # plan, prune, assemble: read_parquet
include("filewriter.jl")    # plan, shred, encode: write_parquet

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
