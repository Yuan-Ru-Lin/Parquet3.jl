module Parquet3

using Dates
using ArraysOfArrays: nestedview

include("types.jl")
include("thrift.jl")
include("metadata.jl")
include("encodings.jl")
include("compression.jl")
include("filereader.jl")
include("pagereader.jl")
include("table.jl")
include("arrow_schema.jl")
include("api.jl")

export
    read_parquet,
    open_parquet,
    num_rows,
    num_row_groups,
    schema,
    column_names,
    schema_string,
    metadata,
    ParquetFile,
    ParquetTable

end # module Parquet3
