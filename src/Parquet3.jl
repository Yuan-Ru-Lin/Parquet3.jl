module Parquet3

using Dates

# Include all components
include("types.jl")
include("thrift.jl")
include("metadata.jl")
include("encodings.jl")
include("compression.jl")
include("filereader.jl")
include("pagereader.jl")
include("api.jl")

# Export public API
export
    # Main function - returns Arrow.Table
    read_parquet,

    # File handle operations
    open_parquet,

    # Inspection
    num_rows,
    num_row_groups,
    schema,
    column_names,
    schema_string,
    metadata,

    # Types (for advanced use)
    ParquetFile

end # module Parquet3
