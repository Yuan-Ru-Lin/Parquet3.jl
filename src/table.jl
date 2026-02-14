# Lightweight table type with Tables.jl interface

using Tables

struct ParquetTable <: Tables.AbstractColumns
    names::Vector{Symbol}
    columns::Vector{AbstractVector}
    lookup::Dict{Symbol,AbstractVector}
end

function ParquetTable(names::Vector{Symbol}, columns)
    cols = collect(AbstractVector, columns)
    lookup = Dict{Symbol,AbstractVector}(zip(names, cols))
    ParquetTable(names, cols, lookup)
end

# --- Tables.jl interface ---

Tables.istable(::Type{ParquetTable}) = true
Tables.columnaccess(::Type{ParquetTable}) = true
Tables.columns(t::ParquetTable) = t
Tables.columnnames(t::ParquetTable) = getfield(t, :names)
Tables.getcolumn(t::ParquetTable, nm::Symbol) = getfield(t, :lookup)[nm]
Tables.getcolumn(t::ParquetTable, i::Int) = getfield(t, :columns)[i]
Tables.schema(t::ParquetTable) = Tables.Schema(getfield(t, :names), map(eltype, getfield(t, :columns)))

# --- Convenience accessors ---

Base.propertynames(t::ParquetTable) = getfield(t, :names)
Base.getproperty(t::ParquetTable, nm::Symbol) = getfield(t, :lookup)[nm]
Base.length(t::ParquetTable) = length(getfield(t, :names))
Base.getindex(t::ParquetTable, nm::Symbol) = getfield(t, :lookup)[nm]

function Base.show(io::IO, t::ParquetTable)
    nc = length(getfield(t, :names))
    nr = nc > 0 ? length(getfield(t, :columns)[1]) : 0
    print(io, "ParquetTable with $nr rows, $nc columns: ")
    join(io, getfield(t, :names), ", ")
end
