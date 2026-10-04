# Minimal Arrow IPC schema parser for extracting FixedSizeList and field metadata info
# Parses the ARROW:schema FlatBuffer from Parquet key-value metadata

import Base64

const _EMPTY_ARROW_SCHEMA = (schema=nothing, fsl=Dict{String,Int}(), field_meta=Dict{String,Base.ImmutableDict{String,String}}())

"""
    parse_arrow_schema(metadata) -> (schema, fsl, field_meta)

Extract the Arrow schema, FixedSizeList field sizes, and per-field custom_metadata from
the ARROW:schema Parquet metadata. Returns a named tuple with:
- `schema::Union{Arrow.Meta.Schema,Nothing}` — the parsed Arrow schema, or nothing
- `fsl::Dict{String,Int}` — field path → list_size for FixedSizeList fields; a top-level
  field is keyed by its name, a struct member by its dotted path (`"wf.values"`)
- `field_meta::Dict{String,ImmutableDict{String,String}}` — field_name → custom metadata
"""
function parse_arrow_schema(metadata::Union{Vector{KeyValue}, Nothing})
    metadata === nothing && return _EMPTY_ARROW_SCHEMA
    idx = findfirst(kv -> kv.key == "ARROW:schema", metadata)
    idx === nothing && return _EMPTY_ARROW_SCHEMA
    _parse_arrow_schema_bytes(Base64.base64decode(metadata[idx].value))
end

function _parse_arrow_schema_bytes(buf::Vector{UInt8})
    schema = Ref{Union{Arrow.Meta.Schema,Nothing}}(nothing)
    fsl = Dict{String,Int}()
    field_meta = Dict{String,Base.ImmutableDict{String,String}}()
    try
        _parse_arrow_schema_bytes!(schema, fsl, field_meta, buf)
    catch e
        @warn "Failed to parse ARROW:schema" exception=(e, catch_backtrace())
    end
    (schema=schema[], fsl=fsl, field_meta=field_meta)
end

function _parse_arrow_schema_bytes!(schema_ref, fsl, field_meta, buf)
    fb_start = (length(buf) >= 8 && buf[1:4] == UInt8[0xff, 0xff, 0xff, 0xff]) ? 8 : 0

    msg = Arrow.FlatBuffers.getrootas(Arrow.Meta.Message, buf, fb_start)
    schema = msg.header
    schema isa Arrow.Meta.Schema || return
    schema_ref[] = schema

    fields = schema.fields
    fields === nothing && return

    for field in fields
        name = field.name
        name === nothing && continue

        # Field-level custom_metadata → ImmutableDict
        cm = field.custom_metadata
        if cm !== nothing && length(cm) > 0
            kv1 = cm[1]
            k1, v1 = kv1.key, kv1.value
            if k1 !== nothing && v1 !== nothing
                d = Base.ImmutableDict(k1 => v1)
                for j in 2:length(cm)
                    kvj = cm[j]
                    kj, vj = kvj.key, kvj.value
                    kj !== nothing && vj !== nothing && (d = Base.ImmutableDict(d, kj => vj))
                end
                field_meta[name] = d
            end
        end

        _collect_fsl!(fsl, field, name)
    end
end

"""Record FixedSizeList fields under `path`, descending through struct members."""
function _collect_fsl!(fsl::Dict{String,Int}, field, path::String)
    t = field.type
    if t isa Arrow.Meta.FixedSizeList
        fsl[path] = Int(t.listSize)
    elseif t isa Arrow.Meta.Struct && field.children !== nothing
        for child in field.children
            child.name === nothing || _collect_fsl!(fsl, child, string(path, ".", child.name))
        end
    end
end
