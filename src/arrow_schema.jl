# Minimal Arrow IPC schema parser for extracting FixedSizeList and field metadata info
# Parses the ARROW:schema FlatBuffer from Parquet key-value metadata

import Base64

const _EMPTY_ARROW_SCHEMA = (fsl=Dict{String,Int}(), field_meta=Dict{String,Base.ImmutableDict{String,String}}())

"""
    parse_arrow_schema(metadata) -> (fsl, field_meta)

Extract FixedSizeList field sizes and per-field custom_metadata from the ARROW:schema
Parquet metadata. Returns a named tuple with:
- `fsl::Dict{String,Int}` — field_name → list_size for FixedSizeList fields
- `field_meta::Dict{String,ImmutableDict{String,String}}` — field_name → custom metadata
"""
function parse_arrow_schema(metadata::Union{Vector{KeyValue}, Nothing})
    metadata === nothing && return _EMPTY_ARROW_SCHEMA
    idx = findfirst(kv -> kv.key == "ARROW:schema", metadata)
    idx === nothing && return _EMPTY_ARROW_SCHEMA
    _parse_arrow_schema_bytes(Base64.base64decode(metadata[idx].value))
end

function _parse_arrow_schema_bytes(buf::Vector{UInt8})
    fsl = Dict{String,Int}()
    field_meta = Dict{String,Base.ImmutableDict{String,String}}()
    try
        _parse_arrow_schema_bytes!(fsl, field_meta, buf)
    catch e
        @warn "Failed to parse ARROW:schema" exception=(e, catch_backtrace())
    end
    (fsl=fsl, field_meta=field_meta)
end

function _parse_arrow_schema_bytes!(fsl, field_meta, buf)
    fb_start = (length(buf) >= 8 && buf[1:4] == UInt8[0xff, 0xff, 0xff, 0xff]) ? 8 : 0

    msg = Arrow.FlatBuffers.getrootas(Arrow.Meta.Message, buf, fb_start)
    schema = msg.header
    schema isa Arrow.Meta.Schema || return

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

        # FixedSizeList detection via union type dispatch
        t = field.type
        t isa Arrow.Meta.FixedSizeList || continue
        fsl[name] = Int(t.listSize)
    end
end
