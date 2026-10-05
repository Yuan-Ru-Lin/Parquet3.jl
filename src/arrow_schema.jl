# Minimal Arrow IPC schema parser: the Arrow schema (the reader finds FixedSizeLists in it) and field metadata
# Parses the ARROW:schema FlatBuffer from Parquet key-value metadata

import Base64

const _EMPTY_ARROW_SCHEMA = (schema=nothing, field_meta=Dict{String,Base.ImmutableDict{String,String}}())

"""
    parse_arrow_schema(metadata) -> (schema, field_meta)

Extract the Arrow schema and per-field custom_metadata from
the ARROW:schema Parquet metadata. Returns a named tuple with:
- `schema::Union{Arrow.Meta.Schema,Nothing}` — the parsed Arrow schema, or nothing
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
    field_meta = Dict{String,Base.ImmutableDict{String,String}}()
    try
        _parse_arrow_schema_bytes!(schema, field_meta, buf)
    catch e
        @warn "Failed to parse ARROW:schema" exception=(e, catch_backtrace())
    end
    (schema=schema[], field_meta=field_meta)
end

function _parse_arrow_schema_bytes!(schema_ref, field_meta, buf)
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

    end
end

"""Parse Parquet key-value metadata into Arrow-compatible ImmutableDict."""
function _parse_kv_metadata(kv::Union{Vector{KeyValue}, Nothing})
    kv === nothing && return nothing
    isempty(kv) && return nothing
    d = Base.ImmutableDict(kv[1].key => something(kv[1].value, ""))
    for i in 2:length(kv)
        d = Base.ImmutableDict(d, kv[i].key => something(kv[i].value, ""))
    end
    d
end
