# Minimal Arrow IPC schema parser for extracting FixedSizeList info
# Parses the ARROW:schema FlatBuffer from Parquet key-value metadata

import Base64

# FlatBuffer type IDs from Arrow spec
const ARROW_TYPE_FIXED_SIZE_LIST = UInt8(16)

"""
    parse_arrow_schema(metadata::Union{Vector{KeyValue}, Nothing}) -> Dict{String, Int}

Extract FixedSizeList field sizes from the ARROW:schema Parquet metadata.
Returns a mapping of field_name → list_size.
"""
function parse_arrow_schema(metadata::Union{Vector{KeyValue}, Nothing})
    metadata === nothing && return Dict{String,Int}()
    idx = findfirst(kv -> kv.key == "ARROW:schema", metadata)
    idx === nothing && return Dict{String,Int}()
    _parse_arrow_schema_bytes(Base64.base64decode(metadata[idx].value))
end

function _parse_arrow_schema_bytes(buf::Vector{UInt8})
    result = Dict{String,Int}()
    try
        _parse_arrow_schema_bytes!(result, buf)
    catch e
        @warn "Failed to parse ARROW:schema" exception=(e, catch_backtrace())
    end
    result
end

# --- Safe FlatBuffer reader (all positions are 0-based byte offsets) ---

function _fb_u16(buf, pos)
    UInt16(buf[pos+1]) | (UInt16(buf[pos+2]) << 8)
end

function _fb_u32(buf, pos)
    UInt32(buf[pos+1]) | (UInt32(buf[pos+2]) << 8) | (UInt32(buf[pos+3]) << 16) | (UInt32(buf[pos+4]) << 24)
end

function _fb_i32(buf, pos)
    reinterpret(Int32, _fb_u32(buf, pos))
end

function _fb_u8(buf, pos)
    buf[pos+1]
end

# Read vtable offset for a given slot index (0-based) from a table at `tpos`
function _fb_slot(buf, tpos, slot_idx)
    vt = tpos - Int(_fb_i32(buf, tpos))                  # vtable position
    vt_size = Int(_fb_u16(buf, vt))                       # vtable byte size
    off = 4 + slot_idx * 2                                # slot byte offset in vtable
    off + 2 > vt_size && return 0                         # slot not present
    Int(_fb_u16(buf, vt + off))
end

# Read a string field at voffset `off` relative to table at `tpos`
function _fb_string(buf, tpos, off)
    abs = tpos + off
    str_pos = abs + Int(_fb_u32(buf, abs))
    len = Int(_fb_u32(buf, str_pos))
    String(buf[str_pos+5 : str_pos+4+len])
end

# Read vector length and data start position for a vector field
function _fb_vector(buf, tpos, off)
    abs = tpos + off
    vec_pos = abs + Int(_fb_u32(buf, abs))
    len = Int(_fb_u32(buf, vec_pos))
    (len, vec_pos + 4)
end

# Get position of i-th table element in a table-vector (0-based index)
function _fb_vec_table(buf, data_start, i)
    p = data_start + i * 4
    p + Int(_fb_u32(buf, p))
end

# --- Schema parsing ---

function _parse_arrow_schema_bytes!(result, buf)
    # ARROW:schema is wrapped in IPC Message: 4-byte continuation (0xFFFFFFFF) + 4-byte size + Message FB
    fb_start = 0
    if length(buf) >= 8 && buf[1:4] == UInt8[0xff, 0xff, 0xff, 0xff]
        fb_start = 8
    end

    # Parse Message FlatBuffer to get Schema
    msg_root = fb_start + Int(_fb_u32(buf, fb_start))

    # Message.header is a union: type at slot 1, value at slot 2
    # The value (slot 2) points to the Schema table
    schema_off = _fb_slot(buf, msg_root, 2)
    schema_off == 0 && return
    schema_abs = msg_root + schema_off
    schema_pos = schema_abs + Int(_fb_u32(buf, schema_abs))

    # Schema.fields → slot 1
    fields_off = _fb_slot(buf, schema_pos, 1)
    fields_off == 0 && return
    nfields, fields_data = _fb_vector(buf, schema_pos, fields_off)

    for i in 0:nfields-1
        field_pos = _fb_vec_table(buf, fields_data, i)

        # Field.name → slot 0
        name_off = _fb_slot(buf, field_pos, 0)
        name_off == 0 && continue
        name = _fb_string(buf, field_pos, name_off)

        # Field.type_type → slot 2 (union discriminator byte)
        tt_off = _fb_slot(buf, field_pos, 2)
        tt_off == 0 && continue
        type_type = _fb_u8(buf, field_pos + tt_off)
        type_type != ARROW_TYPE_FIXED_SIZE_LIST && continue

        # Field.type → slot 3 (union value, offset to FixedSizeList table)
        t_off = _fb_slot(buf, field_pos, 3)
        t_off == 0 && continue
        fsl_abs = field_pos + t_off
        fsl_pos = fsl_abs + Int(_fb_u32(buf, fsl_abs))

        # FixedSizeList.listSize → slot 0
        ls_off = _fb_slot(buf, fsl_pos, 0)
        ls_off == 0 && continue
        list_size = Int(_fb_i32(buf, fsl_pos + ls_off))

        result[name] = list_size
    end
end
