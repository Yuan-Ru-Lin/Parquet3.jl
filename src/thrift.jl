# Thrift Compact Protocol decoder for Parquet metadata
# Reference: https://github.com/apache/thrift/blob/master/doc/specs/thrift-compact-protocol.md

"""Thrift Compact Protocol type IDs"""
const THRIFT_STOP = 0
const THRIFT_TRUE = 1
const THRIFT_FALSE = 2
const THRIFT_BYTE = 3
const THRIFT_I16 = 4
const THRIFT_I32 = 5
const THRIFT_I64 = 6
const THRIFT_DOUBLE = 7
const THRIFT_BINARY = 8
const THRIFT_LIST = 9
const THRIFT_SET = 10
const THRIFT_MAP = 11
const THRIFT_STRUCT = 12

"""Thrift decoder state"""
mutable struct ThriftDecoder
    data::Vector{UInt8}
    pos::Int
    last_field_id::Int
    field_id_stack::Vector{Int}
end

ThriftDecoder(data::Vector{UInt8}) = ThriftDecoder(data, 1, 0, Int[])

"""Read a single byte"""
function read_byte(d::ThriftDecoder)::UInt8
    b = d.data[d.pos]
    d.pos += 1
    b
end

"""Read n bytes"""
function read_bytes(d::ThriftDecoder, n::Int)::Vector{UInt8}
    result = d.data[d.pos:d.pos+n-1]
    d.pos += n
    result
end

"""Read a varint (variable-length integer)"""
function read_varint(d::ThriftDecoder)::UInt64
    result = UInt64(0)
    shift = 0
    while true
        b = read_byte(d)
        result |= UInt64(b & 0x7f) << shift
        if (b & 0x80) == 0
            break
        end
        shift += 7
    end
    result
end

"""Read a signed varint using zigzag encoding"""
function read_zigzag(d::ThriftDecoder)::Int64
    n = read_varint(d)
    # Zigzag decode: (n >> 1) ^ -(n & 1)
    # Use signed arithmetic to handle negative values correctly
    signed_n = reinterpret(Int64, n)
    (signed_n >> 1) ⊻ (-(signed_n & 1))
end

"""Read a 64-bit double"""
function read_double(d::ThriftDecoder)::Float64
    bytes = read_bytes(d, 8)
    reinterpret(Float64, bytes)[1]
end

"""Read a binary/string value"""
function read_binary(d::ThriftDecoder)::Vector{UInt8}
    len = read_varint(d)
    read_bytes(d, Int(len))
end

"""Read a string value"""
function read_string(d::ThriftDecoder)::String
    String(read_binary(d))
end

"""Read field header and return (type, field_id) or (THRIFT_STOP, 0)"""
function read_field_header(d::ThriftDecoder)::Tuple{Int, Int}
    byte = read_byte(d)
    if byte == 0
        return (THRIFT_STOP, 0)
    end

    # Extract type (lower 4 bits) and delta (upper 4 bits)
    type_id = byte & 0x0f
    delta = (byte & 0xf0) >> 4

    if delta == 0
        # Full field id follows as zigzag varint
        field_id = Int(read_zigzag(d))
    else
        field_id = d.last_field_id + delta
    end

    d.last_field_id = field_id
    (Int(type_id), field_id)
end

"""Read list/set header and return (element_type, size)"""
function read_list_header(d::ThriftDecoder)::Tuple{Int, Int}
    byte = read_byte(d)
    size = (byte & 0xf0) >> 4
    elem_type = byte & 0x0f

    if size == 15
        # Large list, size follows as varint
        size = Int(read_varint(d))
    end

    (Int(elem_type), Int(size))
end

"""Read map header and return (key_type, value_type, size)"""
function read_map_header(d::ThriftDecoder)::Tuple{Int, Int, Int}
    size = Int(read_varint(d))
    if size == 0
        return (0, 0, 0)
    end

    types = read_byte(d)
    key_type = (types & 0xf0) >> 4
    val_type = types & 0x0f

    (Int(key_type), Int(val_type), size)
end

"""Push struct context (save current field id)"""
function push_struct(d::ThriftDecoder)
    push!(d.field_id_stack, d.last_field_id)
    d.last_field_id = 0
end

"""Pop struct context (restore previous field id)"""
function pop_struct(d::ThriftDecoder)
    d.last_field_id = pop!(d.field_id_stack)
end

"""Convert Thrift compact type to readable name (for debugging)"""
function type_name(type_id::Int)::String
    names = ["STOP", "TRUE", "FALSE", "BYTE", "I16", "I32", "I64",
             "DOUBLE", "BINARY", "LIST", "SET", "MAP", "STRUCT"]
    type_id < length(names) ? names[type_id + 1] : "UNKNOWN($type_id)"
end

"""Skip a thrift value of given type"""
function skip_value(d::ThriftDecoder, type_id::Int)
    if type_id == THRIFT_TRUE || type_id == THRIFT_FALSE
        # Boolean has no additional bytes
    elseif type_id == THRIFT_BYTE
        read_byte(d)
    elseif type_id == THRIFT_I16 || type_id == THRIFT_I32 || type_id == THRIFT_I64
        read_varint(d)
    elseif type_id == THRIFT_DOUBLE
        read_bytes(d, 8)
    elseif type_id == THRIFT_BINARY
        read_binary(d)
    elseif type_id == THRIFT_LIST || type_id == THRIFT_SET
        elem_type, size = read_list_header(d)
        for _ in 1:size
            skip_value(d, elem_type)
        end
    elseif type_id == THRIFT_MAP
        key_type, val_type, size = read_map_header(d)
        for _ in 1:size
            skip_value(d, key_type)
            skip_value(d, val_type)
        end
    elseif type_id == THRIFT_STRUCT
        push_struct(d)
        while true
            ftype, _ = read_field_header(d)
            if ftype == THRIFT_STOP
                break
            end
            skip_value(d, ftype)
        end
        pop_struct(d)
    end
end
