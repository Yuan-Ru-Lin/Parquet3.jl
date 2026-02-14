# Page reader for Parquet column data

mutable struct ColumnReader
    io::IO
    meta::ColumnMetaData
    schema_node::SchemaNode
    type_length::Int
    dictionary::Union{DictionaryDecoder, Nothing}
end

ColumnReader(io::IO, meta::ColumnMetaData, node::SchemaNode, type_len::Int=0) =
    ColumnReader(io, meta, node, type_len, nothing)

struct DecodedPage{T, V<:AbstractVector{T}}
    values::V
    def_levels::Union{Vector{Int}, Nothing}
    rep_levels::Union{Vector{Int}, Nothing}
    num_values::Int
end

function read_page_header(io::IO)::Tuple{PageHeader, Int}
    start = position(io)
    data = read(io, min(1024, max(0, filesize(io) - start)))
    decoder = ThriftDecoder(data)
    header = parse_page_header(decoder)
    seek(io, start + decoder.pos - 1)
    (header, decoder.pos - 1)
end

function read_levels(data::AbstractVector{UInt8}, count::Int, max_level::Int, encoding::Encoding)
    max_level == 0 && return (zeros(Int, count), 0)

    bit_width = max(1, ceil(Int, log2(max_level + 1)))

    if encoding == RLE
        len = ltoh(reinterpret(UInt32, @view data[1:4])[1])
        levels = decode_rle_bitpacked(@view(data[5:4+len]), count, bit_width)
        return (Int.(levels), 4 + Int(len))
    else
        bytes = cld(count * bit_width, 8)
        levels = unpack_bits(@view(data[1:bytes]), count, bit_width)
        return (Int.(levels), bytes)
    end
end

function decode_values(data, count, ptype, encoding, type_len, dict)
    if encoding == PLAIN
        collect(decode_plain(ptype, data, count, type_len))
    elseif encoding in (PLAIN_DICTIONARY, RLE_DICTIONARY)
        dict === nothing && error("No dictionary for dictionary encoding")
        decode_dictionary(dict, data, count)
    elseif encoding == DELTA_BINARY_PACKED
        vals = decode_delta_binary_packed(data, count)
        ptype == INT32 ? Int32.(vals) : vals
    elseif encoding == DELTA_LENGTH_BYTE_ARRAY
        decode_delta_length_byte_array(data, count)
    elseif encoding == BYTE_STREAM_SPLIT
        ptype == FLOAT ? decode_byte_stream_split_float32(data, count) :
                         decode_byte_stream_split_float64(data, count)
    else
        error("Unsupported encoding: $encoding")
    end
end

function read_page(reader::ColumnReader)
    meta = reader.meta
    chunk_end = something(meta.dictionary_page_offset, meta.data_page_offset) + meta.total_compressed_size
    position(reader.io) >= chunk_end && return nothing

    header, _ = read_page_header(reader.io)
    page_data = read(reader.io, header.compressed_page_size)

    # Decompress for DICTIONARY_PAGE and DATA_PAGE (v1) — entire page is compressed.
    # DATA_PAGE_V2 handles decompression of data portion separately.
    if header.type != DATA_PAGE_V2 && meta.codec != UNCOMPRESSED
        page_data = decompress(page_data, meta.codec, Int(header.uncompressed_page_size))
    end

    if header.type == DICTIONARY_PAGE
        dh = header.dictionary_page_header
        reader.dictionary = DictionaryDecoder(page_data, Int(dh.num_values), meta.type, reader.type_length)
        return read_page(reader)
    end

    max_def = reader.schema_node.max_def_level
    max_rep = reader.schema_node.max_rep_level

    if header.type == DATA_PAGE
        dh = header.data_page_header
        nv = Int(dh.num_values)
        pos = 1

        rep_levels = nothing
        def_levels = nothing

        if max_rep > 0
            rep_levels, bytes = read_levels(@view(page_data[pos:end]), nv, max_rep, dh.repetition_level_encoding)
            pos += bytes
        end
        if max_def > 0
            def_levels, bytes = read_levels(@view(page_data[pos:end]), nv, max_def, dh.definition_level_encoding)
            pos += bytes
        end

        non_null = def_levels === nothing ? nv : count(==(max_def), def_levels)
        values = decode_values(@view(page_data[pos:end]), non_null, meta.type, dh.encoding, reader.type_length, reader.dictionary)

        return DecodedPage(values, def_levels, rep_levels, nv)

    elseif header.type == DATA_PAGE_V2
        dh = header.data_page_header_v2
        nv = Int(dh.num_values)
        pos = 1

        rep_levels = nothing
        def_levels = nothing

        if max_rep > 0 && dh.repetition_levels_byte_length > 0
            bw = max(1, ceil(Int, log2(max_rep + 1)))
            rep_levels = Int.(decode_rle_bitpacked(@view(page_data[pos:pos+dh.repetition_levels_byte_length-1]), nv, bw))
            pos += dh.repetition_levels_byte_length
        end
        if max_def > 0 && dh.definition_levels_byte_length > 0
            bw = max(1, ceil(Int, log2(max_def + 1)))
            def_levels = Int.(decode_rle_bitpacked(@view(page_data[pos:pos+dh.definition_levels_byte_length-1]), nv, bw))
            pos += dh.definition_levels_byte_length
        end

        data_part = @view page_data[pos:end]
        if dh.is_compressed && meta.codec != UNCOMPRESSED
            expected = header.uncompressed_page_size - dh.repetition_levels_byte_length - dh.definition_levels_byte_length
            data_part = decompress(collect(data_part), meta.codec, Int(expected))
        end

        non_null = nv - Int(dh.num_nulls)
        values = decode_values(data_part, non_null, meta.type, dh.encoding, reader.type_length, reader.dictionary)

        return DecodedPage(values, def_levels, rep_levels, nv)
    else
        return read_page(reader)
    end
end

function read_all_pages(reader::ColumnReader)
    start = something(reader.meta.dictionary_page_offset, reader.meta.data_page_offset)
    seek(reader.io, start)

    pages = DecodedPage[]
    while true
        page = read_page(reader)
        page === nothing && break
        push!(pages, page)
    end
    pages
end

"""
    assemble_column(pages, max_def, max_rep, def_thresholds) -> (values, nulls) or nested structure

Reconstruct column data from decoded pages. Handles:
- Flat columns (max_rep = 0)
- List columns (max_rep > 0) - returns Vector{Vector{T}}

`def_thresholds[i]` = min def_level at which rep_level `i` has a defined element.
"""
function assemble_column(pages::Vector{<:DecodedPage}, max_def::Int, max_rep::Int=0, def_thresholds::Vector{Int}=Int[])
    isempty(pages) && return ([], falses(0))

    # Flat column - simple assembly
    if max_rep == 0
        return assemble_flat_column(pages, max_def)
    end

    # Nested column - reconstruct lists
    return assemble_nested_column(pages, max_def, max_rep, def_thresholds)
end

"""Assemble a flat (non-repeated) column."""
function assemble_flat_column(pages::Vector{<:DecodedPage}, max_def::Int)
    total = sum(p.num_values for p in pages)
    T = eltype(first(pages).values)
    values = Vector{T}(undef, total)
    nulls = falses(total)

    out = 1
    for page in pages
        val = 1
        for i in 1:page.num_values
            if page.def_levels !== nothing && page.def_levels[i] < max_def
                nulls[out] = true
            else
                values[out] = page.values[val]
                val += 1
            end
            out += 1
        end
    end

    (values, nulls)
end

"""Collect rep/def levels and raw values from decoded pages."""
function collect_page_data(pages::Vector{<:DecodedPage}, max_def::Int)
    T = eltype(first(pages).values)
    all_rep = Int[]
    all_def = Int[]
    all_values = T[]

    for page in pages
        if page.rep_levels !== nothing
            append!(all_rep, page.rep_levels)
        else
            append!(all_rep, zeros(Int, page.num_values))
        end

        if page.def_levels !== nothing
            append!(all_def, page.def_levels)
        else
            append!(all_def, fill(max_def, page.num_values))
        end

        append!(all_values, page.values)
    end

    (all_rep, all_def, all_values)
end

function assemble_nested_column(pages::Vector{<:DecodedPage}, max_def::Int, max_rep::Int, def_thresholds::Vector{Int}=Int[])
    all_rep, all_def, all_values = collect_page_data(pages, max_def)
    isempty(all_rep) && return ([], falses(0))
    T = eltype(all_values)
    assemble_nested(all_rep, all_def, all_values, max_def, max_rep, T, def_thresholds)
end

"""
Assembly for nested structures (List<T>, List<List<T>>, etc.).

Uses a stack-based approach:
- Stack has max_rep levels, each holding a list being built
- rep_level = k means: finalize and push lists at levels > k, start new lists at level k
- def_thresholds[i] = min def_level at which rep_level i has a defined element
"""
function assemble_nested(all_rep, all_def, all_values, max_def, max_rep, ::Type{T}, def_thresholds::Vector{Int}=Int[]) where T
    num_records = count(==(0), all_rep)

    records = []
    record_nulls = falses(num_records)

    # Innermost threshold: min def_level for a leaf element to exist
    inner_threshold = length(def_thresholds) >= max_rep ? def_thresholds[max_rep] : max_rep

    # Check data for actual leaf nulls (inner_threshold <= def < max_def)
    has_leaf_nulls = inner_threshold < max_def && any(d -> inner_threshold <= d < max_def, all_def)
    LeafT = has_leaf_nulls ? Union{Missing, T} : T

    new_leaf() = LeafT[]

    # Initialize stack - each level holds current list being built
    # Innermost level is typed; intermediate levels stay Any[]
    stack = Vector{Any}(undef, max_rep)
    for level in 1:max_rep
        stack[level] = level == max_rep ? new_leaf() : []
    end

    record_idx = 0
    value_idx = 1

    function finalize_level(level)
        if level > 1
            push!(stack[level-1], stack[level])
        end
        stack[level] = level == max_rep ? new_leaf() : []
    end

    function finalize_from(start_level)
        for level in max_rep:-1:start_level
            finalize_level(level)
        end
    end

    function save_record()
        if record_idx > 0
            finalize_from(2)
            push!(records, stack[1])
            stack[1] = 1 == max_rep ? new_leaf() : []
        end
    end

    for i in eachindex(all_rep)
        rep = all_rep[i]
        def = all_def[i]

        if rep == 0
            save_record()
            record_idx += 1

            if def == 0 && max_def > 0
                record_nulls[record_idx] = true
                push!(records, missing)
                continue
            end
        elseif rep < max_rep
            finalize_from(rep + 1)
        end

        # Only push to innermost list if def reaches the inner threshold
        if def == max_def
            push!(stack[max_rep], all_values[value_idx])
            value_idx += 1
        elseif def >= inner_threshold
            # Innermost element exists but leaf value is null
            push!(stack[max_rep], missing)
        end
        # def < inner_threshold: some intermediate list is null/empty — don't push.
        # The empty stack at that level will be finalized as [] by the next entry.
    end

    save_record()

    (records, record_nulls)
end
