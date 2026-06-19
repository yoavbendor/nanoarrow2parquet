// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Yoav Bendor

// nanoarrow2parquet writer: maps an Arrow C Data Interface struct array to a
// Parquet file. The data path is deliberately thin -- for PLAIN, fixed-width
// columns the Parquet page body is byte-identical to the Arrow values buffer, so
// the column path is essentially a memcpy. The only real format work is the
// per-page headers and the trailing FileMetaData footer.

#include "nanoarrow2parquet/nanoarrow2parquet.h"

#include "compress.hpp"
#include "parquet_types.hpp"
#include "rle_bitpack.hpp"
#include "thrift_compact.hpp"

#include <nanoarrow/nanoarrow.h>

#include <cstdint>
#include <cstring>
#include <fstream>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace n2p {
namespace {

// ---- column type mapping -------------------------------------------------

enum class Extract { MemcpyFixed, WidenInt, Bool, ByteArray };

struct ColumnSpec {
    std::string name;
    pq::Type type{};
    Extract extract{};
    bool has_converted = false;
    pq::ConvertedType converted{};
    int type_length = 0;   // FIXED_LEN_BYTE_ARRAY width
    int src_width = 0;     // MemcpyFixed: element bytes; WidenInt: 1 or 2
    bool sign_extend = false;  // WidenInt
    bool large_offsets = false;  // ByteArray: 64-bit offsets
};

// Parse an Arrow C format string into a ColumnSpec. Returns std::nullopt for
// unsupported types (the caller emits N2P_UNSUPPORTED_TYPE).
std::optional<ColumnSpec> map_format(const char* format, std::string name,
                                     std::string& err) {
    ColumnSpec s;
    s.name = std::move(name);
    auto fixed = [&](pq::Type t, int width) {
        s.type = t;
        s.extract = Extract::MemcpyFixed;
        s.src_width = width;
    };
    if (std::strcmp(format, "b") == 0) {
        s.type = pq::Type::Boolean;
        s.extract = Extract::Bool;
        return s;
    }
    if (std::strcmp(format, "c") == 0) {  // int8
        s.type = pq::Type::Int32; s.extract = Extract::WidenInt; s.src_width = 1;
        s.sign_extend = true; s.has_converted = true; s.converted = pq::ConvertedType::Int8;
        return s;
    }
    if (std::strcmp(format, "C") == 0) {  // uint8
        s.type = pq::Type::Int32; s.extract = Extract::WidenInt; s.src_width = 1;
        s.has_converted = true; s.converted = pq::ConvertedType::Uint8;
        return s;
    }
    if (std::strcmp(format, "s") == 0) {  // int16
        s.type = pq::Type::Int32; s.extract = Extract::WidenInt; s.src_width = 2;
        s.sign_extend = true; s.has_converted = true; s.converted = pq::ConvertedType::Int16;
        return s;
    }
    if (std::strcmp(format, "S") == 0) {  // uint16
        s.type = pq::Type::Int32; s.extract = Extract::WidenInt; s.src_width = 2;
        s.has_converted = true; s.converted = pq::ConvertedType::Uint16;
        return s;
    }
    if (std::strcmp(format, "i") == 0) { fixed(pq::Type::Int32, 4); return s; }
    if (std::strcmp(format, "I") == 0) {  // uint32
        fixed(pq::Type::Int32, 4);
        s.has_converted = true; s.converted = pq::ConvertedType::Uint32; return s;
    }
    if (std::strcmp(format, "l") == 0) { fixed(pq::Type::Int64, 8); return s; }
    if (std::strcmp(format, "L") == 0) {  // uint64
        fixed(pq::Type::Int64, 8);
        s.has_converted = true; s.converted = pq::ConvertedType::Uint64; return s;
    }
    if (std::strcmp(format, "f") == 0) { fixed(pq::Type::Float, 4); return s; }
    if (std::strcmp(format, "g") == 0) { fixed(pq::Type::Double, 8); return s; }
    if (std::strncmp(format, "w:", 2) == 0) {  // fixed_size_binary:N
        const int n = std::atoi(format + 2);
        if (n <= 0) { err = "invalid fixed_size_binary width: " + std::string(format); return std::nullopt; }
        s.type = pq::Type::FixedLenByteArray; s.extract = Extract::MemcpyFixed;
        s.src_width = n; s.type_length = n;
        return s;
    }
    if (std::strcmp(format, "u") == 0 || std::strcmp(format, "U") == 0) {  // utf8 / large_utf8
        s.type = pq::Type::ByteArray; s.extract = Extract::ByteArray;
        s.has_converted = true; s.converted = pq::ConvertedType::Utf8;
        s.large_offsets = (format[0] == 'U');
        return s;
    }
    if (std::strcmp(format, "z") == 0 || std::strcmp(format, "Z") == 0) {  // binary / large_binary
        s.type = pq::Type::ByteArray; s.extract = Extract::ByteArray;
        s.large_offsets = (format[0] == 'Z');
        return s;
    }
    err = "unsupported Arrow type format: " + std::string(format ? format : "(null)");
    return std::nullopt;
}

// ---- per-chunk metadata captured while streaming pages --------------------

struct ColumnChunkMeta {
    pq::Type type{};
    std::vector<pq::Encoding> encodings;
    std::string name;
    std::int64_t num_values = 0;
    std::int64_t total_uncompressed = 0;
    std::int64_t total_compressed = 0;
    std::int64_t data_page_offset = 0;
    std::int64_t dictionary_page_offset = 0;
    bool has_dictionary = false;
    std::int64_t file_offset = 0;
};

struct RowGroupMeta {
    std::vector<ColumnChunkMeta> columns;
    std::int64_t num_rows = 0;
    std::int64_t total_byte_size = 0;
};

// ---- PLAIN body builders --------------------------------------------------

void append_le(std::vector<std::uint8_t>& out, std::uint32_t v) {
    out.push_back(v & 0xFF); out.push_back((v >> 8) & 0xFF);
    out.push_back((v >> 16) & 0xFF); out.push_back((v >> 24) & 0xFF);
}

std::vector<std::uint8_t> build_plain_fixed(const ArrowArray& arr, const ColumnSpec& s) {
    const auto n = static_cast<std::size_t>(arr.length);
    const auto* src = static_cast<const std::uint8_t*>(arr.buffers[1]);
    std::vector<std::uint8_t> body;
    if (s.extract == Extract::MemcpyFixed) {
        const std::size_t bytes = n * static_cast<std::size_t>(s.src_width);
        body.assign(src, src + bytes);
    } else {  // WidenInt -> INT32
        body.reserve(n * 4);
        for (std::size_t i = 0; i < n; ++i) {
            std::int32_t v = 0;
            if (s.src_width == 1) {
                v = s.sign_extend ? static_cast<std::int32_t>(static_cast<std::int8_t>(src[i]))
                                  : static_cast<std::int32_t>(src[i]);
            } else {  // 2 bytes LE
                const std::uint16_t raw = static_cast<std::uint16_t>(src[2 * i]) |
                                          (static_cast<std::uint16_t>(src[2 * i + 1]) << 8);
                v = s.sign_extend ? static_cast<std::int32_t>(static_cast<std::int16_t>(raw))
                                  : static_cast<std::int32_t>(raw);
            }
            append_le(body, static_cast<std::uint32_t>(v));
        }
    }
    return body;
}

std::vector<std::uint8_t> build_plain_bool(const ArrowArray& arr) {
    // Arrow bool data is bit-packed LSB-first -- identical to Parquet PLAIN bool.
    const auto n = static_cast<std::size_t>(arr.length);
    const auto* src = static_cast<const std::uint8_t*>(arr.buffers[1]);
    const std::size_t bytes = (n + 7) / 8;
    return std::vector<std::uint8_t>(src, src + bytes);
}

// Build the dictionary (PLAIN BYTE_ARRAY) page body and the RLE_DICTIONARY data
// page body for a string/binary column.
struct DictPages {
    std::vector<std::uint8_t> dict_body;
    std::vector<std::uint8_t> data_body;
    std::size_t dict_size = 0;
};

DictPages build_dictionary_pages(const ArrowArray& arr, const ColumnSpec& s) {
    const auto n = static_cast<std::size_t>(arr.length);
    const auto* data = static_cast<const std::uint8_t*>(arr.buffers[2]);

    auto value_at = [&](std::size_t i) -> std::string_view {
        std::int64_t start = 0, end = 0;
        if (s.large_offsets) {
            const auto* off = static_cast<const std::int64_t*>(arr.buffers[1]);
            start = off[i]; end = off[i + 1];
        } else {
            const auto* off = static_cast<const std::int32_t*>(arr.buffers[1]);
            start = off[i]; end = off[i + 1];
        }
        return std::string_view(reinterpret_cast<const char*>(data + start),
                                static_cast<std::size_t>(end - start));
    };

    std::unordered_map<std::string_view, std::uint32_t> seen;
    std::vector<std::string_view> dict;
    std::vector<std::uint32_t> indices(n);
    for (std::size_t i = 0; i < n; ++i) {
        const std::string_view v = value_at(i);
        auto it = seen.find(v);
        if (it == seen.end()) {
            const auto idx = static_cast<std::uint32_t>(dict.size());
            seen.emplace(v, idx);
            dict.push_back(v);
            indices[i] = idx;
        } else {
            indices[i] = it->second;
        }
    }

    DictPages out;
    out.dict_size = dict.size();
    for (std::string_view v : dict) {
        append_le(out.dict_body, static_cast<std::uint32_t>(v.size()));
        out.dict_body.insert(out.dict_body.end(), v.begin(), v.end());
    }
    const int bit_width = dictionary_bit_width(dict.size());
    out.data_body.push_back(static_cast<std::uint8_t>(bit_width));
    auto encoded = encode_rle_dictionary_indices(indices, bit_width);
    out.data_body.insert(out.data_body.end(), encoded.begin(), encoded.end());
    return out;
}

// ---- page header serialization -------------------------------------------

std::vector<std::uint8_t> data_page_header(std::int32_t num_values,
                                           pq::Encoding encoding,
                                           std::int32_t uncompressed,
                                           std::int32_t compressed) {
    std::vector<std::uint8_t> buf;
    CompactWriter w(buf);
    w.begin_struct_element();
    w.field_i32(1, static_cast<std::int32_t>(pq::PageType::DataPage));
    w.field_i32(2, uncompressed);
    w.field_i32(3, compressed);
    w.begin_struct_field(5);  // DataPageHeader
    w.field_i32(1, num_values);
    w.field_i32(2, static_cast<std::int32_t>(encoding));
    w.field_i32(3, static_cast<std::int32_t>(pq::Encoding::Rle));  // def levels
    w.field_i32(4, static_cast<std::int32_t>(pq::Encoding::Rle));  // rep levels
    w.end_struct();
    w.end_struct();
    return buf;
}

std::vector<std::uint8_t> dictionary_page_header(std::int32_t num_values,
                                                 std::int32_t uncompressed,
                                                 std::int32_t compressed) {
    std::vector<std::uint8_t> buf;
    CompactWriter w(buf);
    w.begin_struct_element();
    w.field_i32(1, static_cast<std::int32_t>(pq::PageType::DictionaryPage));
    w.field_i32(2, uncompressed);
    w.field_i32(3, compressed);
    w.begin_struct_field(7);  // DictionaryPageHeader
    w.field_i32(1, num_values);
    w.field_i32(2, static_cast<std::int32_t>(pq::Encoding::Plain));
    w.field_bool(3, false);  // is_sorted
    w.end_struct();
    w.end_struct();
    return buf;
}

}  // namespace
}  // namespace n2p

// ---- writer object --------------------------------------------------------

struct N2PWriter {
    std::ofstream out;
    std::string path;
    std::int64_t offset = 0;
    n2p::pq::Codec codec = n2p::pq::Codec::Zstd;
    bool schema_locked = false;
    std::vector<n2p::ColumnSpec> columns;
    std::vector<n2p::RowGroupMeta> row_groups;
    std::int64_t total_rows = 0;
    bool footer_written = false;
    std::string last_error;
};

namespace n2p {
namespace {

constexpr char kMagic[4] = {'P', 'A', 'R', '1'};

void write_bytes(N2PWriter& w, std::span<const std::uint8_t> bytes) {
    w.out.write(reinterpret_cast<const char*>(bytes.data()),
                static_cast<std::streamsize>(bytes.size()));
    w.offset += static_cast<std::int64_t>(bytes.size());
}

// Emit one page: header bytes followed by the compressed body. Returns
// {bytes_on_disk, uncompressed_total} where each total includes the page header,
// matching ColumnMetaData's total_*_size semantics.
struct PageBytes { std::int64_t on_disk; std::int64_t uncompressed; };

PageBytes emit_page(N2PWriter& w, const std::vector<std::uint8_t>& header,
                    const std::vector<std::uint8_t>& compressed_body,
                    std::size_t uncompressed_body) {
    write_bytes(w, header);
    write_bytes(w, compressed_body);
    return {static_cast<std::int64_t>(header.size() + compressed_body.size()),
            static_cast<std::int64_t>(header.size() + uncompressed_body)};
}

// Validate that a child array is a plain, non-null, offset-0 column we can map.
bool validate_child(const ArrowArray& child, const ColumnSpec& s,
                    std::int64_t expected_rows, std::string& err) {
    if (child.length != expected_rows) {
        err = "column '" + s.name + "' length does not match the record batch";
        return false;
    }
    if (child.offset != 0) {
        err = "column '" + s.name + "' has a non-zero array offset (slices unsupported)";
        return false;
    }
    if (child.null_count > 0) {
        err = "column '" + s.name + "' contains nulls (only REQUIRED columns are supported)";
        return false;
    }
    const std::int64_t need = (s.extract == Extract::ByteArray) ? 3 : 2;
    if (child.n_buffers < need || child.buffers == nullptr) {
        err = "column '" + s.name + "' is missing required buffers";
        return false;
    }
    return true;
}

void serialize_schema_element(CompactWriter& w, const ColumnSpec& s) {
    w.begin_struct_element();
    w.field_i32(1, static_cast<std::int32_t>(s.type));
    if (s.type == pq::Type::FixedLenByteArray) {
        w.field_i32(2, s.type_length);
    }
    w.field_i32(3, static_cast<std::int32_t>(pq::Repetition::Required));
    w.field_string(4, s.name);
    if (s.has_converted) {
        w.field_i32(6, static_cast<std::int32_t>(s.converted));
    }
    w.end_struct();
}

void serialize_column_chunk(CompactWriter& w, const ColumnChunkMeta& c, pq::Codec codec) {
    w.begin_struct_element();
    w.field_i64(2, c.file_offset);
    w.begin_struct_field(3);  // ColumnMetaData
    w.field_i32(1, static_cast<std::int32_t>(c.type));
    w.field_list_header(2, CType::I32, c.encodings.size());
    for (auto e : c.encodings) {
        w.put_zigzag_i32(static_cast<std::int32_t>(e));
    }
    w.field_list_header(3, CType::Binary, 1);
    w.put_string(c.name);
    w.field_i32(4, static_cast<std::int32_t>(codec));
    w.field_i64(5, c.num_values);
    w.field_i64(6, c.total_uncompressed);
    w.field_i64(7, c.total_compressed);
    w.field_i64(9, c.data_page_offset);
    if (c.has_dictionary) {
        w.field_i64(11, c.dictionary_page_offset);
    }
    w.end_struct();
    w.end_struct();
}

std::vector<std::uint8_t> serialize_footer(const N2PWriter& w) {
    std::vector<std::uint8_t> buf;
    CompactWriter cw(buf);
    cw.begin_struct_element();  // FileMetaData
    cw.field_i32(1, 1);          // version
    cw.field_list_header(2, CType::Struct, w.columns.size() + 1);
    {
        // root schema element: name + num_children only (no type/repetition).
        cw.begin_struct_element();
        cw.field_string(4, "schema");
        cw.field_i32(5, static_cast<std::int32_t>(w.columns.size()));
        cw.end_struct();
    }
    for (const auto& s : w.columns) {
        serialize_schema_element(cw, s);
    }
    cw.field_i64(3, w.total_rows);
    cw.field_list_header(4, CType::Struct, w.row_groups.size());
    for (const auto& rg : w.row_groups) {
        cw.begin_struct_element();
        cw.field_list_header(1, CType::Struct, rg.columns.size());
        for (const auto& c : rg.columns) {
            serialize_column_chunk(cw, c, w.codec);
        }
        cw.field_i64(2, rg.total_byte_size);
        cw.field_i64(3, rg.num_rows);
        cw.end_struct();
    }
    cw.field_string(6, "nanoarrow2parquet");
    cw.end_struct();
    return buf;
}

int write_one_batch(N2PWriter& w, const ArrowSchema* schema, const ArrowArray* batch) {
    if (schema == nullptr || batch == nullptr) {
        w.last_error = "schema and batch must be non-null";
        return N2P_INVALID_ARGUMENT;
    }
    if (schema->format == nullptr || std::strcmp(schema->format, "+s") != 0) {
        w.last_error = "top-level schema must be a struct (record batch)";
        return N2P_INVALID_ARGUMENT;
    }
    const auto ncols = static_cast<std::size_t>(schema->n_children);
    if (static_cast<std::size_t>(batch->n_children) != ncols) {
        w.last_error = "batch child count does not match schema";
        return N2P_INVALID_ARGUMENT;
    }

    // Derive column specs from this schema.
    std::vector<ColumnSpec> specs;
    specs.reserve(ncols);
    for (std::size_t i = 0; i < ncols; ++i) {
        const ArrowSchema* child = schema->children[i];
        std::string err;
        const char* name = child->name ? child->name : "";
        auto spec = map_format(child->format, name, err);
        if (!spec) {
            w.last_error = err;
            return N2P_UNSUPPORTED_TYPE;
        }
        specs.push_back(std::move(*spec));
    }

    if (!w.schema_locked) {
        w.columns = specs;
        w.schema_locked = true;
    } else if (specs.size() != w.columns.size()) {
        w.last_error = "batch schema is incompatible with the first batch";
        return N2P_INVALID_ARGUMENT;
    }

    RowGroupMeta rg;
    rg.num_rows = batch->length;

    try {
        for (std::size_t i = 0; i < ncols; ++i) {
            const ColumnSpec& s = w.columns[i];
            const ArrowArray* child = batch->children[i];
            std::string err;
            if (!validate_child(*child, s, batch->length, err)) {
                w.last_error = err;
                return N2P_INVALID_ARGUMENT;
            }

            ColumnChunkMeta c;
            c.type = s.type;
            c.name = s.name;
            c.num_values = batch->length;
            c.file_offset = w.offset;

            if (s.extract == Extract::ByteArray) {
                DictPages pages = build_dictionary_pages(*child, s);
                // dictionary page
                auto dict_comp = compress_page(pages.dict_body, w.codec);
                auto dict_hdr = dictionary_page_header(
                    static_cast<std::int32_t>(pages.dict_size),
                    static_cast<std::int32_t>(pages.dict_body.size()),
                    static_cast<std::int32_t>(dict_comp.size()));
                c.dictionary_page_offset = w.offset;
                c.has_dictionary = true;
                PageBytes dp = emit_page(w, dict_hdr, dict_comp, pages.dict_body.size());
                // data page (RLE_DICTIONARY)
                auto data_comp = compress_page(pages.data_body, w.codec);
                auto data_hdr = data_page_header(
                    static_cast<std::int32_t>(batch->length), pq::Encoding::RleDictionary,
                    static_cast<std::int32_t>(pages.data_body.size()),
                    static_cast<std::int32_t>(data_comp.size()));
                c.data_page_offset = w.offset;
                PageBytes vp = emit_page(w, data_hdr, data_comp, pages.data_body.size());
                c.total_uncompressed = dp.uncompressed + vp.uncompressed;
                c.total_compressed = dp.on_disk + vp.on_disk;
                c.encodings = {pq::Encoding::Plain, pq::Encoding::RleDictionary};
            } else {
                std::vector<std::uint8_t> body =
                    (s.extract == Extract::Bool) ? build_plain_bool(*child)
                                                 : build_plain_fixed(*child, s);
                auto comp = compress_page(body, w.codec);
                auto hdr = data_page_header(
                    static_cast<std::int32_t>(batch->length), pq::Encoding::Plain,
                    static_cast<std::int32_t>(body.size()),
                    static_cast<std::int32_t>(comp.size()));
                c.data_page_offset = w.offset;
                PageBytes vp = emit_page(w, hdr, comp, body.size());
                c.total_uncompressed = vp.uncompressed;
                c.total_compressed = vp.on_disk;
                c.encodings = {pq::Encoding::Plain};
            }

            rg.total_byte_size += c.total_uncompressed;
            rg.columns.push_back(std::move(c));
        }
    } catch (const std::exception& e) {
        w.last_error = std::string("page compression failed: ") + e.what();
        return N2P_IO_ERROR;
    }

    w.row_groups.push_back(std::move(rg));
    w.total_rows += batch->length;
    if (!w.out.good()) {
        w.last_error = "I/O error while writing column data";
        return N2P_IO_ERROR;
    }
    return N2P_OK;
}

}  // namespace
}  // namespace n2p

// ---- public C ABI ---------------------------------------------------------

extern "C" {

int n2p_writer_open(N2PWriter** out, const char* path) {
    if (out == nullptr || path == nullptr) {
        return N2P_INVALID_ARGUMENT;
    }
    auto* w = new N2PWriter();
    w->path = path;
    w->out.open(path, std::ios::binary | std::ios::trunc);
    if (!w->out.is_open()) {
        w->last_error = std::string("cannot open output file: ") + path;
        *out = w;  // hand back so the caller can read the error, then close
        return N2P_IO_ERROR;
    }
    n2p::write_bytes(*w, std::span<const std::uint8_t>(
                             reinterpret_cast<const std::uint8_t*>(n2p::kMagic), 4));
    *out = w;
    return N2P_OK;
}

int n2p_writer_set_codec(N2PWriter* w, N2PCodec codec) {
    if (w == nullptr) {
        return N2P_INVALID_ARGUMENT;
    }
    switch (codec) {
        case N2P_CODEC_ZSTD: w->codec = n2p::pq::Codec::Zstd; return N2P_OK;
        case N2P_CODEC_UNCOMPRESSED: w->codec = n2p::pq::Codec::Uncompressed; return N2P_OK;
    }
    w->last_error = "unknown codec";
    return N2P_INVALID_ARGUMENT;
}

int n2p_writer_write_batch(N2PWriter* w, const struct ArrowSchema* schema,
                           const struct ArrowArray* batch) {
    if (w == nullptr) {
        return N2P_INVALID_ARGUMENT;
    }
    if (!w->out.is_open()) {
        w->last_error = "writer is not open";
        return N2P_IO_ERROR;
    }
    return n2p::write_one_batch(*w, schema, batch);
}

int n2p_writer_close(N2PWriter* w) {
    if (w == nullptr) {
        return N2P_INVALID_ARGUMENT;
    }
    int status = N2P_OK;
    if (w->out.is_open() && !w->footer_written) {
        std::vector<std::uint8_t> footer = n2p::serialize_footer(*w);
        n2p::write_bytes(*w, footer);
        std::uint32_t len = static_cast<std::uint32_t>(footer.size());
        std::uint8_t le[4] = {static_cast<std::uint8_t>(len & 0xFF),
                              static_cast<std::uint8_t>((len >> 8) & 0xFF),
                              static_cast<std::uint8_t>((len >> 16) & 0xFF),
                              static_cast<std::uint8_t>((len >> 24) & 0xFF)};
        n2p::write_bytes(*w, std::span<const std::uint8_t>(le, 4));
        n2p::write_bytes(*w, std::span<const std::uint8_t>(
                                 reinterpret_cast<const std::uint8_t*>(n2p::kMagic), 4));
        w->footer_written = true;
        w->out.flush();
        if (!w->out.good()) {
            status = N2P_IO_ERROR;
        }
        w->out.close();
    }
    delete w;
    return status;
}

const char* n2p_writer_last_error(const N2PWriter* w) {
    return (w != nullptr) ? w->last_error.c_str() : "";
}

int n2p_write_file(const char* path, const struct ArrowSchema* schema,
                   const struct ArrowArray* batch, char* err, size_t err_len) {
    auto fail = [&](N2PWriter* w, int status) {
        if (err != nullptr && err_len > 0 && w != nullptr) {
            std::snprintf(err, err_len, "%s", w->last_error.c_str());
        }
        return status;
    };

    N2PWriter* w = nullptr;
    int status = n2p_writer_open(&w, path);
    if (status != N2P_OK) {
        int s = fail(w, status);
        n2p_writer_close(w);
        return s;
    }
    status = n2p_writer_write_batch(w, schema, batch);
    if (status != N2P_OK) {
        int s = fail(w, status);
        n2p_writer_close(w);
        return s;
    }
    status = n2p_writer_close(w);
    // Note: w is freed by close(); the error (if any) was already consumed above.
    return status;
}

}  // extern "C"
