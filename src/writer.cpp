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

enum class Extract { MemcpyFixed, WidenInt, Bool, ByteArray, Null };

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
    bool nullable = false;  // OPTIONAL column -> definition levels in each page
};

// Parse an Arrow C format string into a ColumnSpec. `nullable` comes from the
// Arrow schema's ARROW_FLAG_NULLABLE. Returns std::nullopt for unsupported types
// (the caller emits N2P_UNSUPPORTED_TYPE).
std::optional<ColumnSpec> map_format(const char* format, std::string name,
                                     bool nullable, std::string& err) {
    ColumnSpec s;
    s.name = std::move(name);
    s.nullable = nullable;
    if (std::strcmp(format, "n") == 0) {  // null type: every value is null
        s.type = pq::Type::Int32;
        s.extract = Extract::Null;
        s.nullable = true;  // an all-null column is inherently OPTIONAL
        return s;
    }
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

// ---- nested schema flattening --------------------------------------------
//
// A record batch is a tree of struct groups and leaf columns. Parquet stores it
// as a pre-order list of SchemaElements (groups declare num_children) and one
// column chunk per leaf, where each leaf carries its full dotted path and a
// max definition level equal to the number of OPTIONAL nodes on its path.

struct SchemaNode {
    bool is_group = false;
    std::string name;
    bool optional = false;
    int num_children = 0;          // groups only
    pq::Type type{};               // leaves only
    bool has_converted = false;
    pq::ConvertedType converted{};
    int type_length = 0;
};

struct LeafSpec {
    ColumnSpec col;
    std::vector<int> path_idx;          // child indices from the root to the leaf
    std::vector<std::string> path_names;
    std::vector<bool> path_optional;    // OPTIONAL flag per node on the path
    int max_def_level = 0;              // = count(path_optional == true)
    int def_bit_width = 0;
};

// Recursively flatten a schema child (struct group or leaf). `idx/names/opt`
// already include this node. Appends to `nodes` (footer, pre-order) and, for
// leaves, to `leaves` (write order).
bool flatten_schema(const ArrowSchema* node, std::vector<int> idx,
                    std::vector<std::string> names, std::vector<bool> opt,
                    std::vector<SchemaNode>& nodes, std::vector<LeafSpec>& leaves,
                    std::string& err) {
    const bool nullable = (node->flags & ARROW_FLAG_NULLABLE) != 0;
    if (node->format != nullptr && std::strcmp(node->format, "+s") == 0) {
        SchemaNode g;
        g.is_group = true;
        g.name = node->name ? node->name : "";
        g.optional = nullable;
        g.num_children = static_cast<int>(node->n_children);
        nodes.push_back(g);
        for (std::int64_t i = 0; i < node->n_children; ++i) {
            const ArrowSchema* child = node->children[i];
            const bool child_null = (child->flags & ARROW_FLAG_NULLABLE) != 0 ||
                                    (child->format && std::strcmp(child->format, "n") == 0);
            auto idx2 = idx; idx2.push_back(static_cast<int>(i));
            auto names2 = names; names2.push_back(child->name ? child->name : "");
            auto opt2 = opt; opt2.push_back(child_null);
            if (!flatten_schema(child, idx2, names2, opt2, nodes, leaves, err)) return false;
        }
        return true;
    }
    // leaf
    auto spec = map_format(node->format, node->name ? node->name : "", nullable, err);
    if (!spec) return false;
    SchemaNode ln;
    ln.name = spec->name; ln.optional = spec->nullable; ln.type = spec->type;
    ln.has_converted = spec->has_converted; ln.converted = spec->converted;
    ln.type_length = spec->type_length;
    nodes.push_back(ln);
    LeafSpec leaf;
    leaf.col = std::move(*spec);
    leaf.path_idx = std::move(idx);
    leaf.path_names = std::move(names);
    leaf.path_optional = opt;
    int D = 0;
    for (bool o : opt) if (o) ++D;
    leaf.max_def_level = D;
    leaf.def_bit_width = dictionary_bit_width(static_cast<std::size_t>(D) + 1);
    leaves.push_back(std::move(leaf));
    return true;
}

// ---- per-chunk metadata captured while streaming pages --------------------

struct ColumnChunkMeta {
    pq::Type type{};
    std::vector<pq::Encoding> encodings;
    std::vector<std::string> path;  // path_in_schema (dotted column path)
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

// Arrow validity bitmap is LSB-first, 1 == valid. A null bitmap pointer means the
// column has no nulls (every value valid).
inline bool valid_bit(const std::uint8_t* validity, std::size_t i) {
    return validity == nullptr || ((validity[i >> 3] >> (i & 7)) & 1);
}

// PLAIN body. For an OPTIONAL column with nulls (`validity` non-null) only the
// present values are emitted, as Parquet requires; pass validity == nullptr for
// the REQUIRED / no-null fast path (a plain memcpy for fixed-width).
std::vector<std::uint8_t> build_plain_fixed(const ArrowArray& arr, const ColumnSpec& s,
                                            const std::uint8_t* validity) {
    const auto n = static_cast<std::size_t>(arr.length);
    const auto* src = static_cast<const std::uint8_t*>(arr.buffers[1]);
    std::vector<std::uint8_t> body;
    auto append_widened = [&](std::size_t i) {
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
    };
    if (s.extract == Extract::MemcpyFixed) {
        const std::size_t w = static_cast<std::size_t>(s.src_width);
        if (validity == nullptr) {
            body.assign(src, src + n * w);  // fast path: byte-identical to Arrow
        } else {
            body.reserve(n * w);
            for (std::size_t i = 0; i < n; ++i)
                if (valid_bit(validity, i)) body.insert(body.end(), src + i * w, src + (i + 1) * w);
        }
    } else {  // WidenInt -> INT32
        body.reserve(n * 4);
        for (std::size_t i = 0; i < n; ++i)
            if (valid_bit(validity, i)) append_widened(i);
    }
    return body;
}

std::vector<std::uint8_t> build_plain_bool(const ArrowArray& arr, const std::uint8_t* validity) {
    // Arrow bool data is bit-packed LSB-first -- identical to Parquet PLAIN bool.
    const auto n = static_cast<std::size_t>(arr.length);
    const auto* src = static_cast<const std::uint8_t*>(arr.buffers[1]);
    if (validity == nullptr) {
        const std::size_t bytes = (n + 7) / 8;
        return std::vector<std::uint8_t>(src, src + bytes);
    }
    // Re-pack only the present bits, tightly, into a fresh LSB-first bitmap.
    std::vector<std::uint8_t> body;
    std::size_t out_bit = 0;
    for (std::size_t i = 0; i < n; ++i) {
        if (!valid_bit(validity, i)) continue;
        if ((out_bit & 7) == 0) body.push_back(0);
        if ((src[i >> 3] >> (i & 7)) & 1) body.back() |= static_cast<std::uint8_t>(1u << (out_bit & 7));
        ++out_bit;
    }
    return body;
}

// Build the page body for a string/binary column, choosing between
// RLE_DICTIONARY and PLAIN BYTE_ARRAY encoding. Dictionary encoding wins when
// values repeat; it is pathological for high-cardinality columns (unique ids,
// free text), where the dictionary holds every value once *plus* an index per
// row. We build the dictionary, then compare its encoded size against a PLAIN
// (4-byte length + bytes per present value) layout and keep the smaller. When
// PLAIN wins `use_dictionary` is false and only `data_body` is populated.
struct ByteArrayPages {
    bool use_dictionary = true;
    std::vector<std::uint8_t> dict_body;
    std::vector<std::uint8_t> data_body;
    std::size_t dict_size = 0;
};

ByteArrayPages build_byte_array_pages(const ArrowArray& arr, const ColumnSpec& s,
                                      const std::uint8_t* validity) {
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

    // Present rows only: nulls are carried by the def levels, so `present` /
    // `indices` may be shorter than `n`.
    std::unordered_map<std::string_view, std::uint32_t> seen;
    std::vector<std::string_view> dict;
    std::vector<std::string_view> present;
    std::vector<std::uint32_t> indices;
    indices.reserve(n);
    present.reserve(n);
    std::size_t value_bytes = 0;  // total bytes of present values (PLAIN payload)
    for (std::size_t i = 0; i < n; ++i) {
        if (!valid_bit(validity, i)) continue;
        const std::string_view v = value_at(i);
        present.push_back(v);
        value_bytes += v.size();
        auto it = seen.find(v);
        if (it == seen.end()) {
            const auto idx = static_cast<std::uint32_t>(dict.size());
            seen.emplace(v, idx);
            dict.push_back(v);
            indices.push_back(idx);
        } else {
            indices.push_back(it->second);
        }
    }

    // Encoded size of the dictionary layout: dictionary page (4 + len per
    // distinct value) plus the RLE-encoded index stream (bit-width byte + body).
    std::size_t dict_body_bytes = 4 * dict.size();
    for (std::string_view v : dict) dict_body_bytes += v.size();
    const int bit_width = dictionary_bit_width(dict.size());
    auto idx_encoded = encode_rle_dictionary_indices(indices, bit_width);
    const std::size_t dict_total = dict_body_bytes + 1 + idx_encoded.size();
    // Encoded size of PLAIN: 4-byte length prefix + bytes for each present value.
    const std::size_t plain_total = 4 * present.size() + value_bytes;

    ByteArrayPages out;
    if (plain_total < dict_total) {
        out.use_dictionary = false;
        out.data_body.reserve(plain_total);
        for (std::string_view v : present) {
            append_le(out.data_body, static_cast<std::uint32_t>(v.size()));
            out.data_body.insert(out.data_body.end(), v.begin(), v.end());
        }
        return out;
    }

    out.use_dictionary = true;
    out.dict_size = dict.size();
    out.dict_body.reserve(dict_body_bytes);
    for (std::string_view v : dict) {
        append_le(out.dict_body, static_cast<std::uint32_t>(v.size()));
        out.dict_body.insert(out.dict_body.end(), v.begin(), v.end());
    }
    out.data_body.push_back(static_cast<std::uint8_t>(bit_width));
    out.data_body.insert(out.data_body.end(), idx_encoded.begin(), idx_encoded.end());
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
    std::vector<n2p::SchemaNode> schema_nodes;  // pre-order, for the footer schema
    std::vector<n2p::LeafSpec> leaves;          // one column chunk per leaf
    int top_children = 0;                        // direct children of the root struct
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
    if (child.null_count > 0 && !s.nullable) {
        err = "column '" + s.name + "' contains nulls but its schema is not nullable";
        return false;
    }
    if (s.extract == Extract::Null) return true;  // null type carries no buffers
    const std::int64_t need = (s.extract == Extract::ByteArray) ? 3 : 2;
    if (child.n_buffers < need || child.buffers == nullptr) {
        err = "column '" + s.name + "' is missing required buffers";
        return false;
    }
    return true;
}

void serialize_schema_element(CompactWriter& w, const SchemaNode& s) {
    w.begin_struct_element();
    const auto rep = static_cast<std::int32_t>(s.optional ? pq::Repetition::Optional
                                                          : pq::Repetition::Required);
    if (s.is_group) {
        // A group (struct) has no physical type; it declares num_children.
        w.field_i32(3, rep);
        w.field_string(4, s.name);
        w.field_i32(5, s.num_children);
    } else {
        w.field_i32(1, static_cast<std::int32_t>(s.type));
        if (s.type == pq::Type::FixedLenByteArray) {
            w.field_i32(2, s.type_length);
        }
        w.field_i32(3, rep);
        w.field_string(4, s.name);
        if (s.has_converted) {
            w.field_i32(6, static_cast<std::int32_t>(s.converted));
        }
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
    w.field_list_header(3, CType::Binary, c.path.size());
    for (const auto& p : c.path) {
        w.put_string(p);
    }
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
    cw.field_list_header(2, CType::Struct, w.schema_nodes.size() + 1);
    {
        // root schema element: name + num_children only (no type/repetition).
        cw.begin_struct_element();
        cw.field_string(4, "schema");
        cw.field_i32(5, w.top_children);
        cw.end_struct();
    }
    for (const auto& s : w.schema_nodes) {
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
    if (static_cast<int>(schema->n_children) == 0) {
        w.last_error = "record batch has no columns";
        return N2P_INVALID_ARGUMENT;
    }
    if (batch->n_children != schema->n_children) {
        w.last_error = "batch child count does not match schema";
        return N2P_INVALID_ARGUMENT;
    }

    // Flatten the (possibly nested) schema into leaf columns on the first batch.
    if (!w.schema_locked) {
        std::vector<SchemaNode> nodes;
        std::vector<LeafSpec> leaves;
        for (std::int64_t i = 0; i < schema->n_children; ++i) {
            const ArrowSchema* child = schema->children[i];
            const bool child_null = (child->flags & ARROW_FLAG_NULLABLE) != 0 ||
                                    (child->format && std::strcmp(child->format, "n") == 0);
            std::string err;
            if (!flatten_schema(child, {static_cast<int>(i)},
                                {child->name ? child->name : ""}, {child_null},
                                nodes, leaves, err)) {
                w.last_error = err;
                return N2P_UNSUPPORTED_TYPE;
            }
        }
        if (leaves.empty()) {
            w.last_error = "record batch has no leaf columns";
            return N2P_INVALID_ARGUMENT;
        }
        w.schema_nodes = std::move(nodes);
        w.leaves = std::move(leaves);
        w.top_children = static_cast<int>(schema->n_children);
        w.schema_locked = true;
    } else if (static_cast<int>(schema->n_children) != w.top_children) {
        w.last_error = "batch schema is incompatible with the first batch";
        return N2P_INVALID_ARGUMENT;
    }

    RowGroupMeta rg;
    rg.num_rows = batch->length;
    const auto n = static_cast<std::size_t>(batch->length);

    try {
        for (const LeafSpec& leaf : w.leaves) {
            // Walk the array tree to the leaf, capturing each node's array (used
            // for definition levels at every OPTIONAL node on the path).
            std::vector<const ArrowArray*> nodes_arr;
            nodes_arr.reserve(leaf.path_idx.size());
            const ArrowArray* cur = batch;
            for (const int ci : leaf.path_idx) {
                if (cur->children == nullptr || ci >= cur->n_children) {
                    w.last_error = "batch structure does not match the schema";
                    return N2P_INVALID_ARGUMENT;
                }
                cur = cur->children[ci];
                nodes_arr.push_back(cur);
            }
            const ColumnSpec& s = leaf.col;
            const ArrowArray* child = nodes_arr.back();
            std::string err;
            if (!validate_child(*child, s, batch->length, err)) {
                w.last_error = err;
                return N2P_INVALID_ARGUMENT;
            }

            ColumnChunkMeta c;
            c.type = s.type;
            c.path = leaf.path_names;
            c.num_values = batch->length;
            c.file_offset = w.offset;

            // Definition levels (max level = number of OPTIONAL nodes on the path).
            // For each row, walk the path and count present OPTIONAL nodes until the
            // first null; a value is stored only when all of them are present.
            const int D = leaf.max_def_level;
            std::vector<std::uint8_t> def_prefix;
            std::vector<std::uint8_t> present_map;
            const std::uint8_t* value_validity = nullptr;
            if (D > 0) {
                std::vector<std::uint32_t> def(n);
                present_map.assign((n + 7) / 8, 0);
                std::size_t present_count = 0;
                const bool null_leaf = (s.extract == Extract::Null);
                for (std::size_t i = 0; i < n; ++i) {
                    int d = 0;
                    bool present_all = true;
                    for (std::size_t k = 0; k < nodes_arr.size(); ++k) {
                        if (!leaf.path_optional[k]) continue;  // REQUIRED: always present
                        const ArrowArray* a = nodes_arr[k];
                        const bool is_leaf = (k + 1 == nodes_arr.size());
                        const auto* val = (a->n_buffers > 0)
                            ? static_cast<const std::uint8_t*>(a->buffers[0]) : nullptr;
                        const bool present = (is_leaf && null_leaf) ? false : valid_bit(val, i);
                        if (!present) { present_all = false; break; }
                        ++d;
                    }
                    def[i] = static_cast<std::uint32_t>(d);
                    if (present_all && d == D) {
                        present_map[i >> 3] |= static_cast<std::uint8_t>(1u << (i & 7));
                        ++present_count;
                    }
                }
                def_prefix = encode_definition_levels(def, leaf.def_bit_width);
                if (present_count < n) value_validity = present_map.data();
            }

            if (s.extract == Extract::ByteArray) {
                ByteArrayPages pages = build_byte_array_pages(*child, s, value_validity);
                if (pages.use_dictionary) {
                    // dictionary page
                    auto dict_comp = compress_page(pages.dict_body, w.codec);
                    auto dict_hdr = dictionary_page_header(
                        static_cast<std::int32_t>(pages.dict_size),
                        static_cast<std::int32_t>(pages.dict_body.size()),
                        static_cast<std::int32_t>(dict_comp.size()));
                    c.dictionary_page_offset = w.offset;
                    c.has_dictionary = true;
                    PageBytes dp = emit_page(w, dict_hdr, dict_comp, pages.dict_body.size());
                    // data page (RLE_DICTIONARY), def levels first.
                    std::vector<std::uint8_t> data_body = def_prefix;
                    data_body.insert(data_body.end(), pages.data_body.begin(), pages.data_body.end());
                    auto data_comp = compress_page(data_body, w.codec);
                    auto data_hdr = data_page_header(
                        static_cast<std::int32_t>(batch->length), pq::Encoding::RleDictionary,
                        static_cast<std::int32_t>(data_body.size()),
                        static_cast<std::int32_t>(data_comp.size()));
                    c.data_page_offset = w.offset;
                    PageBytes vp = emit_page(w, data_hdr, data_comp, data_body.size());
                    c.total_uncompressed = dp.uncompressed + vp.uncompressed;
                    c.total_compressed = dp.on_disk + vp.on_disk;
                    c.encodings = {pq::Encoding::Plain, pq::Encoding::RleDictionary};
                } else {
                    // PLAIN BYTE_ARRAY data page, def levels first.
                    std::vector<std::uint8_t> data_body = def_prefix;
                    data_body.insert(data_body.end(), pages.data_body.begin(), pages.data_body.end());
                    auto data_comp = compress_page(data_body, w.codec);
                    auto data_hdr = data_page_header(
                        static_cast<std::int32_t>(batch->length), pq::Encoding::Plain,
                        static_cast<std::int32_t>(data_body.size()),
                        static_cast<std::int32_t>(data_comp.size()));
                    c.data_page_offset = w.offset;
                    PageBytes vp = emit_page(w, data_hdr, data_comp, data_body.size());
                    c.total_uncompressed = vp.uncompressed;
                    c.total_compressed = vp.on_disk;
                    c.encodings = {pq::Encoding::Plain};
                }
            } else {
                std::vector<std::uint8_t> body = def_prefix;
                if (s.extract == Extract::Bool) {
                    auto v = build_plain_bool(*child, value_validity);
                    body.insert(body.end(), v.begin(), v.end());
                } else if (s.extract != Extract::Null) {  // Null type: no values
                    auto v = build_plain_fixed(*child, s, value_validity);
                    body.insert(body.end(), v.begin(), v.end());
                }
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
