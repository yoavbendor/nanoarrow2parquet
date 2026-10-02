// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Yoav Bendor
//
// nanoarrow2parquet -- single-header amalgamation.
//
// GENERATED FILE -- do not edit by hand. Regenerate with:
//     python3 scripts/amalgamate.py
// The editable sources live under include/ and src/.
//
// Usage (STB style): include this header anywhere for the declarations;
// in exactly ONE translation unit define the implementation macro first:
//
//     #define NANOARROW2PARQUET_IMPLEMENTATION
//     #include "nanoarrow2parquet.h"
//
// The implementation still depends on nanoarrow and zstd at compile/link
// time; provide their headers on the include path and link libzstd.

// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Yoav Bendor

#pragma once

// nanoarrow2parquet: a minimal Arrow -> Parquet writer.
//
// Consumes an in-memory Arrow C Data Interface struct array (one record batch ==
// one row group) and writes a valid, self-describing .parquet file readable by
// pandas / pyarrow / DuckDB / polars.
//
// Scope (v1):
//   * fixed-width flat columns: int8/16/32/64, uint8/16/32/64, float, double,
//     bool, fixed_size_binary(N) -- PLAIN encoded.
//   * variable-width strings/binary (utf8/large_utf8/binary/large_binary) via
//     dictionary encoding (RLE_DICTIONARY).
//   * nullable (OPTIONAL) columns: a column whose Arrow schema sets
//     ARROW_FLAG_NULLABLE is written with definition levels; the Arrow null
//     type ("n") becomes an all-null column. REQUIRED columns (non-nullable
//     schema) keep the zero-overhead fast path and reject actual nulls.
//   * nested struct columns ("+s"), arbitrarily deep, nullable or required --
//     each leaf carries its dotted path and multi-level definition levels.
//   * every page body is compressed (ZSTD by default).
//   * one or more row groups (streaming writer).
//
// Out of scope (documented TODO): nested list/map columns (repetition levels),
// page statistics / indexes, bloom filters.

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

struct ArrowArray;
struct ArrowSchema;

typedef enum N2PStatus {
    N2P_OK = 0,
    N2P_INVALID_ARGUMENT = 1,
    N2P_UNSUPPORTED_TYPE = 2,
    N2P_IO_ERROR = 3
} N2PStatus;

// Compression codec used for every page body. ZSTD is the default and is read by
// all modern Parquet readers. UNCOMPRESSED is provided mainly for debugging.
typedef enum N2PCodec {
    N2P_CODEC_ZSTD = 0,
    N2P_CODEC_UNCOMPRESSED = 1
} N2PCodec;

// One-shot: schema + one record batch -> one .parquet file (single row group).
// `schema` must describe a struct (the record batch); `batch` is the matching
// struct array. On failure, a human-readable message is written to `err` (if
// `err_len > 0`). Neither `schema` nor `batch` is released by this call.
int n2p_write_file(const char* path,
                   const struct ArrowSchema* schema,
                   const struct ArrowArray* batch,
                   char* err, size_t err_len);

// Streaming form: multiple batches -> multiple row groups in one file. The footer
// is written exactly once, at close(). If the process dies before close(), the
// file has no footer and is unreadable -- this is an inherent Parquet limitation.
typedef struct N2PWriter N2PWriter;

int n2p_writer_open(N2PWriter** out, const char* path);

// Write one record batch as one row group. The first call fixes the schema; later
// calls must pass a compatible schema (same column types). `schema`/`batch` are
// borrowed, not released.
int n2p_writer_write_batch(N2PWriter* w,
                           const struct ArrowSchema* schema,
                           const struct ArrowArray* batch);

// Serialize the footer and close the file. The writer is freed regardless of the
// return code; `w` must not be used afterwards.
int n2p_writer_close(N2PWriter* w);

const char* n2p_writer_last_error(const N2PWriter* w);

// Select the codec for subsequent batches (default ZSTD). Returns N2P_OK or
// N2P_INVALID_ARGUMENT.
int n2p_writer_set_codec(N2PWriter* w, N2PCodec codec);

#ifdef __cplusplus
}
#endif

#ifdef NANOARROW2PARQUET_IMPLEMENTATION

#include <zstd.h>
#include <nanoarrow/nanoarrow.h>
#include <array>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <nanom/formats/parquet_thrift.hpp>
#include <nanom/tagged_encode.hpp>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

// ---- src/parquet_types.hpp --------

// The Parquet format model this writer emits is nanom's (nanom/formats/parquet_thrift.hpp): the
// same structs and enums parquet2nanoarrow reads, encoded with nanom/tagged_encode.hpp. There is
// no second copy of parquet.thrift here, so the reader and the writer cannot drift apart.


namespace n2p {
namespace pq = nanom_formats::parquet;
}  // namespace n2p

// ---- src/rle_bitpack.hpp --------

// Minimal RLE / bit-packing-hybrid encoder for Parquet dictionary indices.
//
// The hybrid stream is a sequence of runs, each prefixed by a varint header whose
// low bit selects the run kind:
//   * bit-packed run : header = (num_groups << 1) | 1, followed by num_groups
//                      groups of 8 values, each `bit_width` bits, packed LSB-first.
//   * RLE run        : header = (run_length << 1) | 0, followed by one value in
//                      ceil(bit_width/8) bytes.
//
// We emit the simplest spec-compliant form: a single bit-packed run covering all
// indices (the last group zero-padded). Literal RLE runs would improve the ratio
// but are not required for correctness, and page-level compression recovers most
// of the gap anyway.


namespace n2p {

// Smallest bit width that can represent indices [0, dict_size).
inline int dictionary_bit_width(std::size_t dict_size) {
    int w = 0;
    while ((std::size_t{1} << w) < dict_size) {
        ++w;
    }
    return w;  // 0 when dict_size <= 1
}

// LSB-first bit-pack `values` (each masked to `bit_width` bits) into `out`.
inline void bit_pack(std::span<const std::uint32_t> values, int bit_width,
                     std::vector<std::uint8_t>& out) {
    if (bit_width == 0) {
        return;  // every value is implicitly 0; no bytes emitted
    }
    const std::uint64_t mask =
        (bit_width >= 32) ? 0xFFFFFFFFu : ((std::uint32_t{1} << bit_width) - 1);
    std::uint64_t buffer = 0;
    int bits = 0;
    for (std::uint32_t v : values) {
        buffer |= (static_cast<std::uint64_t>(v) & mask) << bits;
        bits += bit_width;
        while (bits >= 8) {
            out.push_back(static_cast<std::uint8_t>(buffer & 0xFF));
            buffer >>= 8;
            bits -= 8;
        }
    }
    if (bits > 0) {
        out.push_back(static_cast<std::uint8_t>(buffer & 0xFF));
    }
}

// Encode `indices` as the body of an RLE_DICTIONARY data page: a single
// zero-padded bit-packed run. Does NOT include the leading bit-width byte (the
// writer prepends that). `bit_width` must come from dictionary_bit_width().
inline std::vector<std::uint8_t> encode_rle_dictionary_indices(
    std::span<const std::uint32_t> indices, int bit_width) {
    std::vector<std::uint8_t> out;
    const std::size_t num_groups = (indices.size() + 7) / 8;

    // Varint header: (num_groups << 1) | 1. num_groups is tiny in practice but
    // varint-encode it for safety with large pages.
    std::uint64_t header = (static_cast<std::uint64_t>(num_groups) << 1) | 1u;
    while (header >= 0x80) {
        out.push_back(static_cast<std::uint8_t>(header) | 0x80);
        header >>= 7;
    }
    out.push_back(static_cast<std::uint8_t>(header));

    // Pad up to num_groups * 8 values with zeros so the run is group-aligned.
    const std::size_t padded = num_groups * 8;
    std::vector<std::uint32_t> tmp(indices.begin(), indices.end());
    tmp.resize(padded, 0);
    bit_pack(tmp, bit_width, out);
    return out;
}

// Encode a definition-level sequence for a flat OPTIONAL column (max def level 1,
// so bit_width 1) as the leading bytes of a DataPage V1 body: a 4-byte
// little-endian length followed by the RLE/bit-pack-hybrid run. `levels[i]` is 1
// for a present value and 0 for a null. Reuses the same single bit-packed run as
// the dictionary indices -- 1 bit/level, which page compression then collapses
// (an all-present column's levels compress to almost nothing).
inline std::vector<std::uint8_t> encode_definition_levels(
    std::span<const std::uint32_t> levels, int bit_width) {
    const std::vector<std::uint8_t> run = encode_rle_dictionary_indices(levels, bit_width);
    if (run.size() > UINT32_MAX) throw std::length_error("definition levels larger than 4 GiB");
    std::vector<std::uint8_t> out;
    out.reserve(4 + run.size());
    const std::uint32_t len = static_cast<std::uint32_t>(run.size());
    out.push_back(len & 0xFF);
    out.push_back((len >> 8) & 0xFF);
    out.push_back((len >> 16) & 0xFF);
    out.push_back((len >> 24) & 0xFF);
    out.insert(out.end(), run.begin(), run.end());
    return out;
}

}  // namespace n2p

// ---- src/compress.hpp --------

// Thin per-page codec wrapper. Parquet compresses each page body independently:
// the codec produces a raw frame (for ZSTD, a plain zstd frame with no extra
// Parquet-level framing or CRC), and the PageHeader records the uncompressed and
// compressed sizes alongside ColumnMetaData.codec.




namespace n2p {

// Compress `src` with the given codec. Returns the compressed bytes. For
// UNCOMPRESSED, returns a copy of the input. Throws std::runtime_error on a codec
// failure (callers translate to N2P_IO_ERROR).
inline std::vector<std::uint8_t> compress_page(std::span<const std::uint8_t> src,
                                               pq::CompressionCodec codec,
                                               int level = 3) {
    if (codec == pq::CompressionCodec::UNCOMPRESSED) {
        return std::vector<std::uint8_t>(src.begin(), src.end());
    }
    // pq::CompressionCodec::ZSTD
    const std::size_t bound = ZSTD_compressBound(src.size());
    std::vector<std::uint8_t> dst(bound);
    const std::size_t n = ZSTD_compress(dst.data(), dst.size(),
                                        src.data(), src.size(), level);
    if (ZSTD_isError(n)) {
        throw std::runtime_error(std::string("zstd compression failed: ") +
                                 ZSTD_getErrorName(n));
    }
    dst.resize(n);
    return dst;
}

}  // namespace n2p

// ---- src/writer.cpp --------

// nanoarrow2parquet writer: maps an Arrow C Data Interface struct array to a
// Parquet file. The data path is deliberately thin -- for PLAIN, fixed-width
// columns the Parquet page body is byte-identical to the Arrow values buffer, so
// the column path is essentially a memcpy. The only real format work is the
// per-page headers and the trailing FileMetaData footer.





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
        s.type = pq::Type::INT32;
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
        s.type = pq::Type::BOOLEAN;
        s.extract = Extract::Bool;
        return s;
    }
    if (std::strcmp(format, "c") == 0) {  // int8
        s.type = pq::Type::INT32; s.extract = Extract::WidenInt; s.src_width = 1;
        s.sign_extend = true; s.has_converted = true; s.converted = pq::ConvertedType::INT_8;
        return s;
    }
    if (std::strcmp(format, "C") == 0) {  // uint8
        s.type = pq::Type::INT32; s.extract = Extract::WidenInt; s.src_width = 1;
        s.has_converted = true; s.converted = pq::ConvertedType::UINT_8;
        return s;
    }
    if (std::strcmp(format, "s") == 0) {  // int16
        s.type = pq::Type::INT32; s.extract = Extract::WidenInt; s.src_width = 2;
        s.sign_extend = true; s.has_converted = true; s.converted = pq::ConvertedType::INT_16;
        return s;
    }
    if (std::strcmp(format, "S") == 0) {  // uint16
        s.type = pq::Type::INT32; s.extract = Extract::WidenInt; s.src_width = 2;
        s.has_converted = true; s.converted = pq::ConvertedType::UINT_16;
        return s;
    }
    if (std::strcmp(format, "i") == 0) { fixed(pq::Type::INT32, 4); return s; }
    if (std::strcmp(format, "I") == 0) {  // uint32
        fixed(pq::Type::INT32, 4);
        s.has_converted = true; s.converted = pq::ConvertedType::UINT_32; return s;
    }
    if (std::strcmp(format, "l") == 0) { fixed(pq::Type::INT64, 8); return s; }
    if (std::strcmp(format, "L") == 0) {  // uint64
        fixed(pq::Type::INT64, 8);
        s.has_converted = true; s.converted = pq::ConvertedType::UINT_64; return s;
    }
    if (std::strcmp(format, "f") == 0) { fixed(pq::Type::FLOAT, 4); return s; }
    if (std::strcmp(format, "g") == 0) { fixed(pq::Type::DOUBLE, 8); return s; }
    if (std::strncmp(format, "w:", 2) == 0) {  // fixed_size_binary:N
        const int n = std::atoi(format + 2);
        if (n <= 0) { err = "invalid fixed_size_binary width: " + std::string(format); return std::nullopt; }
        s.type = pq::Type::FIXED_LEN_BYTE_ARRAY; s.extract = Extract::MemcpyFixed;
        s.src_width = n; s.type_length = n;
        return s;
    }
    if (std::strcmp(format, "u") == 0 || std::strcmp(format, "U") == 0) {  // utf8 / large_utf8
        s.type = pq::Type::BYTE_ARRAY; s.extract = Extract::ByteArray;
        s.has_converted = true; s.converted = pq::ConvertedType::UTF8;
        s.large_offsets = (format[0] == 'U');
        return s;
    }
    if (std::strcmp(format, "z") == 0 || std::strcmp(format, "Z") == 0) {  // binary / large_binary
        s.type = pq::Type::BYTE_ARRAY; s.extract = Extract::ByteArray;
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
//
// Page headers are nanom_formats::parquet::PageHeader values (the model parquet2nanoarrow reads),
// encoded by nanom into a stack buffer: no allocation per page, and a header that does not fit or
// a size beyond the wire's i32 is an error instead of a silently truncated field.

struct HeaderBytes {
    std::array<std::byte, 128> buf;  // a data / dictionary page header is < 40 bytes
    std::size_t size = 0;
    std::span<const std::uint8_t> bytes() const {
        return {reinterpret_cast<const std::uint8_t*>(buf.data()), size};
    }
};

std::int32_t page_size_i32(std::size_t n) {
    if (n > static_cast<std::size_t>(INT32_MAX))
        throw std::length_error("a page larger than 2 GiB (Parquet page sizes are i32)");
    return static_cast<std::int32_t>(n);
}

HeaderBytes encode_page_header(const pq::PageHeader& h) {
    HeaderBytes out;
    nanom::span_sink sink{out.buf};
    auto r = nanom::thrift_compact_encode(h, sink);
    if (!r) throw std::runtime_error(std::string("page header encoding failed: ") + r.error().what);
    out.size = *r;
    return out;
}

HeaderBytes data_page_header(std::size_t num_values, pq::Encoding encoding,
                             std::size_t uncompressed, std::size_t compressed) {
    pq::DataPageHeader d;
    d.num_values = page_size_i32(num_values);
    d.encoding = encoding;
    d.definition_level_encoding = pq::Encoding::RLE;
    d.repetition_level_encoding = pq::Encoding::RLE;
    pq::PageHeader h;
    h.type = pq::PageType::DATA_PAGE;
    h.uncompressed_page_size = page_size_i32(uncompressed);
    h.compressed_page_size = page_size_i32(compressed);
    h.data_page_header = d;
    return encode_page_header(h);
}

HeaderBytes dictionary_page_header(std::size_t num_values, std::size_t uncompressed,
                                   std::size_t compressed) {
    pq::DictionaryPageHeader d;
    d.num_values = page_size_i32(num_values);
    d.encoding = pq::Encoding::PLAIN;
    d.is_sorted = false;
    pq::PageHeader h;
    h.type = pq::PageType::DICTIONARY_PAGE;
    h.uncompressed_page_size = page_size_i32(uncompressed);
    h.compressed_page_size = page_size_i32(compressed);
    h.dictionary_page_header = d;
    return encode_page_header(h);
}

}  // namespace
}  // namespace n2p

// ---- writer object --------------------------------------------------------

struct N2PWriter {
    std::ofstream out;
    std::string path;
    std::int64_t offset = 0;
    n2p::pq::CompressionCodec codec = n2p::pq::CompressionCodec::ZSTD;
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

PageBytes emit_page(N2PWriter& w, const HeaderBytes& hdr,
                    const std::vector<std::uint8_t>& compressed_body,
                    std::size_t uncompressed_body) {
    const auto header = hdr.bytes();
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

// The footer: a pq::FileMetaData built from the writer's state and encoded by nanom. Strings are
// views into the writer's schema / chunk records; lists are list<E>::of views over the vectors
// below, which own the elements until encoding is done. Nothing is copied twice.
pq::SchemaElement schema_element(const SchemaNode& s) {
    pq::SchemaElement e;
    e.repetition_type = s.optional ? pq::FieldRepetitionType::OPTIONAL
                                   : pq::FieldRepetitionType::REQUIRED;
    e.name = std::string_view(s.name);
    if (s.is_group) {
        // A group (struct) has no physical type; it declares num_children.
        e.num_children = s.num_children;
    } else {
        e.type = s.type;
        if (s.type == pq::Type::FIXED_LEN_BYTE_ARRAY) e.type_length = s.type_length;
        if (s.has_converted) e.converted_type = s.converted;
    }
    return e;
}

bool serialize_footer(const N2PWriter& w, std::vector<std::byte>& out, std::string& err) {
    std::vector<pq::SchemaElement> schema;
    schema.reserve(w.schema_nodes.size() + 1);
    {
        // root schema element: name + num_children only (no type/repetition).
        pq::SchemaElement root;
        root.name = std::string_view("schema");
        root.num_children = w.top_children;
        schema.push_back(root);
    }
    for (const auto& s : w.schema_nodes) schema.push_back(schema_element(s));

    std::size_t n_chunks = 0;
    for (const auto& rg : w.row_groups) n_chunks += rg.columns.size();
    std::vector<std::vector<std::string_view>> paths;  // path_in_schema per chunk
    std::vector<std::vector<pq::ColumnChunk>> chunks;  // columns per row group
    std::vector<pq::RowGroup> row_groups;
    paths.reserve(n_chunks);
    chunks.reserve(w.row_groups.size());
    row_groups.reserve(w.row_groups.size());
    for (const auto& rg : w.row_groups) {
        auto& cols = chunks.emplace_back();
        cols.reserve(rg.columns.size());
        for (const auto& c : rg.columns) {
            auto& path = paths.emplace_back(c.path.begin(), c.path.end());
            pq::ColumnMetaData m;
            m.type = c.type;
            m.encodings = nanom::list<pq::Encoding>::of(c.encodings);
            m.path_in_schema = nanom::list<std::string_view>::of(path);
            m.codec = w.codec;
            m.num_values = c.num_values;
            m.total_uncompressed_size = c.total_uncompressed;
            m.total_compressed_size = c.total_compressed;
            m.data_page_offset = c.data_page_offset;
            if (c.has_dictionary) m.dictionary_page_offset = c.dictionary_page_offset;
            pq::ColumnChunk cc;
            cc.file_offset = c.file_offset;
            cc.meta_data = m;
            cols.push_back(cc);
        }
        pq::RowGroup g;
        g.columns = nanom::list<pq::ColumnChunk>::of(cols);
        g.total_byte_size = rg.total_byte_size;
        g.num_rows = rg.num_rows;
        row_groups.push_back(g);
    }

    pq::FileMetaData f;
    f.version = 1;
    f.schema = nanom::list<pq::SchemaElement>::of(schema);
    f.num_rows = w.total_rows;
    f.row_groups = nanom::list<pq::RowGroup>::of(row_groups);
    f.created_by = std::string_view("nanoarrow2parquet");
    auto r = nanom::thrift_compact_encode(f, out);
    if (!r) {
        err = std::string("footer encoding failed: ") + r.error().what + " (in " +
              std::string(r.error().message) + "." + std::string(r.error().field) + ")";
        return false;
    }
    return true;
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
                        pages.dict_size, pages.dict_body.size(), dict_comp.size());
                    c.dictionary_page_offset = w.offset;
                    c.has_dictionary = true;
                    PageBytes dp = emit_page(w, dict_hdr, dict_comp, pages.dict_body.size());
                    // data page (RLE_DICTIONARY), def levels first.
                    std::vector<std::uint8_t> data_body = def_prefix;
                    data_body.insert(data_body.end(), pages.data_body.begin(), pages.data_body.end());
                    auto data_comp = compress_page(data_body, w.codec);
                    auto data_hdr = data_page_header(
                        static_cast<std::size_t>(batch->length), pq::Encoding::RLE_DICTIONARY,
                        data_body.size(), data_comp.size());
                    c.data_page_offset = w.offset;
                    PageBytes vp = emit_page(w, data_hdr, data_comp, data_body.size());
                    c.total_uncompressed = dp.uncompressed + vp.uncompressed;
                    c.total_compressed = dp.on_disk + vp.on_disk;
                    c.encodings = {pq::Encoding::PLAIN, pq::Encoding::RLE_DICTIONARY};
                } else {
                    // PLAIN BYTE_ARRAY data page, def levels first.
                    std::vector<std::uint8_t> data_body = def_prefix;
                    data_body.insert(data_body.end(), pages.data_body.begin(), pages.data_body.end());
                    auto data_comp = compress_page(data_body, w.codec);
                    auto data_hdr = data_page_header(
                        static_cast<std::size_t>(batch->length), pq::Encoding::PLAIN,
                        data_body.size(), data_comp.size());
                    c.data_page_offset = w.offset;
                    PageBytes vp = emit_page(w, data_hdr, data_comp, data_body.size());
                    c.total_uncompressed = vp.uncompressed;
                    c.total_compressed = vp.on_disk;
                    c.encodings = {pq::Encoding::PLAIN};
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
                    static_cast<std::size_t>(batch->length), pq::Encoding::PLAIN,
                    body.size(), comp.size());
                c.data_page_offset = w.offset;
                PageBytes vp = emit_page(w, hdr, comp, body.size());
                c.total_uncompressed = vp.uncompressed;
                c.total_compressed = vp.on_disk;
                c.encodings = {pq::Encoding::PLAIN};
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
        case N2P_CODEC_ZSTD: w->codec = n2p::pq::CompressionCodec::ZSTD; return N2P_OK;
        case N2P_CODEC_UNCOMPRESSED: w->codec = n2p::pq::CompressionCodec::UNCOMPRESSED; return N2P_OK;
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
        std::vector<std::byte> footer;
        std::string err;
        if (!n2p::serialize_footer(*w, footer, err) || footer.size() > UINT32_MAX) {
            // the file is left without a footer: unreadable, never wrong
            w->last_error = err.empty() ? "footer larger than 4 GiB" : err;
            status = N2P_IO_ERROR;
        } else {
            n2p::write_bytes(*w, std::span<const std::uint8_t>(
                                     reinterpret_cast<const std::uint8_t*>(footer.data()), footer.size()));
            std::uint32_t len = static_cast<std::uint32_t>(footer.size());
            std::uint8_t le[4] = {static_cast<std::uint8_t>(len & 0xFF),
                                  static_cast<std::uint8_t>((len >> 8) & 0xFF),
                                  static_cast<std::uint8_t>((len >> 16) & 0xFF),
                                  static_cast<std::uint8_t>((len >> 24) & 0xFF)};
            n2p::write_bytes(*w, std::span<const std::uint8_t>(le, 4));
            n2p::write_bytes(*w, std::span<const std::uint8_t>(
                                     reinterpret_cast<const std::uint8_t*>(n2p::kMagic), 4));
        }
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

#endif  // NANOARROW2PARQUET_IMPLEMENTATION
