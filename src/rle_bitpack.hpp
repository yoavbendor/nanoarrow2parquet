// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Yoav Bendor

#pragma once

// RLE / bit-packed hybrid streams (dictionary indices, definition levels), encoded by nanom's
// columnar encoders (nanom/columnar_encode.hpp): runs of >= 8 equal values become RLE runs, the
// rest bit-packed groups of 8. These are the encoders whose output nanom's decoders (and every
// Parquet reader) read; nanom round-trips and fuzzes each one.

#include <nanom/columnar_encode.hpp>

#include <cstdint>
#include <cstring>
#include <span>
#include <stdexcept>
#include <vector>

namespace n2p {

// Smallest bit width that can represent indices [0, dict_size).
inline int dictionary_bit_width(std::size_t dict_size) {
    int w = 0;
    while ((std::size_t{1} << w) < dict_size) {
        ++w;
    }
    return w;  // 0 when dict_size <= 1
}

inline void append_bytes(std::vector<std::uint8_t>& out, const std::vector<std::byte>& in) {
    const std::size_t at = out.size();
    out.resize(at + in.size());
    if (!in.empty()) std::memcpy(out.data() + at, in.data(), in.size());
}

// The body of an RLE_DICTIONARY data page after its bit-width byte (the writer prepends that).
inline std::vector<std::uint8_t> encode_rle_dictionary_indices(
    std::span<const std::uint32_t> indices, int bit_width) {
    std::vector<std::byte> enc;
    nanom::columnar::rle_hybrid_encode<std::uint32_t>(indices, static_cast<unsigned>(bit_width), enc);
    std::vector<std::uint8_t> out;
    append_bytes(out, enc);
    return out;
}

// The leading bytes of a DataPage V1 body: a 4-byte little-endian length, then the hybrid stream.
inline std::vector<std::uint8_t> with_length_prefix(const std::vector<std::byte>& run) {
    if (run.size() > UINT32_MAX) throw std::length_error("definition levels larger than 4 GiB");
    std::vector<std::uint8_t> out;
    out.reserve(4 + run.size());
    const std::uint32_t len = static_cast<std::uint32_t>(run.size());
    out.push_back(len & 0xFF);
    out.push_back((len >> 8) & 0xFF);
    out.push_back((len >> 16) & 0xFF);
    out.push_back((len >> 24) & 0xFF);
    append_bytes(out, run);
    return out;
}

// Definition levels (each <= 2^bit_width - 1), length-prefixed.
inline std::vector<std::uint8_t> encode_definition_levels(
    std::span<const std::uint32_t> levels, int bit_width) {
    std::vector<std::byte> run;
    nanom::columnar::rle_hybrid_encode<std::uint32_t>(levels, static_cast<unsigned>(bit_width), run);
    return with_length_prefix(run);
}

// Definition levels of a column with max level 1, straight from its presence bitmap (bit i set =
// row i present), length-prefixed. No per-row level array is built.
inline std::vector<std::uint8_t> encode_definition_levels_bitmap(const std::uint8_t* present, std::size_t n) {
    std::vector<std::byte> run;
    nanom::columnar::rle_bitmap_encode(present, n, run);
    return with_length_prefix(run);
}

}  // namespace n2p
