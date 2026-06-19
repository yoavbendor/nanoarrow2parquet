// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Yoav Bendor

#pragma once

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

#include <cstdint>
#include <span>
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

}  // namespace n2p
