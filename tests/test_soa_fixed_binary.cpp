// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Yoav Bendor

// Native struct-of-arrays path: std::array<uint8_t, N> columns map to Arrow fixed_size_binary
// ("w:N"), the recommended shape for MAC/IP/clock-identity-style fixed-width byte fields (the gap
// this test closes: arrow_traits<std::array<uint8_t, N>> did not exist before). Zero-copy: the
// column's own std::array storage IS the Parquet data page, exactly like any other fixed-width
// numeric column. tests/check_soa_fixed_binary.py reads it back and checks values + logical type.

#include "nanoarrow2parquet/soa.hpp"

#include <array>
#include <cstdint>
#include <cstdio>
#include <string_view>
#include <vector>

using namespace n2p::soa;

// ---- compile-time golden mapping (no execution) ---------------------------
static_assert(std::string_view(arrow_traits<std::array<std::uint8_t, 4>>::format)  == "w:4");
static_assert(std::string_view(arrow_traits<std::array<std::uint8_t, 6>>::format)  == "w:6");
static_assert(std::string_view(arrow_traits<std::array<std::uint8_t, 16>>::format) == "w:16");
static_assert(SupportedField<std::array<std::uint8_t, 6>>);

int main(int argc, char** argv) {
    const char* path = (argc > 1) ? argv[1] : "n2p_soa_fixed_binary.parquet";

    using mac_t  = std::array<std::uint8_t, 6>;
    using ipv4_t = std::array<std::uint8_t, 4>;

    Writer<Field<"mac", mac_t>, Field<"ipv4", ipv4_t>> w(path);
    if (!w.ok()) { std::fprintf(stderr, "soa fixed_binary open failed: %s\n", w.last_error()); return 1; }

    // Chunk 1 -> row group 1 (2 rows).
    std::vector<mac_t>  m1{mac_t{0x00, 0x11, 0x22, 0x33, 0x44, 0x55}, mac_t{0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF}};
    std::vector<ipv4_t> a1{ipv4_t{192, 168, 1, 1}, ipv4_t{10, 0, 0, 1}};
    if (w.write_chunk(m1, a1) != N2P_OK) {
        std::fprintf(stderr, "soa fixed_binary chunk 1 failed: %s\n", w.last_error());
        return 1;
    }

    // Chunk 2 -> row group 2 (1 row; caller chose this boundary).
    std::vector<mac_t>  m2{mac_t{0x01, 0x02, 0x03, 0x04, 0x05, 0x06}};
    std::vector<ipv4_t> a2{ipv4_t{255, 255, 255, 0}};
    if (w.write_chunk(m2, a2) != N2P_OK) {
        std::fprintf(stderr, "soa fixed_binary chunk 2 failed: %s\n", w.last_error());
        return 1;
    }

    if (w.close() != N2P_OK) { std::fprintf(stderr, "soa fixed_binary close failed\n"); return 1; }
    std::printf("wrote %s (SoA native path, fixed_size_binary columns, 2 row groups)\n", path);
    return 0;
}
