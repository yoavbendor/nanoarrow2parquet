// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Yoav Bendor

// Native SoA path with OPTIONAL (nullable) fixed-width columns. No nanoarrow.
// Exercises both nullable forms: present(values, mask) which packs a per-element
// presence mask into an Arrow validity bitmap, and valid_bits(values, bitmap,
// nulls) which aliases a pre-packed bitmap with no copy. A REQUIRED column rides
// alongside. tests/check_soa_nullable.py verifies the null positions.

#include "nanoarrow2parquet/soa.hpp"

#include <cstdint>
#include <cstdio>
#include <vector>

using namespace n2p::soa;

int main(int argc, char** argv) {
    const char* path = (argc > 1) ? argv[1] : "n2p_soa_nullable.parquet";

    Writer<Field<"id", std::int32_t>,             // REQUIRED
           Nullable<"a", std::int64_t>,           // nullable via present(mask)
           Nullable<"x", double>> w(path);        // nullable via valid_bits(bitmap)
    if (!w.ok()) { std::fprintf(stderr, "open failed: %s\n", w.last_error()); return 1; }

    // Chunk 1 (5 rows).
    std::vector<std::int32_t> id1{0, 1, 2, 3, 4};
    std::vector<std::int64_t> a1{100, 0, 300, 0, 500};       // 0s stand in for nulls
    std::vector<std::uint8_t> a1_present{1, 0, 1, 0, 1};      // a[1], a[3] are null
    std::vector<double> x1{1.5, 2.5, 0.0, 4.5, 0.0};
    // Pre-packed LSB-first bitmap for x1: present at 0,1,3 -> bits 0b01011 = 0x0B.
    std::vector<std::uint8_t> x1_bits{0x0B};
    if (w.write_chunk(id1, present(a1, a1_present), valid_bits(x1, x1_bits.data(), 2)) != N2P_OK) {
        std::fprintf(stderr, "chunk 1 failed: %s\n", w.last_error());
        return 1;
    }

    // Chunk 2 (3 rows), all of `a` present, `x` all null.
    std::vector<std::int32_t> id2{5, 6, 7};
    std::vector<std::int64_t> a2{600, 700, 800};
    std::vector<std::uint8_t> a2_present{1, 1, 1};
    std::vector<double> x2{0.0, 0.0, 0.0};
    std::vector<std::uint8_t> x2_bits{0x00};                  // none present
    if (w.write_chunk(id2, present(a2, a2_present), valid_bits(x2, x2_bits.data(), 3)) != N2P_OK) {
        std::fprintf(stderr, "chunk 2 failed: %s\n", w.last_error());
        return 1;
    }

    if (w.close() != N2P_OK) { std::fprintf(stderr, "close failed\n"); return 1; }
    std::printf("wrote %s (SoA nullable, 2 row groups)\n", path);
    return 0;
}
