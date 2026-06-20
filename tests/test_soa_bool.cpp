// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Yoav Bendor

// Native SoA path with bool columns, REQUIRED and nullable. No nanoarrow. A SoA
// bool column is byte-per-bool storage (here std::vector<bool> and a byte array),
// packed into Arrow's 1-bit-per-value LSB-first layout per chunk.
// tests/check_soa_bool.py reads it back.

#include "nanoarrow2parquet/soa.hpp"

#include <cstdint>
#include <cstdio>
#include <vector>

using namespace n2p::soa;

static_assert(SupportedField<bool>);

int main(int argc, char** argv) {
    const char* path = (argc > 1) ? argv[1] : "n2p_soa_bool.parquet";

    Writer<Field<"id", std::int32_t>,
           Field<"flag", bool>,            // REQUIRED bool
           Nullable<"opt", bool>> w(path); // nullable bool
    if (!w.ok()) { std::fprintf(stderr, "open failed: %s\n", w.last_error()); return 1; }

    // Chunk 1 (5 rows). std::vector<bool> is fine here -- this path iterates.
    std::vector<std::int32_t> id1{0, 1, 2, 3, 4};
    std::vector<bool> flag1{true, false, true, true, false};
    std::vector<bool> opt1{true, false, false, true, false};
    std::vector<std::uint8_t> opt1_present{1, 1, 0, 1, 0};   // opt[2], opt[4] null
    if (w.write_chunk(id1, flag1, present(opt1, opt1_present)) != N2P_OK) {
        std::fprintf(stderr, "chunk 1 failed: %s\n", w.last_error());
        return 1;
    }

    // Chunk 2 (3 rows). Spans more than one packed byte across the file.
    std::vector<std::int32_t> id2{5, 6, 7};
    std::vector<bool> flag2{false, false, true};
    std::vector<bool> opt2{true, true, false};
    std::vector<std::uint8_t> opt2_present{1, 0, 1};         // opt[1] null
    if (w.write_chunk(id2, flag2, present(opt2, opt2_present)) != N2P_OK) {
        std::fprintf(stderr, "chunk 2 failed: %s\n", w.last_error());
        return 1;
    }

    if (w.close() != N2P_OK) { std::fprintf(stderr, "close failed\n"); return 1; }
    std::printf("wrote %s (SoA bool, 2 row groups)\n", path);
    return 0;
}
