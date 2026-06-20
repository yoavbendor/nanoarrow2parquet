// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Yoav Bendor

// Native struct-of-arrays path: describe the schema as C++ types and dump SoA
// columns straight to Parquet, with no nanoarrow involved (note: this file does
// not include nanoarrow). Two chunks become two row groups, demonstrating that
// batching policy lives with the caller. tests/check_soa.py reads it back.

#include "nanoarrow2parquet/soa.hpp"

#include <cstdint>
#include <cstdio>
#include <string_view>
#include <vector>

using namespace n2p::soa;

// Compile-time tests: the static type mapping is checked by the build itself.
static_assert(std::string_view(arrow_traits<std::int64_t>::format) == "l");
static_assert(std::string_view(arrow_traits<double>::format) == "g");
static_assert(std::string_view(arrow_traits<std::uint32_t>::format) == "I");
static_assert(SupportedField<std::int32_t> && !SupportedField<long double>);

int main(int argc, char** argv) {
    const char* path = (argc > 1) ? argv[1] : "n2p_soa.parquet";

    Writer<Field<"id", std::int64_t>,
           Field<"x", double>,
           Field<"flags", std::uint32_t>> w(path);
    if (!w.ok()) { std::fprintf(stderr, "soa open failed: %s\n", w.last_error()); return 1; }

    // Chunk 1 -> row group 1.
    std::vector<std::int64_t> id1{1, 2, 3};
    std::vector<double> x1{1.5, 2.5, 3.5};
    std::vector<std::uint32_t> f1{10, 20, 30};
    if (w.write_chunk(id1, x1, f1) != N2P_OK) {
        std::fprintf(stderr, "soa chunk 1 failed: %s\n", w.last_error());
        return 1;
    }

    // Chunk 2 -> row group 2 (caller chose this boundary).
    std::vector<std::int64_t> id2{4, 5};
    std::vector<double> x2{4.5, 5.5};
    std::vector<std::uint32_t> f2{40, 50};
    if (w.write_chunk(id2, x2, f2) != N2P_OK) {
        std::fprintf(stderr, "soa chunk 2 failed: %s\n", w.last_error());
        return 1;
    }

    if (w.close() != N2P_OK) { std::fprintf(stderr, "soa close failed\n"); return 1; }
    std::printf("wrote %s (SoA native path, 2 row groups, no nanoarrow)\n", path);
    return 0;
}
