// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Yoav Bendor

// Native struct-of-arrays path: describe the schema as C++ types and dump SoA
// columns straight to Parquet, with no nanoarrow involved (note: this file does
// not include nanoarrow). Exercises the full fixed-width numeric trait table and
// asserts the Arrow format mapping at compile time. Two chunks become two row
// groups, demonstrating that batching policy lives with the caller.
// tests/check_soa.py reads it back and checks values + logical types.

#include "nanoarrow2parquet/soa.hpp"

#include <cstdint>
#include <cstdio>
#include <limits>
#include <string_view>
#include <vector>

using namespace n2p::soa;

// ---- compile-time golden mapping (no execution) ---------------------------
// The Arrow format string is the whole of the static type mapping (the writer
// derives the Parquet physical + converted type from it), so locking this table
// at compile time pins n2p's type contract. check_soa.py proves each of these
// formats actually roundtrips to the matching Arrow logical type via pyarrow.
static_assert(std::string_view(arrow_traits<std::int8_t>::format)   == "c");
static_assert(std::string_view(arrow_traits<std::uint8_t>::format)  == "C");
static_assert(std::string_view(arrow_traits<std::int16_t>::format)  == "s");
static_assert(std::string_view(arrow_traits<std::uint16_t>::format) == "S");
static_assert(std::string_view(arrow_traits<std::int32_t>::format)  == "i");
static_assert(std::string_view(arrow_traits<std::uint32_t>::format) == "I");
static_assert(std::string_view(arrow_traits<std::int64_t>::format)  == "l");
static_assert(std::string_view(arrow_traits<std::uint64_t>::format) == "L");
static_assert(std::string_view(arrow_traits<float>::format)         == "f");
static_assert(std::string_view(arrow_traits<double>::format)        == "g");
static_assert(SupportedField<std::int32_t> && SupportedField<double>);
static_assert(!SupportedField<bool> && !SupportedField<long double>);

int main(int argc, char** argv) {
    const char* path = (argc > 1) ? argv[1] : "n2p_soa.parquet";

    using i8 = std::int8_t;   using u8 = std::uint8_t;
    using i16 = std::int16_t; using u16 = std::uint16_t;
    using i32 = std::int32_t; using u32 = std::uint32_t;
    using i64 = std::int64_t; using u64 = std::uint64_t;
    constexpr i8 i8min = std::numeric_limits<i8>::min();
    constexpr i16 i16min = std::numeric_limits<i16>::min();
    constexpr i32 i32min = std::numeric_limits<i32>::min();
    constexpr i64 i64min = std::numeric_limits<i64>::min();

    Writer<Field<"i8", i8>,  Field<"u8", u8>,
           Field<"i16", i16>, Field<"u16", u16>,
           Field<"i32", i32>, Field<"u32", u32>,
           Field<"i64", i64>, Field<"u64", u64>,
           Field<"f32", float>, Field<"f64", double>> w(path);
    if (!w.ok()) { std::fprintf(stderr, "soa open failed: %s\n", w.last_error()); return 1; }

    // Chunk 1 -> row group 1 (3 rows). Edge values exercise sign + width.
    std::vector<i8>  a1{-1, 0, 127};       std::vector<u8>  b1{0, 200, 255};
    std::vector<i16> c1{-1, 1000, 32767};  std::vector<u16> d1{0, 60000, 65535};
    std::vector<i32> e1{-1, 2, 2147483647};
    std::vector<u32> f1{0u, 4000000000u, 4294967295u};
    std::vector<i64> g1{-1, 2, 9223372036854775807LL};
    std::vector<u64> h1{0ull, 18446744073709551615ull, 5ull};
    std::vector<float>  x1{1.5f, -2.5f, 3.5f};
    std::vector<double> y1{1.5, -2.5, 3.5};
    if (w.write_chunk(a1, b1, c1, d1, e1, f1, g1, h1, x1, y1) != N2P_OK) {
        std::fprintf(stderr, "soa chunk 1 failed: %s\n", w.last_error());
        return 1;
    }

    // Chunk 2 -> row group 2 (2 rows; caller chose this boundary).
    std::vector<i8>  a2{i8min, 5};         std::vector<u8>  b2{1, 254};
    std::vector<i16> c2{i16min, 0};        std::vector<u16> d2{1, 2};
    std::vector<i32> e2{i32min, 0};        std::vector<u32> f2{1u, 2u};
    std::vector<i64> g2{i64min, 0};        std::vector<u64> h2{1ull, 2ull};
    std::vector<float>  x2{4.5f, 5.5f};    std::vector<double> y2{4.5, 5.5};
    if (w.write_chunk(a2, b2, c2, d2, e2, f2, g2, h2, x2, y2) != N2P_OK) {
        std::fprintf(stderr, "soa chunk 2 failed: %s\n", w.last_error());
        return 1;
    }

    if (w.close() != N2P_OK) { std::fprintf(stderr, "soa close failed\n"); return 1; }
    std::printf("wrote %s (SoA native path, all numeric types, 2 row groups)\n", path);
    return 0;
}
