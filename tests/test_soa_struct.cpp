// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Yoav Bendor

// Native SoA path with nested struct groups, REQUIRED and nullable, nested two
// deep. No nanoarrow. Mirrors the multi-level definition-level case that the
// ArrowArray test_struct exercises, but assembled from SoA columns via group().
// tests/check_soa_struct.py reads it back.

#include "nanoarrow2parquet/soa.hpp"

#include <cstdint>
#include <cstdio>
#include <string>
#include <vector>

using namespace n2p::soa;

int main(int argc, char** argv) {
    const char* path = (argc > 1) ? argv[1] : "n2p_soa_struct.parquet";

    // schema:
    //   id        int32 REQUIRED
    //   addr      struct REQUIRED { city utf8 REQUIRED, zip int32 REQUIRED }
    //   meta      struct OPTIONAL {
    //               score double REQUIRED,
    //               geo   struct OPTIONAL { lat double REQUIRED, lon double REQUIRED }
    //             }
    Writer<Field<"id", std::int32_t>,
           Struct<"addr",
                  Field<"city", utf8>,
                  Field<"zip", std::int32_t>>,
           NullableStruct<"meta",
                  Field<"score", double>,
                  NullableStruct<"geo",
                         Field<"lat", double>,
                         Field<"lon", double>>>> w(path);
    if (!w.ok()) { std::fprintf(stderr, "open failed: %s\n", w.last_error()); return 1; }

    // Chunk 1 (3 rows).
    std::vector<std::int32_t> id1{0, 1, 2};
    std::vector<std::string> city1{"NYC", "LA", "SF"};
    std::vector<std::int32_t> zip1{10001, 90001, 94101};
    std::vector<double> score1{1.5, 2.5, 3.5};
    std::vector<double> lat1{40.7, 0.0, 37.8};
    std::vector<double> lon1{-74.0, 0.0, -122.4};
    std::vector<std::uint8_t> meta_present1{1, 0, 1};   // meta null for row 1
    std::vector<std::uint8_t> geo_present1{1, 1, 0};    // geo null for row 2 (and row1 via meta)
    if (w.write_chunk(id1,
                      group(city1, zip1),
                      group_present(meta_present1,
                                    score1,
                                    group_present(geo_present1, lat1, lon1))) != N2P_OK) {
        std::fprintf(stderr, "chunk 1 failed: %s\n", w.last_error());
        return 1;
    }

    // Chunk 2 (2 rows). All meta present, geo mixed.
    std::vector<std::int32_t> id2{3, 4};
    std::vector<std::string> city2{"BOS", "SEA"};
    std::vector<std::int32_t> zip2{2101, 98101};
    std::vector<double> score2{4.5, 5.5};
    std::vector<double> lat2{42.3, 47.6};
    std::vector<double> lon2{-71.0, -122.3};
    std::vector<std::uint8_t> meta_present2{1, 1};
    std::vector<std::uint8_t> geo_present2{0, 1};       // geo null for row 3
    if (w.write_chunk(id2,
                      group(city2, zip2),
                      group_present(meta_present2,
                                    score2,
                                    group_present(geo_present2, lat2, lon2))) != N2P_OK) {
        std::fprintf(stderr, "chunk 2 failed: %s\n", w.last_error());
        return 1;
    }

    if (w.close() != N2P_OK) { std::fprintf(stderr, "close failed\n"); return 1; }
    std::printf("wrote %s (SoA nested structs, 2 row groups)\n", path);
    return 0;
}
