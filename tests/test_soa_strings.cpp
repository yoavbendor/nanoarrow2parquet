// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Yoav Bendor

// Native SoA path with variable-length columns (utf8 / binary), REQUIRED and
// nullable. No nanoarrow. A SoA string column is a range of std::string, which
// is materialized into Arrow offsets + data buffers per chunk.
// tests/check_soa_strings.py reads it back.

#include "nanoarrow2parquet/soa.hpp"

#include <cstdint>
#include <cstdio>
#include <string>
#include <vector>

using namespace n2p::soa;

static_assert(SupportedField<utf8> && SupportedField<binary>);

int main(int argc, char** argv) {
    const char* path = (argc > 1) ? argv[1] : "n2p_soa_strings.parquet";

    Writer<Field<"id", std::int32_t>,
           Field<"name", utf8>,            // REQUIRED utf8
           Nullable<"note", utf8>,         // nullable utf8
           Field<"blob", binary>> w(path);
    if (!w.ok()) { std::fprintf(stderr, "open failed: %s\n", w.last_error()); return 1; }

    // Chunk 1 (3 rows).
    std::vector<std::int32_t> id1{0, 1, 2};
    std::vector<std::string> name1{"alice", "bob", "carol"};
    std::vector<std::string> note1{"hi", "", "skip"};
    std::vector<std::uint8_t> note1_present{1, 1, 0};        // note[2] null
    std::vector<std::string> blob1{std::string("\x00\x01", 2), "xy", "z"};
    if (w.write_chunk(id1, name1, present(note1, note1_present), blob1) != N2P_OK) {
        std::fprintf(stderr, "chunk 1 failed: %s\n", w.last_error());
        return 1;
    }

    // Chunk 2 (2 rows).
    std::vector<std::int32_t> id2{3, 4};
    std::vector<std::string> name2{"dave", "erin"};
    std::vector<std::string> note2{"", "ok"};
    std::vector<std::uint8_t> note2_present{0, 1};           // note[0] null
    std::vector<std::string> blob2{"", "qq"};
    if (w.write_chunk(id2, name2, present(note2, note2_present), blob2) != N2P_OK) {
        std::fprintf(stderr, "chunk 2 failed: %s\n", w.last_error());
        return 1;
    }

    if (w.close() != N2P_OK) { std::fprintf(stderr, "close failed\n"); return 1; }
    std::printf("wrote %s (SoA utf8/binary, 2 row groups)\n", path);
    return 0;
}
