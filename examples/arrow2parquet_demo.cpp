// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Yoav Bendor

// Builds an in-memory Arrow record batch covering every supported column type
// and writes it to a .parquet file via the nanoarrow2parquet C ABI. The values
// are deterministic so tests/smoke_pyarrow_roundtrip.sh can assert on them.

#include "nanoarrow2parquet/nanoarrow2parquet.h"

#include <nanoarrow/nanoarrow.h>

#include <array>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <string>

namespace {

constexpr int kRows = 5;
const char* const kNames[kRows] = {"alpha", "beta", "alpha", "gamma", "beta"};

void die(const char* msg) {
    std::fprintf(stderr, "demo: %s\n", msg);
    std::exit(1);
}

bool build_batch(ArrowSchema& schema, ArrowArray& array) {
    ArrowSchemaInit(&schema);
    if (ArrowSchemaSetTypeStruct(&schema, 8) != NANOARROW_OK) return false;
    struct Col { const char* name; int type; int fixed; };
    const Col cols[8] = {
        {"i32", NANOARROW_TYPE_INT32, 0},
        {"i64", NANOARROW_TYPE_INT64, 0},
        {"u32", NANOARROW_TYPE_UINT32, 0},
        {"f32", NANOARROW_TYPE_FLOAT, 0},
        {"f64", NANOARROW_TYPE_DOUBLE, 0},
        {"flag", NANOARROW_TYPE_BOOL, 0},
        {"fsb", NANOARROW_TYPE_FIXED_SIZE_BINARY, 4},
        {"name", NANOARROW_TYPE_STRING, 0},
    };
    for (int i = 0; i < 8; ++i) {
        ArrowSchema* c = schema.children[i];
        int rc = cols[i].fixed
                     ? ArrowSchemaSetTypeFixedSize(c, NANOARROW_TYPE_FIXED_SIZE_BINARY, cols[i].fixed)
                     : ArrowSchemaSetType(c, static_cast<ArrowType>(cols[i].type));
        if (rc != NANOARROW_OK) return false;
        if (ArrowSchemaSetName(c, cols[i].name) != NANOARROW_OK) return false;
        c->flags = 0;  // non-nullable
    }
    schema.flags = 0;

    if (ArrowArrayInitFromSchema(&array, &schema, nullptr) != NANOARROW_OK) return false;
    if (ArrowArrayStartAppending(&array) != NANOARROW_OK) return false;
    for (int i = 0; i < kRows; ++i) {
        if (ArrowArrayAppendInt(array.children[0], i * 1000 - 2000) != NANOARROW_OK) return false;
        if (ArrowArrayAppendInt(array.children[1], static_cast<int64_t>(i) * 1000000) != NANOARROW_OK) return false;
        if (ArrowArrayAppendUInt(array.children[2], 4000000000ull + i) != NANOARROW_OK) return false;
        if (ArrowArrayAppendDouble(array.children[3], i + 0.5) != NANOARROW_OK) return false;
        if (ArrowArrayAppendDouble(array.children[4], i * 1.25) != NANOARROW_OK) return false;
        if (ArrowArrayAppendInt(array.children[5], (i % 2 == 0) ? 1 : 0) != NANOARROW_OK) return false;
        std::array<uint8_t, 4> fsb = {static_cast<uint8_t>(i), static_cast<uint8_t>(i + 1),
                                      static_cast<uint8_t>(i + 2), static_cast<uint8_t>(i + 3)};
        ArrowBufferView bv{};
        bv.data.data = fsb.data();
        bv.size_bytes = 4;
        if (ArrowArrayAppendBytes(array.children[6], bv) != NANOARROW_OK) return false;
        ArrowStringView sv{kNames[i], static_cast<int64_t>(std::string(kNames[i]).size())};
        if (ArrowArrayAppendString(array.children[7], sv) != NANOARROW_OK) return false;
        if (ArrowArrayFinishElement(&array) != NANOARROW_OK) return false;
    }
    return ArrowArrayFinishBuildingDefault(&array, nullptr) == NANOARROW_OK;
}

}  // namespace

// Streaming mode: write the same 8-column schema as three separate row groups
// (rows split 2/2/1) via the streaming writer, exercising the multi-row-group
// path. The values match the single-shot demo so tests can compare both.
int run_streaming(const char* path) {
    N2PWriter* w = nullptr;
    if (n2p_writer_open(&w, path) != N2P_OK) {
        std::fprintf(stderr, "demo: open failed: %s\n", n2p_writer_last_error(w));
        n2p_writer_close(w);
        return 1;
    }
    const int splits[3] = {2, 2, 1};
    int base = 0;
    for (int chunk = 0; chunk < 3; ++chunk) {
        ArrowSchema schema{};
        ArrowArray array{};
        // Build a batch of just this chunk's rows by reusing build_batch's layout.
        ArrowSchemaInit(&schema);
        if (ArrowSchemaSetTypeStruct(&schema, 8) != NANOARROW_OK) die("schema");
        const char* names[8] = {"i32", "i64", "u32", "f32", "f64", "flag", "fsb", "name"};
        const int types[8] = {NANOARROW_TYPE_INT32, NANOARROW_TYPE_INT64, NANOARROW_TYPE_UINT32,
                              NANOARROW_TYPE_FLOAT, NANOARROW_TYPE_DOUBLE, NANOARROW_TYPE_BOOL,
                              NANOARROW_TYPE_FIXED_SIZE_BINARY, NANOARROW_TYPE_STRING};
        for (int i = 0; i < 8; ++i) {
            ArrowSchema* c = schema.children[i];
            int rc = (i == 6) ? ArrowSchemaSetTypeFixedSize(c, NANOARROW_TYPE_FIXED_SIZE_BINARY, 4)
                              : ArrowSchemaSetType(c, static_cast<ArrowType>(types[i]));
            if (rc != NANOARROW_OK || ArrowSchemaSetName(c, names[i]) != NANOARROW_OK) die("col");
            c->flags = 0;
        }
        schema.flags = 0;
        if (ArrowArrayInitFromSchema(&array, &schema, nullptr) != NANOARROW_OK) die("array");
        if (ArrowArrayStartAppending(&array) != NANOARROW_OK) die("append");
        for (int r = 0; r < splits[chunk]; ++r) {
            const int i = base + r;
            ArrowArrayAppendInt(array.children[0], i * 1000 - 2000);
            ArrowArrayAppendInt(array.children[1], static_cast<int64_t>(i) * 1000000);
            ArrowArrayAppendUInt(array.children[2], 4000000000ull + i);
            ArrowArrayAppendDouble(array.children[3], i + 0.5);
            ArrowArrayAppendDouble(array.children[4], i * 1.25);
            ArrowArrayAppendInt(array.children[5], (i % 2 == 0) ? 1 : 0);
            std::array<uint8_t, 4> fsb = {static_cast<uint8_t>(i), static_cast<uint8_t>(i + 1),
                                          static_cast<uint8_t>(i + 2), static_cast<uint8_t>(i + 3)};
            ArrowBufferView bv{};
            bv.data.data = fsb.data();
            bv.size_bytes = 4;
            ArrowArrayAppendBytes(array.children[6], bv);
            ArrowStringView sv{kNames[i], static_cast<int64_t>(std::string(kNames[i]).size())};
            ArrowArrayAppendString(array.children[7], sv);
            if (ArrowArrayFinishElement(&array) != NANOARROW_OK) die("finish");
        }
        if (ArrowArrayFinishBuildingDefault(&array, nullptr) != NANOARROW_OK) die("build");

        int status = n2p_writer_write_batch(w, &schema, &array);
        if (status != N2P_OK) {
            std::fprintf(stderr, "demo: write_batch failed: %s\n", n2p_writer_last_error(w));
            ArrowArrayRelease(&array);
            ArrowSchemaRelease(&schema);
            n2p_writer_close(w);
            return 1;
        }
        ArrowArrayRelease(&array);
        ArrowSchemaRelease(&schema);
        base += splits[chunk];
    }
    if (n2p_writer_close(w) != N2P_OK) {
        std::fprintf(stderr, "demo: close failed\n");
        return 1;
    }
    std::printf("wrote %s (%d rows, 3 row groups)\n", path, kRows);
    return 0;
}

int main(int argc, char** argv) {
    const char* path = (argc > 1) ? argv[1] : "demo.parquet";
    if (argc > 2 && std::string(argv[2]) == "--streaming") {
        return run_streaming(path);
    }

    ArrowSchema schema{};
    ArrowArray array{};
    if (!build_batch(schema, array)) die("failed to build Arrow batch");

    char err[256] = {0};
    int status = n2p_write_file(path, &schema, &array, err, sizeof err);

    ArrowArrayRelease(&array);
    ArrowSchemaRelease(&schema);

    if (status != N2P_OK) {
        std::fprintf(stderr, "demo: write failed (%d): %s\n", status, err);
        return 1;
    }
    std::printf("wrote %s (%d rows)\n", path, kRows);
    return 0;
}
