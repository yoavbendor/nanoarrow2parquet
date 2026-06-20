// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Yoav Bendor

// Regression test for the "mixed-type corruption" bug report: a wide table mixing
// uint8/uint16/uint32/uint64/float32/float64, written with enough rows to cross
// the 2^14 (16384) boundary called out in the report, both REQUIRED and nullable.
// Builds canonical Arrow arrays via nanoarrow and writes two files; mirrored by
// tests/check_mixed_types.py, which asserts every value is byte-exact.

#include "nanoarrow2parquet/nanoarrow2parquet.h"

#include <nanoarrow/nanoarrow.h>

#include <cstdint>
#include <cstdio>
#include <cstring>

namespace {

constexpr int64_t kRows = 40000;  // > 16384

void set_leaf(ArrowSchema* s, ArrowType t, const char* name, bool required) {
    ArrowSchemaSetType(s, t);
    ArrowSchemaSetName(s, name);
    s->flags = required ? 0 : ARROW_FLAG_NULLABLE;
}

// Deterministic value generators (kept simple so check_mixed_types.py can mirror
// them exactly, including float32 rounding).
uint64_t u8a(int64_t i) { return static_cast<uint64_t>(i % 256); }
uint64_t u8b(int64_t i) { return static_cast<uint64_t>(1u << (i % 8)) & 0xFF; }
uint64_t u16v(int64_t i) { return static_cast<uint64_t>(i % 65536); }
uint64_t u32v(int64_t i) { return static_cast<uint32_t>(i * 2654435761u); }
uint64_t u64v(int64_t i) { return static_cast<uint64_t>(i) * 1099511628211ull; }
double f32a(int64_t i) { return static_cast<double>(static_cast<float>(i) * 0.1f); }
double f32b(int64_t i) { return static_cast<double>(-static_cast<float>(i) * 0.05f); }
double f64a(int64_t i) { return 3.14159265359 * static_cast<double>(i + 1); }
double f64b(int64_t i) { return 1e10 + static_cast<double>(i) * 100.0; }

int write_one(const char* path, bool required) {
    ArrowSchema schema;
    ArrowArray array;
    ArrowSchemaInit(&schema);
    ArrowSchemaSetTypeStruct(&schema, 9);
    set_leaf(schema.children[0], NANOARROW_TYPE_UINT8, "u8a", required);
    set_leaf(schema.children[1], NANOARROW_TYPE_UINT8, "u8b", required);
    set_leaf(schema.children[2], NANOARROW_TYPE_UINT16, "u16", required);
    set_leaf(schema.children[3], NANOARROW_TYPE_UINT32, "u32", required);
    set_leaf(schema.children[4], NANOARROW_TYPE_UINT64, "u64", required);
    set_leaf(schema.children[5], NANOARROW_TYPE_FLOAT, "f32a", required);
    set_leaf(schema.children[6], NANOARROW_TYPE_FLOAT, "f32b", required);
    set_leaf(schema.children[7], NANOARROW_TYPE_DOUBLE, "f64a", required);
    set_leaf(schema.children[8], NANOARROW_TYPE_DOUBLE, "f64b", required);
    schema.flags = 0;

    if (ArrowArrayInitFromSchema(&array, &schema, nullptr) != NANOARROW_OK) return 2;
    if (ArrowArrayStartAppending(&array) != NANOARROW_OK) return 2;

    for (int64_t i = 0; i < kRows; ++i) {
        // In the nullable file, null every 4th row (deterministic, crosses 16384).
        const bool nul = !required && (i % 4) == 2;
        if (nul) {
            for (int c = 0; c < 9; ++c) ArrowArrayAppendNull(array.children[c], 1);
        } else {
            ArrowArrayAppendUInt(array.children[0], u8a(i));
            ArrowArrayAppendUInt(array.children[1], u8b(i));
            ArrowArrayAppendUInt(array.children[2], u16v(i));
            ArrowArrayAppendUInt(array.children[3], u32v(i));
            ArrowArrayAppendUInt(array.children[4], u64v(i));
            ArrowArrayAppendDouble(array.children[5], f32a(i));
            ArrowArrayAppendDouble(array.children[6], f32b(i));
            ArrowArrayAppendDouble(array.children[7], f64a(i));
            ArrowArrayAppendDouble(array.children[8], f64b(i));
        }
        if (ArrowArrayFinishElement(&array) != NANOARROW_OK) return 2;
    }
    if (ArrowArrayFinishBuildingDefault(&array, nullptr) != NANOARROW_OK) return 2;

    char err[256] = {0};
    int rc = n2p_write_file(path, &schema, &array, err, sizeof err);
    array.release(&array);
    schema.release(&schema);
    if (rc != N2P_OK) { std::fprintf(stderr, "mixed write failed (%s): %s\n", path, err); return 1; }
    return 0;
}

}  // namespace

int main(int argc, char** argv) {
    const char* req = (argc > 1) ? argv[1] : "n2p_mixed.parquet";
    const char* nul = (argc > 2) ? argv[2] : "n2p_mixed_null.parquet";
    if (int rc = write_one(req, /*required=*/true)) return rc;
    if (int rc = write_one(nul, /*required=*/false)) return rc;
    std::printf("wrote %s and %s (%lld rows, mixed types)\n", req, nul, (long long)kRows);
    return 0;
}
