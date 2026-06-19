// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Yoav Bendor

// Compiles and exercises the amalgamated single-header build. This translation
// unit defines NANOARROW2PARQUET_IMPLEMENTATION, so it pulls in the full writer
// implementation from single_include/nanoarrow2parquet.h. A second TU
// (test_single_header_decls.cpp) includes the same header WITHOUT the macro to
// prove the declaration-only path links against the symbols defined here.

#define NANOARROW2PARQUET_IMPLEMENTATION
#include "nanoarrow2parquet.h"

#include <nanoarrow/nanoarrow.h>

#include <cstdint>
#include <cstdio>
#include <string>

namespace {

constexpr int kRows = 4;
const char* const kNames[kRows] = {"x", "yy", "x", "zzz"};

bool build_batch(ArrowSchema& schema, ArrowArray& array) {
    ArrowSchemaInit(&schema);
    if (ArrowSchemaSetTypeStruct(&schema, 2) != NANOARROW_OK) return false;
    if (ArrowSchemaSetType(schema.children[0], NANOARROW_TYPE_INT64) != NANOARROW_OK) return false;
    if (ArrowSchemaSetName(schema.children[0], "id") != NANOARROW_OK) return false;
    schema.children[0]->flags = 0;
    if (ArrowSchemaSetType(schema.children[1], NANOARROW_TYPE_STRING) != NANOARROW_OK) return false;
    if (ArrowSchemaSetName(schema.children[1], "name") != NANOARROW_OK) return false;
    schema.children[1]->flags = 0;
    schema.flags = 0;

    if (ArrowArrayInitFromSchema(&array, &schema, nullptr) != NANOARROW_OK) return false;
    if (ArrowArrayStartAppending(&array) != NANOARROW_OK) return false;
    for (int i = 0; i < kRows; ++i) {
        if (ArrowArrayAppendInt(array.children[0], i) != NANOARROW_OK) return false;
        ArrowStringView sv{kNames[i], static_cast<int64_t>(std::string(kNames[i]).size())};
        if (ArrowArrayAppendString(array.children[1], sv) != NANOARROW_OK) return false;
        if (ArrowArrayFinishElement(&array) != NANOARROW_OK) return false;
    }
    return ArrowArrayFinishBuildingDefault(&array, nullptr) == NANOARROW_OK;
}

}  // namespace

// Defined here, declared (and called) from the declaration-only TU.
const char* single_header_path() { return "single_header_test.parquet"; }

int main() {
    ArrowSchema schema{};
    ArrowArray array{};
    if (!build_batch(schema, array)) {
        std::fprintf(stderr, "single-header test: failed to build Arrow batch\n");
        return 1;
    }

    char err[256] = {0};
    int status = n2p_write_file(single_header_path(), &schema, &array, err, sizeof err);

    ArrowArrayRelease(&array);
    ArrowSchemaRelease(&schema);

    if (status != N2P_OK) {
        std::fprintf(stderr, "single-header test: write failed (%d): %s\n", status, err);
        return 1;
    }
    std::printf("single-header build OK: wrote %s (%d rows)\n", single_header_path(), kRows);
    return 0;
}
