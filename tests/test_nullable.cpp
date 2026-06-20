// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Yoav Bendor

// Writes a batch exercising OPTIONAL (nullable) columns and the Arrow null type
// to a .parquet file. tests/check_nullable.py reads it back and asserts the null
// positions. Mirrors the deterministic pattern the checker expects.

#include "nanoarrow2parquet/nanoarrow2parquet.h"

#include <nanoarrow/nanoarrow.h>

#include <cstdio>
#include <cstring>

int main(int argc, char** argv) {
    const char* path = (argc > 1) ? argv[1] : "n2p_nulls.parquet";

    ArrowSchema schema;
    ArrowArray array;
    ArrowSchemaInit(&schema);
    if (ArrowSchemaSetTypeStruct(&schema, 5) != NANOARROW_OK) return 2;
    // nanoarrow marks children nullable by default; only `ctrl` is cleared.
    ArrowSchemaSetType(schema.children[0], NANOARROW_TYPE_INT64);  ArrowSchemaSetName(schema.children[0], "a");
    ArrowSchemaSetType(schema.children[1], NANOARROW_TYPE_STRING); ArrowSchemaSetName(schema.children[1], "s");
    ArrowSchemaSetType(schema.children[2], NANOARROW_TYPE_BOOL);   ArrowSchemaSetName(schema.children[2], "f");
    ArrowSchemaSetType(schema.children[3], NANOARROW_TYPE_NA);     ArrowSchemaSetName(schema.children[3], "nul");
    ArrowSchemaSetType(schema.children[4], NANOARROW_TYPE_INT32);  ArrowSchemaSetName(schema.children[4], "ctrl");
    schema.children[4]->flags = 0;  // REQUIRED
    schema.flags = 0;

    if (ArrowArrayInitFromSchema(&array, &schema, nullptr) != NANOARROW_OK) return 2;
    if (ArrowArrayStartAppending(&array) != NANOARROW_OK) return 2;

    const int n = 6;
    const bool a_null[6] = {0, 1, 0, 1, 0, 0};
    const bool s_null[6] = {1, 0, 0, 0, 1, 0};
    const bool f_null[6] = {0, 0, 1, 0, 0, 0};
    const char* svals[6] = {"x", "alpha", "beta", "alpha", "y", "beta"};
    for (int i = 0; i < n; ++i) {
        if (a_null[i]) ArrowArrayAppendNull(array.children[0], 1);
        else ArrowArrayAppendInt(array.children[0], static_cast<int64_t>(i) * 100);
        if (s_null[i]) ArrowArrayAppendNull(array.children[1], 1);
        else { ArrowStringView sv{svals[i], static_cast<int64_t>(std::strlen(svals[i]))};
               ArrowArrayAppendString(array.children[1], sv); }
        if (f_null[i]) ArrowArrayAppendNull(array.children[2], 1);
        else ArrowArrayAppendInt(array.children[2], i % 2);
        ArrowArrayAppendNull(array.children[3], 1);             // null type: always null
        ArrowArrayAppendInt(array.children[4], i);              // REQUIRED control
        if (ArrowArrayFinishElement(&array) != NANOARROW_OK) return 2;
    }
    if (ArrowArrayFinishBuildingDefault(&array, nullptr) != NANOARROW_OK) return 2;

    char err[256] = {0};
    int rc = n2p_write_file(path, &schema, &array, err, sizeof err);
    ArrowArrayRelease(&array);
    ArrowSchemaRelease(&schema);
    if (rc != N2P_OK) { std::fprintf(stderr, "nullable write failed: %s\n", err); return 1; }
    std::printf("wrote %s (nullable + null-type columns)\n", path);
    return 0;
}
