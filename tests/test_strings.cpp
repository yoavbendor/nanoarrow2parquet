// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Yoav Bendor

// Exercises the BYTE_ARRAY encoding chooser: a high-cardinality (all-distinct)
// string column should fall back to PLAIN, while a low-cardinality column stays
// RLE_DICTIONARY. The high-cardinality column is also nullable, so PLAIN is
// checked together with definition levels. tests/check_strings.py reads the
// file back and asserts both the values and the per-column encodings.

#include "nanoarrow2parquet/nanoarrow2parquet.h"

#include <nanoarrow/nanoarrow.h>

#include <cstdio>
#include <string>

static void append_str(ArrowArray* a, const std::string& s) {
    ArrowStringView v{s.data(), static_cast<int64_t>(s.size())};
    ArrowArrayAppendString(a, v);
}

int main(int argc, char** argv) {
    const char* path = (argc > 1) ? argv[1] : "n2p_strings.parquet";

    ArrowSchema schema;
    ArrowArray array;
    ArrowSchemaInit(&schema);
    if (ArrowSchemaSetTypeStruct(&schema, 3) != NANOARROW_OK) return 2;
    ArrowSchemaSetType(schema.children[0], NANOARROW_TYPE_STRING); ArrowSchemaSetName(schema.children[0], "hi");
    ArrowSchemaSetType(schema.children[1], NANOARROW_TYPE_STRING); ArrowSchemaSetName(schema.children[1], "lo");
    ArrowSchemaSetType(schema.children[2], NANOARROW_TYPE_INT32);  ArrowSchemaSetName(schema.children[2], "ctrl");
    schema.children[1]->flags = 0;  // REQUIRED
    schema.children[2]->flags = 0;  // REQUIRED
    schema.flags = 0;

    if (ArrowArrayInitFromSchema(&array, &schema, nullptr) != NANOARROW_OK) return 2;
    if (ArrowArrayStartAppending(&array) != NANOARROW_OK) return 2;

    const int n = 64;
    const char* palette[3] = {"red", "green", "blue"};
    for (int i = 0; i < n; ++i) {
        // hi: every value distinct -> PLAIN should win. A few nulls exercise
        // PLAIN together with definition levels.
        if (i % 17 == 5) ArrowArrayAppendNull(array.children[0], 1);
        else append_str(array.children[0], "unique-string-value-" + std::to_string(i));
        // lo: 3 distinct values repeated -> RLE_DICTIONARY should win.
        append_str(array.children[1], palette[i % 3]);
        ArrowArrayAppendInt(array.children[2], i);
        if (ArrowArrayFinishElement(&array) != NANOARROW_OK) return 2;
    }
    if (ArrowArrayFinishBuildingDefault(&array, nullptr) != NANOARROW_OK) return 2;

    char err[256] = {0};
    int rc = n2p_write_file(path, &schema, &array, err, sizeof err);
    ArrowArrayRelease(&array);
    ArrowSchemaRelease(&schema);
    if (rc != N2P_OK) { std::fprintf(stderr, "strings write failed: %s\n", err); return 1; }
    std::printf("wrote %s (high- and low-cardinality string columns)\n", path);
    return 0;
}
