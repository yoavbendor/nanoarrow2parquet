// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Yoav Bendor

// Writes a batch with a nested (struct) column to a .parquet file;
// tests/check_struct.py reads it back and asserts the nested values + nulls,
// including the two-level definition case (a null leaf inside a present struct
// vs. a wholly-null struct).

#include "nanoarrow2parquet/nanoarrow2parquet.h"

#include <nanoarrow/nanoarrow.h>

#include <cstdio>
#include <cstring>

static void append_str(ArrowArray* a, const char* s) {
    ArrowStringView v{s, static_cast<int64_t>(std::strlen(s))};
    ArrowArrayAppendString(a, v);
}

int main(int argc, char** argv) {
    const char* path = (argc > 1) ? argv[1] : "n2p_structs.parquet";

    ArrowSchema schema;
    ArrowArray array;
    ArrowSchemaInit(&schema);
    if (ArrowSchemaSetTypeStruct(&schema, 3) != NANOARROW_OK) return 2;
    ArrowSchemaSetType(schema.children[0], NANOARROW_TYPE_INT64);
    ArrowSchemaSetName(schema.children[0], "id"); schema.children[0]->flags = 0;
    // addr: nullable struct { city (utf8, nullable), zip (int32, REQUIRED) }
    ArrowSchemaSetTypeStruct(schema.children[1], 2);
    ArrowSchemaSetName(schema.children[1], "addr");
    ArrowSchemaSetType(schema.children[1]->children[0], NANOARROW_TYPE_STRING);
    ArrowSchemaSetName(schema.children[1]->children[0], "city");
    ArrowSchemaSetType(schema.children[1]->children[1], NANOARROW_TYPE_INT32);
    ArrowSchemaSetName(schema.children[1]->children[1], "zip");
    schema.children[1]->children[1]->flags = 0;
    ArrowSchemaSetType(schema.children[2], NANOARROW_TYPE_DOUBLE);
    ArrowSchemaSetName(schema.children[2], "score"); schema.children[2]->flags = 0;
    schema.flags = 0;

    if (ArrowArrayInitFromSchema(&array, &schema, nullptr) != NANOARROW_OK) return 2;
    if (ArrowArrayStartAppending(&array) != NANOARROW_OK) return 2;
    ArrowArray* addr = array.children[1];

    // row0: id=10 addr={NYC,10001} score=1.5
    ArrowArrayAppendInt(array.children[0], 10);
    append_str(addr->children[0], "NYC"); ArrowArrayAppendInt(addr->children[1], 10001);
    ArrowArrayFinishElement(addr); ArrowArrayAppendDouble(array.children[2], 1.5);
    ArrowArrayFinishElement(&array);
    // row1: id=20 addr=NULL score=2.5
    ArrowArrayAppendInt(array.children[0], 20);
    ArrowArrayAppendNull(addr, 1); ArrowArrayAppendDouble(array.children[2], 2.5);
    ArrowArrayFinishElement(&array);
    // row2: id=30 addr={NULL,30003} score=3.5  (struct present, city null)
    ArrowArrayAppendInt(array.children[0], 30);
    ArrowArrayAppendNull(addr->children[0], 1); ArrowArrayAppendInt(addr->children[1], 30003);
    ArrowArrayFinishElement(addr); ArrowArrayAppendDouble(array.children[2], 3.5);
    ArrowArrayFinishElement(&array);
    // row3: id=40 addr={LA,40004} score=4.5
    ArrowArrayAppendInt(array.children[0], 40);
    append_str(addr->children[0], "LA"); ArrowArrayAppendInt(addr->children[1], 40004);
    ArrowArrayFinishElement(addr); ArrowArrayAppendDouble(array.children[2], 4.5);
    ArrowArrayFinishElement(&array);

    ArrowError e;
    if (ArrowArrayFinishBuildingDefault(&array, &e) != NANOARROW_OK) {
        std::fprintf(stderr, "build: %s\n", e.message); return 2;
    }

    char err[256] = {0};
    int rc = n2p_write_file(path, &schema, &array, err, sizeof err);
    ArrowArrayRelease(&array);
    ArrowSchemaRelease(&schema);
    if (rc != N2P_OK) { std::fprintf(stderr, "struct write failed: %s\n", err); return 1; }
    std::printf("wrote %s (nested struct column)\n", path);
    return 0;
}
