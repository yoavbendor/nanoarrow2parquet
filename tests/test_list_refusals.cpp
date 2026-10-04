// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Yoav Bendor

// Lists and maps from a caller are untrusted structure: offsets that run backwards or past their
// child, a null where the schema says there is none, a sliced array. Each must be refused with an
// error, never read out of bounds (run this under ASan to see that). tests/oracle_roundtrip.py
// checks the valid lists; this checks the invalid ones.

#include "nanoarrow2parquet/nanoarrow2parquet.h"

#include <nanoarrow/nanoarrow.h>

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <functional>
#include <string>

static int failures = 0;

static void check(bool ok, const char* what) {
    if (!ok) {
        std::printf("FAIL: %s\n", what);
        ++failures;
    }
}

// A batch { l: list<int32 element> } with rows [1, 2], [], [3, 4, 5]; `element_nullable` sets the
// element field's nullability. The caller may corrupt it before writing.
static bool make_batch(ArrowSchema* schema, ArrowArray* array, bool element_nullable) {
    ArrowSchemaInit(schema);
    if (ArrowSchemaSetTypeStruct(schema, 1) != NANOARROW_OK) return false;
    ArrowSchema* l = schema->children[0];
    if (ArrowSchemaSetType(l, NANOARROW_TYPE_LIST) != NANOARROW_OK) return false;
    ArrowSchemaSetName(l, "l");
    ArrowSchemaSetType(l->children[0], NANOARROW_TYPE_INT32);
    ArrowSchemaSetName(l->children[0], "item");
    l->children[0]->flags = element_nullable ? ARROW_FLAG_NULLABLE : 0;
    schema->flags = 0;
    if (ArrowArrayInitFromSchema(array, schema, nullptr) != NANOARROW_OK) return false;
    if (ArrowArrayStartAppending(array) != NANOARROW_OK) return false;
    ArrowArray* la = array->children[0];
    const int rows[3][3] = {{1, 2, 0}, {0, 0, 0}, {3, 4, 5}};
    const int lens[3] = {2, 0, 3};
    for (int r = 0; r < 3; ++r) {
        for (int k = 0; k < lens[r]; ++k) ArrowArrayAppendInt(la->children[0], rows[r][k]);
        ArrowArrayFinishElement(la);
        ArrowArrayFinishElement(array);
    }
    return ArrowArrayFinishBuildingDefault(array, nullptr) == NANOARROW_OK;
}

// Write the batch after `corrupt`; true when the writer accepted it. The error goes to `err`.
static bool write_with(const char* path, bool element_nullable, const std::function<void(ArrowArray*)>& corrupt,
                       std::string& err) {
    ArrowSchema schema;
    ArrowArray array;
    if (!make_batch(&schema, &array, element_nullable)) {
        err = "could not build the batch";
        return false;
    }
    corrupt(&array);
    N2PWriter* w = nullptr;
    bool ok = n2p_writer_open(&w, path) == N2P_OK;
    if (ok) {
        ok = n2p_writer_write_batch(w, &schema, &array) == N2P_OK;
        if (!ok) err = n2p_writer_last_error(w);
    }
    n2p_writer_close(w);
    array.release(&array);
    schema.release(&schema);
    return ok;
}

static std::int32_t* offsets(ArrowArray* batch) {
    return const_cast<std::int32_t*>(static_cast<const std::int32_t*>(batch->children[0]->buffers[1]));
}

int main(int argc, char** argv) {
    const char* path = (argc > 1) ? argv[1] : "n2p_list_refusals.parquet";
    std::string err;

    check(write_with(path, true, [](ArrowArray*) {}, err), "a valid list batch is written");

    err.clear();
    check(!write_with(path, true, [](ArrowArray* b) { offsets(b)[2] = 1; /* [0,2,1,5]: row 1 runs backwards */ }, err),
          "offsets running backwards are refused");
    check(err.find("offsets") != std::string::npos, "  ... and the error names the offsets");

    err.clear();
    check(!write_with(path, true, [](ArrowArray* b) { offsets(b)[3] = 9; /* past the 5 child values */ }, err),
          "offsets past the child are refused");
    check(err.find("offsets") != std::string::npos, "  ... and the error names the offsets");

    err.clear();
    check(!write_with(path, true, [](ArrowArray* b) { offsets(b)[0] = -1; }, err),
          "a negative offset is refused");

    err.clear();
    check(!write_with(path, false, [](ArrowArray* b) {
              // element 1 marked null in a field declared non-nullable
              ArrowArray* items = b->children[0]->children[0];
              static std::uint8_t validity = 0xFD;
              items->buffers[0] = &validity;
              items->null_count = 1;
          }, err),
          "a null element in a non-nullable field is refused");
    check(err.find("null") != std::string::npos, "  ... and the error says so");

    err.clear();
    check(!write_with(path, true, [](ArrowArray* b) { b->children[0]->offset = 1; b->children[0]->length = 2; }, err),
          "a sliced list is refused");

    if (failures != 0) {
        std::printf("%d failure(s)\n", failures);
        return 1;
    }
    std::printf("list refusals: all checks passed\n");
    return 0;
}
