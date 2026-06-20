// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Yoav Bendor

// Unit-level checks for nanoarrow2parquet. The end-to-end oracle (does pyarrow
// read back the exact values?) lives in tests/smoke_pyarrow_roundtrip.sh; here we
// exercise the low-level encoders and the file framing directly in C++.

#include "nanoarrow2parquet/nanoarrow2parquet.h"

#include "rle_bitpack.hpp"
#include "thrift_compact.hpp"

#include <nanoarrow/nanoarrow.h>

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <string>
#include <vector>

namespace {

int g_failures = 0;

void require(bool cond, const char* msg) {
    if (!cond) {
        std::fprintf(stderr, "FAIL: %s\n", msg);
        ++g_failures;
    }
}

// Decode a ULEB128 varint to validate the compact-protocol primitives.
std::uint64_t read_varint(const std::vector<std::uint8_t>& b, std::size_t& pos) {
    std::uint64_t v = 0;
    int shift = 0;
    while (true) {
        std::uint8_t byte = b[pos++];
        v |= static_cast<std::uint64_t>(byte & 0x7F) << shift;
        if ((byte & 0x80) == 0) break;
        shift += 7;
    }
    return v;
}

void test_varint() {
    std::vector<std::uint8_t> buf;
    n2p::CompactWriter w(buf);
    w.put_varint(0);
    w.put_varint(1);
    w.put_varint(127);
    w.put_varint(128);
    w.put_varint(300);
    w.put_varint(16384);
    std::size_t pos = 0;
    require(read_varint(buf, pos) == 0, "varint 0");
    require(read_varint(buf, pos) == 1, "varint 1");
    require(read_varint(buf, pos) == 127, "varint 127");
    require(read_varint(buf, pos) == 128, "varint 128");
    require(read_varint(buf, pos) == 300, "varint 300");
    require(read_varint(buf, pos) == 16384, "varint 16384");
    require(pos == buf.size(), "varint stream fully consumed");
}

void test_zigzag() {
    std::vector<std::uint8_t> buf;
    n2p::CompactWriter w(buf);
    // zigzag mapping: 0->0, -1->1, 1->2, -2->3, 2->4
    w.put_zigzag_i32(0);
    w.put_zigzag_i32(-1);
    w.put_zigzag_i32(1);
    w.put_zigzag_i32(-2);
    w.put_zigzag_i32(2);
    const std::vector<std::uint8_t> expected = {0, 1, 2, 3, 4};
    require(buf == expected, "zigzag i32 mapping");
}

void test_field_delta() {
    // Two consecutive small fields should use the single-byte delta form.
    std::vector<std::uint8_t> buf;
    n2p::CompactWriter w(buf);
    w.field_i32(1, 5);  // header byte (delta=1, type I32=5) then zigzag(5)=10
    w.field_i32(2, 7);  // header byte (delta=1, type I32=5) then zigzag(7)=14
    require(buf.size() == 4, "delta-encoded field headers are one byte each");
    require(buf[0] == ((1 << 4) | 5), "field 1 header nibble");
    require(buf[1] == 10, "field 1 value zigzag");
    require(buf[2] == ((1 << 4) | 5), "field 2 header nibble");
    require(buf[3] == 14, "field 2 value zigzag");
}

void test_bit_width() {
    require(n2p::dictionary_bit_width(0) == 0, "bit width of empty dict");
    require(n2p::dictionary_bit_width(1) == 0, "bit width of 1-entry dict");
    require(n2p::dictionary_bit_width(2) == 1, "bit width of 2-entry dict");
    require(n2p::dictionary_bit_width(3) == 2, "bit width of 3-entry dict");
    require(n2p::dictionary_bit_width(4) == 2, "bit width of 4-entry dict");
    require(n2p::dictionary_bit_width(5) == 3, "bit width of 5-entry dict");
    require(n2p::dictionary_bit_width(256) == 8, "bit width of 256-entry dict");
    require(n2p::dictionary_bit_width(257) == 9, "bit width of 257-entry dict");
}

void test_bit_pack() {
    // Three 2-bit values 1,2,3 -> 0b11_10_01 = 0x39 in the low byte (LSB-first).
    std::vector<std::uint32_t> vals = {1, 2, 3};
    std::vector<std::uint8_t> out;
    n2p::bit_pack(vals, 2, out);
    require(out.size() == 1, "3x2-bit values pack into one byte");
    require(out[0] == 0x39, "LSB-first bit packing");
}

// Build a small all-fixed batch and assert n2p_write_file produces a framed file.
bool build_batch(ArrowSchema& schema, ArrowArray& array) {
    ArrowSchemaInit(&schema);
    if (ArrowSchemaSetTypeStruct(&schema, 2) != NANOARROW_OK) return false;
    if (ArrowSchemaSetType(schema.children[0], NANOARROW_TYPE_INT64) != NANOARROW_OK) return false;
    if (ArrowSchemaSetName(schema.children[0], "id") != NANOARROW_OK) return false;
    if (ArrowSchemaSetType(schema.children[1], NANOARROW_TYPE_STRING) != NANOARROW_OK) return false;
    if (ArrowSchemaSetName(schema.children[1], "tag") != NANOARROW_OK) return false;
    schema.flags = 0;
    schema.children[0]->flags = 0;
    schema.children[1]->flags = 0;
    if (ArrowArrayInitFromSchema(&array, &schema, nullptr) != NANOARROW_OK) return false;
    if (ArrowArrayStartAppending(&array) != NANOARROW_OK) return false;
    const char* tags[3] = {"a", "b", "a"};
    for (int i = 0; i < 3; ++i) {
        if (ArrowArrayAppendInt(array.children[0], i) != NANOARROW_OK) return false;
        ArrowStringView sv{tags[i], 1};
        if (ArrowArrayAppendString(array.children[1], sv) != NANOARROW_OK) return false;
        if (ArrowArrayFinishElement(&array) != NANOARROW_OK) return false;
    }
    return ArrowArrayFinishBuildingDefault(&array, nullptr) == NANOARROW_OK;
}

void test_write_framing() {
    const std::string path = "test_roundtrip_out.parquet";
    ArrowSchema schema{};
    ArrowArray array{};
    require(build_batch(schema, array), "build test batch");

    char err[256] = {0};
    int status = n2p_write_file(path.c_str(), &schema, &array, err, sizeof err);
    require(status == N2P_OK, err[0] ? err : "n2p_write_file returned OK");
    ArrowArrayRelease(&array);
    ArrowSchemaRelease(&schema);

    std::ifstream f(path, std::ios::binary);
    require(f.is_open(), "output file exists");
    std::vector<char> bytes((std::istreambuf_iterator<char>(f)), {});
    require(bytes.size() > 12, "file has content");
    require(bytes[0] == 'P' && bytes[1] == 'A' && bytes[2] == 'R' && bytes[3] == '1',
            "leading PAR1 magic");
    const std::size_t n = bytes.size();
    require(bytes[n - 4] == 'P' && bytes[n - 3] == 'A' && bytes[n - 2] == 'R' &&
                bytes[n - 1] == '1',
            "trailing PAR1 magic");
    // footer length is the 4 bytes before the trailing magic; must be sane.
    std::uint32_t flen = static_cast<std::uint8_t>(bytes[n - 8]) |
                         (static_cast<std::uint8_t>(bytes[n - 7]) << 8) |
                         (static_cast<std::uint8_t>(bytes[n - 6]) << 16) |
                         (static_cast<std::uint32_t>(static_cast<std::uint8_t>(bytes[n - 5])) << 24);
    require(flen > 0 && flen < n, "footer length is in range");
    std::remove(path.c_str());
}

// Build a single INT32 column containing one value then one null. `nullable`
// controls the schema flag; returns the writer status.
int write_int32_with_null(bool nullable, const char* path) {
    ArrowSchema schema{};
    ArrowArray array{};
    ArrowSchemaInit(&schema);
    require(ArrowSchemaSetTypeStruct(&schema, 1) == NANOARROW_OK, "init struct");
    ArrowSchemaSetType(schema.children[0], NANOARROW_TYPE_INT32);
    ArrowSchemaSetName(schema.children[0], "x");
    if (!nullable) schema.children[0]->flags = 0;  // REQUIRED
    require(ArrowArrayInitFromSchema(&array, &schema, nullptr) == NANOARROW_OK, "init array");
    ArrowArrayStartAppending(&array);
    ArrowArrayAppendInt(array.children[0], 1);
    ArrowArrayFinishElement(&array);
    ArrowArrayAppendNull(array.children[0], 1);
    ArrowArrayFinishElement(&array);
    ArrowArrayFinishBuildingDefault(&array, nullptr);

    char err[256] = {0};
    int status = n2p_write_file(path, &schema, &array, err, sizeof err);
    ArrowArrayRelease(&array);
    ArrowSchemaRelease(&schema);
    return status;
}

void test_null_handling() {
    // A REQUIRED (non-nullable) column with actual nulls is rejected.
    int req = write_int32_with_null(/*nullable=*/false, "should_not_exist.parquet");
    require(req == N2P_INVALID_ARGUMENT, "REQUIRED column with nulls is rejected");
    std::remove("should_not_exist.parquet");

    // A nullable column with nulls is accepted (OPTIONAL, definition levels).
    int opt = write_int32_with_null(/*nullable=*/true, "nullable_ok.parquet");
    require(opt == N2P_OK, "nullable column with nulls is accepted");
    std::remove("nullable_ok.parquet");
}

}  // namespace

int main() {
    test_varint();
    test_zigzag();
    test_field_delta();
    test_bit_width();
    test_bit_pack();
    test_write_framing();
    test_null_handling();
    if (g_failures != 0) {
        std::fprintf(stderr, "%d check(s) failed\n", g_failures);
        return 1;
    }
    std::printf("all checks passed\n");
    return 0;
}
