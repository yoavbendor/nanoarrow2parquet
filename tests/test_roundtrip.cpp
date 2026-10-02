// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Yoav Bendor

// Unit-level checks for nanoarrow2parquet. The end-to-end oracles (do pyarrow, arrow-rs and
// parquet2nanoarrow read back the exact values?) live in tests/oracle_roundtrip.py and
// tests/smoke_pyarrow_roundtrip.sh; here we exercise the low-level encoders and the file framing
// directly in C++. The Thrift metadata is nanom's (its encoder is tested in nanom); here every
// written footer and page header is decoded back through the same nanom model.

#include "nanoarrow2parquet/nanoarrow2parquet.h"

#include "rle_bitpack.hpp"
#include "parquet_types.hpp"

#include <nanoarrow/nanoarrow.h>

#include <cstdint>
#include <span>
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

    // The footer and every page header decode through nanom's model (what readers use), and they
    // say what was written: 3 rows, schema root + 2 leaves, and page sizes that tile each chunk.
    namespace pq = n2p::pq;
    const auto file = std::span<const std::byte>(reinterpret_cast<const std::byte*>(bytes.data()), n);
    auto md = pq::read_file_metadata(nanom::from(file));
    require(md.has_value(), "footer decodes through nanom");
    if (md) {
        const pq::FileMetaData& m = md->value;
        require(*m.num_rows == 3 && m.schema->size() == 3 && m.row_groups->size() == 1, "footer contents");
        require(m.created_by->has_value() && **m.created_by == "nanoarrow2parquet", "created_by");
        auto rg = m.row_groups->at(0);
        require(rg && *rg->num_rows == 3 && rg->columns->size() == 2, "row group contents");
        if (rg) {
            (void)rg->columns->for_each([&](const pq::ColumnChunk& cc) {
                const auto& c = **cc.meta_data;
                // walk the chunk's pages: headers decode, sizes add up to the chunk's total
                std::int64_t at = c.dictionary_page_offset->value_or(*c.data_page_offset);
                const std::int64_t end = at + *c.total_compressed_size;
                std::int64_t values = 0;
                while (at < end) {
                    auto ph = nanom::thrift_compact<pq::PageHeader>()(
                        nanom::from(file.subspan(static_cast<std::size_t>(at))));
                    require(ph.has_value(), "page header decodes through nanom");
                    if (!ph) return;
                    const auto hdr_len = static_cast<std::int64_t>(ph->rest.first - (file.data() + at));
                    if (ph->value.data_page_header->has_value())
                        values += *(**ph->value.data_page_header).num_values;
                    at += hdr_len + *ph->value.compressed_page_size;
                }
                require(at == end, "pages tile the column chunk exactly");
                require(values == *c.num_values, "data pages hold the chunk's values");
            });
        }
    }
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
