// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Yoav Bendor

// The s3:// output of the streaming writer (built with N2P_S3=ON). The same batches are written to a local file and
// to an S3 object; the object, read back with nanos3reader, must be byte-identical to the local file. A bucket that
// does not exist must fail the close and leave no object.
//
// Needs an S3-compatible endpoint (moto in CI). N2P_S3_TEST_PREFIX=s3://bucket/prefix/ names where to write; without
// it the test exits 77 (CTest "skipped"). Credentials and AWS_ENDPOINT_URL come from the environment, as for nanos3reader.

#include "nanoarrow2parquet/nanoarrow2parquet.h"

#include <nanoarrow/nanoarrow.h>
#include <nanos3reader/s3_range.h>

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <iterator>
#include <memory>
#include <string>
#include <vector>

namespace {

constexpr int kSkip = 77;

// id: int64, name: utf8 (distinct values, with nulls), score: float64. The schema is shared by every batch.
bool build_schema(ArrowSchema& schema) {
    ArrowSchemaInit(&schema);
    if (ArrowSchemaSetTypeStruct(&schema, 3) != NANOARROW_OK) return false;
    ArrowSchemaSetType(schema.children[0], NANOARROW_TYPE_INT64);  ArrowSchemaSetName(schema.children[0], "id");
    ArrowSchemaSetType(schema.children[1], NANOARROW_TYPE_STRING); ArrowSchemaSetName(schema.children[1], "name");
    ArrowSchemaSetType(schema.children[2], NANOARROW_TYPE_DOUBLE); ArrowSchemaSetName(schema.children[2], "score");
    return true;
}

bool build_batch(const ArrowSchema& schema, int64_t first, int64_t rows, ArrowArray& array) {
    if (ArrowArrayInitFromSchema(&array, &schema, nullptr) != NANOARROW_OK) return false;
    if (ArrowArrayStartAppending(&array) != NANOARROW_OK) return false;
    for (int64_t i = first; i < first + rows; ++i) {
        ArrowArrayAppendInt(array.children[0], i);
        if (i % 11 == 3) {
            ArrowArrayAppendNull(array.children[1], 1);
        } else {
            std::string s = "name-" + std::to_string(i * 2654435761ULL % 1000000007ULL);
            ArrowStringView v{s.data(), static_cast<int64_t>(s.size())};
            ArrowArrayAppendString(array.children[1], v);
        }
        ArrowArrayAppendDouble(array.children[2], static_cast<double>(i) * 0.25);
        if (ArrowArrayFinishElement(&array) != NANOARROW_OK) return false;
    }
    return ArrowArrayFinishBuildingDefault(&array, nullptr) == NANOARROW_OK;
}

// Writes `batches` batches of `rows_per_batch` rows to `path`. Returns the writer's status; `error` gets its message.
int write_file(const char* path, const ArrowSchema& schema, int batches, int64_t rows_per_batch, std::string& error) {
    N2PWriter* w = nullptr;
    int status = n2p_writer_open(&w, path);
    if (status != N2P_OK) {
        error = n2p_writer_last_error(w);
        n2p_writer_close(w);
        return status;
    }
    for (int b = 0; b < batches; ++b) {
        ArrowArray array;
        if (!build_batch(schema, static_cast<int64_t>(b) * rows_per_batch, rows_per_batch, array)) {
            error = "could not build a batch";
            n2p_writer_close(w);
            return 2;
        }
        status = n2p_writer_write_batch(w, &schema, &array);
        ArrowArrayRelease(&array);
        if (status != N2P_OK) {
            error = n2p_writer_last_error(w);
            n2p_writer_close(w);
            return status;
        }
    }
    status = n2p_writer_close(w);
    if (status != N2P_OK) error = "close failed";
    return status;
}

int failures = 0;
#define CHECK(cond)                                                          \
    do {                                                                     \
        if (!(cond)) {                                                       \
            std::fprintf(stderr, "%s:%d: FAIL %s\n", __FILE__, __LINE__, #cond); \
            ++failures;                                                      \
        }                                                                    \
    } while (0)

}  // namespace

int main(int argc, char** argv) {
    const char* prefix = std::getenv("N2P_S3_TEST_PREFIX");
    if (prefix == nullptr || *prefix == '\0') {
        std::printf("s3 output: N2P_S3_TEST_PREFIX not set; skipped\n");
        return kSkip;
    }
    const int64_t rows_per_batch = (argc > 1) ? std::atoll(argv[1]) : 500000;  // ~20 MB per batch

    ArrowSchema schema;
    if (!build_schema(schema)) return 2;

    const std::string local = "n2p_s3_test_local.parquet";
    const std::string key_uri = std::string(prefix) + "n2p_s3_test.parquet";
    std::string error;

    CHECK(write_file(local.c_str(), schema, 4, rows_per_batch, error) == N2P_OK);
    CHECK(write_file(key_uri.c_str(), schema, 4, rows_per_batch, error) == N2P_OK);
    if (!error.empty()) std::fprintf(stderr, "s3 write: %s\n", error.c_str());

    // Read the local file and the object back; the object must be the same bytes.
    std::vector<std::uint8_t> want;
    {
        std::ifstream in(local, std::ios::binary);
        want.assign(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
    }
    auto client = std::make_shared<nanos3reader::S3Client>();
    std::uint64_t size = 0;
    std::string err;
    CHECK(client->head(key_uri, size, err));
    CHECK(size == want.size());
    if (size == want.size()) {
        std::vector<std::uint8_t> got(static_cast<std::size_t>(size));
        const std::uint64_t chunk = 8U << 20U;
        for (std::uint64_t off = 0; off < size; off += chunk) {
            const std::uint64_t n = std::min<std::uint64_t>(chunk, size - off);
            CHECK(client->read_range(key_uri, off, n, got.data() + off, err));
        }
        CHECK(got == want);
    }
    std::printf("wrote %s (%zu bytes, local and s3)\n", key_uri.c_str(), want.size());

    // A bucket that does not exist: the close must fail and no object may appear.
    const std::string missing = "s3://n2p-no-such-bucket-7f3a/" + std::string("x.parquet");
    std::string missing_error;
    const int status = write_file(missing.c_str(), schema, 1, 1000, missing_error);
    CHECK(status != N2P_OK);
    std::uint64_t msize = 0;
    CHECK(!client->head(missing, msize, err));

    ArrowSchemaRelease(&schema);
    if (failures != 0) {
        std::fprintf(stderr, "%d check(s) failed\n", failures);
        return 1;
    }
    std::printf("s3 output tests passed (failed-bucket case: %s)\n", missing_error.c_str());
    return 0;
}
