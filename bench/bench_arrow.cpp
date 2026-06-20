// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Yoav Bendor

// Apache Parquet C++ write benchmark -- the comparison baseline for bench_n2p.
// Streams the same deterministic chunks (one row group each) through
// parquet::arrow::FileWriter, timing generation and writing separately.
//
// Fairness knobs (mirroring carquet's Arrow benchmark): dictionary disabled so
// the numeric columns use PLAIN like n2p; ZSTD level 3 to match n2p's hardcoded
// level; max_row_group_length == rows-per-chunk so each imported batch becomes
// exactly one row group. Chunks are imported zero-copy from the C Data Interface
// (arrow::ImportRecordBatch), so data generation is identical to the n2p run.

#include <arrow/api.h>
#include <arrow/c/bridge.h>
#include <arrow/io/file.h>
#include <parquet/arrow/writer.h>
#include <parquet/properties.h>

#include "bench_common.hpp"  // uses the C Data Interface structs from arrow/c/abi.h

#include <cstdio>
#include <memory>

namespace {

#define CHECK_OK(expr, what)                                                  \
    do {                                                                      \
        auto _st = (expr);                                                    \
        if (!_st.ok()) {                                                      \
            std::fprintf(stderr, "%s: %s\n", what, _st.ToString().c_str());   \
            std::exit(1);                                                     \
        }                                                                     \
    } while (0)

template <typename T>
T unwrap(arrow::Result<T> r, const char* what) {
    if (!r.ok()) {
        std::fprintf(stderr, "%s: %s\n", what, r.status().ToString().c_str());
        std::exit(1);
    }
    return std::move(r).ValueUnsafe();
}

}  // namespace

int main(int argc, char** argv) {
    const bench::Config cfg = bench::parse_config(argc, argv, "/tmp/arrow_bench.parquet");

    // Import the shared schema once (consumes the C schema struct).
    ArrowSchema schema_c{};
    bench::make_schema(cfg, &schema_c);
    std::shared_ptr<arrow::Schema> schema =
        unwrap(arrow::ImportSchema(&schema_c), "ImportSchema");

    // Writer properties: numeric columns PLAIN (no dictionary) like n2p; string
    // columns dictionary-encoded like n2p (which always RLE_DICTIONARYs strings).
    // Codec + level matched, one row group per chunk.
    parquet::WriterProperties::Builder pb;
    pb.disable_dictionary();                 // default off (numerics PLAIN)
    if (cfg.strings) {
        pb.enable_dictionary("level");       // match n2p's string path
        pb.enable_dictionary("path");
    }
    pb.max_row_group_length(static_cast<std::int64_t>(cfg.rows_per_chunk()));
    if (cfg.compress) {
        pb.compression(parquet::Compression::ZSTD);
        pb.compression_level(3);
    } else {
        pb.compression(parquet::Compression::UNCOMPRESSED);
    }
    auto props = pb.build();
    auto arrow_props = parquet::ArrowWriterProperties::Builder().build();

    auto sink = unwrap(arrow::io::FileOutputStream::Open(cfg.out), "Open output");
    auto writer = unwrap(
        parquet::arrow::FileWriter::Open(*schema, arrow::default_memory_pool(), sink,
                                         props, arrow_props),
        "FileWriter::Open");

    const std::uint64_t rows = cfg.rows_per_chunk();
    double gen_s = 0, write_s = 0;
    std::uint64_t uncompressed = 0;

    for (std::uint64_t c = 0; c < cfg.num_chunks(); ++c) {
        ArrowArray batch_c{};
        auto t0 = bench::Clock::now();
        uncompressed += bench::make_chunk_array(cfg, /*seed=*/c + 1, rows, &batch_c);
        auto t1 = bench::Clock::now();

        // Zero-copy wrap + write one row group; rb releases the chunk at scope end.
        {
            auto rb = unwrap(arrow::ImportRecordBatch(&batch_c, schema), "ImportRecordBatch");
            CHECK_OK(writer->WriteRecordBatch(*rb), "WriteRecordBatch");
        }
        auto t2 = bench::Clock::now();

        gen_s += bench::secs(t1 - t0);
        write_s += bench::secs(t2 - t1);
    }

    CHECK_OK(writer->Close(), "writer Close");
    CHECK_OK(sink->Close(), "sink Close");

    bench::print_result({"arrow", cfg, cfg.total_rows(), uncompressed, gen_s, write_s,
                         bench::file_size(cfg.out.c_str()), bench::peak_rss_kb()});
    return 0;
}
