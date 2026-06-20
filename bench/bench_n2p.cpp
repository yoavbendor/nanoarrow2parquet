// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Yoav Bendor

// nanoarrow2parquet write benchmark. Streams `total_mb / chunk_mb` chunks through
// the streaming writer (one row group per chunk), timing data generation and
// writing separately. Emits one JSON result line (see bench_common.hpp).

#include "nanoarrow2parquet/nanoarrow2parquet.h"

#include <nanoarrow/nanoarrow.h>  // provides the Arrow C Data Interface structs

#include "bench_common.hpp"

#include <cstdio>

int main(int argc, char** argv) {
    const bench::Config cfg = bench::parse_config(argc, argv, "/tmp/n2p_bench.parquet");

    ArrowSchema schema{};
    bench::make_schema(&schema);

    N2PWriter* w = nullptr;
    if (n2p_writer_open(&w, cfg.out.c_str()) != N2P_OK) {
        std::fprintf(stderr, "n2p open failed: %s\n", w ? n2p_writer_last_error(w) : "?");
        n2p_writer_close(w);
        return 1;
    }
    if (!cfg.compress && n2p_writer_set_codec(w, N2P_CODEC_UNCOMPRESSED) != N2P_OK) {
        std::fprintf(stderr, "n2p set codec failed\n");
        n2p_writer_close(w);
        return 1;
    }

    const std::uint64_t rows = cfg.rows_per_chunk();
    double gen_s = 0, write_s = 0;

    for (std::uint64_t c = 0; c < cfg.num_chunks(); ++c) {
        ArrowArray batch{};
        auto t0 = bench::Clock::now();
        bench::make_chunk_array(/*seed=*/c + 1, rows, &batch);
        auto t1 = bench::Clock::now();

        int status = n2p_writer_write_batch(w, &schema, &batch);
        auto t2 = bench::Clock::now();

        gen_s += bench::secs(t1 - t0);
        write_s += bench::secs(t2 - t1);

        if (batch.release) batch.release(&batch);  // free this chunk before the next
        if (status != N2P_OK) {
            std::fprintf(stderr, "n2p write_batch failed: %s\n", n2p_writer_last_error(w));
            n2p_writer_close(w);
            return 1;
        }
    }

    if (n2p_writer_close(w) != N2P_OK) {
        std::fprintf(stderr, "n2p close/footer failed\n");
        return 1;
    }
    if (schema.release) schema.release(&schema);

    bench::print_result({"n2p", cfg, gen_s, write_s,
                         bench::file_size(cfg.out.c_str()), bench::peak_rss_kb()});
    return 0;
}
