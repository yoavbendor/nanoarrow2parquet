// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Yoav Bendor

// nanoarrow2parquet *compile-time SoA* write benchmark -- the counterpart to
// bench_n2p (which drives the runtime ArrowArray C API). Same data, same chunking
// (one row group per chunk), same JSON result line (lib="n2p_soa"), but the
// columns are produced in natural struct-of-arrays form (one std::vector per
// column; strings as std::string_view into the shared pools) and written through
// n2p::soa::Writer -- no nanoarrow, fixed-width columns aliased with no copy.
//
// The gen/write split is the interesting contrast vs bench_n2p: for fixed-width
// columns the SoA write just aliases the vectors, while string/bool columns are
// materialized (offsets+data / bit-pack) during write rather than during gen.

#include "nanoarrow2parquet/soa.hpp"  // provides the Arrow C structs + SoA API

#include "bench_common.hpp"

#include <cstdint>
#include <cstdio>
#include <string_view>
#include <vector>

namespace {

// Wrap a column for write_chunk: a nullable field takes present(values, mask); a
// required field takes the plain range (returned by reference, no copy).
template <bool N, class V>
decltype(auto) col(const V& v, const std::vector<std::uint8_t>& mask) {
    if constexpr (N) return n2p::soa::present(v, mask);
    else return (v);
}

std::vector<std::uint8_t> make_mask(bench::Lcg& rng, std::uint64_t n, int null_pct) {
    std::vector<std::uint8_t> m(n);
    for (std::uint64_t i = 0; i < n; ++i)
        m[i] = (static_cast<int>(rng.next() % 100) >= null_pct) ? 1 : 0;  // 1 == present
    return m;
}

std::uint64_t validity_bytes(const bench::Config& cfg, std::uint64_t n) {
    return cfg.nullable() ? static_cast<std::uint64_t>(cfg.num_cols()) * ((n + 7) / 8) : 0;
}

template <class Writer>
int finish(Writer& w, const bench::Config& cfg, std::uint64_t rows,
           std::uint64_t uncompressed, double gen_s, double write_s) {
    if (w.close() != N2P_OK) { std::fprintf(stderr, "n2p_soa close failed\n"); return 1; }
    bench::Result r{"n2p_soa", cfg, rows, uncompressed, gen_s, write_s,
                    bench::file_size(cfg.out.c_str()), bench::peak_rss_kb()};
    bench::print_result(r);
    return 0;
}

template <bool N>
int run_base(const bench::Config& cfg) {
    using namespace n2p::soa;
    using IdF = std::conditional_t<N, Nullable<"id", std::int64_t>, Field<"id", std::int64_t>>;
    using ValF = std::conditional_t<N, Nullable<"value", double>, Field<"value", double>>;
    using CatF = std::conditional_t<N, Nullable<"category", std::int32_t>, Field<"category", std::int32_t>>;
    Writer<IdF, ValF, CatF> w(cfg.out.c_str());
    if (!w.ok()) { std::fprintf(stderr, "n2p_soa open: %s\n", w.last_error()); return 1; }
    if (!cfg.compress) w.set_codec(N2P_CODEC_UNCOMPRESSED);

    const std::uint64_t n = cfg.rows_per_chunk();
    double gen_s = 0, write_s = 0;
    std::uint64_t uncompressed = 0;
    for (std::uint64_t c = 0; c < cfg.num_chunks(); ++c) {
        auto t0 = bench::Clock::now();
        std::vector<std::int64_t> id(n);
        std::vector<double> value(n);
        std::vector<std::int32_t> category(n);
        bench::Lcg rng(c + 1);
        for (std::uint64_t i = 0; i < n; ++i) {
            const std::uint64_t r = rng.next();
            id[i] = static_cast<std::int64_t>(r);
            value[i] = static_cast<double>(r % 1000000ull) * 0.001;
            category[i] = static_cast<std::int32_t>(r % 1000ull);
        }
        std::vector<std::uint8_t> m0, m1, m2;
        if constexpr (N) { m0 = make_mask(rng, n, cfg.null_pct); m1 = make_mask(rng, n, cfg.null_pct); m2 = make_mask(rng, n, cfg.null_pct); }
        auto t1 = bench::Clock::now();
        int st = w.write_chunk(col<N>(id, m0), col<N>(value, m1), col<N>(category, m2));
        auto t2 = bench::Clock::now();
        gen_s += bench::secs(t1 - t0);
        write_s += bench::secs(t2 - t1);
        uncompressed += n * 20 + validity_bytes(cfg, n);
        if (st != N2P_OK) { std::fprintf(stderr, "n2p_soa write: %s\n", w.last_error()); return 1; }
    }
    return finish(w, cfg, cfg.total_rows(), uncompressed, gen_s, write_s);
}

template <bool N>
int run_strings(const bench::Config& cfg) {
    using namespace n2p::soa;
    using IdF = std::conditional_t<N, Nullable<"id", std::int64_t>, Field<"id", std::int64_t>>;
    using ValF = std::conditional_t<N, Nullable<"value", double>, Field<"value", double>>;
    using CatF = std::conditional_t<N, Nullable<"category", std::int32_t>, Field<"category", std::int32_t>>;
    using LevF = std::conditional_t<N, Nullable<"level", utf8>, Field<"level", utf8>>;
    using PathF = std::conditional_t<N, Nullable<"path", utf8>, Field<"path", utf8>>;
    Writer<IdF, ValF, CatF, LevF, PathF> w(cfg.out.c_str());
    if (!w.ok()) { std::fprintf(stderr, "n2p_soa open: %s\n", w.last_error()); return 1; }
    if (!cfg.compress) w.set_codec(N2P_CODEC_UNCOMPRESSED);

    const auto& lp = bench::level_pool();
    const auto& pp = bench::path_pool();
    const std::uint64_t n = cfg.rows_per_chunk();
    double gen_s = 0, write_s = 0;
    std::uint64_t uncompressed = 0;
    for (std::uint64_t c = 0; c < cfg.num_chunks(); ++c) {
        auto t0 = bench::Clock::now();
        std::vector<std::int64_t> id(n);
        std::vector<double> value(n);
        std::vector<std::int32_t> category(n);
        std::vector<std::string_view> level(n), path(n);
        bench::Lcg rng(c + 1);
        std::uint64_t strbytes = 0;
        for (std::uint64_t i = 0; i < n; ++i) {
            const std::uint64_t r = rng.next();
            id[i] = static_cast<std::int64_t>(r);
            value[i] = static_cast<double>(r % 1000000ull) * 0.001;
            category[i] = static_cast<std::int32_t>(r % 1000ull);
            level[i] = lp[r % lp.size()];
            path[i] = pp[rng.next() % pp.size()];
            strbytes += level[i].size() + path[i].size();
        }
        std::vector<std::uint8_t> m[5];
        if constexpr (N) for (auto& m_ : m) m_ = make_mask(rng, n, cfg.null_pct);
        auto t1 = bench::Clock::now();
        int st = w.write_chunk(col<N>(id, m[0]), col<N>(value, m[1]), col<N>(category, m[2]),
                               col<N>(level, m[3]), col<N>(path, m[4]));
        auto t2 = bench::Clock::now();
        gen_s += bench::secs(t1 - t0);
        write_s += bench::secs(t2 - t1);
        uncompressed += n * 20 + strbytes + 2 * (n + 1) * sizeof(std::int32_t) + validity_bytes(cfg, n);
        if (st != N2P_OK) { std::fprintf(stderr, "n2p_soa write: %s\n", w.last_error()); return 1; }
    }
    return finish(w, cfg, cfg.total_rows(), uncompressed, gen_s, write_s);
}

template <bool N>
int run_wide(const bench::Config& cfg) {
    using namespace n2p::soa;
    using IdF = std::conditional_t<N, Nullable<"id", std::int64_t>, Field<"id", std::int64_t>>;
    using ValF = std::conditional_t<N, Nullable<"value", double>, Field<"value", double>>;
    using CatF = std::conditional_t<N, Nullable<"category", std::int32_t>, Field<"category", std::int32_t>>;
    using I8F = std::conditional_t<N, Nullable<"small", std::int8_t>, Field<"small", std::int8_t>>;
    using I16F = std::conditional_t<N, Nullable<"small16", std::int16_t>, Field<"small16", std::int16_t>>;
    using U32F = std::conditional_t<N, Nullable<"ucount", std::uint32_t>, Field<"ucount", std::uint32_t>>;
    using U64F = std::conditional_t<N, Nullable<"subtotal", std::uint64_t>, Field<"subtotal", std::uint64_t>>;
    using F32F = std::conditional_t<N, Nullable<"ratio", float>, Field<"ratio", float>>;
    using FlagF = std::conditional_t<N, Nullable<"flag", bool>, Field<"flag", bool>>;
    Writer<IdF, ValF, CatF, I8F, I16F, U32F, U64F, F32F, FlagF> w(cfg.out.c_str());
    if (!w.ok()) { std::fprintf(stderr, "n2p_soa open: %s\n", w.last_error()); return 1; }
    if (!cfg.compress) w.set_codec(N2P_CODEC_UNCOMPRESSED);

    const std::uint64_t n = cfg.rows_per_chunk();
    double gen_s = 0, write_s = 0;
    std::uint64_t uncompressed = 0;
    for (std::uint64_t c = 0; c < cfg.num_chunks(); ++c) {
        auto t0 = bench::Clock::now();
        std::vector<std::int64_t> id(n);
        std::vector<double> value(n);
        std::vector<std::int32_t> category(n);
        std::vector<std::int8_t> small(n);
        std::vector<std::int16_t> small16(n);
        std::vector<std::uint32_t> ucount(n);
        std::vector<std::uint64_t> subtotal(n);
        std::vector<float> ratio(n);
        std::vector<bool> flag(n);
        bench::Lcg rng(c + 1);
        for (std::uint64_t i = 0; i < n; ++i) {
            const std::uint64_t r = rng.next();
            id[i] = static_cast<std::int64_t>(r);
            value[i] = static_cast<double>(r % 1000000ull) * 0.001;
            category[i] = static_cast<std::int32_t>(r % 1000ull);
            small[i] = static_cast<std::int8_t>(r);
            small16[i] = static_cast<std::int16_t>(r >> 8);
            ucount[i] = static_cast<std::uint32_t>(r);
            subtotal[i] = r;
            ratio[i] = static_cast<float>(r % 100000ull) * 0.01f;
            flag[i] = (r & 1) != 0;
        }
        std::vector<std::uint8_t> m[9];
        if constexpr (N) for (auto& m_ : m) m_ = make_mask(rng, n, cfg.null_pct);
        auto t1 = bench::Clock::now();
        int st = w.write_chunk(col<N>(id, m[0]), col<N>(value, m[1]), col<N>(category, m[2]),
                               col<N>(small, m[3]), col<N>(small16, m[4]), col<N>(ucount, m[5]),
                               col<N>(subtotal, m[6]), col<N>(ratio, m[7]), col<N>(flag, m[8]));
        auto t2 = bench::Clock::now();
        gen_s += bench::secs(t1 - t0);
        write_s += bench::secs(t2 - t1);
        uncompressed += n * (20 + 1 + 2 + 4 + 8 + 4) + (n + 7) / 8 + validity_bytes(cfg, n);
        if (st != N2P_OK) { std::fprintf(stderr, "n2p_soa write: %s\n", w.last_error()); return 1; }
    }
    return finish(w, cfg, cfg.total_rows(), uncompressed, gen_s, write_s);
}

}  // namespace

int main(int argc, char** argv) {
    const bench::Config cfg = bench::parse_config(argc, argv, "/tmp/n2p_soa_bench.parquet");
    if (cfg.wide) return cfg.nullable() ? run_wide<true>(cfg) : run_wide<false>(cfg);
    if (cfg.strings) return cfg.nullable() ? run_strings<true>(cfg) : run_strings<false>(cfg);
    return cfg.nullable() ? run_base<true>(cfg) : run_base<false>(cfg);
}
