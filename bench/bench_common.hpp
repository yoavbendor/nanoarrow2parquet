// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Yoav Bendor

// Shared benchmark scaffolding for comparing nanoarrow2parquet against the Apache
// Parquet C++ writer. Methodology mirrors carquet's benchmark/ (deterministic LCG
// data, write timed separately from data generation, fixed row-group sizing, file
// size reported) but adds larger-than-RAM *streaming*: data is produced one chunk
// at a time, written as one row group, and freed before the next chunk -- so the
// dataset total (1/4/8 GB) far exceeds peak resident memory (~one chunk).
//
// Columns (default, --strings): a mix that exercises both code paths --
//   id        int64    PLAIN fixed-width (memcpy)
//   value     double   PLAIN fixed-width
//   category  int32    PLAIN fixed-width
//   level     utf8     RLE_DICTIONARY  (pool of 6 short categories: INFO/WARN/...)
//   path      utf8     RLE_DICTIONARY  (pool of 64 file-path-like strings)
// The two string columns are low-cardinality and assigned at random per row --
// the case dictionary encoding is built for (real workloads: log levels, file
// paths, enum-ish categories). With --no-strings only the three numeric columns
// are written (carquet-parity, no dictionary), for the pure fixed-width story.
//
// Two phases are timed independently for every chunk:
//   * gen   -- materialize a chunk into the Arrow C Data Interface.
//   * write -- serialize one row group, then free the chunk.
//
// REQUIREMENT: the including TU must already provide the Arrow C Data Interface
// struct definitions (ArrowSchema / ArrowArray). Both nanoarrow.h and
// arrow/c/abi.h define them behind the shared ARROW_C_DATA_INTERFACE guard.

#pragma once

#include <sys/resource.h>

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

namespace bench {

// ---- string pools (low cardinality, dictionary-friendly) ------------------

inline const std::vector<std::string>& level_pool() {
    static const std::vector<std::string> p = {
        "DEBUG", "INFO", "WARN", "ERROR", "TRACE", "FATAL"};
    return p;
}

// 64 distinct file-path-like strings -- the kind of short, repetitive string
// that shows up as a category in real captures/logs.
inline const std::vector<std::string>& path_pool() {
    static const std::vector<std::string> p = [] {
        std::vector<std::string> v;
        v.reserve(64);
        char buf[80];
        for (int i = 0; i < 64; ++i) {
            std::snprintf(buf, sizeof buf,
                          "/var/log/svc%02d/2026-06-20/shard-%04d.parquet", i % 16, i);
            v.emplace_back(buf);
        }
        return v;
    }();
    return p;
}

inline double avg_len(const std::vector<std::string>& pool) {
    std::size_t total = 0;
    for (const auto& s : pool) total += s.size();
    return pool.empty() ? 0.0 : static_cast<double>(total) / pool.size();
}

// ---- config ---------------------------------------------------------------

struct Config {
    std::uint64_t total_mb = 1024;
    std::uint64_t chunk_mb = 200;
    bool compress = true;
    bool strings = true;             // include the dictionary-encoded string cols
    std::string out = "/tmp/n2p_bench.parquet";

    int num_cols() const { return strings ? 5 : 3; }

    // Estimated bytes/row for chunk sizing: fixed widths plus, for string columns,
    // the average value length + a 4-byte offset entry.
    double est_row_bytes() const {
        double b = sizeof(std::int64_t) + sizeof(double) + sizeof(std::int32_t);  // 20
        if (strings) {
            b += avg_len(level_pool()) + 4;
            b += avg_len(path_pool()) + 4;
        }
        return b;
    }
    std::uint64_t rows_per_chunk() const {
        return static_cast<std::uint64_t>((chunk_mb << 20) / est_row_bytes());
    }
    std::uint64_t num_chunks() const { return total_mb / chunk_mb; }
    std::uint64_t total_rows() const { return rows_per_chunk() * num_chunks(); }
};

inline Config parse_config(int argc, char** argv, const char* default_out) {
    Config c;
    c.out = default_out;
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        auto next = [&]() -> const char* {
            if (i + 1 >= argc) { std::fprintf(stderr, "missing arg for %s\n", a.c_str()); std::exit(2); }
            return argv[++i];
        };
        if (a == "--total-mb") c.total_mb = std::strtoull(next(), nullptr, 10);
        else if (a == "--chunk-mb") c.chunk_mb = std::strtoull(next(), nullptr, 10);
        else if (a == "--codec") { std::string v = next(); c.compress = (v != "uncompressed" && v != "none"); }
        else if (a == "--strings") c.strings = true;
        else if (a == "--no-strings") c.strings = false;
        else if (a == "--out") c.out = next();
        else { std::fprintf(stderr, "unknown flag: %s\n", a.c_str()); std::exit(2); }
    }
    if (c.chunk_mb == 0 || c.total_mb < c.chunk_mb) {
        std::fprintf(stderr, "require chunk-mb>0 and total-mb>=chunk-mb\n"); std::exit(2);
    }
    return c;
}

// ---- timing / memory ------------------------------------------------------

using Clock = std::chrono::steady_clock;
inline double secs(Clock::duration d) { return std::chrono::duration<double>(d).count(); }
inline long peak_rss_kb() { struct rusage ru{}; getrusage(RUSAGE_SELF, &ru); return ru.ru_maxrss; }

// ---- deterministic PRNG ---------------------------------------------------

struct Lcg {
    std::uint64_t s;
    explicit Lcg(std::uint64_t seed) : s(seed ? seed : 0x9E3779B97F4A7C15ull) {}
    std::uint64_t next() {
        s = s * 6364136223846793005ull + 1442695040888963407ull;
        return s ^ (s >> 31);
    }
};

// ---- Arrow C Data Interface builder ---------------------------------------

namespace detail {

// Every heap allocation for a chunk is tracked here and freed by the parent
// struct array's release callback (after the writer is done with the chunk).
struct ArrayOwner { std::vector<void*> allocs; };

inline void release_child(ArrowArray* a) {
    if (a->release == nullptr) return;
    std::free(const_cast<void*>(static_cast<const void*>(a->buffers)));
    a->release = nullptr;
}

inline void release_parent(ArrowArray* a) {
    if (a->release == nullptr) return;
    auto* owner = static_cast<ArrayOwner*>(a->private_data);
    for (void* p : owner->allocs) std::free(p);
    delete owner;
    for (std::int64_t i = 0; i < a->n_children; ++i) {
        ArrowArray* ch = a->children[i];
        if (ch->release) ch->release(ch);
        delete ch;
    }
    std::free(a->children);
    std::free(const_cast<void*>(static_cast<const void*>(a->buffers)));
    a->release = nullptr;
}

inline void release_schema(ArrowSchema* s) {
    if (s->release == nullptr) return;
    for (std::int64_t i = 0; i < s->n_children; ++i) {
        ArrowSchema* ch = s->children[i];
        if (ch->release) ch->release(ch);
        delete ch;
    }
    std::free(s->children);
    s->release = nullptr;
}
inline void release_schema_child(ArrowSchema* s) { s->release = nullptr; }

inline ArrowSchema* make_schema_child(const char* fmt, const char* name) {
    auto* s = new ArrowSchema{};
    s->format = fmt; s->name = name; s->metadata = nullptr;
    s->flags = 0;  // REQUIRED (ARROW_FLAG_NULLABLE unset)
    s->n_children = 0; s->children = nullptr; s->dictionary = nullptr;
    s->release = release_schema_child; s->private_data = nullptr;
    return s;
}

// Fixed-width child: 2 buffers {validity=null, data}.
inline ArrowArray* make_fixed_child(std::int64_t n, const void* data) {
    auto* a = new ArrowArray{};
    auto** buffers = static_cast<const void**>(std::malloc(2 * sizeof(void*)));
    buffers[0] = nullptr; buffers[1] = data;
    a->length = n; a->null_count = 0; a->offset = 0; a->n_buffers = 2;
    a->n_children = 0; a->buffers = buffers; a->children = nullptr;
    a->dictionary = nullptr; a->release = release_child; a->private_data = nullptr;
    return a;
}

// utf8/binary child: 3 buffers {validity=null, offsets(int32), data}.
inline ArrowArray* make_string_child(std::int64_t n, const void* offsets, const void* data) {
    auto* a = new ArrowArray{};
    auto** buffers = static_cast<const void**>(std::malloc(3 * sizeof(void*)));
    buffers[0] = nullptr; buffers[1] = offsets; buffers[2] = data;
    a->length = n; a->null_count = 0; a->offset = 0; a->n_buffers = 3;
    a->n_children = 0; a->buffers = buffers; a->children = nullptr;
    a->dictionary = nullptr; a->release = release_child; a->private_data = nullptr;
    return a;
}

// Build offsets + tightly-packed data for a random-from-pool string column.
// Returns the data byte count; pushes both buffers onto `owner`.
inline std::uint64_t build_string_col(Lcg& rng, std::uint64_t n,
                                      const std::vector<std::string>& pool,
                                      ArrayOwner& owner,
                                      std::int32_t** offsets_out, char** data_out) {
    auto* idx = static_cast<std::uint8_t*>(std::malloc(n));  // pool size <= 256
    auto* offsets = static_cast<std::int32_t*>(std::malloc((n + 1) * sizeof(std::int32_t)));
    std::uint64_t total = 0;
    for (std::uint64_t i = 0; i < n; ++i) {
        const std::uint8_t k = static_cast<std::uint8_t>(rng.next() % pool.size());
        idx[i] = k;
        offsets[i] = static_cast<std::int32_t>(total);
        total += pool[k].size();
    }
    offsets[n] = static_cast<std::int32_t>(total);
    auto* data = static_cast<char*>(std::malloc(total ? total : 1));
    for (std::uint64_t i = 0; i < n; ++i) {
        const std::string& s = pool[idx[i]];
        std::memcpy(data + offsets[i], s.data(), s.size());
    }
    std::free(idx);
    owner.allocs.push_back(offsets);
    owner.allocs.push_back(data);
    *offsets_out = offsets;
    *data_out = data;
    return total;
}

}  // namespace detail

inline void make_schema(const Config& cfg, ArrowSchema* out) {
    *out = ArrowSchema{};
    out->format = "+s"; out->name = nullptr; out->metadata = nullptr; out->flags = 0;
    out->n_children = cfg.num_cols();
    out->children = static_cast<ArrowSchema**>(std::malloc(cfg.num_cols() * sizeof(ArrowSchema*)));
    out->children[0] = detail::make_schema_child("l", "id");
    out->children[1] = detail::make_schema_child("g", "value");
    out->children[2] = detail::make_schema_child("i", "category");
    if (cfg.strings) {
        out->children[3] = detail::make_schema_child("u", "level");
        out->children[4] = detail::make_schema_child("u", "path");
    }
    out->dictionary = nullptr; out->release = detail::release_schema; out->private_data = nullptr;
}

// Allocate + fill + wrap one chunk of `n` rows. Returns the materialized byte
// count (logical uncompressed size). Time this call as the gen phase.
inline std::uint64_t make_chunk_array(const Config& cfg, std::uint64_t seed,
                                      std::uint64_t n, ArrowArray* out) {
    auto* owner = new detail::ArrayOwner{};
    auto* id = static_cast<std::int64_t*>(std::malloc(n * sizeof(std::int64_t)));
    auto* value = static_cast<double*>(std::malloc(n * sizeof(double)));
    auto* category = static_cast<std::int32_t*>(std::malloc(n * sizeof(std::int32_t)));
    if (!id || !value || !category) { std::fprintf(stderr, "OOM building chunk\n"); std::exit(1); }
    owner->allocs = {id, value, category};

    Lcg rng(seed);
    for (std::uint64_t i = 0; i < n; ++i) {
        const std::uint64_t r = rng.next();
        id[i] = static_cast<std::int64_t>(r);
        value[i] = static_cast<double>(r % 1000000ull) * 0.001;
        category[i] = static_cast<std::int32_t>(r % 1000ull);
    }

    std::uint64_t bytes = n * (sizeof(std::int64_t) + sizeof(double) + sizeof(std::int32_t));

    auto** children = static_cast<ArrowArray**>(std::malloc(cfg.num_cols() * sizeof(ArrowArray*)));
    children[0] = detail::make_fixed_child(static_cast<std::int64_t>(n), id);
    children[1] = detail::make_fixed_child(static_cast<std::int64_t>(n), value);
    children[2] = detail::make_fixed_child(static_cast<std::int64_t>(n), category);

    if (cfg.strings) {
        std::int32_t* loff; char* ldat;
        std::int32_t* poff; char* pdat;
        // Distinct draws per column so the two strings vary independently per row.
        bytes += detail::build_string_col(rng, n, level_pool(), *owner, &loff, &ldat);
        bytes += detail::build_string_col(rng, n, path_pool(), *owner, &poff, &pdat);
        bytes += 2 * (n + 1) * sizeof(std::int32_t);  // offset buffers
        children[3] = detail::make_string_child(static_cast<std::int64_t>(n), loff, ldat);
        children[4] = detail::make_string_child(static_cast<std::int64_t>(n), poff, pdat);
    }

    auto** buffers = static_cast<const void**>(std::malloc(sizeof(void*)));
    buffers[0] = nullptr;

    *out = ArrowArray{};
    out->length = static_cast<std::int64_t>(n);
    out->null_count = 0; out->offset = 0; out->n_buffers = 1;
    out->n_children = cfg.num_cols(); out->buffers = buffers; out->children = children;
    out->dictionary = nullptr; out->release = detail::release_parent; out->private_data = owner;
    return bytes;
}

// ---- result reporting (one machine-readable JSON line) --------------------

struct Result {
    const char* lib;
    Config cfg;
    std::uint64_t rows;
    std::uint64_t uncompressed_bytes;
    double gen_s;
    double write_s;
    std::uint64_t file_bytes;
    long peak_rss_kb;
};

inline void print_result(const Result& r) {
    const double gb = r.uncompressed_bytes / (1024.0 * 1024.0 * 1024.0);
    const double write_gbs = r.write_s > 0 ? gb / r.write_s : 0.0;
    const double mrows = r.rows / 1e6;
    std::printf(
        "{\"lib\":\"%s\",\"codec\":\"%s\",\"strings\":%s,\"total_mb\":%llu,\"chunk_mb\":%llu,"
        "\"rows\":%llu,\"chunks\":%llu,\"gen_s\":%.4f,\"write_s\":%.4f,"
        "\"write_gbps\":%.4f,\"mrows_per_s\":%.3f,\"file_bytes\":%llu,"
        "\"uncompressed_bytes\":%llu,\"bytes_per_row\":%.3f,\"peak_rss_mb\":%.1f}\n",
        r.lib, r.cfg.compress ? "zstd" : "uncompressed", r.cfg.strings ? "true" : "false",
        (unsigned long long)r.cfg.total_mb, (unsigned long long)r.cfg.chunk_mb,
        (unsigned long long)r.rows, (unsigned long long)r.cfg.num_chunks(),
        r.gen_s, r.write_s, write_gbs, r.write_s > 0 ? mrows / r.write_s : 0.0,
        (unsigned long long)r.file_bytes, (unsigned long long)r.uncompressed_bytes,
        r.rows ? (double)r.file_bytes / r.rows : 0.0, r.peak_rss_kb / 1024.0);
}

inline std::uint64_t file_size(const char* path) {
    std::FILE* f = std::fopen(path, "rb");
    if (!f) return 0;
    std::fseek(f, 0, SEEK_END);
    long n = std::ftell(f);
    std::fclose(f);
    return n < 0 ? 0 : static_cast<std::uint64_t>(n);
}

}  // namespace bench
