// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Yoav Bendor

// Shared benchmark scaffolding for comparing nanoarrow2parquet against the Apache
// Parquet C++ writer. Methodology mirrors carquet's benchmark/ (deterministic LCG
// data, write timed separately from data generation, fixed row-group sizing, file
// size reported) but adds larger-than-RAM *streaming*: data is produced one chunk
// at a time, written as one row group, and freed before the next chunk -- so the
// dataset total (1/4/8 GB) far exceeds peak resident memory (~one chunk).
//
// The two phases are timed independently for every chunk:
//   * gen   -- materialize a chunk of columnar data into the Arrow C Data
//              Interface (the "making/acquiring the nanoarrow data" cost).
//   * write -- hand that chunk to the writer under test and serialize one row
//              group (the cost we actually want to compare).
//
// Both benchmark binaries consume the *same* C Data Interface arrays produced
// here, so the only thing that differs between runs is the writer.
//
// REQUIREMENT: the including translation unit must already provide the Arrow C
// Data Interface struct definitions (ArrowSchema / ArrowArray). Both nanoarrow.h
// and arrow/c/abi.h define them behind the shared ARROW_C_DATA_INTERFACE guard,
// so including either (or both) before this header is sufficient.

#pragma once

#include <sys/resource.h>

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

namespace bench {

// ---- schema: 3 columns, 20 bytes/row (matches carquet's INT64+DOUBLE+INT32) --

constexpr int kNumCols = 3;
constexpr std::size_t kRowBytes =
    sizeof(std::int64_t) + sizeof(double) + sizeof(std::int32_t);

// ---- config ---------------------------------------------------------------

struct Config {
    std::uint64_t total_mb = 1024;   // whole-dataset size (e.g. 1024/4096/8192)
    std::uint64_t chunk_mb = 200;    // per-chunk / per-row-group size (200/500)
    bool compress = true;            // zstd (default) vs uncompressed
    std::string out = "/tmp/n2p_bench.parquet";

    std::uint64_t rows_per_chunk() const { return (chunk_mb << 20) / kRowBytes; }
    std::uint64_t num_chunks() const { return total_mb / chunk_mb; }
    std::uint64_t total_rows() const { return rows_per_chunk() * num_chunks(); }
    std::uint64_t total_uncompressed_bytes() const { return total_rows() * kRowBytes; }
};

inline Config parse_config(int argc, char** argv, const char* default_out) {
    Config c;
    c.out = default_out;
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        auto next = [&]() -> const char* {
            if (i + 1 >= argc) {
                std::fprintf(stderr, "missing arg for %s\n", a.c_str());
                std::exit(2);
            }
            return argv[++i];
        };
        if (a == "--total-mb") c.total_mb = std::strtoull(next(), nullptr, 10);
        else if (a == "--chunk-mb") c.chunk_mb = std::strtoull(next(), nullptr, 10);
        else if (a == "--codec") { std::string v = next(); c.compress = (v != "uncompressed" && v != "none"); }
        else if (a == "--out") c.out = next();
        else { std::fprintf(stderr, "unknown flag: %s\n", a.c_str()); std::exit(2); }
    }
    if (c.chunk_mb == 0 || c.total_mb < c.chunk_mb) {
        std::fprintf(stderr, "require chunk-mb>0 and total-mb>=chunk-mb\n");
        std::exit(2);
    }
    return c;
}

// ---- timing / memory ------------------------------------------------------

using Clock = std::chrono::steady_clock;
inline double secs(Clock::duration d) { return std::chrono::duration<double>(d).count(); }

// Peak resident set size in KiB (Linux ru_maxrss reports KiB).
inline long peak_rss_kb() {
    struct rusage ru{};
    getrusage(RUSAGE_SELF, &ru);
    return ru.ru_maxrss;
}

// ---- deterministic data generation ----------------------------------------

// 64-bit LCG (same family as carquet's deterministic PRNG). Bit-identical across
// runs so codec ratios and file sizes are reproducible.
struct Lcg {
    std::uint64_t s;
    explicit Lcg(std::uint64_t seed) : s(seed ? seed : 0x9E3779B97F4A7C15ull) {}
    std::uint64_t next() {
        s = s * 6364136223846793005ull + 1442695040888963407ull;
        return s ^ (s >> 31);
    }
};

// ---- Arrow C Data Interface builder ---------------------------------------
//
// Build the three column buffers directly and wrap them in C Data Interface
// structs with release callbacks, rather than using nanoarrow's per-element
// append path (which would dominate gen time and isn't how a real producer hands
// columnar buffers to a writer). n2p reads buffers[1] directly; Arrow imports the
// same structs zero-copy via arrow::ImportRecordBatch.

namespace detail {

// Backing storage owned by the parent struct array's release callback.
struct ArrayOwner {
    std::int64_t* id;
    double* value;
    std::int32_t* category;
};

inline void release_child(ArrowArray* a) {
    if (a->release == nullptr) return;
    std::free(const_cast<void*>(static_cast<const void*>(a->buffers)));
    a->release = nullptr;
}

inline void release_parent(ArrowArray* a) {
    if (a->release == nullptr) return;
    auto* owner = static_cast<ArrayOwner*>(a->private_data);
    std::free(owner->id);
    std::free(owner->value);
    std::free(owner->category);
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
    s->format = fmt;        // string literal, no free needed
    s->name = name;         // string literal
    s->metadata = nullptr;
    s->flags = 0;           // ARROW_FLAG_NULLABLE not set => REQUIRED
    s->n_children = 0;
    s->children = nullptr;
    s->dictionary = nullptr;
    s->release = release_schema_child;
    s->private_data = nullptr;
    return s;
}

inline ArrowArray* make_array_child(std::int64_t length, const void* data) {
    auto* a = new ArrowArray{};
    auto** buffers = static_cast<const void**>(std::malloc(2 * sizeof(void*)));
    buffers[0] = nullptr;   // validity bitmap: none (REQUIRED)
    buffers[1] = data;
    a->length = length;
    a->null_count = 0;
    a->offset = 0;
    a->n_buffers = 2;
    a->n_children = 0;
    a->buffers = buffers;
    a->children = nullptr;
    a->dictionary = nullptr;
    a->release = release_child;
    a->private_data = nullptr;
    return a;
}

}  // namespace detail

// Fill an existing static schema describing the 3-column struct.
inline void make_schema(ArrowSchema* out) {
    *out = ArrowSchema{};
    out->format = "+s";
    out->name = nullptr;
    out->metadata = nullptr;
    out->flags = 0;
    out->n_children = kNumCols;
    out->children = static_cast<ArrowSchema**>(std::malloc(kNumCols * sizeof(ArrowSchema*)));
    out->children[0] = detail::make_schema_child("l", "id");        // int64
    out->children[1] = detail::make_schema_child("g", "value");     // double
    out->children[2] = detail::make_schema_child("i", "category");  // int32
    out->dictionary = nullptr;
    out->release = detail::release_schema;
    out->private_data = nullptr;
}

// Allocate + fill + wrap one chunk of `n` rows. Time this call as the gen phase.
inline void make_chunk_array(std::uint64_t seed, std::uint64_t n, ArrowArray* out) {
    auto* id = static_cast<std::int64_t*>(std::malloc(n * sizeof(std::int64_t)));
    auto* value = static_cast<double*>(std::malloc(n * sizeof(double)));
    auto* category = static_cast<std::int32_t*>(std::malloc(n * sizeof(std::int32_t)));
    if (!id || !value || !category) { std::fprintf(stderr, "OOM building chunk\n"); std::exit(1); }

    Lcg rng(seed);
    for (std::uint64_t i = 0; i < n; ++i) {
        const std::uint64_t r = rng.next();
        id[i] = static_cast<std::int64_t>(r);
        value[i] = static_cast<double>(r % 1000000ull) * 0.001;   // 0..1000, 3 dp
        category[i] = static_cast<std::int32_t>(r % 1000ull);      // 1000 categories
    }

    auto* owner = new detail::ArrayOwner{id, value, category};
    auto** children = static_cast<ArrowArray**>(std::malloc(kNumCols * sizeof(ArrowArray*)));
    children[0] = detail::make_array_child(static_cast<std::int64_t>(n), id);
    children[1] = detail::make_array_child(static_cast<std::int64_t>(n), value);
    children[2] = detail::make_array_child(static_cast<std::int64_t>(n), category);

    auto** buffers = static_cast<const void**>(std::malloc(1 * sizeof(void*)));
    buffers[0] = nullptr;  // struct validity bitmap: none

    *out = ArrowArray{};
    out->length = static_cast<std::int64_t>(n);
    out->null_count = 0;
    out->offset = 0;
    out->n_buffers = 1;
    out->n_children = kNumCols;
    out->buffers = buffers;
    out->children = children;
    out->dictionary = nullptr;
    out->release = detail::release_parent;
    out->private_data = owner;
}

// ---- result reporting (one machine-readable JSON line) --------------------

struct Result {
    const char* lib;
    Config cfg;
    double gen_s;
    double write_s;
    std::uint64_t file_bytes;
    long peak_rss_kb;
};

inline void print_result(const Result& r) {
    const std::uint64_t uncompressed = r.cfg.total_uncompressed_bytes();
    const double gb = uncompressed / (1024.0 * 1024.0 * 1024.0);
    const double write_gbs = r.write_s > 0 ? gb / r.write_s : 0.0;
    const double mrows = r.cfg.total_rows() / 1e6;
    // JSON line consumed by run_bench.py; also human-skimmable.
    std::printf(
        "{\"lib\":\"%s\",\"codec\":\"%s\",\"total_mb\":%llu,\"chunk_mb\":%llu,"
        "\"rows\":%llu,\"chunks\":%llu,\"gen_s\":%.4f,\"write_s\":%.4f,"
        "\"write_gbps\":%.4f,\"mrows_per_s\":%.3f,\"file_bytes\":%llu,"
        "\"uncompressed_bytes\":%llu,\"bytes_per_row\":%.3f,\"peak_rss_mb\":%.1f}\n",
        r.lib, r.cfg.compress ? "zstd" : "uncompressed",
        (unsigned long long)r.cfg.total_mb, (unsigned long long)r.cfg.chunk_mb,
        (unsigned long long)r.cfg.total_rows(), (unsigned long long)r.cfg.num_chunks(),
        r.gen_s, r.write_s, write_gbs, r.write_s > 0 ? mrows / r.write_s : 0.0,
        (unsigned long long)r.file_bytes, (unsigned long long)uncompressed,
        (double)r.file_bytes / r.cfg.total_rows(), r.peak_rss_kb / 1024.0);
}

// File size helper.
inline std::uint64_t file_size(const char* path) {
    std::FILE* f = std::fopen(path, "rb");
    if (!f) return 0;
    std::fseek(f, 0, SEEK_END);
    long n = std::ftell(f);
    std::fclose(f);
    return n < 0 ? 0 : static_cast<std::uint64_t>(n);
}

}  // namespace bench
