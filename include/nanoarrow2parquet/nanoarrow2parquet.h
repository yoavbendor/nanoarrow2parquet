// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Yoav Bendor

#pragma once

// nanoarrow2parquet: a minimal Arrow -> Parquet writer.
//
// Consumes an in-memory Arrow C Data Interface struct array (one record batch ==
// one row group) and writes a valid, self-describing .parquet file readable by
// pandas / pyarrow / DuckDB / polars.
//
// Scope (v1):
//   * fixed-width, REQUIRED (non-nullable), flat columns: int8/16/32/64,
//     uint8/16/32/64, float, double, bool, fixed_size_binary(N) -- PLAIN encoded.
//   * variable-width strings/binary (utf8/large_utf8/binary/large_binary) via
//     dictionary encoding (RLE_DICTIONARY).
//   * every page body is compressed (ZSTD by default).
//   * one or more row groups (streaming writer).
//
// Out of scope (documented TODO): nullable / definition levels, nested
// list/struct/map columns, page statistics / indexes, bloom filters.

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

struct ArrowArray;
struct ArrowSchema;

typedef enum N2PStatus {
    N2P_OK = 0,
    N2P_INVALID_ARGUMENT = 1,
    N2P_UNSUPPORTED_TYPE = 2,
    N2P_IO_ERROR = 3
} N2PStatus;

// Compression codec used for every page body. ZSTD is the default and is read by
// all modern Parquet readers. UNCOMPRESSED is provided mainly for debugging.
typedef enum N2PCodec {
    N2P_CODEC_ZSTD = 0,
    N2P_CODEC_UNCOMPRESSED = 1
} N2PCodec;

// One-shot: schema + one record batch -> one .parquet file (single row group).
// `schema` must describe a struct (the record batch); `batch` is the matching
// struct array. On failure, a human-readable message is written to `err` (if
// `err_len > 0`). Neither `schema` nor `batch` is released by this call.
int n2p_write_file(const char* path,
                   const struct ArrowSchema* schema,
                   const struct ArrowArray* batch,
                   char* err, size_t err_len);

// Streaming form: multiple batches -> multiple row groups in one file. The footer
// is written exactly once, at close(). If the process dies before close(), the
// file has no footer and is unreadable -- this is an inherent Parquet limitation.
typedef struct N2PWriter N2PWriter;

int n2p_writer_open(N2PWriter** out, const char* path);

// Write one record batch as one row group. The first call fixes the schema; later
// calls must pass a compatible schema (same column types). `schema`/`batch` are
// borrowed, not released.
int n2p_writer_write_batch(N2PWriter* w,
                           const struct ArrowSchema* schema,
                           const struct ArrowArray* batch);

// Serialize the footer and close the file. The writer is freed regardless of the
// return code; `w` must not be used afterwards.
int n2p_writer_close(N2PWriter* w);

const char* n2p_writer_last_error(const N2PWriter* w);

// Select the codec for subsequent batches (default ZSTD). Returns N2P_OK or
// N2P_INVALID_ARGUMENT.
int n2p_writer_set_codec(N2PWriter* w, N2PCodec codec);

#ifdef __cplusplus
}
#endif
