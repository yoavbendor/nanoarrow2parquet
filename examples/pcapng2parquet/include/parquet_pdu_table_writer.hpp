// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Yoav Bendor

#pragma once

// Write one DAG node's columnar table (nanotins::dag_pdu_table<Spec>) as its own Parquet file: a
// `packet_id` column (join back to the packets table) followed by the spec's field columns. This is the
// nanoarrow2parquet sink twin of pcapng2lance's dag_table_writer.hpp -- the schema/batch builders are
// identical (they only touch soatins' column->Arrow glue + nanoarrow), and ONLY the endpoint differs: the
// six nano_lance_writer_* calls become n2p_writer_* calls. Because both sinks consume the SAME
// ArrowSchema/ArrowArray, the per-PDU Parquet tables are byte-for-byte the same columns as the Lance ones.
//
// Each emitted PDU column is scalar or fixed_size_binary (MAC / IPv4 / IPv6 addresses, PTP clock_identity),
// all of which nanoarrow2parquet supports (PLAIN-encoded REQUIRED columns).

#include "soatins/arrow_glue.hpp"          // nt_set_column_schema
#include "nanotins/dag_decode.hpp"         // dag_pdu_table
#include "nanotins/wire_spec_soa.hpp"      // columns_of_spec (spec_col: kind / fixed_width / name / elem)

#include "nanoarrow2parquet/nanoarrow2parquet.h"

#include <nanoarrow/nanoarrow.h>

#include <cstdint>
#include <filesystem>
#include <string>
#include <tuple>
#include <utility>

namespace pq_pdu_io {

// Schema for a DAG PDU table: struct[ packet_id u64, <spec field columns...> ].
template <class Spec>
bool build_dag_pdu_schema(ArrowSchema& schema, std::string& error) {
    using cols = nanotins::columns_of_spec<Spec>;
    constexpr std::size_t kCols = std::tuple_size_v<cols>;
    ArrowSchemaInit(&schema);
    if (ArrowSchemaSetTypeStruct(&schema, static_cast<int64_t>(kCols + 1)) != NANOARROW_OK) {
        error = "dag pdu schema alloc failed";
        return false;
    }
    if (!soatins::nt_set_column_schema(schema.children[0], soatins::arrow_kind::u64, 0, "packet_id", error)) {
        ArrowSchemaRelease(&schema);
        return false;
    }
    bool ok = true;
    std::string err;
    [&]<std::size_t... I>(std::index_sequence<I...>) {
        ((ok = ok && soatins::nt_set_column_schema(schema.children[I + 1], std::tuple_element_t<I, cols>::kind,
                                                   std::tuple_element_t<I, cols>::fixed_width,
                                                   std::tuple_element_t<I, cols>::name(), err)),
         ...);
    }(std::make_index_sequence<kCols>{});
    if (!ok) {
        error = err;
        ArrowSchemaRelease(&schema);
        return false;
    }
    schema.flags = 0;
    return true;
}

// Build a record-batch array for `table` against a schema from build_dag_pdu_schema<Spec>. One bulk append
// per column (packet_id, then each spec field) -- the columns are already the contiguous Arrow data layout.
template <class Spec>
bool build_dag_pdu_batch(const ArrowSchema& schema, const nanotins::dag_pdu_table<Spec>& table,
                         ArrowArray& batch, std::string& error) {
    using cols = nanotins::columns_of_spec<Spec>;
    constexpr std::size_t kCols = std::tuple_size_v<cols>;
    if (ArrowArrayInitFromSchema(&batch, const_cast<ArrowSchema*>(&schema), nullptr) != NANOARROW_OK) {
        error = "dag pdu array init failed";
        return false;
    }
    const std::int64_t n = static_cast<std::int64_t>(table.size());
    bool ok = ArrowBufferAppend(ArrowArrayBuffer(batch.children[0], 1), table.packet_id.data(),
                                n * static_cast<std::int64_t>(sizeof(std::uint64_t))) == NANOARROW_OK;
    batch.children[0]->length = n;
    batch.children[0]->null_count = 0;
    [&]<std::size_t... I>(std::index_sequence<I...>) {
        ((ok = ok && ArrowBufferAppend(
                         ArrowArrayBuffer(batch.children[I + 1], 1), table.template column<I>().data(),
                         n * static_cast<std::int64_t>(sizeof(typename std::tuple_element_t<I, cols>::elem))) ==
                         NANOARROW_OK,
          batch.children[I + 1]->length = n, batch.children[I + 1]->null_count = 0),
         ...);
    }(std::make_index_sequence<kCols>{});
    if (!ok) {
        error = "dag pdu bulk fill failed";
        ArrowArrayRelease(&batch);
        return false;
    }
    batch.length = n;
    batch.null_count = 0;
    if (ArrowArrayFinishBuildingDefault(&batch, nullptr) != NANOARROW_OK) {
        error = "dag pdu array finalize failed";
        ArrowArrayRelease(&batch);
        return false;
    }
    return true;
}

// One-shot: write a single-row-group Parquet file for a DAG PDU table. An empty table writes nothing (a
// PDU type absent from the whole capture yields no file), matching pcapng2lance's lazy-table behaviour.
template <class Spec>
bool write_dag_pdu_table(const std::filesystem::path& path, const nanotins::dag_pdu_table<Spec>& table,
                         bool compress, std::string& error) {
    if (table.size() == 0) {
        return true;
    }
    ArrowSchema schema{};
    if (!build_dag_pdu_schema<Spec>(schema, error)) {
        return false;
    }
    ArrowArray batch{};
    if (!build_dag_pdu_batch<Spec>(schema, table, batch, error)) {
        ArrowSchemaRelease(&schema);
        return false;
    }
    // ---- the endpoint: nanoarrow2parquet instead of nanolance --------------------------------------
    N2PWriter* w = nullptr;
    if (n2p_writer_open(&w, path.string().c_str()) != N2P_OK) {
        error = "n2p_writer_open failed for " + path.string();
        batch.release(&batch);
        ArrowSchemaRelease(&schema);
        return false;
    }
    n2p_writer_set_codec(w, compress ? N2P_CODEC_ZSTD : N2P_CODEC_UNCOMPRESSED);
    const bool wrote = n2p_writer_write_batch(w, &schema, &batch) == N2P_OK;  // one batch == one row group
    if (!wrote) {
        error = std::string("n2p_writer_write_batch: ") + n2p_writer_last_error(w);
    }
    batch.release(&batch);
    if (n2p_writer_close(w) != N2P_OK && wrote) {
        error = "n2p_writer_close failed for " + path.string();
        ArrowSchemaRelease(&schema);
        return false;
    }
    ArrowSchemaRelease(&schema);
    return wrote;
}

}  // namespace pq_pdu_io
