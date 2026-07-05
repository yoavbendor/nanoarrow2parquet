// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Yoav Bendor
//
// Python bindings for nanoarrow2parquet: zero-copy Arrow -> Parquet writer.

#include "arrow_capsule.hpp"

#include <nanoarrow2parquet/nanoarrow2parquet.h>

#include <nanobind/nanobind.h>
#include <nanobind/stl/filesystem.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/string_view.h>

#include <filesystem>
#include <stdexcept>
#include <string>

namespace nb = nanobind;
using nanoarrow_io::arrow_capsule::BatchIterator;
using nanoarrow_io::arrow_capsule::detail::OwnedArray;
using nanoarrow_io::arrow_capsule::detail::OwnedSchema;

namespace {

void throw_n2p(const char* context, int code, const char* err) {
    const char* label = "unknown error";
    switch (code) {
        case N2P_INVALID_ARGUMENT:
            label = "invalid argument";
            break;
        case N2P_UNSUPPORTED_TYPE:
            label = "unsupported type";
            break;
        case N2P_IO_ERROR:
            label = "I/O error";
            break;
        default:
            break;
    }
    std::string msg = std::string(context) + ": " + label;
    if (err != nullptr && err[0] != '\0') {
        msg += " (";
        msg += err;
        msg += ")";
    }
    throw std::runtime_error(msg);
}

N2PCodec parse_codec(std::string_view codec) {
    if (codec == "zstd" || codec == "ZSTD") {
        return N2P_CODEC_ZSTD;
    }
    if (codec == "uncompressed" || codec == "none" || codec == "UNCOMPRESSED") {
        return N2P_CODEC_UNCOMPRESSED;
    }
    throw std::invalid_argument("codec must be 'zstd' or 'uncompressed'");
}

void write_batches(nb::handle table, const std::filesystem::path& path, N2PCodec codec) {
    BatchIterator it(table);
    OwnedSchema schema;
    OwnedArray array;
    bool first = true;

    N2PWriter* writer = nullptr;
    while (it.next(&schema.schema, &array.array)) {
        if (first) {
            int rc = n2p_writer_open(&writer, path.string().c_str());
            if (rc != N2P_OK) {
                throw_n2p("n2p_writer_open", rc, writer ? n2p_writer_last_error(writer) : "");
            }
            rc = n2p_writer_set_codec(writer, codec);
            if (rc != N2P_OK) {
                throw_n2p("n2p_writer_set_codec", rc, n2p_writer_last_error(writer));
            }
            first = false;
        }
        int rc = n2p_writer_write_batch(writer, &schema.schema, &array.array);
        if (rc != N2P_OK) {
            throw_n2p("n2p_writer_write_batch", rc, n2p_writer_last_error(writer));
        }
        array.reset();
    }

    if (first) {
        throw std::runtime_error("cannot write empty table with no record batches");
    }

    int rc = n2p_writer_close(writer);
    if (rc != N2P_OK) {
        throw_n2p("n2p_writer_close", rc, "");
    }
}

void write_table(nb::handle table, const std::filesystem::path& path, std::string_view codec) {
    write_batches(table, path, parse_codec(codec));
}

void write_batch(nb::handle batch, const std::filesystem::path& path, std::string_view codec) {
  // One-shot path for a single RecordBatch.
    auto imported = nanoarrow_io::arrow_capsule::import_batch(batch);
    char err[512] = {};
    int rc = n2p_write_file(path.string().c_str(), imported.first.get(), imported.second.get(), err,
                            sizeof(err));
    if (rc != N2P_OK) {
        throw_n2p("n2p_write_file", rc, err);
    }
}

}  // namespace

NB_MODULE(_n2p, m) {
    m.doc() = "nanoarrow2parquet: fast zero-copy Arrow -> Parquet writer";

    m.def("write_table", &write_table, nb::arg("table"), nb::arg("path"),
          nb::arg("codec") = "zstd",
          nb::rv_policy::move,
          "Write an Arrow Table/RecordBatch stream to Parquet (one row group per batch).");

    m.def("write_batch", &write_batch, nb::arg("batch"), nb::arg("path"),
          nb::arg("codec") = "zstd",
          nb::rv_policy::move,
          "Write a single Arrow RecordBatch to a one-row-group Parquet file.");

    nb::enum_<N2PCodec>(m, "Codec")
        .value("ZSTD", N2P_CODEC_ZSTD)
        .value("UNCOMPRESSED", N2P_CODEC_UNCOMPRESSED);
}
