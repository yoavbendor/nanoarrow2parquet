// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Yoav Bendor

// The s3:// output of the writer, through nanos3reader's spilling writer: the file is held in memory up to 8 MiB,
// spilled to a temporary file beyond that, and published by one PUT or a multipart upload when the writer closes.
// Built only with -DN2P_S3=ON (the writer declares these functions under N2P_HAVE_S3).

#include "nanos3reader/s3_range.h"

#include <memory>
#include <ostream>
#include <string>

namespace n2p::s3 {

struct Sink {
    std::shared_ptr<nanos3reader::S3Client> client;
    std::unique_ptr<nanos3reader::SpillingObjectWriter> writer;
};

Sink* open(const std::string& uri, std::string& error) {
    auto sink = std::make_unique<Sink>();
    sink->client = std::make_shared<nanos3reader::S3Client>();  // credentials and endpoint from the environment
    nanos3reader::S3WriterOptions options;
    options.upload.part_concurrency = 8;
    sink->writer = nanos3reader::open_s3_writer(sink->client, uri, options);
    if (!sink->writer) {
        error = "not an s3:// URI with a bucket: " + uri;
        return nullptr;
    }
    return sink.release();
}

std::ostream& stream(Sink* sink) { return *sink->writer; }

bool commit(Sink* sink, std::string& error) {
    std::unique_ptr<Sink> owned(sink);
    return owned->writer->commit(error);
}

void discard(Sink* sink) { delete sink; }

}  // namespace n2p::s3
