// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Yoav Bendor

#pragma once

// The Parquet format model this writer emits is nanom's (nanom/formats/parquet_thrift.hpp): the
// same structs and enums parquet2nanoarrow reads, encoded with nanom/tagged_encode.hpp. There is
// no second copy of parquet.thrift here, so the reader and the writer cannot drift apart.

#include <nanom/formats/parquet_thrift.hpp>
#include <nanom/tagged_encode.hpp>

namespace n2p {
namespace pq = nanom_formats::parquet;
}  // namespace n2p
