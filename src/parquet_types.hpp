// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Yoav Bendor

#pragma once

// Parquet format enums (values from parquet.thrift). Only the subset this writer
// emits is defined here. These are the *logical* enum values stored as i32 fields
// in the Thrift compact stream -- unrelated to the compact-protocol type nibbles
// in thrift_compact.hpp.

#include <cstdint>

namespace n2p::pq {

// Physical types (SchemaElement.type, ColumnMetaData.type).
enum class Type : std::int32_t {
    Boolean = 0,
    Int32 = 1,
    Int64 = 2,
    Int96 = 3,
    Float = 4,
    Double = 5,
    ByteArray = 6,
    FixedLenByteArray = 7,
};

// Column / page encodings.
enum class Encoding : std::int32_t {
    Plain = 0,
    PlainDictionary = 2,
    Rle = 3,
    BitPacked = 4,
    RleDictionary = 8,
};

// Page compression codecs.
enum class Codec : std::int32_t {
    Uncompressed = 0,
    Snappy = 1,
    Gzip = 2,
    Brotli = 4,
    Lz4 = 5,
    Zstd = 6,
    Lz4Raw = 7,
};

enum class Repetition : std::int32_t {
    Required = 0,
    Optional = 1,
    Repeated = 2,
};

// Legacy "ConvertedType" annotations -- widely read and simpler than the
// LogicalType union; sufficient for unsigned ints, narrow ints, and UTF8 strings.
enum class ConvertedType : std::int32_t {
    Utf8 = 0,
    Uint8 = 11,
    Uint16 = 12,
    Uint32 = 13,
    Uint64 = 14,
    Int8 = 15,
    Int16 = 16,
    Int32 = 17,
    Int64 = 18,
};

enum class PageType : std::int32_t {
    DataPage = 0,
    IndexPage = 1,
    DictionaryPage = 2,
    DataPageV2 = 3,
};

}  // namespace n2p::pq
