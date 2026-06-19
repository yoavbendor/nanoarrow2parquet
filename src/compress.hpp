// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Yoav Bendor

#pragma once

// Thin per-page codec wrapper. Parquet compresses each page body independently:
// the codec produces a raw frame (for ZSTD, a plain zstd frame with no extra
// Parquet-level framing or CRC), and the PageHeader records the uncompressed and
// compressed sizes alongside ColumnMetaData.codec.

#include "parquet_types.hpp"

#include <cstdint>
#include <span>
#include <stdexcept>
#include <string>
#include <vector>

#include <zstd.h>

namespace n2p {

// Compress `src` with the given codec. Returns the compressed bytes. For
// UNCOMPRESSED, returns a copy of the input. Throws std::runtime_error on a codec
// failure (callers translate to N2P_IO_ERROR).
inline std::vector<std::uint8_t> compress_page(std::span<const std::uint8_t> src,
                                               pq::Codec codec,
                                               int level = 3) {
    if (codec == pq::Codec::Uncompressed) {
        return std::vector<std::uint8_t>(src.begin(), src.end());
    }
    // pq::Codec::Zstd
    const std::size_t bound = ZSTD_compressBound(src.size());
    std::vector<std::uint8_t> dst(bound);
    const std::size_t n = ZSTD_compress(dst.data(), dst.size(),
                                        src.data(), src.size(), level);
    if (ZSTD_isError(n)) {
        throw std::runtime_error(std::string("zstd compression failed: ") +
                                 ZSTD_getErrorName(n));
    }
    dst.resize(n);
    return dst;
}

}  // namespace n2p
