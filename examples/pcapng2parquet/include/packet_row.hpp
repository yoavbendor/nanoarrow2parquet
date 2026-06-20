// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Yoav Bendor

#pragma once

// The all-scalar L1 packet row that flows through the nanotins reflection core (one Parquet row per
// packet). This mirrors pcapng2lance's PacketRow exactly so the two converters emit the same L1 schema --
// the only difference is the output endpoint (Parquet vs Lance). Unlike the Lance example there is no
// external payload_ref blob struct: Parquet has no out-of-band blob store, so payloads are not stored
// (this is a parsing-parity demo, not a storage-parity demo).

#include "soatins/describe.hpp"

#include <boost/describe.hpp>

#include <cstdint>

namespace pcapng2parquet {

struct PacketRow {
    std::uint64_t packet_id;  // stable row id; join key for the per-PDU tables
    std::uint32_t interface_id;
    std::uint64_t ts_raw;
    std::uint32_t caplen;
    std::uint32_t origlen;
    std::uint16_t link_type;  // denormalized from the interface (so a row self-describes its link)
    std::uint8_t ts_resol;    // denormalized; lets a row self-describe its time unit
    std::uint32_t epb_flags;
};
BOOST_DESCRIBE_STRUCT(PacketRow, (),
                      (packet_id, interface_id, ts_raw, caplen, origlen, link_type, ts_resol, epb_flags))

}  // namespace pcapng2parquet
