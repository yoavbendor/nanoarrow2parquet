#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Yoav Bendor
#
# End-to-end check for the pcapng2parquet example: synthesize a tiny classic-pcap capture (no scapy --
# pure struct), run the converter, and verify the per-layer Parquet tables it emits. Exits 77 (CTest
# "skip") when pyarrow is unavailable, matching the repo's other interop tests.

import os
import struct
import subprocess
import sys
import tempfile

try:
    import pyarrow.parquet as pq
except ImportError:
    print("pyarrow not installed; skipping", file=sys.stderr)
    sys.exit(77)

BIN = sys.argv[1] if len(sys.argv) > 1 else "pcapng2parquet"


def lldp_tlv(t, value):
    return struct.pack(">H", (t << 9) | len(value)) + value


def eth(dst, src, ethertype, payload):
    return bytes.fromhex(dst) + bytes.fromhex(src) + struct.pack(">H", ethertype) + payload


def ipv4_udp():
    # UDP: sport 4000, dport 5000, len 8+4 payload, csum 0
    udp = struct.pack(">HHHH", 4000, 5000, 8 + 4, 0) + b"data"
    # IPv4: ihl 5, proto 17 (UDP), src 10.0.0.1 dst 10.0.0.2
    total = 20 + len(udp)
    ip = struct.pack(">BBHHHBBH4s4s", 0x45, 0, total, 1, 0, 64, 17, 0,
                     bytes([10, 0, 0, 1]), bytes([10, 0, 0, 2])) + udp
    return eth("020000000002", "020000000001", 0x0800, ip)


def lldp_frame():
    payload = (lldp_tlv(1, bytes([4]) + bytes.fromhex("001122334455"))  # chassis id (MAC)
               + lldp_tlv(2, bytes([5]) + b"eth0")                       # port id (ifname)
               + lldp_tlv(3, struct.pack(">H", 120))                     # ttl
               + lldp_tlv(5, b"switch01")                                # system name
               + lldp_tlv(0, b""))                                       # end
    return eth("0180c200000e", "001122334455", 0x88CC, payload)


def write_pcap(path, frames):
    # classic pcap, little-endian, linktype 1 (Ethernet)
    out = struct.pack("<IHHiIII", 0xA1B2C3D4, 2, 4, 0, 0, 65535, 1)
    for f in frames:
        out += struct.pack("<IIII", 0, 0, len(f), len(f)) + f
    with open(path, "wb") as fh:
        fh.write(out)


def main():
    tmp = tempfile.mkdtemp(prefix="pcapng2parquet_")
    pcap = os.path.join(tmp, "in.pcap")
    stem = os.path.join(tmp, "out")
    write_pcap(pcap, [ipv4_udp(), lldp_frame()])

    subprocess.run([BIN, "--decode-l2l3", "--lldp", pcap, stem], check=True)

    def table(suffix):
        return pq.read_table(stem + suffix + ".parquet")

    # L1 packets: 2 rows.
    pkts = table("_packets")
    assert pkts.num_rows == 2, pkts.num_rows
    assert pkts.schema.field("packet_id").type == "uint64"

    # Ethernet: 2 frames; MAC columns are fixed_size_binary[6]; ethertypes match.
    e = table("_ethernet").to_pydict()
    assert len(e["packet_id"]) == 2
    assert str(table("_ethernet").schema.field("dst").type) == "fixed_size_binary[6]"
    assert sorted(e["ethertype"]) == [0x0800, 0x88CC], e["ethertype"]

    # IPv4 + UDP from the first frame.
    ip = table("_ipv4").to_pydict()
    assert ip["protocol"] == [17], ip["protocol"]
    assert bytes(ip["src"][0]) == bytes([10, 0, 0, 1])
    assert bytes(ip["dst"][0]) == bytes([10, 0, 0, 2])
    udp = table("_udp").to_pydict()
    assert udp["src_port"] == [4000] and udp["dst_port"] == [5000], udp

    # LLDP: 4 TLV rows (chassis/port/ttl/sysname; End terminates the walk).
    lldp = table("_lldp").to_pydict()
    assert len(lldp["tlv_index"]) == 4, lldp["tlv_index"]
    assert lldp["tlv_type"] == [1, 2, 3, 5], lldp["tlv_type"]
    assert lldp["ttl_seconds"][2] == 120, lldp["ttl_seconds"]
    assert str(table("_lldp").schema.field("value_head").type) == "fixed_size_binary[32]"

    # Every PDU table joins back to packets via packet_id.
    for suffix in ("_ethernet", "_ipv4", "_udp", "_lldp"):
        assert "packet_id" in table(suffix).schema.names

    print("pcapng2parquet check: OK (packets=2, eth=2, ipv4=1, udp=1, lldp=4 TLVs)")


if __name__ == "__main__":
    main()
