#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Verify the file written by test_soa_fixed_binary.cpp: std::array<uint8_t, N> columns roundtrip
# to Arrow fixed_size_binary(N), values survive byte-for-byte, and the two chunks landed as two
# row groups (caller-owned batching).
import sys
import pyarrow.parquet as pq

path = sys.argv[1] if len(sys.argv) > 1 else "n2p_soa_fixed_binary.parquet"
pf = pq.ParquetFile(path)
t = pf.read()
d = t.to_pydict()

expect = {
    "mac": (
        "fixed_size_binary[6]",
        [
            bytes([0x00, 0x11, 0x22, 0x33, 0x44, 0x55]),
            bytes([0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF]),
            bytes([0x01, 0x02, 0x03, 0x04, 0x05, 0x06]),
        ],
    ),
    "ipv4": (
        "fixed_size_binary[4]",
        [
            bytes([192, 168, 1, 1]),
            bytes([10, 0, 0, 1]),
            bytes([255, 255, 255, 0]),
        ],
    ),
}

fields = {f.name: f for f in t.schema}
problems = []
for name, (logical, values) in expect.items():
    if str(fields[name].type) != logical:
        problems.append(f"{name}: type {fields[name].type} != {logical}")
    if d.get(name) != values:
        problems.append(f"{name}: values {d.get(name)} != {values}")
if pf.metadata.num_row_groups != 2:
    problems.append(f"expected 2 row groups, got {pf.metadata.num_row_groups}")

if problems:
    print("SOA FIXED_BINARY CHECK FAILED:")
    for p in problems:
        print("  -", p)
    sys.exit(1)
print(f"soa fixed_binary check OK: 2 fixed_size_binary columns, "
      f"{pf.metadata.num_row_groups} row groups, {t.num_rows} rows")
