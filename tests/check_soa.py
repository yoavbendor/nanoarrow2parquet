#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Verify the file written by test_soa.cpp (native SoA path): every fixed-width
# numeric type roundtrips to the matching Arrow logical type, the values survive,
# and the two chunks landed as two row groups (caller-owned batching).
import sys
import pyarrow.parquet as pq

path = sys.argv[1] if len(sys.argv) > 1 else "n2p_soa.parquet"
pf = pq.ParquetFile(path)
t = pf.read()
d = t.to_pydict()

# name -> (expected pyarrow logical type, expected values across both chunks)
expect = {
    "i8":  ("int8",   [-1, 0, 127, -128, 5]),
    "u8":  ("uint8",  [0, 200, 255, 1, 254]),
    "i16": ("int16",  [-1, 1000, 32767, -32768, 0]),
    "u16": ("uint16", [0, 60000, 65535, 1, 2]),
    "i32": ("int32",  [-1, 2, 2147483647, -2147483648, 0]),
    "u32": ("uint32", [0, 4000000000, 4294967295, 1, 2]),
    "i64": ("int64",  [-1, 2, 9223372036854775807, -9223372036854775808, 0]),
    "u64": ("uint64", [0, 18446744073709551615, 5, 1, 2]),
    "f32": ("float",  [1.5, -2.5, 3.5, 4.5, 5.5]),
    "f64": ("double", [1.5, -2.5, 3.5, 4.5, 5.5]),
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
    print("SOA CHECK FAILED:")
    for p in problems:
        print("  -", p)
    sys.exit(1)
print(f"soa check OK: 10 numeric types, {pf.metadata.num_row_groups} row groups, "
      f"{t.num_rows} rows")
