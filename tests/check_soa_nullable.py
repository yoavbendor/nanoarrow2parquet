#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Verify the nullable SoA file written by test_soa_nullable.cpp: the validity
# bitmaps (packed from a mask, and aliased pre-packed) produce the right null
# positions, the REQUIRED column stays non-nullable, and there are 2 row groups.
import sys
import pyarrow.parquet as pq

path = sys.argv[1] if len(sys.argv) > 1 else "n2p_soa_nullable.parquet"
pf = pq.ParquetFile(path)
t = pf.read()
d = t.to_pydict()

expect = {
    "id": [0, 1, 2, 3, 4, 5, 6, 7],
    "a":  [100, None, 300, None, 500, 600, 700, 800],
    "x":  [1.5, 2.5, None, 4.5, None, None, None, None],
}
fields = {f.name: f for f in t.schema}
problems = []
for name, exp in expect.items():
    if d.get(name) != exp:
        problems.append(f"{name}: {d.get(name)} != {exp}")
if fields["id"].nullable:
    problems.append("id should be REQUIRED (not nullable)")
for name in ("a", "x"):
    if not fields[name].nullable:
        problems.append(f"{name} should be OPTIONAL (nullable)")
if pf.metadata.num_row_groups != 2:
    problems.append(f"expected 2 row groups, got {pf.metadata.num_row_groups}")

if problems:
    print("SOA NULLABLE CHECK FAILED:")
    for p in problems:
        print("  -", p)
    sys.exit(1)
print("soa nullable check OK:", {k: d[k] for k in ("a", "x")})
