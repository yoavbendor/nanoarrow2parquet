#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Verify the bool SoA file written by test_soa_bool.cpp: byte-per-bool storage
# packed into Arrow's 1-bit layout roundtrips, the nullable column carries nulls,
# and there are 2 row groups.
import sys
import pyarrow.parquet as pq

path = sys.argv[1] if len(sys.argv) > 1 else "n2p_soa_bool.parquet"
pf = pq.ParquetFile(path)
t = pf.read()
d = t.to_pydict()

expect = {
    "id":   [0, 1, 2, 3, 4, 5, 6, 7],
    "flag": [True, False, True, True, False, False, False, True],
    "opt":  [True, False, None, True, None, True, None, False],
}
fields = {f.name: f for f in t.schema}
problems = []
for name, exp in expect.items():
    if d.get(name) != exp:
        problems.append(f"{name}: {d.get(name)} != {exp}")
if str(fields["flag"].type) != "bool":
    problems.append(f"flag type: {fields['flag'].type}")
if fields["flag"].nullable:
    problems.append("flag should be REQUIRED")
if not fields["opt"].nullable:
    problems.append("opt should be OPTIONAL")
if pf.metadata.num_row_groups != 2:
    problems.append(f"expected 2 row groups, got {pf.metadata.num_row_groups}")

if problems:
    print("SOA BOOL CHECK FAILED:")
    for p in problems:
        print("  -", p)
    sys.exit(1)
print("soa bool check OK:", {k: d[k] for k in ("flag", "opt")})
