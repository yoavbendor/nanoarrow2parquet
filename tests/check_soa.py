#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Verify the file written by test_soa.cpp (native SoA path): values roundtrip,
# the two chunks landed as two row groups, and the unsigned column kept its type.
import sys
import pyarrow.parquet as pq

path = sys.argv[1] if len(sys.argv) > 1 else "n2p_soa.parquet"
pf = pq.ParquetFile(path)
t = pf.read()
d = t.to_pydict()

problems = []
if d.get("id") != [1, 2, 3, 4, 5]:
    problems.append(f"id mismatch: {d.get('id')}")
if d.get("x") != [1.5, 2.5, 3.5, 4.5, 5.5]:
    problems.append(f"x mismatch: {d.get('x')}")
if d.get("flags") != [10, 20, 30, 40, 50]:
    problems.append(f"flags mismatch: {d.get('flags')}")
if pf.metadata.num_row_groups != 2:
    problems.append(f"expected 2 row groups, got {pf.metadata.num_row_groups}")

fields = {f.name: f for f in t.schema}
if str(fields["id"].type) != "int64":
    problems.append(f"id type: {fields['id'].type}")
if str(fields["x"].type) != "double":
    problems.append(f"x type: {fields['x'].type}")
if str(fields["flags"].type) != "uint32":
    problems.append(f"flags type: {fields['flags'].type}")

if problems:
    print("SOA CHECK FAILED:")
    for p in problems:
        print("  -", p)
    sys.exit(1)
print("soa check OK:", d, f"({pf.metadata.num_row_groups} row groups)")
