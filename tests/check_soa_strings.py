#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Verify the utf8/binary SoA file written by test_soa_strings.cpp: string and
# binary values (including empty strings and embedded NULs) roundtrip, the
# nullable column carries its nulls, and logical types are string/binary.
import sys
import pyarrow.parquet as pq

path = sys.argv[1] if len(sys.argv) > 1 else "n2p_soa_strings.parquet"
pf = pq.ParquetFile(path)
t = pf.read()
d = t.to_pydict()

expect = {
    "id":   [0, 1, 2, 3, 4],
    "name": ["alice", "bob", "carol", "dave", "erin"],
    "note": ["hi", "", None, None, "ok"],
    "blob": [b"\x00\x01", b"xy", b"z", b"", b"qq"],
}
fields = {f.name: f for f in t.schema}
problems = []
for name, exp in expect.items():
    if d.get(name) != exp:
        problems.append(f"{name}: {d.get(name)!r} != {exp!r}")
if str(fields["name"].type) != "string":
    problems.append(f"name type: {fields['name'].type}")
if str(fields["blob"].type) != "binary":
    problems.append(f"blob type: {fields['blob'].type}")
if fields["name"].nullable:
    problems.append("name should be REQUIRED")
if not fields["note"].nullable:
    problems.append("note should be OPTIONAL")
if pf.metadata.num_row_groups != 2:
    problems.append(f"expected 2 row groups, got {pf.metadata.num_row_groups}")

if problems:
    print("SOA STRINGS CHECK FAILED:")
    for p in problems:
        print("  -", p)
    sys.exit(1)
print("soa strings check OK:", {k: d[k] for k in ("name", "note", "blob")})
