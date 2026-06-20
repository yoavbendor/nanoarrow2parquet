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
    "id":    [0, 1, 2, 3, 4],
    "name":  ["alice", "bob", "carol", "dave", "erin"],
    "note":  ["hi", "", None, None, "ok"],
    "blob":  [b"\x00\x01", b"xy", b"z", b"", b"qq"],
    "lname": ["LA", "LB", "LC", "LD", "LE"],
    "lblob": [b"p", None, b"r", b"ss", b"tt"],
}
fields = {f.name: f for f in t.schema}
problems = []
for name, exp in expect.items():
    if d.get(name) != exp:
        problems.append(f"{name}: {d.get(name)!r} != {exp!r}")
# Parquet has no separate "large" string/binary, so large_utf8/large_binary read
# back as string/binary -- the large-ness is only the input offset width.
if str(fields["name"].type) != "string":
    problems.append(f"name type: {fields['name'].type}")
if str(fields["blob"].type) != "binary":
    problems.append(f"blob type: {fields['blob'].type}")
if str(fields["lname"].type) != "string":
    problems.append(f"lname type: {fields['lname'].type}")
if str(fields["lblob"].type) != "binary":
    problems.append(f"lblob type: {fields['lblob'].type}")
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
print("soa strings check OK:", {k: d[k] for k in ("name", "note", "blob", "lname", "lblob")})
