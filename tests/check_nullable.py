#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Verify the nullable / null-type file written by test_nullable.cpp: read it back
# with pyarrow and assert the exact null positions and values.
import sys
import pyarrow.parquet as pq

path = sys.argv[1] if len(sys.argv) > 1 else "n2p_nulls.parquet"
t = pq.read_table(path)
d = t.to_pydict()

expect = {
    "a":   [0, None, 200, None, 400, 500],
    "s":   [None, "alpha", "beta", "alpha", None, "beta"],
    "f":   [False, True, None, True, False, True],
    "nul": [None, None, None, None, None, None],
    "ctrl": [0, 1, 2, 3, 4, 5],
}
fields = {f.name: f for f in t.schema}
problems = []
for name, exp in expect.items():
    if d.get(name) != exp:
        problems.append(f"{name}: got {d.get(name)} expected {exp}")
# Repetition: only ctrl is REQUIRED (not nullable); the rest OPTIONAL.
if fields["ctrl"].nullable:
    problems.append("ctrl should be REQUIRED (not nullable)")
for name in ("a", "s", "f", "nul"):
    if not fields[name].nullable:
        problems.append(f"{name} should be OPTIONAL (nullable)")

if problems:
    print("NULLABLE CHECK FAILED:")
    for p in problems:
        print("  -", p)
    sys.exit(1)
print("nullable check OK:", {k: d[k] for k in ("a", "s", "f")})
