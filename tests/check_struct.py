#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Verify the nested-struct file from test_struct.cpp: read it back with pyarrow
# and assert the struct values and the two-level null cases.
import sys
import pyarrow.parquet as pq

path = sys.argv[1] if len(sys.argv) > 1 else "n2p_structs.parquet"
t = pq.read_table(path)
rows = t.to_pylist()

expect = [
    {"id": 10, "addr": {"city": "NYC", "zip": 10001}, "score": 1.5},
    {"id": 20, "addr": None, "score": 2.5},                          # whole struct null
    {"id": 30, "addr": {"city": None, "zip": 30003}, "score": 3.5},  # leaf null in present struct
    {"id": 40, "addr": {"city": "LA", "zip": 40004}, "score": 4.5},
]

problems = []
if rows != expect:
    problems.append(f"rows mismatch:\n  got      {rows}\n  expected {expect}")

# Schema shape: addr is a struct; its zip child is REQUIRED, city is OPTIONAL.
addr = t.schema.field("addr")
if not str(addr.type).startswith("struct"):
    problems.append(f"addr should be a struct, got {addr.type}")
else:
    if addr.type.field("zip").nullable:
        problems.append("addr.zip should be REQUIRED (not nullable)")
    if not addr.type.field("city").nullable:
        problems.append("addr.city should be OPTIONAL (nullable)")
    if not addr.nullable:
        problems.append("addr struct should be OPTIONAL (nullable)")

if problems:
    print("STRUCT CHECK FAILED:")
    for p in problems:
        print("  -", p)
    sys.exit(1)
print("struct check OK:", rows[2]["addr"], "/", rows[1]["addr"])
