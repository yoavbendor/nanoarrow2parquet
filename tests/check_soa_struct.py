#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Verify the nested-struct SoA file written by test_soa_struct.cpp: struct values
# (assembled from SoA columns via group()) roundtrip, multi-level nulls land in
# the right place (a null can come from the leaf or any ancestor struct), the
# REQUIRED/OPTIONAL flags are right, and there are 2 row groups.
import sys
import pyarrow.parquet as pq

path = sys.argv[1] if len(sys.argv) > 1 else "n2p_soa_struct.parquet"
pf = pq.ParquetFile(path)
t = pf.read()
d = t.to_pydict()

exp_id = [0, 1, 2, 3, 4]
exp_addr = [
    {"city": "NYC", "zip": 10001},
    {"city": "LA", "zip": 90001},
    {"city": "SF", "zip": 94101},
    {"city": "BOS", "zip": 2101},
    {"city": "SEA", "zip": 98101},
]
exp_meta = [
    {"score": 1.5, "geo": {"lat": 40.7, "lon": -74.0}},
    None,                                   # meta null (ancestor null)
    {"score": 3.5, "geo": None},            # geo null
    {"score": 4.5, "geo": None},            # geo null
    {"score": 5.5, "geo": {"lat": 47.6, "lon": -122.3}},
]

problems = []
if d.get("id") != exp_id:
    problems.append(f"id: {d.get('id')} != {exp_id}")
if d.get("addr") != exp_addr:
    problems.append(f"addr: {d.get('addr')} != {exp_addr}")
if d.get("meta") != exp_meta:
    problems.append(f"meta: {d.get('meta')} != {exp_meta}")

addr_f = t.schema.field("addr")
meta_f = t.schema.field("meta")
if addr_f.nullable:
    problems.append("addr should be REQUIRED")
if not meta_f.nullable:
    problems.append("meta should be OPTIONAL")
# Nested flags: score REQUIRED, geo OPTIONAL within meta.
if meta_f.type.field("score").nullable:
    problems.append("meta.score should be REQUIRED")
if not meta_f.type.field("geo").nullable:
    problems.append("meta.geo should be OPTIONAL")
if pf.metadata.num_row_groups != 2:
    problems.append(f"expected 2 row groups, got {pf.metadata.num_row_groups}")

if problems:
    print("SOA STRUCT CHECK FAILED:")
    for p in problems:
        print("  -", p)
    sys.exit(1)
print("soa struct check OK:", d.get("meta"))
