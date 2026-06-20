#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Verify the string-encoding file written by test_strings.cpp: read it back with
# pyarrow, assert the values roundtrip, and assert the encoding chooser fell back
# to PLAIN for the high-cardinality column while keeping the low-cardinality one
# dictionary-encoded.
import sys
import pyarrow.parquet as pq

path = sys.argv[1] if len(sys.argv) > 1 else "n2p_strings.parquet"
pf = pq.ParquetFile(path)
t = pf.read()
d = t.to_pydict()

n = 64
palette = ["red", "green", "blue"]
exp_hi = [None if i % 17 == 5 else f"unique-string-value-{i}" for i in range(n)]
exp_lo = [palette[i % 3] for i in range(n)]
exp_ctrl = list(range(n))

problems = []
if d.get("hi") != exp_hi:
    problems.append(f"hi values mismatch: {d.get('hi')}")
if d.get("lo") != exp_lo:
    problems.append(f"lo values mismatch: {d.get('lo')}")
if d.get("ctrl") != exp_ctrl:
    problems.append(f"ctrl values mismatch: {d.get('ctrl')}")

# Per-column-chunk encodings (row group 0).
names = [t.schema.field(i).name for i in range(t.num_columns)]
col_idx = {name: i for i, name in enumerate(names)}
rg = pf.metadata.row_group(0)


def encodings(name):
    return set(rg.column(col_idx[name]).encodings)


hi_enc = encodings("hi")
lo_enc = encodings("lo")
dict_markers = {"RLE_DICTIONARY", "PLAIN_DICTIONARY"}
# High-cardinality: PLAIN, not dictionary-encoded.
if "PLAIN" not in hi_enc:
    problems.append(f"hi should be PLAIN-encoded; got {sorted(hi_enc)}")
if hi_enc & dict_markers:
    problems.append(f"hi should not be dictionary-encoded; got {sorted(hi_enc)}")
# Low-cardinality: dictionary-encoded.
if not (lo_enc & dict_markers):
    problems.append(f"lo should be dictionary-encoded; got {sorted(lo_enc)}")

if problems:
    print("STRINGS CHECK FAILED:")
    for p in problems:
        print("  -", p)
    sys.exit(1)
print(f"strings check OK: hi={sorted(hi_enc)} lo={sorted(lo_enc)}")
