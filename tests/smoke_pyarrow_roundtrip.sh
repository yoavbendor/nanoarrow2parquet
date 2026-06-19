#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Yoav Bendor
#
# End-to-end oracle: write Parquet files with the demo, then read them back with
# pyarrow (and, if available, pandas/polars) and assert the values, dtypes, codec,
# and row-group structure all match. Skips gracefully if pyarrow is unavailable.

set -euo pipefail

DEMO="${N2P_DEMO:-./arrow2parquet_demo}"
PY="${PYTHON:-python3}"

if [[ ! -x "$DEMO" ]]; then
    echo "smoke: demo binary not found at $DEMO" >&2
    exit 1
fi
if ! "$PY" -c "import pyarrow" 2>/dev/null; then
    echo "smoke: pyarrow not installed; skipping" >&2
    exit 0
fi

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

SINGLE="$WORK/single.parquet"
STREAM="$WORK/stream.parquet"

"$DEMO" "$SINGLE"
"$DEMO" "$STREAM" --streaming

"$PY" - "$SINGLE" "$STREAM" <<'PY'
import sys
import pyarrow.parquet as pq

single_path, stream_path = sys.argv[1], sys.argv[2]

expected = {
    "i32":  [-2000, -1000, 0, 1000, 2000],
    "i64":  [0, 1000000, 2000000, 3000000, 4000000],
    "u32":  [4000000000, 4000000001, 4000000002, 4000000003, 4000000004],
    "f32":  [0.5, 1.5, 2.5, 3.5, 4.5],
    "f64":  [0.0, 1.25, 2.5, 3.75, 5.0],
    "flag": [True, False, True, False, True],
    "name": ["alpha", "beta", "alpha", "gamma", "beta"],
}
expected_fsb = [bytes([i, i+1, i+2, i+3]) for i in range(5)]

def check_values(path, label):
    t = pq.read_table(path)
    d = t.to_pydict()
    for col, exp in expected.items():
        got = d[col]
        assert got == exp, f"{label}: column {col} mismatch: {got} != {exp}"
    assert d["fsb"] == expected_fsb, f"{label}: fsb mismatch: {d['fsb']}"
    # dtypes
    schema = t.schema
    assert str(schema.field("u32").type) == "uint32", f"{label}: u32 not unsigned"
    assert str(schema.field("fsb").type) == "fixed_size_binary[4]", f"{label}: fsb type"
    assert str(schema.field("name").type) == "string", f"{label}: name type"
    print(f"  {label}: values + dtypes OK ({t.num_rows} rows)")

# Single-shot file: one row group, every column ZSTD-compressed.
check_values(single_path, "single")
md = pq.ParquetFile(single_path).metadata
assert md.num_row_groups == 1, f"single: expected 1 row group, got {md.num_row_groups}"
for i in range(md.row_group(0).num_columns):
    c = md.row_group(0).column(i)
    assert c.compression == "ZSTD", f"single: column {c.path_in_schema} codec {c.compression}"
name_col = md.row_group(0).column(md.num_columns - 1)  # 'name' is the last column
assert "RLE_DICTIONARY" in [str(e) for e in name_col.encodings], "single: name not dict-encoded"
print("  single: 1 row group, ZSTD, dictionary-encoded strings OK")

# Streaming file: three row groups, same concatenated values.
check_values(stream_path, "stream")
md2 = pq.ParquetFile(stream_path).metadata
assert md2.num_row_groups == 3, f"stream: expected 3 row groups, got {md2.num_row_groups}"
assert md2.num_rows == 5, f"stream: expected 5 rows, got {md2.num_rows}"
print("  stream: 3 row groups, 5 rows OK")

# Optional cross-readers: confirm stricter readers accept the footer too.
try:
    import pandas as pd
    assert pd.read_parquet(single_path).shape == (5, 8)
    print("  pandas: read OK")
except ImportError:
    pass
try:
    import polars as pl
    assert pl.read_parquet(stream_path).shape == (5, 8)
    print("  polars: read OK")
except ImportError:
    pass

print("smoke_pyarrow_roundtrip: PASS")
PY
