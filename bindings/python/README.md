# nanoarrow-io Python bindings

Fast, dependency-light Python bindings for [nanoarrow2parquet](https://github.com/yoavbendor/nanoarrow2parquet).

Data moves through the [Arrow PyCapsule interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html) — no buffer copies on the Python ↔ native boundary. Output files are validated against pyarrow, pandas, and polars readers.

## Lance datasets

Lance reader/writer bindings live in the sibling [nanolance](https://github.com/yoavbendor/nanolance) project. Install from that repo's `bindings/python` directory:

```bash
cd /path/to/nanolance/bindings/python
pip install -e ".[test]"
```

```python
import pyarrow as pa
import nanolance

table = pa.table({"id": [1, 2, 3], "name": ["alpha", "beta", "gamma"]})
nanolance.write_table(table, "out.lance", compression=True)
assert pa.table(nanolance.read_table("out.lance")).equals(table)
```

The same Arrow table can feed either writer — `nanoarrow-io` for Parquet, `nanolance` for Lance — but each repo ships its own Python package.

## Install (development)

```bash
cd bindings/python
python -m pip install -e ".[test]"
pytest -m "not bench"
```

## Quick start

```python
import pyarrow as pa
from nanoarrow_io import parquet

table = pa.table({
    "id": [1, 2, 3],
    "name": ["alpha", "beta", "gamma"],
    "value": [1.5, 2.5, 3.5],
})

parquet.write_table(table, "out.parquet")
assert pa.parquet.read_table("out.parquet").equals(table)
```

### NumPy (zero-copy columns)

```python
import numpy as np
from nanoarrow_io import numpy as naio_numpy, parquet

cols = {
    "id": np.arange(1_000_000, dtype=np.int64),
    "value": np.arange(1_000_000, dtype=np.float64) * 0.01,
}
parquet.write_table(naio_numpy.record_batch(cols), "out.parquet")
```

Columns must be 1-D, C-contiguous, and use a supported numeric dtype (bool, int/uint/float
1/2/4/8-byte widths). String/object columns still require pyarrow/polars export today.

## API

| Module | Function | Description |
|--------|----------|-------------|
| `nanoarrow_io.parquet` | `write_table(table, path, codec="zstd")` | Stream one or more record batches to Parquet |
| `nanoarrow_io.parquet` | `write_batch(batch, path, codec="zstd")` | Single row-group file |
| `nanoarrow_io.numpy` | `record_batch(columns)` | Zero-copy NumPy column dict → Arrow export |

## Parquet format support

nanoarrow-io is a **write-only** Parquet encoder. Files are standard Parquet v1 with Thrift compact footer metadata, readable by pyarrow, polars, DuckDB, and pandas.

### Format features

| Feature | Supported | Unsupported / notes |
|---------|:---------:|---------------------|
| Parquet format version | v1 | v2 pages |
| Writer | yes | — |
| Reader | — | use pyarrow / polars / DuckDB |
| Streaming row groups | yes | one Arrow record batch → one row group |
| Multi-page columns | — | one data page per column per row group |
| Nested struct columns | yes | `+s`, arbitrarily deep; dotted `path_in_schema` |
| Nullable columns | yes | definition levels (`ARROW_FLAG_NULLABLE`) |
| Column statistics | — | min/max/null_count not written |
| Bloom filters / column indexes | — | — |
| Arrow schema in footer KV metadata | — | pyarrow adds this; n2p does not |
| Sliced arrays (`offset != 0`) | — | error at write time |

### Compression codecs

| Codec | Write | Notes |
|-------|:-----:|-------|
| **ZSTD** | yes | default; ZSTD level 3 (hardcoded) |
| **UNCOMPRESSED** | yes | `codec="uncompressed"` |
| SNAPPY | — | — |
| GZIP | — | — |
| LZ4 / LZ4_RAW | — | — |
| BROTLI | — | — |

### Column encodings

| Encoding | Write | When used |
|----------|:-----:|-----------|
| **PLAIN** | yes | fixed-width numerics/bool; high-cardinality strings/binary |
| **RLE_DICTIONARY** | yes | `utf8` / `binary` when dictionary shrinks the column |
| **RLE** | yes | definition levels on nullable columns only |
| **BIT_PACKED** | yes | boolean value bytes only |
| PLAIN_DICTIONARY | — | legacy dictionary form |
| DELTA_BINARY_PACKED | — | — |
| DELTA_LENGTH_BYTE_ARRAY | — | — |
| DELTA_BYTE_ARRAY | — | — |
| BYTE_STREAM_SPLIT | — | — |

### Arrow data types

| Arrow (C format) | Parquet physical | Encoding | Notes |
|------------------|------------------|----------|-------|
| `i` `l` `f` `g` `b` | INT32 / INT64 / FLOAT / DOUBLE / BOOLEAN | PLAIN | values buffer ≈ memcpy |
| `I` `L` | INT32 / INT64 + `UINT_*` | PLAIN | bits identical |
| `c` `s` `C` `S` | INT32 + `INT_8/16` / `UINT_8/16` | PLAIN | widened 1/2 → 4 bytes |
| `w:N` | FIXED_LEN_BYTE_ARRAY | PLAIN | `type_length=N` |
| `u` `U` `z` `Z` | BYTE_ARRAY | RLE_DICTIONARY or PLAIN | auto per column |
| `n` | all-null INT32 | OPTIONAL | null type |
| `+s` | struct group | — | recurses to leaves |
| `+l` `+L` list | — | — | no repetition levels |
| `+m` map | — | — | no repetition levels |
| `td*`, `ts*`, `tD`, `tT` timestamps/dates | — | — | use pyarrow cast first |
| `d:…` decimal | — | — | — |
| Arrow dictionary (`i`/`l` + dictionary) | — | — | export plain values first |

## Benchmarks

Compare **nanoarrow-io** (`parquet.write_table`) against **pyarrow** and **polars** on the same in-memory Arrow tables.

```bash
cd bindings/python
pip install -e ".[test]"    # or ".[bench]" for benchmarks only

# pytest smoke / regression gates (soft ratios)
pytest tests/test_benchmarks.py -m bench

# full matrix + publishable table
python bench/run_bench.py --repeat 3 --json /tmp/py-bench.json
python bench/render_results.py /tmp/py-bench.json --inject README.md
```

`bench/run_bench.py --quick` runs a 50k-row smoke matrix. `--check` exits non-zero on generous write regressions and ZSTD size drift (uncompressed string layouts are not size-gated — encoders differ).

### Latest results

Generated by `bench/render_results.py` from `bench/run_bench.py --json`; regenerate after changes.

<!-- PY_BENCH_RESULTS_START -->

_nanoarrow-io **v0.1.0** @ `3af237d` (dirty) · 2026-07-06_

- **Host:** Intel(R) Xeon(R) Processor · 4 cores · Linux 6.12.58+
- **Baselines:** pyarrow 24.0.0, polars 1.42.1 (same Arrow table, ZSTD / uncompressed)
- **Profiles:** `mixed` (i64 + low-card string + f64), `numeric` (i64/f64/i32), `string_dict` (repetitive URI runs)
- **Metric:** median wall-clock write time; file size after write

| profile | rows | codec | n2p write | pyarrow | polars | n2p/pyarrow | n2p/polars | n2p MB | pyarrow MB | polars MB |
|:---|---:|:--|---:|---:|---:|:--:|:--:|---:|---:|---:|
| mixed | 500,000 | uncompressed | 0.020s | 0.029s | 0.021s | **1.48×** | **1.05×** | 8.41 | 8.76 | 8.28 |
| mixed | 500,000 | zstd | 0.036s | 0.047s | 0.018s | **1.30×** | **0.49×** | 1.15 | 1.65 | 1.16 |
| numeric | 500,000 | uncompressed | 0.011s | 0.029s | 0.014s | **2.66×** | **1.25×** | 9.72 | 8.63 | 8.12 |
| numeric | 500,000 | zstd | 0.026s | 0.045s | 0.010s | **1.73×** | **0.39×** | 1.15 | 1.63 | 1.16 |
| string_dict | 500,000 | uncompressed | 0.016s | 0.022s | 0.014s | **1.35×** | **0.88×** | 8.23 | 4.08 | 3.82 |
| string_dict | 500,000 | zstd | 0.025s | 0.032s | 0.016s | **1.26×** | **0.64×** | 0.95 | 1.37 | 0.95 |

_`n2p/pyarrow` and `n2p/polars` are speedups (>1 = nanoarrow-io faster). Regenerate with `python bench/run_bench.py --json …` then `python bench/render_results.py … --inject README.md`._

<!-- PY_BENCH_RESULTS_END -->

## Tests

- **Parity**: compares output against pyarrow / pandas / polars readers
- **Full cycle**: Python → native writer → standard reader → Python
- **Memory safety**: repeated writes under RSS caps
- **Benchmarks**: `pytest tests/test_benchmarks.py -m bench` (optional)

## Build layout

Follows the [nanom](https://github.com/yoavbendor/nanom) binding pattern: `scikit-build-core` + `nanobind`, shared `arrow_capsule.hpp` bridge, extension modules `_n2p` and `_numpy`.
