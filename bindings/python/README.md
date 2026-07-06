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
| `nanoarrow_io.parquet` | `write_table(table, path, codec="zstd")` | Write all record batches from a table/stream |
| `nanoarrow_io.parquet` | `write_batch(batch, path, codec="zstd")` | Single row-group file |
| `nanoarrow_io.parquet` | `ParquetWriter(path, codec="zstd")` | Streaming writer — one row group per `write_batch` |
| `nanoarrow_io.numpy` | `record_batch(columns)` | Zero-copy NumPy column dict → Arrow export |

## Streaming writes (larger than RAM)

`write_table` already maps each Arrow record batch to one row group, but it still requires the
full table (or stream) to be iterable from Python. For capture pipelines that **generate one chunk
at a time**, use :class:`~nanoarrow_io.parquet.ParquetWriter`:

```python
import pyarrow as pa
from nanoarrow_io import parquet

schema = pa.schema([("id", pa.int64()), ("value", pa.float64())])
with parquet.ParquetWriter("capture.parquet", codec="zstd") as writer:
    for chunk_id in range(10_000):
        batch = pa.record_batch(
            {
                "id": pa.array(range(chunk_id * 5_000, (chunk_id + 1) * 5_000)),
                "value": pa.array([float(i) for i in range(5_000)]),
            },
            schema=schema,
        )
        writer.write_batch(batch)
        del batch  # drop the chunk before generating the next one
# writer.close() runs automatically; footer is written here
```

**Rules:**

- One `write_batch` call → one Parquet row group (same as the C `n2p_writer_write_batch` API).
- Call `close()` (or exit a `with` block) to write the footer; skipping `close` after partial
  writes leaves an unreadable file (inherent Parquet limitation).
- At least one batch is required before `close()`.
- Schema is fixed by the first batch; later batches must match.

Runnable example: [`examples/stream_chunks.py`](examples/stream_chunks.py).

```bash
python examples/stream_chunks.py --rows 5000000 --chunk-rows 100000 --out /tmp/big.parquet
```

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
python bench/run_bench.py --streaming --repeat 3 --json /tmp/py-stream-bench.json
python bench/render_results.py /tmp/py-bench.json --inject README.md
# merge stream rows into the same JSON before render, or re-run render after appending
```

`bench/run_bench.py --quick` runs a 50k-row smoke matrix. `--streaming` benchmarks chunked
`ParquetWriter` loops vs materializing a full table. `--check` exits non-zero on generous write
regressions and ZSTD size drift (uncompressed string layouts are not size-gated — encoders differ).

### Latest results (one-shot `write_table`)

Generated by `bench/render_results.py` from `bench/run_bench.py --json`; regenerate after changes.

<!-- PY_BENCH_RESULTS_START -->

_nanoarrow-io **v0.1.0** @ `c8f964f` (dirty) · 2026-07-06_

- **Host:** Intel(R) Xeon(R) Processor · 4 cores · Linux 6.12.58+
- **Baselines:** pyarrow 24.0.0, polars 1.42.1 (same Arrow table, ZSTD / uncompressed)
- **Profiles:** `mixed` (i64 + low-card string + f64), `numeric` (i64/f64/i32), `string_dict` (repetitive URI runs)
- **Metric:** median wall-clock write time; file size after write

| profile | rows | codec | n2p write | pyarrow | polars | n2p/pyarrow | n2p/polars | n2p MB | pyarrow MB | polars MB |
|:---|---:|:--|---:|---:|---:|:--:|:--:|---:|---:|---:|
| mixed | 500,000 | uncompressed | 0.020s | 0.028s | 0.021s | **1.40×** | **1.05×** | 8.41 | 8.76 | 8.28 |
| mixed | 500,000 | zstd | 0.040s | 0.055s | 0.023s | **1.35×** | **0.58×** | 1.15 | 1.65 | 1.16 |
| numeric | 500,000 | uncompressed | 0.012s | 0.022s | 0.013s | **1.83×** | **1.04×** | 9.72 | 8.63 | 8.12 |
| numeric | 500,000 | zstd | 0.028s | 0.045s | 0.011s | **1.60×** | **0.40×** | 1.15 | 1.63 | 1.16 |
| string_dict | 500,000 | uncompressed | 0.017s | 0.022s | 0.018s | **1.29×** | **1.07×** | 8.23 | 4.08 | 3.82 |
| string_dict | 500,000 | zstd | 0.026s | 0.032s | 0.016s | **1.21×** | **0.61×** | 0.95 | 1.37 | 0.95 |

_`n2p/pyarrow` and `n2p/polars` are speedups (>1 = nanoarrow-io faster). Regenerate with `python bench/run_bench.py --json …` then `python bench/render_results.py … --inject README.md`._

<!-- PY_BENCH_RESULTS_END -->

### Streaming results (`ParquetWriter` loop)

Chunked writes: only one row group materialized at a time. Compares `ParquetWriter.write_batch` in a
loop against building a full `pa.Table` then `write_table`, and against `pyarrow.parquet.ParquetWriter`.

<!-- PY_STREAM_BENCH_RESULTS_START -->

_Streaming write @ `c8f964f` (dirty) · 2026-07-06_ (chunk=100,000 rows/row-group)

- **Baselines:** `n2p_stream` = :class:`ParquetWriter` loop, `n2p_table` = materialize then `write_table`, `pyarrow_stream` = `pq.ParquetWriter` loop

| profile | rows | codec | n2p_stream | n2p_table | pyarrow_stream | stream/table | stream/pyarrow | n2p_stream MB | pyarrow MB |
|:---|---:|:--|---:|---:|---:|:--:|:--:|---:|---:|
| mixed | 500,000 | uncompressed | 0.132s | 0.133s | 0.162s | **1.01×** | **1.23×** | 8.46 | 10.22 |
| mixed | 500,000 | zstd | 0.244s | 0.164s | 0.191s | **0.67×** | **0.78×** | 1.16 | 3.02 |
| numeric | 500,000 | uncompressed | 0.092s | 0.091s | 0.119s | **0.99×** | **1.30×** | 9.72 | 10.05 |
| numeric | 500,000 | zstd | 0.114s | 0.114s | 0.135s | **1.00×** | **1.19×** | 1.15 | 3.00 |
| string_dict | 500,000 | uncompressed | 0.216s | 0.216s | 0.231s | **1.00×** | **1.07×** | 8.11 | 4.79 |
| string_dict | 500,000 | zstd | 0.234s | 0.235s | 0.241s | **1.01×** | **1.03×** | 0.95 | 2.58 |

_Chunked writes keep peak RSS near one row group — see `examples/stream_chunks.py`. Regenerate with `python bench/run_bench.py --streaming --json …`._

<!-- PY_STREAM_BENCH_RESULTS_END -->

## Tests

- **Parity**: compares output against pyarrow / pandas / polars readers
- **Full cycle**: Python → native writer → standard reader → Python
- **Memory safety**: repeated writes under RSS caps
- **Benchmarks**: `pytest tests/test_benchmarks.py -m bench` (optional)

## Build layout

Follows the [nanom](https://github.com/yoavbendor/nanom) binding pattern: `scikit-build-core` + `nanobind`, shared `arrow_capsule.hpp` bridge, extension modules `_n2p` and `_numpy`.
