# nanoarrow-io Python bindings

Fast, dependency-light Python bindings for [nanoarrow2parquet](https://github.com/yoavbendor/nanoarrow2parquet) and [nanolance](https://github.com/yoavbendor/nanolance).

Data moves through the [Arrow PyCapsule interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html) — no buffer copies on the Python ↔ native boundary. Output files are validated against the standard Python readers (pyarrow, pandas, polars, and the `lance` package).

## Install (development)

```bash
cd bindings/python
python -m pip install -e ".[test]"
pytest
```

## Quick start

```python
import pyarrow as pa
from nanoarrow_io import parquet, lance

table = pa.table({
    "id": [1, 2, 3],
    "name": ["alpha", "beta", "gamma"],
    "value": [1.5, 2.5, 3.5],
})

# Parquet (nanoarrow2parquet)
parquet.write_table(table, "out.parquet")
assert pa.parquet.read_table("out.parquet").equals(table)

# Lance (nanolance)
lance.write_table(table, "out.lance")
roundtrip = pa.table(lance.read_table("out.lance"))
assert roundtrip.equals(table)
```

## API

| Module | Function | Description |
|--------|----------|-------------|
| `nanoarrow_io.parquet` | `write_table(table, path, codec="zstd")` | Stream one or more record batches to Parquet |
| `nanoarrow_io.parquet` | `write_batch(batch, path, codec="zstd")` | Single row-group file |
| `nanoarrow_io.lance` | `write_table(table, path, **opts)` | Write a Lance dataset |
| `nanoarrow_io.lance` | `read_table(path)` | Arrow-exportable Lance reader handle |

## Tests

- **Parity**: compares output against pyarrow / pandas / polars readers
- **Full cycle**: Python → native writer → standard reader → Python
- **Memory safety**: repeated write/read under RSS caps
- **Benchmarks**: `pytest tests/test_benchmarks.py -k bench` (optional)

## Build layout

Follows the [nanom](https://github.com/yoavbendor/nanom) binding pattern: `scikit-build-core` + `nanobind`, shared `arrow_capsule.hpp` bridge, two extension modules (`_n2p`, `_nanolance`).
