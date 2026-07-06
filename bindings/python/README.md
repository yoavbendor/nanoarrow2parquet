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

## Benchmarks

```bash
pytest tests/test_benchmarks.py -m bench
```

Compares nanoarrow2parquet write speed and compressed size against pyarrow on a mixed int/string/float table.

## Tests

- **Parity**: compares output against pyarrow / pandas / polars readers
- **Full cycle**: Python → native writer → standard reader → Python
- **Memory safety**: repeated writes under RSS caps
- **Benchmarks**: `pytest tests/test_benchmarks.py -m bench` (optional)

## Build layout

Follows the [nanom](https://github.com/yoavbendor/nanom) binding pattern: `scikit-build-core` + `nanobind`, shared `arrow_capsule.hpp` bridge, extension modules `_n2p` and `_numpy`.
