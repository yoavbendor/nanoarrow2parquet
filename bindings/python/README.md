# nanoarrow-io Python bindings

Fast, dependency-light Python bindings for [nanoarrow2parquet](https://github.com/yoavbendor/nanoarrow2parquet): **Parquet** writing and a **NumPy** zero-copy bridge.

Lance I/O lives in the separate **[nanolance](https://github.com/yoavbendor/nanolance)** Python package (`pip install nanolance`). The same PyArrow table or NumPy-backed batch can be passed to either writer:

```python
import pyarrow as pa
from nanoarrow_io import parquet
import nanolance  # separate package

table = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})
parquet.write_table(table, "out.parquet")
nanolance.write_table(table, "out.lance")
```

## Install (development)

```bash
cd bindings/python
python -m pip install -e ".[test]"
pytest
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

parquet.write_table(table, "out.parquet", codec="zstd")
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

Compares nanoarrow2parquet vs pyarrow Parquet write speed and size.

## Tests

- **Parity**: compares output against pyarrow / pandas / polars readers
- **Full cycle**: Python → native writer → standard reader → Python
- **Memory safety**: repeated write/read under RSS caps

## Build layout

Follows the [nanom](https://github.com/yoavbendor/nanom) binding pattern: `scikit-build-core` + `nanobind`, shared `arrow_capsule.hpp` bridge, extension modules `_n2p` and `_numpy`.
