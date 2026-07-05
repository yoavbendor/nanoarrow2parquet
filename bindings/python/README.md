# nanoarrow-io Python bindings

Fast, dependency-light Python bindings for [nanoarrow2parquet](https://github.com/yoavbendor/nanoarrow2parquet) and [nanolance](https://github.com/yoavbendor/nanolance).

Data moves through the [Arrow PyCapsule interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html) — no buffer copies on the Python ↔ native boundary. Output files are validated against pyarrow, pandas, polars, and the official Lance format SDK.

## Naming: we do not shadow `import lance`

The official Lance columnar format SDK is published on PyPI as **`pylance`** but imported as **`import lance`** ([lance-format/lance](https://github.com/lance-format/lance)). That is unrelated to Microsoft's **Pylance** VS Code language server.

This package deliberately exposes its fast C++ bindings as **`nanoarrow_io.nanolance`**, not `nanoarrow_io.lance`, so you can use both side by side:

```python
import lance                              # official SDK (pip install pylance)
from nanoarrow_io import nanolance        # fast C++ nanolance bindings
```

There is also an unrelated PyPI package named `lance` (JSON code generator) — do **not** `pip install lance` for Lance datasets; use **`pip install pylance`**.

## Install (development)

```bash
cd bindings/python
python -m pip install -e ".[test]"
pytest
```

Optional interoperability tests also need the official SDK:

```bash
pip install pylance
```

## Quick start

```python
import pyarrow as pa
from nanoarrow_io import parquet, nanolance

table = pa.table({
    "id": [1, 2, 3],
    "name": ["alpha", "beta", "gamma"],
    "value": [1.5, 2.5, 3.5],
})

# Parquet (nanoarrow2parquet)
parquet.write_table(table, "out.parquet")
assert pa.parquet.read_table("out.parquet").equals(table)

# Lance (nanolance C++ bindings)
nanolance.write_table(table, "out.lance")
roundtrip = pa.table(nanolance.read_table("out.lance"))
assert roundtrip.equals(table)

# Official pylance reader (pip install pylance)
import lance
assert lance.dataset("out.lance").to_table().equals(table)
```

## API

| Module | Function | Description |
|--------|----------|-------------|
| `nanoarrow_io.parquet` | `write_table(table, path, codec="zstd")` | Stream one or more record batches to Parquet |
| `nanoarrow_io.parquet` | `write_batch(batch, path, codec="zstd")` | Single row-group file |
| `nanoarrow_io.nanolance` | `write_table(table, path, **opts)` | Write a Lance dataset |
| `nanoarrow_io.nanolance` | `read_table(path)` | Arrow-exportable Lance reader handle |

## Benchmarks

```bash
pytest tests/test_benchmarks.py -m bench
```

Nanolance vs pylance depends strongly on column shape (see nanolance `tools/bench.py` / `bench/linux-ci-results.md`):

| profile | nanolance vs pylance (write) | notes |
|---------|------------------------------|-------|
| integer-heavy (`wide_int`) | ~1.3× | near parity |
| repetitive string runs + zstd (`pcap_ref`) | ~0.8–1× | dict-RLE + zstd |
| scattered low-card strings (`row-{i%500}`) | ~20–40× | plain variable-width path |

**Threading:** pylance (Rust/Rayon) uses modest parallelism — limiting `RAYON_NUM_THREADS=1` typically costs only ~20–30%, not an order of magnitude. Nanolance bindings are single-threaded today (~100% of one core on CPU-bound writes). The scattered-string gap is encoder selection, not core count; multi-threaded zstd would not fix the plain miniblock string path.

To compare fairly against pylance in your own scripts, pin Rayon when needed:

```bash
RAYON_NUM_THREADS=1 pytest tests/test_benchmarks.py -m bench
```

## Tests

- **Parity**: compares output against pyarrow / pandas / polars readers
- **pylance interop**: files written by `nanolance` read back via `lance.dataset()`
- **Full cycle**: Python → native writer → standard reader → Python
- **Memory safety**: repeated write/read under RSS caps
- **Benchmarks**: `pytest tests/test_benchmarks.py -m bench` (optional; see Benchmarks above)

## Build layout

Follows the [nanom](https://github.com/yoavbendor/nanom) binding pattern: `scikit-build-core` + `nanobind`, shared `arrow_capsule.hpp` bridge, two extension modules (`_n2p`, `_nanolance`).
