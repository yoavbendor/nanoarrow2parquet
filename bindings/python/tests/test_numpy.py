"""Tests for zero-copy NumPy -> Arrow bridge."""

from __future__ import annotations

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from nanoarrow_io import numpy as naio_numpy, parquet


@pytest.fixture
def numeric_columns() -> dict[str, np.ndarray]:
    n = 10_000
    return {
        "id": np.arange(n, dtype=np.int64),
        "flags": np.array([i % 2 == 0 for i in range(n)], dtype=np.int8),
        "value": (np.arange(n, dtype=np.float64) * 0.01),
    }


def test_record_batch_zero_copy_numeric(numeric_columns):
    batch = naio_numpy.record_batch(numeric_columns)
    assert batch.length == len(numeric_columns["id"])
    assert batch.column_address("id") == numeric_columns["id"].__array_interface__["data"][0]


def test_numpy_parquet_roundtrip(numeric_columns, tmp_path):
    batch = naio_numpy.record_batch(numeric_columns)
    n = batch.length
    path = tmp_path / "from_numpy.parquet"
    parquet.write_table(batch, path)

    table = pq.read_table(path)
    assert table.num_rows == n
    assert table.column("id").to_numpy().tolist() == numeric_columns["id"].tolist()


def test_rejects_non_contiguous():
    arr = np.arange(8, dtype=np.int32)[::2]
    with pytest.raises(ValueError, match="C-contiguous"):
        naio_numpy.record_batch({"x": arr})


def test_rejects_object_dtype_strings():
    arr = np.array(["a", "b", "c"], dtype=object)
    with pytest.raises(ValueError, match="unsupported numpy dtype"):
        naio_numpy.record_batch({"s": arr})
