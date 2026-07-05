"""Memory safety smoke tests for the bindings."""

from __future__ import annotations

import gc

import pyarrow as pa
import pytest

from nanoarrow_io import nanolance, parquet

psutil = pytest.importorskip("psutil")
PROC = psutil.Process()


def _rss_mb() -> float:
    return PROC.memory_info().rss / (1024 * 1024)


def test_parquet_repeated_writes_bounded(sample_table, tmp_path):
    baseline = _rss_mb()
    for i in range(50):
        parquet.write_table(sample_table, tmp_path / f"mem_{i}.parquet")
    gc.collect()
    growth = _rss_mb() - baseline
    assert growth < 200, f"RSS grew by {growth:.1f} MB after 50 parquet writes"


def test_lance_repeated_roundtrip_bounded(sample_table, tmp_path):
    baseline = _rss_mb()
    for i in range(20):
        ds = tmp_path / f"mem_{i}.lance"
        nanolance.write_table(sample_table, ds, compression=True)
        _ = pa.table(nanolance.read_table(ds))
    gc.collect()
    growth = _rss_mb() - baseline
    assert growth < 250, f"RSS grew by {growth:.1f} MB after 20 lance cycles"


def test_large_single_batch_zero_copy(tmp_path):
    n = 1_000_000
    table = pa.table({"x": pa.array(range(n), type=pa.int64())})
    path = tmp_path / "big.parquet"
    before = _rss_mb()
    parquet.write_table(table, path)
    gc.collect()
    after = _rss_mb()
    assert after - before < 300
