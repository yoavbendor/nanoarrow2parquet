"""Performance and output-size benchmarks (informational; soft gates)."""

from __future__ import annotations

import os
import statistics
import time

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from nanoarrow_io import parquet


def _mb(path: os.PathLike) -> float:
    return os.path.getsize(path) / (1024 * 1024)


def _median_seconds(fn, *, iters: int = 3) -> float:
    return statistics.median(fn() for _ in range(iters))


def _write_once(write_fn, table: pa.Table, path) -> float:
    if os.path.exists(path):
        os.remove(path)
    t0 = time.perf_counter()
    write_fn(table, path)
    return time.perf_counter() - t0


@pytest.mark.bench
def test_parquet_write_speed_and_size(tmp_path):
    n = 500_000
    table = pa.table(
        {
            "id": pa.array(range(n), type=pa.int64()),
            "tag": pa.array([f"row-{i % 1000}" for i in range(n)], type=pa.string()),
            "value": pa.array([float(i) * 0.01 for i in range(n)], type=pa.float64()),
        }
    )

    n2p_path = tmp_path / "n2p.parquet"
    n2p_s = _median_seconds(
        lambda: _write_once(lambda t, p: parquet.write_table(t, p, codec="zstd"), table, n2p_path),
    )
    n2p_sz = _mb(n2p_path)

    arrow_path = tmp_path / "arrow.parquet"
    arrow_s = _median_seconds(
        lambda: _write_once(lambda t, p: pq.write_table(t, p, compression="zstd"), table, arrow_path),
    )
    arrow_sz = _mb(arrow_path)

    assert pq.read_table(n2p_path).num_rows == n
    assert pq.read_table(arrow_path).num_rows == n

    ratio = n2p_s / max(arrow_s, 1e-9)
    size_ratio = n2p_sz / max(arrow_sz, 1e-9)
    print(
        f"parquet write: n2p={n2p_s:.3f}s arrow={arrow_s:.3f}s ratio={ratio:.2f}x; "
        f"size n2p={n2p_sz:.2f}MB arrow={arrow_sz:.2f}MB ratio={size_ratio:.2f}x"
    )
    assert ratio < 5.0
