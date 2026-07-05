"""Performance and output-size benchmarks (informational; soft gates)."""

from __future__ import annotations

import os
import time

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from nanoarrow_io import lance, parquet


def _mb(path: os.PathLike) -> float:
    return os.path.getsize(path) / (1024 * 1024)


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
    t0 = time.perf_counter()
    parquet.write_table(table, n2p_path, codec="zstd")
    n2p_s = time.perf_counter() - t0
    n2p_sz = _mb(n2p_path)

    arrow_path = tmp_path / "arrow.parquet"
    t0 = time.perf_counter()
    pq.write_table(table, arrow_path, compression="zstd")
    arrow_s = time.perf_counter() - t0
    arrow_sz = _mb(arrow_path)

    # Sanity: output readable and same row count
    assert pq.read_table(n2p_path).num_rows == n
    assert pq.read_table(arrow_path).num_rows == n

    ratio = n2p_s / max(arrow_s, 1e-9)
    size_ratio = n2p_sz / max(arrow_sz, 1e-9)
    print(
        f"parquet write: n2p={n2p_s:.3f}s arrow={arrow_s:.3f}s ratio={ratio:.2f}x; "
        f"size n2p={n2p_sz:.2f}MB arrow={arrow_sz:.2f}MB ratio={size_ratio:.2f}x"
    )
    # Soft gate: n2p should not be orders of magnitude slower on CI runners.
    assert ratio < 5.0


@pytest.mark.bench
def test_lance_write_speed(tmp_path):
    n = 200_000
    table = pa.table(
        {
            "id": pa.array(range(n), type=pa.int64()),
            "tag": pa.array([f"row-{i % 500}" for i in range(n)], type=pa.string()),
        }
    )

    nl_path = tmp_path / "nanolance.lance"
    t0 = time.perf_counter()
    lance.write_table(table, nl_path, compression=True)
    nl_s = time.perf_counter() - t0

    pq_path = tmp_path / "parquet_ref.parquet"
    t0 = time.perf_counter()
    pq.write_table(table, pq_path, compression="zstd")
    pq_s = time.perf_counter() - t0

    assert pa.table(lance.read_table(nl_path)).num_rows == n

    ratio = nl_s / max(pq_s, 1e-9)
    print(f"lance write: nanolance={nl_s:.3f}s parquet-ref={pq_s:.3f}s ratio={ratio:.2f}x")
    assert ratio < 8.0
