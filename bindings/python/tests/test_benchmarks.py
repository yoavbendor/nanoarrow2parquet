"""Performance and output-size benchmarks (informational; soft gates).

Nanolance vs pylance is highly dataset-dependent. Prior C++ benches (nanolance
``tools/bench.py``) show write parity on repetitive string / integer shapes and
a larger gap on high-cardinality scattered strings. Threading is not the main
factor: limiting pylance to one Rayon worker only modestly slows it (~20–30%),
while nanolance string writes are dominated by the plain variable-width encode
path when dictionary encoding does not apply.
"""

from __future__ import annotations

import os
import random
import statistics
import time

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from nanoarrow_io import nanolance, parquet

from tests.support import require_pylance


def _mb(path: os.PathLike) -> float:
    return os.path.getsize(path) / (1024 * 1024)


def _median_seconds(fn, *, iters: int = 3) -> float:
    return statistics.median(fn() for _ in range(iters))


def _pylance_write_seconds(lance, table: pa.Table, path, *, rayon_threads: int | None) -> float:
    old = os.environ.get("RAYON_NUM_THREADS")
    if rayon_threads is not None:
        os.environ["RAYON_NUM_THREADS"] = str(rayon_threads)
    try:
        return _median_seconds(
            lambda: _write_once(lance.write_dataset, table, path),
        )
    finally:
        if old is None:
            os.environ.pop("RAYON_NUM_THREADS", None)
        else:
            os.environ["RAYON_NUM_THREADS"] = old


def _write_once(write_fn, table: pa.Table, path) -> float:
    if os.path.exists(path):
        if os.path.isdir(path):
            import shutil

            shutil.rmtree(path)
        else:
            os.remove(path)
    t0 = time.perf_counter()
    write_fn(table, path)
    return time.perf_counter() - t0


def _wide_int_table(n: int) -> pa.Table:
    random.seed(1)
    return pa.table(
        {
            "ts": pa.array(
                [1_700_000_000_000_000 + i * 1500 + random.randint(0, 200) for i in range(n)],
                pa.uint64(),
            ),
            "caplen": pa.array([random.randint(60, 1514) for _ in range(n)], pa.uint32()),
            "iface": pa.array([(i // 10_000) % 4 for i in range(n)], pa.uint8()),
            "ipproto": pa.array([random.choice([6, 17, 6, 6, 1]) for _ in range(n)], pa.uint8()),
        }
    )


def _pcap_ref_table(n: int) -> pa.Table:
    min_distinct = n // 5000
    return pa.table(
        {
            "uri": pa.array(
                [f"s3://bucket/cap_{(i * min_distinct // n):02d}.pcapng" for i in range(n)],
                pa.string(),
            ),
            "position": pa.array([i * 1500 for i in range(n)], pa.uint64()),
            "size": pa.array([1500] * n, pa.uint64()),
        }
    )


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


@pytest.mark.bench
def test_lance_write_wide_int_near_pylance(tmp_path):
    """Integer-heavy shape: nanolance C++ bench is ~1.3× pylance on this profile."""
    lance = require_pylance()
    n = 200_000
    table = _wide_int_table(n)

    nl_path = tmp_path / "nanolance.lance"
    nl_s = _median_seconds(
        lambda: _write_once(
            lambda t, p: nanolance.write_table(t, p, compression=False),
            table,
            nl_path,
        ),
    )

    pl_path = tmp_path / "pylance.lance"
    pl_s = _pylance_write_seconds(lance, table, pl_path, rayon_threads=1)
    pl_mt_s = _pylance_write_seconds(lance, table, pl_path, rayon_threads=None)

    assert pa.table(nanolance.read_table(nl_path)).num_rows == n

    ratio = nl_s / max(pl_s, 1e-9)
    print(
        f"lance wide_int write: nanolance={nl_s:.3f}s pylance(1t)={pl_s:.3f}s "
        f"pylance(default)={pl_mt_s:.3f}s ratio={ratio:.2f}x"
    )
    assert ratio < 4.0


@pytest.mark.bench
def test_lance_write_pcap_ref_with_zstd_near_pylance(tmp_path):
    """Repetitive URI runs: nanolance dict-RLE + zstd matches or beats pylance."""
    lance = require_pylance()
    n = 200_000
    table = _pcap_ref_table(n)

    nl_path = tmp_path / "nanolance.lance"
    nl_s = _median_seconds(
        lambda: _write_once(
            lambda t, p: nanolance.write_table(t, p, compression=True),
            table,
            nl_path,
        ),
    )

    pl_path = tmp_path / "pylance.lance"
    pl_s = _pylance_write_seconds(lance, table, pl_path, rayon_threads=1)

    assert pa.table(nanolance.read_table(nl_path)).num_rows == n

    ratio = nl_s / max(pl_s, 1e-9)
    print(f"lance pcap_ref+zstd write: nanolance={nl_s:.3f}s pylance(1t)={pl_s:.3f}s ratio={ratio:.2f}x")
    assert ratio < 3.0


@pytest.mark.bench
def test_lance_cycling_strings_near_pylance(tmp_path):
    """Cycling low-card strings: structural dictionary should be near pylance parity."""
    lance = require_pylance()
    n = 200_000
    table = pa.table(
        {
            "id": pa.array(range(n), type=pa.int64()),
            "tag": pa.array([f"row-{i % 500}" for i in range(n)], type=pa.string()),
            "value": pa.array([float(i) * 0.01 for i in range(n)], type=pa.float64()),
        }
    )

    nl_path = tmp_path / "nanolance.lance"
    nl_s = _median_seconds(
        lambda: _write_once(
            lambda t, p: nanolance.write_table(t, p, compression=True),
            table,
            nl_path,
        ),
    )
    nl_sz = _mb(nl_path)

    pl_path = tmp_path / "pylance.lance"
    pl_1t = _pylance_write_seconds(lance, table, pl_path, rayon_threads=1)
    pl_sz = _mb(pl_path)

    assert pa.table(nanolance.read_table(nl_path)).num_rows == n

    ratio_1t = nl_s / max(pl_1t, 1e-9)
    size_ratio = nl_sz / max(pl_sz, 1e-9)
    print(
        f"lance cycling-string write: nanolance={nl_s:.3f}s pylance(1t)={pl_1t:.3f}s "
        f"ratio={ratio_1t:.2f}x; size nanolance={nl_sz:.2f}MB pylance={pl_sz:.2f}MB ratio={size_ratio:.2f}x"
    )
    assert ratio_1t < 4.0
    assert size_ratio < 2.5
