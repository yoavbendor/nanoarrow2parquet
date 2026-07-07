"""Performance and output-size benchmarks (informational; soft gates)."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from bench.common import check_results, run_matrix


@pytest.mark.bench
def test_parquet_write_speed_and_size():
    with tempfile.TemporaryDirectory() as tmp:
        out_dir = Path(tmp)
        results = run_matrix(
            profiles=["mixed"],
            codecs=["zstd"],
            rows=500_000,
            repeat=3,
            libs=["n2p", "pyarrow", "polars"],
            out_dir=out_dir,
        )

        by_lib = {row.lib: row for row in results}
        n2p = by_lib["n2p"]
        arrow = by_lib["pyarrow"]

        assert pq.read_table(out_dir / "n2p_mixed_zstd.parquet").num_rows == 500_000
        ratio = n2p.write_s / max(arrow.write_s, 1e-9)
        size_ratio = n2p.file_bytes / max(arrow.file_bytes, 1e-9)
        print(
            f"parquet write (mixed/zstd): n2p={n2p.write_s:.3f}s pyarrow={arrow.write_s:.3f}s "
            f"ratio={ratio:.2f}x; size n2p={n2p.file_bytes / 2**20:.2f}MB "
            f"pyarrow={arrow.file_bytes / 2**20:.2f}MB ratio={size_ratio:.2f}x"
        )
        if "polars" in by_lib:
            pl = by_lib["polars"]
            print(
                f"polars={pl.write_s:.3f}s size={pl.file_bytes / 2**20:.2f}MB "
                f"n2p/polars={pl.write_s / max(n2p.write_s, 1e-9):.2f}x"
            )

        errors = check_results(results, max_write_ratio=5.0, max_size_ratio=1.15)
        assert not errors, "; ".join(errors)


@pytest.mark.bench
@pytest.mark.parametrize("profile", ["numeric", "string_dict"])
def test_parquet_profile_bench(profile):
    with tempfile.TemporaryDirectory() as tmp:
        results = run_matrix(
            profiles=[profile],  # type: ignore[list-item]
            codecs=["zstd", "uncompressed"],
            rows=200_000,
            repeat=2,
            libs=["n2p", "pyarrow", "polars"],
            out_dir=Path(tmp),
        )

    # ZSTD size should stay close; uncompressed string layouts differ by encoder.
    errors = check_results(results, max_write_ratio=5.0, max_size_ratio=1.15, size_codecs=("zstd",))
    assert not errors, "; ".join(errors)


@pytest.mark.bench
def test_streaming_writer_bench():
    from bench.common import run_stream_matrix

    with tempfile.TemporaryDirectory() as tmp:
        results = run_stream_matrix(
            profiles=["mixed"],
            codecs=["zstd"],
            total_rows=300_000,
            chunk_rows=50_000,
            repeat=2,
            libs=["n2p_stream", "pyarrow_stream"],
            out_dir=Path(tmp),
        )

    by_lib = {row.lib: row for row in results}
    stream = by_lib["n2p_stream"]
    pyarrow = by_lib["pyarrow_stream"]
    ratio = stream.write_s / max(pyarrow.write_s, 1e-9)
    print(
        f"streaming mixed/zstd: n2p_stream={stream.write_s:.3f}s "
        f"pyarrow_stream={pyarrow.write_s:.3f}s ratio={ratio:.2f}x"
    )
    assert stream.file_bytes > 0
    assert ratio < 5.0
