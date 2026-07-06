"""Shared helpers for Python Parquet write benchmarks."""

from __future__ import annotations

import os
import shutil
import statistics
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Callable, Literal

import pyarrow as pa
import pyarrow.parquet as pq

from nanoarrow_io import parquet as n2p_parquet

Profile = Literal["mixed", "numeric", "string_dict"]
Codec = Literal["zstd", "uncompressed"]
Lib = Literal["n2p", "pyarrow", "polars"]


@dataclass(frozen=True)
class BenchRow:
    lib: Lib
    profile: Profile
    rows: int
    codec: Codec
    write_s: float
    file_bytes: int
    mrows_per_s: float

    def to_json(self) -> dict:
        return asdict(self)


def make_table(profile: Profile, rows: int) -> pa.Table:
    if profile == "mixed":
        return pa.table(
            {
                "id": pa.array(range(rows), type=pa.int64()),
                "tag": pa.array([f"row-{i % 1000}" for i in range(rows)], type=pa.string()),
                "value": pa.array([float(i) * 0.01 for i in range(rows)], type=pa.float64()),
            }
        )
    if profile == "numeric":
        return pa.table(
            {
                "id": pa.array(range(rows), type=pa.int64()),
                "value": pa.array([float(i) * 0.01 for i in range(rows)], type=pa.float64()),
                "category": pa.array([i % 256 for i in range(rows)], type=pa.int32()),
            }
        )
    if profile == "string_dict":
        min_distinct = max(1, rows // 5000)
        return pa.table(
            {
                "uri": pa.array(
                    [f"s3://bucket/cap_{(i * min_distinct // rows):02d}.pcapng" for i in range(rows)],
                    pa.string(),
                ),
                "position": pa.array([i * 1500 for i in range(rows)], pa.uint64()),
                "size": pa.array([1500] * rows, pa.uint64()),
            }
        )
    raise ValueError(f"unknown profile: {profile}")


def _remove_path(path: Path) -> None:
    if path.is_dir():
        shutil.rmtree(path)
    elif path.exists():
        path.unlink()


def _write_n2p(table: pa.Table, path: Path, codec: Codec) -> None:
    n2p_parquet.write_table(table, path, codec=codec)


def _write_pyarrow(table: pa.Table, path: Path, codec: Codec) -> None:
    compression = "zstd" if codec == "zstd" else "none"
    pq.write_table(table, path, compression=compression)


def _write_polars(table: pa.Table, path: Path, codec: Codec) -> None:
    import polars as pl

    compression = "zstd" if codec == "zstd" else "uncompressed"
    pl.from_arrow(table).write_parquet(path, compression=compression)


WRITERS: dict[Lib, Callable[[pa.Table, Path, Codec], None]] = {
    "n2p": _write_n2p,
    "pyarrow": _write_pyarrow,
    "polars": _write_polars,
}


def write_once(writer: Callable[[pa.Table, Path, Codec], None], table: pa.Table, path: Path, codec: Codec) -> float:
    _remove_path(path)
    t0 = time.perf_counter()
    writer(table, path, codec)
    return time.perf_counter() - t0


def median_seconds(fn, *, iters: int) -> float:
    return statistics.median(fn() for _ in range(iters))


def run_one(
    lib: Lib,
    profile: Profile,
    rows: int,
    codec: Codec,
    *,
    repeat: int,
    out_dir: Path,
) -> BenchRow:
    table = make_table(profile, rows)
    writer = WRITERS[lib]
    path = out_dir / f"{lib}_{profile}_{codec}.parquet"

    write_s = median_seconds(lambda: write_once(writer, table, path, codec), iters=repeat)
    file_bytes = path.stat().st_size
    mrows_per_s = rows / write_s / 1_000_000 if write_s else 0.0
    return BenchRow(lib=lib, profile=profile, rows=rows, codec=codec, write_s=write_s, file_bytes=file_bytes, mrows_per_s=mrows_per_s)


def run_matrix(
    *,
    profiles: list[Profile],
    codecs: list[Codec],
    rows: int,
    repeat: int,
    libs: list[Lib],
    out_dir: Path,
) -> list[BenchRow]:
    out_dir.mkdir(parents=True, exist_ok=True)
    results: list[BenchRow] = []
    for profile in profiles:
        for codec in codecs:
            for lib in libs:
                results.append(
                    run_one(lib, profile, rows, codec, repeat=repeat, out_dir=out_dir),
                )
    return results


def check_results(
    results: list[BenchRow],
    *,
    max_write_ratio: float = 5.0,
    max_size_ratio: float = 1.15,
    size_codecs: tuple[Codec, ...] = ("zstd",),
) -> list[str]:
    """Return human-readable failure messages (empty => pass)."""
    errors: list[str] = []
    by_key: dict[tuple[Profile, Codec], dict[Lib, BenchRow]] = {}
    for row in results:
        by_key.setdefault((row.profile, row.codec), {})[row.lib] = row

    for (profile, codec), libs in sorted(by_key.items()):
        n2p = libs.get("n2p")
        if not n2p:
            continue
        if n2p.file_bytes <= 0:
            errors.append(f"{profile}/{codec}: n2p wrote an empty file")
        for baseline in ("pyarrow", "polars"):
            other = libs.get(baseline)
            if not other:
                continue
            if other.write_s and n2p.write_s > other.write_s * max_write_ratio:
                errors.append(
                    f"{profile}/{codec}: n2p write {n2p.write_s:.3f}s > {max_write_ratio}× "
                    f"{baseline} {other.write_s:.3f}s",
                )
            if codec in size_codecs and other.file_bytes and n2p.file_bytes > other.file_bytes * max_size_ratio:
                errors.append(
                    f"{profile}/{codec}: n2p size {n2p.file_bytes} > {max_size_ratio}× "
                    f"{baseline} {other.file_bytes}",
                )
    return errors
