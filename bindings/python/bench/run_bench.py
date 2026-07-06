#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Yoav Bendor
"""Benchmark nanoarrow-io Parquet writes vs pyarrow and polars.

Usage:
    cd bindings/python
    pip install -e ".[test]"
    python bench/run_bench.py --json /tmp/py-bench.json
    python bench/render_results.py /tmp/py-bench.json --inject README.md
"""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
from pathlib import Path

# Allow running as `python bench/run_bench.py` from bindings/python.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from bench.common import (  # noqa: E402
    Codec,
    Lib,
    Profile,
    check_results,
    run_matrix,
)


def _parse_list(raw: str) -> list[str]:
    return [part.strip() for part in raw.split(",") if part.strip()]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--profiles", default="mixed,numeric,string_dict", help="comma-separated profiles")
    ap.add_argument("--codecs", default="zstd,uncompressed", help="comma-separated codecs")
    ap.add_argument("--libs", default="n2p,pyarrow,polars", help="comma-separated writers")
    ap.add_argument("--rows", type=int, default=500_000, help="rows per profile")
    ap.add_argument("--repeat", type=int, default=3, help="median of N timed writes")
    ap.add_argument("--json", dest="json_path", default="", help="write raw results JSON")
    ap.add_argument("--check", action="store_true", help="exit 1 on regression thresholds")
    ap.add_argument("--max-write-ratio", type=float, default=5.0)
    ap.add_argument("--max-size-ratio", type=float, default=1.15)
    ap.add_argument("--quick", action="store_true", help="smoke: 50k rows, 1 repeat")
    args = ap.parse_args()

    profiles: list[Profile] = _parse_list(args.profiles)  # type: ignore[assignment]
    codecs: list[Codec] = _parse_list(args.codecs)  # type: ignore[assignment]
    libs: list[Lib] = _parse_list(args.libs)  # type: ignore[assignment]
    rows = 50_000 if args.quick else args.rows
    repeat = 1 if args.quick else args.repeat

    if "polars" in libs:
        try:
            import polars  # noqa: F401
        except ImportError:
            sys.stderr.write("polars not installed; skipping polars baseline\n")
            libs = [lib for lib in libs if lib != "polars"]

    with tempfile.TemporaryDirectory(prefix="nanoarrow-io-bench-") as tmp:
        results = run_matrix(
            profiles=profiles,
            codecs=codecs,
            rows=rows,
            repeat=repeat,
            libs=libs,
            out_dir=Path(tmp),
        )

    payload = [row.to_json() for row in results]
    if args.json_path:
        with open(args.json_path, "w") as f:
            json.dump(payload, f, indent=2)
            f.write("\n")

    hdr = f"{'profile':>12} {'rows':>8} {'codec':>12} {'lib':>8} {'write_s':>8} {'MB':>8} {'Mrows/s':>8}"
    print(hdr)
    print("-" * len(hdr))
    for row in results:
        print(
            f"{row.profile:>12} {row.rows:>8} {row.codec:>12} {row.lib:>8} "
            f"{row.write_s:>8.3f} {row.file_bytes / 2**20:>8.2f} {row.mrows_per_s:>8.2f}"
        )

    if args.check:
        errors = check_results(
            results,
            max_write_ratio=args.max_write_ratio,
            max_size_ratio=args.max_size_ratio,
            size_codecs=("zstd",),
        )
        if errors:
            for err in errors:
                print(f"CHECK FAIL: {err}", file=sys.stderr)
            return 1
        print("CHECK OK", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
