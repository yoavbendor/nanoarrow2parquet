#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Yoav Bendor
"""Render Python binding benchmark JSON into README.md markers.

Usage:
    python bench/run_bench.py --json /tmp/py-bench.json
    python bench/render_results.py /tmp/py-bench.json --inject README.md
"""

from __future__ import annotations

import argparse
import datetime
import json
import os
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
BINDINGS = Path(__file__).resolve().parents[1]
START = "<!-- PY_BENCH_RESULTS_START -->"
END = "<!-- PY_BENCH_RESULTS_END -->"
STREAM_START = "<!-- PY_STREAM_BENCH_RESULTS_START -->"
STREAM_END = "<!-- PY_STREAM_BENCH_RESULTS_END -->"


def sh(*cmd: str) -> str:
    try:
        return subprocess.check_output(cmd, cwd=ROOT, text=True, stderr=subprocess.DEVNULL).strip()
    except Exception:
        return ""


def cpu_model() -> str:
    try:
        with open("/proc/cpuinfo") as f:
            for line in f:
                if line.startswith("model name"):
                    return line.split(":", 1)[1].strip()
    except OSError:
        pass
    return "unknown CPU"


def pkg_version(module: str) -> str:
    try:
        mod = __import__(module)
        return getattr(mod, "__version__", "?")
    except Exception:
        return "?"


def gather_meta() -> dict:
    return {
        "version": "0.1.0",
        "commit": sh("git", "rev-parse", "--short", "HEAD") or "?",
        "dirty": " (dirty)" if sh("git", "status", "--porcelain") else "",
        "date": datetime.date.today().isoformat(),
        "cpu": cpu_model(),
        "cores": os.cpu_count(),
        "pyarrow": pkg_version("pyarrow"),
        "polars": pkg_version("polars"),
        "kernel": sh("uname", "-sr"),
    }


def _fmt_seconds(row: dict | None) -> str:
    return f"{row['write_s']:.3f}s" if row else "—"


def _fmt_mb(row: dict | None) -> str:
    return f"{row['file_bytes'] / 2**20:.2f}" if row else "—"


def _speedup(n2p: dict, other: dict | None) -> str:
    if not other or not n2p["write_s"]:
        return "—"
    return f"**{other['write_s'] / n2p['write_s']:.2f}×**"


def render_table(results: list[dict], meta: dict) -> str:
    table_rows = [r for r in results if r.get("mode", "table") == "table"]
    def key(r: dict) -> tuple:
        return (r["profile"], r["rows"], r["codec"])

    groups: dict[tuple, dict[str, dict]] = {}
    for row in table_rows:
        groups.setdefault(key(row), {})[row["lib"]] = row

    out: list[str] = []
    out.append(START)
    out.append("")
    out.append(
        f"_nanoarrow-io **v{meta['version']}** @ `{meta['commit']}`"
        f"{meta['dirty']} · {meta['date']}_"
    )
    out.append("")
    out.append(f"- **Host:** {meta['cpu']} · {meta['cores']} cores · {meta['kernel']}")
    out.append(
        f"- **Baselines:** pyarrow {meta['pyarrow']}, polars {meta['polars']} "
        "(same Arrow table, ZSTD / uncompressed)"
    )
    out.append(
        "- **Profiles:** `mixed` (i64 + low-card string + f64), "
        "`numeric` (i64/f64/i32), `string_dict` (repetitive URI runs)"
    )
    out.append("- **Metric:** median wall-clock write time; file size after write")
    out.append("")
    out.append(
        "| profile | rows | codec | n2p write | pyarrow | polars | n2p/pyarrow | n2p/polars | "
        "n2p MB | pyarrow MB | polars MB |"
    )
    out.append("|:---|---:|:--|---:|---:|---:|:--:|:--:|---:|---:|---:|")

    for k in sorted(groups):
        profile, rows, codec = k
        libs = groups[k]
        n2p = libs.get("n2p")
        if not n2p:
            continue
        pa_row = libs.get("pyarrow")
        pl_row = libs.get("polars")
        out.append(
            f"| {profile} | {rows:,} | {codec} | {_fmt_seconds(n2p)} | {_fmt_seconds(pa_row)} | "
            f"{_fmt_seconds(pl_row)} | {_speedup(n2p, pa_row)} | {_speedup(n2p, pl_row)} | "
            f"{_fmt_mb(n2p)} | {_fmt_mb(pa_row)} | {_fmt_mb(pl_row)} |"
        )

    out.append("")
    out.append(
        "_`n2p/pyarrow` and `n2p/polars` are speedups (>1 = nanoarrow-io faster). "
        "Regenerate with `python bench/run_bench.py --json …` then "
        "`python bench/render_results.py … --inject README.md`._"
    )
    out.append("")
    out.append(END)
    return "\n".join(out)


def render_stream(results: list[dict], meta: dict) -> str:
    stream_rows = [r for r in results if r.get("mode") == "stream"]
    if not stream_rows:
        return ""

    def key(r: dict) -> tuple:
        return (r["profile"], r["rows"], r["chunk_rows"], r["codec"])

    groups: dict[tuple, dict[str, dict]] = {}
    for row in stream_rows:
        groups.setdefault(key(row), {})[row["lib"]] = row

    chunk_rows = stream_rows[0]["chunk_rows"]
    out: list[str] = []
    out.append(STREAM_START)
    out.append("")
    out.append(
        f"_Streaming write @ `{meta['commit']}`{meta['dirty']} · {meta['date']}_ "
        f"(chunk={chunk_rows:,} rows/row-group)"
    )
    out.append("")
    out.append(
        f"- **Baselines:** `n2p_stream` = :class:`ParquetWriter` loop, "
        f"`n2p_table` = materialize then `write_table`, `pyarrow_stream` = `pq.ParquetWriter` loop"
    )
    out.append("")
    out.append(
        "| profile | rows | codec | n2p_stream | n2p_table | pyarrow_stream | stream/table | stream/pyarrow | "
        "n2p_stream MB | pyarrow MB |"
    )
    out.append("|:---|---:|:--|---:|---:|---:|:--:|:--:|---:|---:|")

    for k in sorted(groups):
        profile, rows, chunk, codec = k
        libs = groups[k]
        stream = libs.get("n2p_stream")
        if not stream:
            continue
        table = libs.get("n2p_table")
        pa_row = libs.get("pyarrow_stream")
        out.append(
            f"| {profile} | {rows:,} | {codec} | {_fmt_seconds(stream)} | {_fmt_seconds(table)} | "
            f"{_fmt_seconds(pa_row)} | {_speedup(stream, table)} | {_speedup(stream, pa_row)} | "
            f"{_fmt_mb(stream)} | {_fmt_mb(pa_row)} |"
        )

    out.append("")
    out.append(
        "_Chunked writes keep peak RSS near one row group — see `examples/stream_chunks.py`. "
        "Regenerate with `python bench/run_bench.py --streaming --json …`._"
    )
    out.append("")
    out.append(STREAM_END)
    return "\n".join(out)


def render(results: list[dict], meta: dict) -> tuple[str, str]:
    return render_table(results, meta), render_stream(results, meta)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("results", nargs="+", help="JSON from run_bench.py --json (one or more files)")
    ap.add_argument("--inject", default="", help="Markdown file to update in-place")
    args = ap.parse_args()

    results: list[dict] = []
    for path in args.results:
        with open(path) as f:
            results.extend(json.load(f))
    meta = gather_meta()
    table_block, stream_block = render(results, meta)

    if not args.inject:
        print(table_block)
        if stream_block:
            print()
            print(stream_block)
        return

    inject_path = Path(args.inject)
    if not inject_path.is_absolute():
        inject_path = BINDINGS / inject_path
    doc = inject_path.read_text()

    if START in doc and END in doc:
        doc = re.sub(re.escape(START) + r".*?" + re.escape(END), table_block, doc, flags=re.S)
    else:
        sys.stderr.write(f"table markers not found in {inject_path}; appending\n")
        doc = doc.rstrip() + "\n\n" + table_block + "\n"

    if stream_block:
        if STREAM_START in doc and STREAM_END in doc:
            doc = re.sub(
                re.escape(STREAM_START) + r".*?" + re.escape(STREAM_END),
                stream_block,
                doc,
                flags=re.S,
            )
        else:
            sys.stderr.write(f"stream markers not found in {inject_path}; appending\n")
            doc = doc.rstrip() + "\n\n" + stream_block + "\n"

    inject_path.write_text(doc)
    sys.stderr.write(f"published {len(results)} rows into {inject_path}\n")


if __name__ == "__main__":
    main()
