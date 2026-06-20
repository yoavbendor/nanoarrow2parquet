#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Yoav Bendor
"""Render a benchmark results JSON (from run_bench.py --json) into a Markdown
table and publish it into bench/README.md between the BENCH_RESULTS markers.

Captures provenance so a published table is attributable: nanoarrow2parquet
version + git commit, date, and the host (CPU / cores / zstd / pyarrow) the
numbers were produced on -- benchmark numbers are meaningless without it.

Usage:
    python3 bench/run_bench.py ... --json results.json
    python3 bench/render_results.py results.json --inject bench/README.md
    python3 bench/render_results.py results.json          # print to stdout
"""

import argparse
import datetime
import json
import os
import re
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
START = "<!-- BENCH_RESULTS_START -->"
END = "<!-- BENCH_RESULTS_END -->"


def sh(*cmd):
    try:
        return subprocess.check_output(cmd, cwd=ROOT, text=True,
                                       stderr=subprocess.DEVNULL).strip()
    except Exception:
        return ""


def project_version():
    try:
        with open(os.path.join(ROOT, "CMakeLists.txt")) as f:
            m = re.search(r"project\([^)]*VERSION\s+(\d+\.\d+\.\d+)", f.read())
            return m.group(1) if m else "?"
    except OSError:
        return "?"


def cpu_model():
    try:
        with open("/proc/cpuinfo") as f:
            for line in f:
                if line.startswith("model name"):
                    return line.split(":", 1)[1].strip()
    except OSError:
        pass
    return "unknown CPU"


def pyarrow_version():
    try:
        import pyarrow
        return pyarrow.__version__
    except Exception:
        return "?"


def gather_meta():
    return {
        "version": project_version(),
        "commit": sh("git", "rev-parse", "--short", "HEAD") or "?",
        "dirty": " (dirty)" if sh("git", "status", "--porcelain") else "",
        "date": datetime.date.today().isoformat(),
        "cpu": cpu_model(),
        "cores": os.cpu_count(),
        "pyarrow": pyarrow_version(),
        "kernel": sh("uname", "-sr"),
    }


def render(results, meta):
    # Pair n2p and arrow rows by (total, chunk, codec, strings).
    def key(r):
        return (r["total_mb"], r["chunk_mb"], r["codec"], r.get("strings", True))

    pairs = {}
    for r in results:
        pairs.setdefault(key(r), {})[r["lib"]] = r

    strings = all(k[3] for k in pairs) and len(pairs) > 0
    cols = "id(i64), value(f64), category(i32), level(utf8 dict), path(utf8 dict)" \
        if strings else "id(i64), value(f64), category(i32)"

    out = []
    out.append(START)
    out.append("")
    out.append(f"_nanoarrow2parquet **v{meta['version']}** @ `{meta['commit']}`"
               f"{meta['dirty']} · {meta['date']}_")
    out.append("")
    out.append(f"- **Host:** {meta['cpu']} · {meta['cores']} cores · {meta['kernel']}")
    out.append(f"- **Baseline:** Apache Parquet C++ via pyarrow {meta['pyarrow']} "
               "(`parquet::arrow::FileWriter`), same settings")
    out.append(f"- **Columns:** {cols}")
    out.append("- **Phases:** write only (data generation excluded); ZSTD level 3; "
               "one row group per chunk; streamed so peak RSS ≈ one chunk.")
    out.append("")
    out.append("| total | chunk | codec | n2p GB/s | arrow GB/s | n2p write | "
               "n2p file | arrow file | n2p/arrow size | peak RSS (n2p/arrow) |")
    out.append("|---:|---:|:--|---:|---:|:--:|---:|---:|:--:|:--:|")

    for k in sorted(pairs):
        total, chunk, codec, _ = k
        n = pairs[k].get("n2p")
        a = pairs[k].get("arrow")
        if not n:
            continue
        if a:
            spd = a["write_s"] / n["write_s"] if n["write_s"] else 0
            sz = n["file_bytes"] / a["file_bytes"] if a["file_bytes"] else 0
            row = (f"| {total} MB | {chunk} MB | {codec} | "
                   f"{n['write_gbps']:.3f} | {a['write_gbps']:.3f} | "
                   f"**{spd:.2f}×** | {n['file_bytes']/2**20:.1f} MB | "
                   f"{a['file_bytes']/2**20:.1f} MB | {sz:.3f}× | "
                   f"{n['peak_rss_mb']:.0f}/{a['peak_rss_mb']:.0f} MB |")
        else:
            row = (f"| {total} MB | {chunk} MB | {codec} | {n['write_gbps']:.3f} | "
                   f"— | — | {n['file_bytes']/2**20:.1f} MB | — | — | "
                   f"{n['peak_rss_mb']:.0f} MB |")
        out.append(row)

    out.append("")
    out.append("_`n2p write` is the speedup vs Arrow (>1 = n2p faster). Numbers are "
               "the median of the run's repeats on a shared cloud VM — treat small "
               "(<1.1×) differences as noise; regenerate with "
               "`bench/render_results.py`._")
    out.append("")
    out.append(END)
    return "\n".join(out)


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("results", help="JSON produced by run_bench.py --json")
    ap.add_argument("--inject", default="", help="Markdown file to update in-place")
    args = ap.parse_args()

    with open(args.results) as f:
        results = json.load(f)
    table = render(results, gather_meta())

    if not args.inject:
        print(table)
        return

    with open(args.inject) as f:
        doc = f.read()
    if START in doc and END in doc:
        doc = re.sub(re.escape(START) + r".*?" + re.escape(END), table, doc, flags=re.S)
    else:
        sys.stderr.write(f"markers not found in {args.inject}; appending\n")
        doc = doc.rstrip() + "\n\n" + table + "\n"
    with open(args.inject, "w") as f:
        f.write(doc)
    sys.stderr.write(f"published {len(results)} results into {args.inject}\n")


if __name__ == "__main__":
    main()
