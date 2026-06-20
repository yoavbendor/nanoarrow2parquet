#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Yoav Bendor
"""Drive the nanoarrow2parquet vs Apache Parquet C++ write benchmark matrix.

Runs bench_n2p and bench_arrow across {total} x {chunk} x {codec}, parses their
JSON result lines, and prints a comparison table (write speed + output size, with
n2p/arrow ratios). Each output file is deleted after measuring so transient disk
stays bounded by one dataset.

The dataset is streamed one chunk (row group) at a time and freed before the next,
so the totals (1/4/8 GB) exceed peak resident memory (~one chunk) -- the
"bigger than RAM" requirement -- while both libraries consume identical data.

Build first:
    cmake -S . -B build -DN2P_BUILD_BENCHMARKS=ON
    cmake --build build --target bench_n2p bench_arrow

Example:
    python3 bench/run_bench.py --totals 1024,4096,8192 --chunks 200,500
    python3 bench/run_bench.py --quick            # tiny smoke run
"""

import argparse
import json
import os
import shutil
import statistics
import subprocess
import sys


def run_one(binary, total_mb, chunk_mb, codec, strings, null_pct, out_path, repeat, wide=False):
    """Run a benchmark binary `repeat` times; return the result with the median
    write_s (data generation is excluded from the write timing by the binary)."""
    results = []
    for _ in range(repeat):
        proc = subprocess.run(
            [binary, "--total-mb", str(total_mb), "--chunk-mb", str(chunk_mb),
             "--codec", codec, "--strings" if strings else "--no-strings",
             "--wide" if wide else "--no-wide",
             "--null-pct", str(null_pct), "--out", out_path],
            capture_output=True, text=True)
        if proc.returncode != 0:
            sys.stderr.write(proc.stdout + proc.stderr)
            raise SystemExit(f"{binary} failed (total={total_mb} chunk={chunk_mb} {codec})")
        line = proc.stdout.strip().splitlines()[-1]
        results.append(json.loads(line))
        try:
            os.remove(out_path)
        except FileNotFoundError:
            pass
    results.sort(key=lambda r: r["write_s"])
    return results[len(results) // 2]  # median write run


def fmt_bytes(n):
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024 or unit == "GB":
            return f"{n:.2f}{unit}"
        n /= 1024.0


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bin-dir", default="build", help="directory with bench_n2p / bench_arrow")
    ap.add_argument("--totals", default="1024,4096,8192", help="dataset sizes in MB (comma list)")
    ap.add_argument("--chunks", default="200,500", help="chunk/row-group sizes in MB (comma list)")
    ap.add_argument("--codecs", default="zstd,uncompressed", help="codecs (comma list)")
    ap.add_argument("--repeat", type=int, default=1, help="iterations per config (median write taken)")
    ap.add_argument("--out-dir", default="/tmp", help="scratch dir for output parquet files")
    ap.add_argument("--null-pct", type=int, default=0, help="percent nulls (>0 makes columns OPTIONAL)")
    ap.add_argument("--no-strings", dest="strings", action="store_false",
                    help="numeric-only columns (no dictionary-encoded strings)")
    ap.add_argument("--wide", action="store_true",
                    help="add the extra-type columns (int8/16, uint32/64, float, bool)")
    ap.add_argument("--no-soa", dest="soa", action="store_false",
                    help="skip the compile-time SoA path (bench_n2p_soa)")
    ap.add_argument("--quick", action="store_true", help="tiny smoke matrix (200MB totals)")
    ap.add_argument("--check", action="store_true",
                    help="regression gate: assert n2p stays competitive with Arrow on the "
                         "same runner; exit nonzero on violation (requires bench_arrow)")
    ap.add_argument("--max-size-ratio", type=float, default=1.10,
                    help="fail if n2p file_bytes > arrow file_bytes * this (default 1.10)")
    ap.add_argument("--max-write-ratio", type=float, default=2.5,
                    help="fail if n2p write_s > arrow write_s * this (default 2.5; generous "
                         "for CI noise -- it compares two libs on the same machine)")
    ap.add_argument("--json", default="", help="also write all raw results to this JSON file")
    args = ap.parse_args()

    if args.quick:
        args.totals, args.chunks = "200", "100"

    totals = [int(x) for x in args.totals.split(",")]
    chunks = [int(x) for x in args.chunks.split(",")]
    codecs = args.codecs.split(",")

    bn = os.path.join(args.bin_dir, "bench_n2p")
    ba = os.path.join(args.bin_dir, "bench_arrow")
    bs = os.path.join(args.bin_dir, "bench_n2p_soa")
    if not os.path.exists(bn):
        raise SystemExit(f"missing {bn}; build with -DN2P_BUILD_BENCHMARKS=ON")
    have_arrow = os.path.exists(ba)
    have_soa = args.soa and os.path.exists(bs)
    if not have_arrow:
        if args.check:
            raise SystemExit(f"--check needs the Apache baseline ({ba}); build it with "
                             "pyarrow installed (-DN2P_BUILD_BENCHMARKS=ON)")
        sys.stderr.write(f"note: {ba} not found -- running n2p only (no Apache baseline)\n")

    free_gb = shutil.disk_usage(args.out_dir).free / 2**30
    cols = "id,value,category" + (",level,path" if args.strings else "")
    if args.wide:
        cols += ",small,small16,ucount,subtotal,ratio,flag"
    sys.stderr.write(f"scratch={args.out_dir} free={free_gb:.1f}GB repeat={args.repeat} "
                     f"soa={'on' if have_soa else 'off'} columns=[{cols}]\n")

    hdr = (f"{'total':>7} {'chunk':>6} {'codec':>12} {'lib':>6} "
           f"{'write_s':>9} {'write_GB/s':>11} {'Mrows/s':>9} {'file':>10} "
           f"{'B/row':>7} {'gen_s':>8} {'peakRSS':>9}")
    print(hdr)
    print("-" * len(hdr))

    raw = []
    failures = []
    for total in totals:
        for chunk in chunks:
            if chunk > total:
                continue
            for codec in codecs:
                row_n = run_one(bn, total, chunk, codec, args.strings, args.null_pct,
                                os.path.join(args.out_dir, "n2p_bench.parquet"), args.repeat,
                                wide=args.wide)
                raw.append(row_n)
                rows = [("n2p", row_n)]
                row_s = None
                if have_soa:
                    row_s = run_one(bs, total, chunk, codec, args.strings, args.null_pct,
                                    os.path.join(args.out_dir, "n2p_soa_bench.parquet"), args.repeat,
                                    wide=args.wide)
                    raw.append(row_s)
                    rows.append(("n2p_soa", row_s))
                if have_arrow:
                    row_a = run_one(ba, total, chunk, codec, args.strings, args.null_pct,
                                    os.path.join(args.out_dir, "arrow_bench.parquet"), args.repeat,
                                    wide=args.wide)
                    raw.append(row_a)
                    rows.append(("arrow", row_a))
                for lib, r in rows:
                    print(f"{total:>6}M {chunk:>5}M {codec:>12} {lib:>6} "
                          f"{r['write_s']:>9.3f} {r['write_gbps']:>11.3f} "
                          f"{r['mrows_per_s']:>9.2f} {fmt_bytes(r['file_bytes']):>10} "
                          f"{r['bytes_per_row']:>7.2f} {r['gen_s']:>8.3f} "
                          f"{r['peak_rss_mb']:>8.0f}M")
                if row_s is not None:
                    # SoA (compile-time) write time relative to the runtime path; >1
                    # means SoA is faster. Same encoding, so file sizes should match.
                    swr = row_n["write_s"] / row_s["write_s"] if row_s["write_s"] else 0
                    sfaster = "faster" if swr >= 1 else "slower"
                    ssize = row_s["file_bytes"] / row_n["file_bytes"] if row_n["file_bytes"] else 0
                    print(f"{'':>7} {'':>6} {codec:>12} {'ratio':>6} "
                          f"  n2p_soa write {swr:.2f}x n2p ({sfaster}),  "
                          f"soa file {ssize:.3f}x n2p")
                if have_arrow:
                    spd = row_a["write_s"] / row_n["write_s"] if row_n["write_s"] else 0
                    size = row_n["file_bytes"] / row_a["file_bytes"] if row_a["file_bytes"] else 0
                    faster = "faster" if spd >= 1 else "slower"
                    print(f"{'':>7} {'':>6} {codec:>12} {'ratio':>6} "
                          f"  n2p write {spd:.2f}x ({faster}),  n2p file {size:.3f}x arrow")
                    if args.check:
                        cfg = f"total={total}M chunk={chunk}M {codec} strings={args.strings}"
                        # write_s ratio: n2p time vs arrow time (>1 means n2p slower).
                        wr = row_n["write_s"] / row_a["write_s"] if row_a["write_s"] else 0
                        if wr > args.max_write_ratio:
                            failures.append(f"WRITE  {cfg}: n2p {wr:.2f}x arrow time "
                                            f"(> {args.max_write_ratio})")
                        if size > args.max_size_ratio:
                            failures.append(f"SIZE   {cfg}: n2p file {size:.3f}x arrow "
                                            f"(> {args.max_size_ratio})")
                        if row_n["rows"] != row_a["rows"]:
                            failures.append(f"ROWS   {cfg}: n2p {row_n['rows']} != arrow {row_a['rows']}")
                        if row_n["file_bytes"] == 0:
                            failures.append(f"EMPTY  {cfg}: n2p produced an empty file")
                if args.check and row_s is not None:
                    cfg = f"total={total}M chunk={chunk}M {codec} strings={args.strings} wide={args.wide}"
                    # SoA writes the same encoding as the runtime path, so the file
                    # should be within a few percent; rows must match exactly.
                    if row_s["rows"] != row_n["rows"]:
                        failures.append(f"SOAROW {cfg}: soa {row_s['rows']} != n2p {row_n['rows']}")
                    if row_s["file_bytes"] == 0:
                        failures.append(f"SOAEMP {cfg}: soa produced an empty file")
                    elif row_n["file_bytes"]:
                        sr = row_s["file_bytes"] / row_n["file_bytes"]
                        if sr > 1.05 or sr < 0.95:
                            failures.append(f"SOASZ  {cfg}: soa file {sr:.3f}x n2p (expected ~1.0)")
                print()

    if args.json:
        with open(args.json, "w") as f:
            json.dump(raw, f, indent=2)
        sys.stderr.write(f"wrote raw results to {args.json}\n")

    if args.check:
        if failures:
            print("\nREGRESSION CHECK: FAIL")
            for f in failures:
                print(f"  - {f}")
            print(f"\nthresholds: write <= {args.max_write_ratio}x arrow time, "
                  f"size <= {args.max_size_ratio}x arrow")
            raise SystemExit(1)
        print("\nREGRESSION CHECK: PASS "
              f"(write <= {args.max_write_ratio}x arrow time, size <= {args.max_size_ratio}x)")


if __name__ == "__main__":
    main()
