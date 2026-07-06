#!/usr/bin/env python3
"""Stream many record batches to one Parquet file (larger-than-RAM friendly).

Build one row group per loop iteration; only the current chunk needs to be in
memory. Close the writer (or exit the ``with`` block) to write the footer.

Usage:
    cd bindings/python
    pip install -e .
    python examples/stream_chunks.py --rows 5000000 --chunk-rows 100000 --out /tmp/big.parquet
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from nanoarrow_io import parquet as n2p_parquet


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", default="streamed.parquet", help="output Parquet path")
    ap.add_argument("--rows", type=int, default=1_000_000, help="total rows to generate")
    ap.add_argument("--chunk-rows", type=int, default=100_000, help="rows per row group")
    ap.add_argument("--codec", choices=("zstd", "uncompressed"), default="zstd")
    args = ap.parse_args()

    if args.chunk_rows <= 0 or args.rows <= 0:
        ap.error("--rows and --chunk-rows must be positive")

    schema = pa.schema(
        {
            "id": pa.int64(),
            "tag": pa.string(),
            "value": pa.float64(),
        }
    )

    out = Path(args.out)
    n_chunks = (args.rows + args.chunk_rows - 1) // args.chunk_rows
    print(f"Writing {args.rows:,} rows in {n_chunks} row groups -> {out}")

    with n2p_parquet.ParquetWriter(out, codec=args.codec) as writer:
        written = 0
        chunk_idx = 0
        while written < args.rows:
            n = min(args.chunk_rows, args.rows - written)
            start = written
            batch = pa.record_batch(
                {
                    "id": pa.array(range(start, start + n), type=pa.int64()),
                    "tag": pa.array([f"row-{(start + i) % 1000}" for i in range(n)], type=pa.string()),
                    "value": pa.array([float(start + i) * 0.01 for i in range(n)], type=pa.float64()),
                },
                schema=schema,
            )
            writer.write_batch(batch)
            written += n
            chunk_idx += 1
            if chunk_idx % 10 == 0 or written >= args.rows:
                print(f"  {written:,}/{args.rows:,} rows ({writer.row_groups} row groups)")

    table = pq.read_table(out)
    md = pq.ParquetFile(out).metadata
    print(
        f"Done: {table.num_rows:,} rows, {md.num_row_groups} row groups, "
        f"{out.stat().st_size / (1024 * 1024):.2f} MB on disk"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
