#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Verify the mixed-type files written by test_mixed_types.cpp are byte-exact --
# the regression guard for the "mixed-type corruption" report. Reproduces the
# same deterministic generators (including float32 rounding) and asserts every
# value, with explicit attention to the 16384 boundary the report flagged.
import struct
import sys

import pyarrow.parquet as pq

ROWS = 40000


def f32(x):  # emulate a float32 round-trip
    return struct.unpack("f", struct.pack("f", x))[0]


def gen(i):
    return {
        "u8a": i % 256,
        "u8b": (1 << (i % 8)) & 0xFF,
        "u16": i % 65536,
        "u32": (i * 2654435761) % (2 ** 32),
        "u64": (i * 1099511628211) % (2 ** 64),
        "f32a": f32(f32(i) * f32(0.1)),
        "f32b": f32(-(f32(i)) * f32(0.05)),
        "f64a": 3.14159265359 * (i + 1),
        "f64b": 1e10 + i * 100.0,
    }


def check(path, nullable):
    t = pq.read_table(path)
    d = t.to_pydict()
    problems = []
    if t.num_rows != ROWS:
        problems.append(f"{path}: {t.num_rows} rows != {ROWS}")
    cols = ["u8a", "u8b", "u16", "u32", "u64", "f32a", "f32b", "f64a", "f64b"]
    for k in cols:
        got = d[k]
        bad = 0
        first = None
        for i in range(ROWS):
            nul = nullable and (i % 4) == 2
            exp = None if nul else gen(i)[k]
            if got[i] != exp:
                bad += 1
                if first is None:
                    first = (i, got[i], exp)
        if bad:
            problems.append(f"{path}:{k}: {bad} mismatches, first={first}")
    return problems


def main():
    req = sys.argv[1] if len(sys.argv) > 1 else "n2p_mixed.parquet"
    nul = sys.argv[2] if len(sys.argv) > 2 else "n2p_mixed_null.parquet"
    problems = check(req, nullable=False) + check(nul, nullable=True)
    if problems:
        print("MIXED-TYPE CHECK FAILED:")
        for p in problems:
            print("  -", p)
        sys.exit(1)
    print(f"mixed-type check OK: {ROWS} rows, REQUIRED + nullable, all values exact "
          "(incl. across the 16384 boundary)")


if __name__ == "__main__":
    main()
