#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Yoav Bendor
"""Amalgamate the split nanoarrow2parquet sources into one STB-style header.

The split files under include/ and src/ remain the source of truth; this script
concatenates them into a single self-contained header where the implementation is
gated behind the NANOARROW2PARQUET_IMPLEMENTATION macro (the STB convention).

Usage:
    python3 scripts/amalgamate.py [-o single_include/nanoarrow2parquet.h] [--check]

--check regenerates into memory and fails (non-zero exit) if the on-disk output is
stale, so CI can guarantee the amalgamation is kept in sync with the sources.
"""

import argparse
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# Public C declarations (verbatim; already a self-contained #pragma once header).
PUBLIC_HEADER = ROOT / "include" / "nanoarrow2parquet" / "nanoarrow2parquet.h"

# Implementation units, in dependency order. parquet_types defines the pq enums
# every other unit relies on; writer.cpp is the only translation unit and must
# come last.
IMPL_FILES = [
    ROOT / "src" / "parquet_types.hpp",
    ROOT / "src" / "thrift_compact.hpp",
    ROOT / "src" / "rle_bitpack.hpp",
    ROOT / "src" / "compress.hpp",
    ROOT / "src" / "writer.cpp",
]

DEFAULT_OUTPUT = ROOT / "single_include" / "nanoarrow2parquet.h"

ANGLE_INCLUDE = re.compile(r'^\s*#\s*include\s*<([^>]+)>\s*$')
QUOTE_INCLUDE = re.compile(r'^\s*#\s*include\s*"[^"]+"\s*$')
PRAGMA_ONCE = re.compile(r'^\s*#\s*pragma\s+once\s*$')
LICENSE_LINE = re.compile(r'^\s*//\s*(SPDX-License-Identifier|Copyright)\b')


def strip_unit(path: Path, angle_includes: list[str]) -> str:
    """Return the body of an impl unit with #pragma once, local includes, the
    license boilerplate, and the leading blank lines removed. Angle-bracket
    system/external includes are collected into `angle_includes` (dedup, in
    first-seen order) so they can be hoisted to the top of the impl block."""
    out_lines: list[str] = []
    for line in path.read_text().splitlines():
        m = ANGLE_INCLUDE.match(line)
        if m:
            inc = m.group(1)
            if inc not in angle_includes:
                angle_includes.append(inc)
            continue
        if QUOTE_INCLUDE.match(line):
            continue          # resolved by concatenation
        if PRAGMA_ONCE.match(line):
            continue
        if LICENSE_LINE.match(line):
            continue          # one banner for the whole file is enough
        out_lines.append(line)

    text = "\n".join(out_lines).strip("\n")
    banner = f"// ---- {path.relative_to(ROOT).as_posix()} " + "-" * 8
    return f"{banner}\n\n{text}\n"


def build() -> str:
    angle_includes: list[str] = []
    bodies = [strip_unit(p, angle_includes) for p in IMPL_FILES]

    # External deps first (they pull in the most), then the C++ standard library
    # sorted for stable output.
    external = [i for i in angle_includes if i in ("nanoarrow/nanoarrow.h", "zstd.h")]
    std = sorted(i for i in angle_includes if i not in external)
    include_block = "\n".join(f"#include <{i}>" for i in external + std)

    parts = [
        "// SPDX-License-Identifier: Apache-2.0",
        "// Copyright (c) 2026 Yoav Bendor",
        "//",
        "// nanoarrow2parquet -- single-header amalgamation.",
        "//",
        "// GENERATED FILE -- do not edit by hand. Regenerate with:",
        "//     python3 scripts/amalgamate.py",
        "// The editable sources live under include/ and src/.",
        "//",
        "// Usage (STB style): include this header anywhere for the declarations;",
        "// in exactly ONE translation unit define the implementation macro first:",
        "//",
        "//     #define NANOARROW2PARQUET_IMPLEMENTATION",
        "//     #include \"nanoarrow2parquet.h\"",
        "//",
        "// The implementation still depends on nanoarrow and zstd at compile/link",
        "// time; provide their headers on the include path and link libzstd.",
        "",
        PUBLIC_HEADER.read_text().strip("\n"),
        "",
        "#ifdef NANOARROW2PARQUET_IMPLEMENTATION",
        "",
        include_block,
        "",
        "\n".join(bodies),
        "#endif  // NANOARROW2PARQUET_IMPLEMENTATION",
        "",
    ]
    return "\n".join(parts)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("-o", "--output", type=Path, default=DEFAULT_OUTPUT)
    ap.add_argument("--check", action="store_true",
                    help="fail if the on-disk output is stale instead of writing")
    args = ap.parse_args()

    content = build()

    if args.check:
        existing = args.output.read_text() if args.output.exists() else ""
        if existing != content:
            sys.stderr.write(
                f"{args.output} is out of date; run python3 scripts/amalgamate.py\n")
            return 1
        print(f"{args.output} is up to date")
        return 0

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(content)
    print(f"wrote {args.output} ({len(content.splitlines())} lines)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
