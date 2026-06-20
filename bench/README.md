# Write benchmarks: nanoarrow2parquet vs Apache Parquet C++

Measures **write throughput** and **output size** of nanoarrow2parquet against the
Apache Parquet C++ writer (`parquet::arrow::FileWriter`), on datasets streamed in
chunks so the total exceeds peak resident memory.

Methodology follows [carquet's benchmark suite](https://github.com/Vitruves/carquet/tree/master/benchmark):
deterministic LCG-generated data, **write timed separately from data generation**,
dictionary disabled (PLAIN), fixed row-group sizing, file size reported.

## What it measures

Schema (default, `--strings`) — a mix that exercises both write paths:

| column | type | encoding | notes |
|---|---|---|---|
| `id` | int64 | PLAIN | fixed-width (near-memcpy) |
| `value` | double | PLAIN | fixed-width |
| `category` | int32 | PLAIN | fixed-width |
| `level` | utf8 | **RLE_DICTIONARY** | 6 short categories (`INFO`/`WARN`/…), random per row |
| `path` | utf8 | **RLE_DICTIONARY** | pool of 64 file-path-like strings, random per row |

The two string columns are **low-cardinality and randomly spread across rows** —
the case dictionary encoding exists for, and what real workloads (log levels, file
paths, enum-ish categories) look like. n2p always dictionary-encodes strings; the
Apache baseline is told to `enable_dictionary` on exactly those columns, so both
emit RLE_DICTIONARY there and PLAIN for the numerics.

Use `--no-strings` for the carquet-parity numeric-only schema (3 columns, no
dictionary) — the pure fixed-width story. For each `(total, chunk, codec)` config,
both writers process an **identical** stream of chunks:

1. **gen** — a chunk of `chunk_mb` of columnar data is materialized into the Arrow
   C Data Interface (`bench_common.hpp::make_chunk_array`). Timed separately; this
   is the "making/acquiring the nanoarrow data" cost.
2. **write** — the chunk is handed to the writer and serialized as **one row
   group**, then freed before the next chunk. Only this is the comparison metric.

Because each chunk is freed before the next, **peak RSS ≈ one chunk** while the
dataset total (1/4/8 GB) is far larger — the larger-than-RAM requirement. Both
binaries print a JSON result line; `run_bench.py` aggregates and ratios them.

## Fairness

Both writers consume the *same* generated C Data Interface arrays. The Apache
baseline imports them zero-copy (`arrow::ImportRecordBatch`) and is configured to
match n2p's choices:

| knob | setting | why |
|---|---|---|
| dictionary | numerics off (PLAIN), strings on (RLE_DICTIONARY) | matches n2p: PLAIN fixed-width, dictionary for `utf8`/`binary` |
| ZSTD level | **3** | matches n2p's hardcoded `compress_page` level |
| row group | `max_row_group_length = rows_per_chunk` | one row group per chunk, like n2p |
| codecs | `zstd`, `uncompressed` | the two n2p supports |

Differences left as-is (and noted): Arrow splits column data into ~1 MB pages
(n2p writes one page per column per row group) and stores an Arrow schema in the
footer key-value metadata (a small constant). Neither materially affects GB-scale
size or speed.

## Building

```sh
cmake -S . -B build -DN2P_BUILD_BENCHMARKS=ON
cmake --build build --target bench_n2p bench_arrow
```

`bench_arrow` links the Apache Parquet C++ libraries. By default CMake discovers
the `libarrow`/`libparquet` shared objects and headers bundled in an installed
**pyarrow** wheel, so no source build of Arrow is needed. To use a system or conda
Arrow instead:

```sh
cmake -S . -B build -DN2P_BUILD_BENCHMARKS=ON \
      -DARROW_INCLUDE_DIR=/path/include -DARROW_LIB_DIR=/path/lib
```

If Arrow isn't found, `bench_n2p` still builds and `run_bench.py` runs n2p-only.

> Note: pyarrow's writer *is* this same `libparquet` C++ code, so pyarrow write
> throughput on large row groups is within a percent or two of these native
> numbers — the C++ harness just removes the Python per-call overhead.

## Running

```sh
# the requested matrix: 1/4/8 GB totals, 200/500 MB chunks, both codecs
python3 bench/run_bench.py --totals 1024,4096,8192 --chunks 200,500

python3 bench/run_bench.py --quick                 # 200MB smoke run
python3 bench/run_bench.py --repeat 5 --json out.json   # median of 5, dump raw
```

Each output file is deleted after measuring, so transient disk stays bounded by a
single dataset (≤ the largest `--totals` entry). Use `--repeat N` to report the
median write time across N iterations.

## Regression gate (CI)

`run_bench.py --check` turns the benchmark into a pass/fail gate that guards
against performance and size regressions as new features / data types land:

```sh
python3 bench/run_bench.py --check --bin-dir build \
    --totals 128 --chunks 64 --codecs zstd,uncompressed --repeat 3
```

It compares n2p against Apache Parquet C++ **on the same machine at the same
moment**, so it asserts *ratios*, not absolute throughput — stable even on noisy
shared CI runners. It fails (exit 1) if, for any config:

- `n2p write_s > arrow write_s * --max-write-ratio` (default 2.5 — generous; n2p
  is normally faster or on par), or
- `n2p file_bytes > arrow file_bytes * --max-size-ratio` (default 1.10), or
- row counts disagree / n2p emits an empty file.

This runs two ways:

- **CTest** — registered as `bench_regression` when built with
  `-DN2P_BUILD_TESTS=ON -DN2P_BUILD_BENCHMARKS=ON`; a few-second 128 MB matrix, so
  `ctest` covers it alongside the correctness tests.
- **GitHub Actions** — [`.github/workflows/ci.yml`](../.github/workflows/ci.yml)
  builds everything, runs `ctest`, then runs a larger 1 GB report (`--json`) and
  uploads `bench-report.txt` / `bench-results.json` as an artifact so each PR shows
  write speed and file size next to Arrow.

Tighten the thresholds (e.g. `--max-write-ratio 1.3`) once you have a feel for the
runner's variance, to catch smaller regressions.

## Direct binary use

```sh
./build/bench_n2p   --total-mb 4096 --chunk-mb 500 --codec zstd --out /tmp/n.parquet
./build/bench_arrow --total-mb 4096 --chunk-mb 500 --codec zstd --out /tmp/a.parquet
```

Each prints one JSON line: `gen_s`, `write_s`, `write_gbps`, `mrows_per_s`,
`file_bytes`, `bytes_per_row`, `peak_rss_mb`.

## Interpreting

- **ZSTD**: both writers bottleneck on zstd; speed and size land within ~1%,
  confirming n2p's framing/footer overhead is negligible.
- **Uncompressed**: n2p's fixed-width path is close to `memcpy`, so it tends to
  write faster than Arrow with near-zero size overhead (~20.00 B/row vs the 20 B
  of raw values).
- **Strings (dictionary)**: with `level`/`path` drawn from small pools, both
  writers emit RLE_DICTIONARY and land within ~0.1% on size; n2p's dictionary
  builder keeps write speed on par with (often slightly ahead of) Arrow.
- **Peak RSS** tracks the chunk size, not the dataset total — the point of the
  streaming design.
