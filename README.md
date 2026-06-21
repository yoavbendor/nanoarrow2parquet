# nanoarrow2parquet

A tiny, dependency-light Arrow → Parquet **writer** in C++23. It consumes an
in-memory Arrow C Data Interface struct array (`ArrowSchema` + `ArrowArray`, one
record batch == one row group) and writes a valid, self-describing `.parquet`
file readable by pandas, pyarrow, DuckDB, and polars.

It is the write-only counterpart to a full Parquet library. A *reader* must handle
whatever a file happens to contain (every encoding, every codec, dictionaries,
nested rep/def levels). A *writer* gets to **choose** the simplest valid
representation, so it controls the complexity:

- For PLAIN-encoded, uncompressed, fixed-width columns the Parquet data-page body
  is **byte-identical to the Arrow values buffer** — the column data path is
  essentially a `memcpy`.
- Strings pick their encoding **per column**: dictionary (`RLE_DICTIONARY`,
  dedup + best compression) when values repeat, falling back to `PLAIN` when the
  data is high-cardinality and the dictionary would be larger than the raw bytes.
- Every page body is **compressed** (ZSTD by default).

The only real format work is a small Thrift *compact protocol* encoder
(`src/thrift_compact.hpp`) and the metadata structs (`src/parquet_types.hpp`,
`src/writer.cpp`) — no Thrift library, no SIMD, no decode path.

## Scope

**Supported (v1):**

- Fixed-width flat columns: `int8/16/32/64`, `uint8/16/32/64`, `float`, `double`,
  `bool`, `fixed_size_binary(N)` — PLAIN.
- Variable-width strings/binary (`utf8`, `large_utf8`, `binary`, `large_binary`):
  dictionary-encoded (`RLE_DICTIONARY`) when it shrinks the column, otherwise
  `PLAIN` BYTE_ARRAY — chosen automatically per column.
- **Nullable (OPTIONAL) columns**: a column whose Arrow schema sets
  `ARROW_FLAG_NULLABLE` is written with definition levels (only present values are
  stored); the Arrow null type (`n`) becomes an all-null column. Non-nullable
  columns keep the zero-overhead REQUIRED fast path and reject actual nulls.
- **Nested struct columns** (`+s`), arbitrarily deep, nullable or required — each
  leaf carries its dotted `path_in_schema` and multi-level definition levels (so a
  null can come from the leaf *or* any ancestor struct). This is the Parquet analog
  of nanolance's grouped/blob.v2 columns.
- Per-page compression: ZSTD (default) or uncompressed.
- One or more row groups via the streaming writer.

This matches [nanolance](https://github.com/yoavbendor/nanolance)'s type coverage,
so the same Arrow data feeds either writer (Parquet or Lance).

| Arrow (C format) | Parquet physical | Notes |
|---|---|---|
| `i` `l` `f` `g` `b` | INT32 / INT64 / FLOAT / DOUBLE / BOOLEAN | pure memcpy of the values buffer |
| `I` `L` | INT32 / INT64 + `UINT_*` | bits identical ⇒ memcpy |
| `c` `s` `C` `S` | INT32 + `INT_8/16` / `UINT_8/16` | widened 1/2 → 4 bytes |
| `w:N` | FIXED_LEN_BYTE_ARRAY (`type_length=N`) | memcpy |
| `u` `U` `z` `Z` | BYTE_ARRAY | `RLE_DICTIONARY` or `PLAIN`, chosen per column by size |
| `n` | all-null INT32 | OPTIONAL, every value null |
| `+s` | group (struct) | nested, recurses to leaf columns |
| any nullable | + definition levels | OPTIONAL repetition, present values only |

**Out of scope (TODO):** nested list/map columns (repetition levels), page
statistics / indexes, bloom filters, `DELTA_*` / `BYTE_STREAM_SPLIT` encodings.

### Gotchas & limits

- **Write-only.** There is no reader; round-trip with pyarrow/DuckDB/polars/lance.
- **One record batch == one row group.** The producer controls batching; the writer
  never buffers the whole dataset.
- **Crash safety:** the footer is written only at `close()`. A file whose process died
  before `close()` has no footer and is unreadable (no partial-read fallback) — roll to
  a new file periodically for long captures.
- **REQUIRED columns reject actual nulls.** A non-nullable Arrow schema with a null in
  the data is an error; mark the field `ARROW_FLAG_NULLABLE` (definition levels) if it
  can be null.
- **`uint64` is written as INT64 + the `UINT_64` logical type** (bits identical). Some
  readers surface it as signed if they ignore the logical type — values are exact.
- **Lists/maps are unsupported** (no repetition levels). Flatten, or store a child table
  joined by an id column (this is what the `pcapng2parquet` example does per layer).

## For AI agents

**Use this library when** you have in-memory Arrow data (`ArrowSchema` + `ArrowArray`, or a
soatins SoA) and want a `.parquet` file. It is **write-only** — there is no reader.

**Pick a sibling instead when:** you need Lance datasets, external payload references, or
appendable fragments → [nanolance](https://github.com/yoavbendor/nanolance) (the *same* Arrow
batch feeds either writer). You need to *produce* the Arrow from packets or wire structs →
[nanotins / soatins](https://github.com/yoavbendor/nanotins).

**Minimal program** (C ABI; `target_link_libraries(app PRIVATE nanoarrow2parquet nanoarrow_static)`):

```c
#include "nanoarrow2parquet/nanoarrow2parquet.h"
// schema + batch are an Arrow struct array you built (nanoarrow, or soatins::to_arrow).

// one-shot (single row group):
char err[256];
if (n2p_write_file("out.parquet", &schema, &batch, err, sizeof err) != N2P_OK)
    fprintf(stderr, "n2p: %s\n", err);

// streaming (one batch == one row group):
N2PWriter* w;
n2p_writer_open(&w, "out.parquet");
n2p_writer_set_codec(w, N2P_CODEC_ZSTD);          // MUST be before the first write_batch
for (/* each batch */) n2p_writer_write_batch(w, &schema, &batch);
n2p_writer_close(w);                              // writes the footer, frees w
```

**Do**
- Set the codec **before** the first `write_batch`; pass the *same* schema every call.
- Treat one batch as one row group — let the producer decide batch size.
- Call `close()` exactly once; check return codes and read `n2p_writer_last_error(w)` on failure.
- Mark a column `ARROW_FLAG_NULLABLE` if its data can contain nulls.

**Don't**
- Don't put a null in a REQUIRED (non-nullable) column — it is rejected.
- Don't expect lists/maps, page statistics, bloom filters, or a read path — none exist. Flatten
  lists or emit a child table joined by an id column.
- Don't skip `close()` — a file with no footer is unreadable (no partial-read fallback).
- Don't rely on `uint64` deserializing as unsigned in every reader; it is stored as INT64 + the
  `UINT_64` logical type (bits exact).

## API

The public surface is a small C ABI (`include/nanoarrow2parquet/nanoarrow2parquet.h`);
internals are C++23.

```c
// One-shot: schema + one record batch -> one .parquet file.
int n2p_write_file(const char* path,
                   const struct ArrowSchema* schema,
                   const struct ArrowArray* batch,
                   char* err, size_t err_len);

// Streaming: multiple batches -> multiple row groups in one file.
int  n2p_writer_open (N2PWriter** out, const char* path);
int  n2p_writer_set_codec(N2PWriter* w, N2PCodec codec);   // ZSTD (default) or UNCOMPRESSED
int  n2p_writer_write_batch(N2PWriter* w, const struct ArrowSchema*, const struct ArrowArray*);
int  n2p_writer_close(N2PWriter* w);                       // writes the footer; frees the writer
const char* n2p_writer_last_error(const N2PWriter* w);
```

Example (`examples/arrow2parquet_demo.cpp`):

```cpp
ArrowSchema schema; ArrowArray batch;   // built via nanoarrow
char err[256];
if (n2p_write_file("out.parquet", &schema, &batch, err, sizeof err) != N2P_OK) {
    std::fprintf(stderr, "write failed: %s\n", err);
}
```

### Example: pcapng → Parquet (full protocol parsing)

[`examples/pcapng2parquet`](examples/pcapng2parquet/) is a full pcap/pcapng → Parquet
converter — the Parquet-output twin of nanolance's `pcapng2lance`. It runs the *same*
header-only [nanotins](https://github.com/yoavbendor/nanotins) parsing stack (vendored
as a git submodule) and writes **one table per layer**: L1 packets plus, with
`--decode-l2l3`, ethernet / vlan / ipv4 / ipv6 / tcp / udp / **PTP** (+ bodies) / IPv6
extension headers / SOME/IP, and with `--lldp` an LLDP TLV table. Only the output
endpoint differs from `pcapng2lance` — both sinks consume the identical
`ArrowSchema`/`ArrowArray`, so the per-PDU tables are byte-for-byte the same columns.
It is deliberately **parse-only** (Parquet has no external blob store): payloads are
not stored. See [`examples/pcapng2parquet/README.md`](examples/pcapng2parquet/README.md).

```sh
git submodule update --init --recursive
cmake --build build --target pcapng2parquet
./build/pcapng2parquet --decode-l2l3 capture.pcapng out   # -> out_packets.parquet, out_ipv4.parquet, ...
```

### With soatins (nanotins)

`soatins::arrow_schema<T>()` + `soatins::to_arrow(soa, batch)` already produce
exactly the non-nullable struct `ArrowSchema` + `ArrowArray` this writer takes, so
dumping a reflected SoA to Parquet is three calls:

```cpp
ArrowSchema schema; ArrowArray batch; std::string e;
soatins::arrow_schema<T>(schema, e);
soatins::to_arrow(soa, batch, e);
char err[256];
n2p_write_file("out.parquet", &schema, &batch, err, sizeof err);
```

### Native SoA path (compile-time schema)

For SoA storage you can skip the Arrow C structs entirely. The header-only
[`nanoarrow2parquet/soa.hpp`](include/nanoarrow2parquet/soa.hpp) takes the schema
as C++ types and one contiguous range per column; for fixed-width columns the page
body is the column's own storage (zero copy), and **no nanoarrow is involved** — it
fills borrowed Arrow C *views* and drives the same page/footer back-end. Unsupported
types, duplicate names, and column/field type mismatches are `static_assert`s.

```cpp
#include "nanoarrow2parquet/soa.hpp"
using namespace n2p::soa;

Writer<Field<"id", std::int64_t>,     // REQUIRED fixed-width (zero-copy)
       Nullable<"x", double>,         // OPTIONAL: supply validity per chunk
       Field<"name", utf8>> w("out.parquet");

w.write_chunk(ids,
              present(xs, x_mask),    // per-element presence mask -> bitmap
              names);                 // range of std::string / std::string_view
w.write_chunk(more_ids, present(more_xs, more_mask), more_names);
w.close();
```

Nested structs are described with `Struct<>` / `NullableStruct<>` and assembled
from their children's columns with `group(...)` (or `group_present(mask, ...)` /
`group_valid(bitmap, nulls, ...)` for an OPTIONAL struct), to any depth:

```cpp
Writer<Field<"id", std::int32_t>,
       Struct<"addr", Field<"city", utf8>, Field<"zip", std::int32_t>>,
       NullableStruct<"meta", Field<"score", double>>> w("out.parquet");

w.write_chunk(ids, group(cities, zips), group_present(meta_mask, scores));
```

Each `write_chunk` is exactly one row group, so the producer keeps batching policy
and the writer never buffers the whole dataset. Supported: REQUIRED/OPTIONAL
fixed-width numeric columns (zero-copy aliases of your storage), `bool` (packed to
1 bit per value per chunk), `utf8`/`binary`/`large_utf8`/`large_binary` columns
(offsets + data materialized per chunk), and nested `Struct`/`NullableStruct`
groups (multi-level definition levels — a null can come from a leaf or any
ancestor struct). Nullable columns take `present(values, mask)` or
`valid_bits(values, bitmap, null_count)` (the latter aliases a pre-packed bitmap).
Unsupported types, duplicate names, and type mismatches are `static_assert`s.

## Single-header build

The library is also published as one self-contained, STB-style header at
[`single_include/nanoarrow2parquet.h`](single_include/nanoarrow2parquet.h). Include
it anywhere for the declarations; in **exactly one** translation unit define the
implementation macro first:

```cpp
#define NANOARROW2PARQUET_IMPLEMENTATION
#include "nanoarrow2parquet.h"     // brings in the writer implementation
```

Everywhere else, `#include "nanoarrow2parquet.h"` with no macro for just the C ABI.
This is link-time equivalent to compiling `src/writer.cpp`: the header still needs
the **nanoarrow** and **zstd** headers on the include path and links **libzstd**
(use `N2P_CODEC_UNCOMPRESSED` if you want to avoid zstd at runtime). With CMake,
link `nanoarrow2parquet::single` instead of the static library.

The split files under `include/` and `src/` remain the source of truth; the single
header is generated from them by [`scripts/amalgamate.py`](scripts/amalgamate.py)
(`cmake --build build --target n2p_amalgamate`, or run the script directly). The
`amalgamation_up_to_date` ctest fails if the committed header drifts from the
sources.

## File layout

```
PAR1                       # 4-byte magic
<row group 0 pages>        # per column: [dictionary page] + data page, each compressed
...
<FileMetaData>             # Thrift compact protocol
<uint32 LE footer length>
PAR1                       # 4-byte magic
```

The footer (`FileMetaData`) is written **once, at `close()`** — never per chunk and
never rewritten. Each `write_batch` streams its column data immediately and appends
a `RowGroup` entry to an in-memory list; `close()` serializes the single footer
covering all schema + row groups. **Crash safety:** every Parquet reader seeks to
EOF to find the footer, so a file with no footer (process died before `close()`) is
unreadable — there is no partial-read fallback. For long captures, roll to a new
file periodically.

## Build

```sh
cmake -S . -B build -DN2P_BUILD_TESTS=ON
cmake --build build
ctest --test-dir build --output-on-failure
```

Dependencies (`nanoarrow`, `zstd`) are fetched via CMake `FetchContent`; nanoarrow
is pinned to the same commit nanolance uses. Embedding parents that already provide
`nanoarrow_static` / a `zstd` target are reused automatically.

`ctest` runs:
- `test_roundtrip` — unit checks of the compact-protocol encoder, the RLE/bit-pack
  encoder, file framing, and null rejection.
- `test_single_header` — compiles two TUs against the amalgamated header (one
  defining `NANOARROW2PARQUET_IMPLEMENTATION`) and writes a file, proving the
  single-header build compiles and links with no ODR clashes.
- `amalgamation_up_to_date` — asserts `single_include/` matches the split sources.
- `smoke_pyarrow` — writes single-shot and 3-row-group files via the demo, then
  reads them back with pyarrow (and pandas/polars if installed) and asserts the
  values, dtypes, ZSTD codec, dictionary encoding, and row-group count.

## Benchmarks

[`bench/`](bench/) compares write throughput and output size against the Apache
Parquet C++ writer, streaming datasets in chunks (row groups) so the total exceeds
peak memory. Data generation is timed separately from writing, and the Apache
baseline is configured to match n2p's choices (PLAIN, ZSTD level 3, one row group
per chunk). The Arrow side links the `libparquet` bundled in pyarrow, so no Arrow
source build is needed. See [`bench/README.md`](bench/README.md).

```sh
cmake -S . -B build -DN2P_BUILD_BENCHMARKS=ON
cmake --build build --target bench_n2p bench_arrow
python3 bench/run_bench.py --totals 1024,4096,8192 --chunks 200,500
```

At parity settings, ZSTD speed/size land within ~1% of Arrow (both bottleneck on
zstd), while n2p's near-`memcpy` uncompressed path writes faster with near-zero
size overhead and lower peak RSS.

The same harness doubles as a **regression gate** (`run_bench.py --check`): it
compares n2p to Apache Parquet C++ on the same runner and fails if write time or
file size drifts beyond a ratio threshold — registered as the `bench_regression`
CTest and run in CI ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)) so new
features / data types can't silently regress speed or size.

## License

Apache-2.0. See [LICENSE](LICENSE), [NOTICE](NOTICE), and
[THIRD-PARTY-LICENSES.md](THIRD-PARTY-LICENSES.md).
