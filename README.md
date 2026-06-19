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
- Strings are stored with **dictionary encoding** (the standard, best-compressing
  representation), which also handles deduplication.
- Every page body is **compressed** (ZSTD by default).

The only real format work is a small Thrift *compact protocol* encoder
(`src/thrift_compact.hpp`) and the metadata structs (`src/parquet_types.hpp`,
`src/writer.cpp`) — no Thrift library, no SIMD, no decode path.

## Scope

**Supported (v1):**

- Fixed-width, **REQUIRED (non-nullable)**, flat columns: `int8/16/32/64`,
  `uint8/16/32/64`, `float`, `double`, `bool`, `fixed_size_binary(N)` — PLAIN.
- Variable-width strings/binary (`utf8`, `large_utf8`, `binary`, `large_binary`)
  via dictionary encoding (`RLE_DICTIONARY`).
- Per-page compression: ZSTD (default) or uncompressed.
- One or more row groups via the streaming writer.

| Arrow (C format) | Parquet physical | Notes |
|---|---|---|
| `i` `l` `f` `g` `b` | INT32 / INT64 / FLOAT / DOUBLE / BOOLEAN | pure memcpy of the values buffer |
| `I` `L` | INT32 / INT64 + `UINT_*` | bits identical ⇒ memcpy |
| `c` `s` `C` `S` | INT32 + `INT_8/16` / `UINT_8/16` | widened 1/2 → 4 bytes |
| `w:N` | FIXED_LEN_BYTE_ARRAY (`type_length=N`) | memcpy |
| `u` `U` `z` `Z` | BYTE_ARRAY via dictionary | `RLE_DICTIONARY` data page |

**Out of scope (TODO):** nullable / definition levels, nested list/struct/map
columns, page statistics / indexes, bloom filters, `DELTA_*` / `BYTE_STREAM_SPLIT`
encodings.

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
- `smoke_pyarrow` — writes single-shot and 3-row-group files via the demo, then
  reads them back with pyarrow (and pandas/polars if installed) and asserts the
  values, dtypes, ZSTD codec, dictionary encoding, and row-group count.

## License

Apache-2.0. See [LICENSE](LICENSE), [NOTICE](NOTICE), and
[THIRD-PARTY-LICENSES.md](THIRD-PARTY-LICENSES.md).
