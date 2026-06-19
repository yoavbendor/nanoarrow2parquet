# pcapng2parquet

The Parquet-output counterpart to nanolance's
[`pcapng2lance`](https://github.com/yoavbendor/nanolance/tree/main/examples/pcapng2parquet)
example. It reads a classic **pcap** or **pcapng** capture and writes the
all-scalar **L1 packet-metadata** table as a single `.parquet` file via the
nanoarrow2parquet streaming writer.

```sh
pcapng2parquet [--no-compress] [--window-rows N] [-d|--drop N] [-c|--count N] \
               <input.pcap|pcapng> <output.parquet>
```

## Schema

One row per packet, matching `pcapng2lance`'s `PacketRow` exactly — all
non-nullable scalars, which is precisely what this writer emits:

| column | type | meaning |
|---|---|---|
| `packet_id` | uint64 | global, monotonic across the whole capture (and slices) |
| `interface_id` | uint32 | pcapng interface index (0 for classic pcap) |
| `ts_raw` | uint64 | raw timestamp ticks at the interface resolution |
| `caplen` | uint32 | captured bytes |
| `origlen` | uint32 | original on-wire bytes |
| `link_type` | uint16 | `LINKTYPE_*` of the capturing interface |
| `ts_resol` | uint8 | timestamp resolution code (`if_tsresol`; default 6 ⇒ 10⁻⁶ s) |
| `epb_flags` | uint32 | EPB `epb_flags` option (0 when absent) |

## What's ported, and what isn't

`pcapng2lance` showcases the full nanotins/nanolance stack. This port keeps only
what maps cleanly onto a non-nullable, flat-column **Parquet** writer:

- **Ported:** the self-contained pcap/pcapng parser (SHB/IDB/EPB/SPB, both
  endiannesses; classic pcap µs/ns magic), the L1 scalar schema, windowed
  streaming (`--window-rows` ⇒ one row group per window, payloads never
  buffered), and the global-`packet_id` `-d`/`-c` slice semantics.
- **Out of scope:** external payload blobs (`payload_ref`). Parquet has no
  out-of-band blob store, so payloads are **seeked past, not stored** — only
  metadata is written. The GPU paths, nanotins reflection core, and staged
  L2–L4 multi-table enrichment are likewise omitted; decoding optional protocol
  layers into separate tables would require nullable columns / joins this writer
  does not target.

## Flags

- `--no-compress` — write `UNCOMPRESSED` pages instead of the default ZSTD.
- `--window-rows N` — flush a row group every `N` packets (default 65536) to bound
  memory on large captures.
- `-d, --drop N` / `-c, --count N` — packet slice: skip the first `N`, then emit at
  most `N`. `packet_id` stays global, so independently-converted slices stitch back
  into a bit-exact replica of the full dataset.

## Build & run

```sh
cmake -S . -B build -DN2P_BUILD_EXAMPLES=ON
cmake --build build --target pcapng2parquet
./build/pcapng2parquet capture.pcapng packets.parquet
```

Read it back with anything that speaks Parquet:

```python
import pyarrow.parquet as pq
print(pq.read_table("packets.parquet").to_pandas())
```
