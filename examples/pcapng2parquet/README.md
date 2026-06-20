# pcapng → Parquet (full nanotins parsing)

The Parquet-output counterpart to nanolance's
[`pcapng2lance`](https://github.com/yoavbendor/nanolance/tree/main/examples/pcapng2lance). It converts
legacy **pcap** and **pcapng** captures into Parquet, decoding the full protocol stack —
**L1 → L2 → L3 → L4, plus PTP and LLDP** — and writing **one table per layer**.

The whole point of this example is parity: it runs the *same* parsing stack as `pcapng2lance` (the
header-only [nanotins](https://github.com/yoavbendor/nanotins) pcap/pcapng scanner + the `wire_spec` /
`spec_dag` decoder, and the `soatins` *describe-a-struct-once → SoA → Arrow* reflection). **Only the
output endpoint changes** — `nanoarrow2parquet` instead of `nanolance`. Both sinks consume the identical
`ArrowSchema` / `ArrowArray`, so the per-PDU tables carry byte-for-byte the same columns; only the file
format differs.

## Tables produced

| File | Contents |
|---|---|
| `<stem>_packets.parquet` | L1: one row per packet (`packet_id`, `interface_id`, `ts_raw`, `caplen`, `origlen`, `link_type`, `ts_resol`, `epb_flags`) |
| `<stem>_ethernet.parquet` | L2 Ethernet (`dst`/`src` MAC as `fixed_size_binary[6]`, `ethertype`) — with `--decode-l2l3` |
| `<stem>_vlan.parquet` | 802.1Q VLAN tags (`pcp`, `dei`, `vid`, `inner_ethertype`) |
| `<stem>_ipv4.parquet` | IPv4 header (addresses as `fixed_size_binary[4]`) |
| `<stem>_ipv6.parquet` | IPv6 header (addresses as `fixed_size_binary[16]`) |
| `<stem>_tcp.parquet`, `_udp.parquet` | L4 headers (the L4 row appears only on the first IPv4 fragment) |
| `<stem>_ptp.parquet` + `_ptp_timestamp` / `_ptp_ts_port` / `_ptp_announce` / `_ptp_signaling` | PTPv2 / gPTP common header + per-message-type bodies |
| `<stem>_ipv6_*.parquet` | IPv6 extension-header node tables (hop-by-hop, routing, fragment, dest-opts, AH) |
| `<stem>_someip.parquet` | SOME/IP header (on the well-known port) |
| `<stem>_lldp.parquet` | LLDP, one row per TLV — with `--lldp` |

Every PDU table starts with a `packet_id` column that joins back to `<stem>_packets.parquet`. All columns
are scalars or `fixed_size_binary`, which `nanoarrow2parquet` writes as PLAIN-encoded REQUIRED columns.

## Build & run

`nanotins` is vendored as a git submodule under `extern/nanotins`:

```
git submodule update --init --recursive
cmake -S . -B build -DN2P_BUILD_EXAMPLES=ON
cmake --build build --target pcapng2parquet

build/pcapng2parquet --decode-l2l3 capture.pcapng out      # -> out_packets.parquet, out_ipv4.parquet, ...
build/pcapng2parquet --lldp        lldp.pcap       out      # -> out_lldp.parquet
```

Usage: `pcapng2parquet [--no-compress] [--decode-l2l3] [--lldp] [--window-bytes N] [-d|--drop N]
[-c|--count N] <input.pcap|pcapng> <output-stem>`.

- `--decode-l2l3` — decode L2/L3/L4 + PTP via the `wire_spec`/`spec_dag` core; emits the per-PDU tables.
- `--lldp` — also run the LLDP DPAR parser (the user-written palette from nanotins's lldp example).
- `--no-compress` — write uncompressed columns (default: ZSTD).
- `--window-bytes N` — RAM budget; the capture is streamed in bounded windows, one row group per window.
- `-d/--drop N`, `-c/--count N` — emit a packet slice; `packet_id` stays global so slices stitch into a
  bit-exact replica of the full dataset.

## Differences from pcapng2lance (intentional)

Parquet has no out-of-band blob store, so — unlike the Lance example — this converter is **parse-only**:

- **no payloads** are stored (no `payload_ref`, no `remainder_after_l4` external-blob tables);
- **no `--stage` staged enrichment** (it is built on external-blob refetch);
- decode runs **serially** (no stdexec bulk path) — serial and bulk produce identical tables, so this
  costs nothing in correctness, and it keeps the example's dependency surface to just `boost::describe`
  (header-only) + the nanoarrow this repo already vendors. The IPv4/IPv6 *variable-length* child tables
  (SRv6 segments, options) need the bulk children pass and are out of scope; the fixed IPv6
  extension-header node tables are included.
