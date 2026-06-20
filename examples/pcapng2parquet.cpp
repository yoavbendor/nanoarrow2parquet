// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Yoav Bendor

// pcapng2parquet: convert a legacy pcap / pcapng capture into a Parquet file of
// per-packet L1 metadata, using the nanoarrow2parquet streaming writer.
//
// This is the Parquet-output counterpart to nanolance's pcapng2lance example. It
// keeps only the parts that map cleanly onto a non-nullable, flat-column Parquet
// writer: the all-scalar L1 packet row. The Lance example's external payload
// blobs (payload_ref), GPU paths, and staged L2-L4 multi-table enrichment have no
// equivalent here -- Parquet has no out-of-band blob store, and this writer emits
// only REQUIRED columns -- so packet payloads are skipped (seeked past) and only
// metadata is written. The schema matches pcapng2lance's PacketRow exactly:
//
//   packet_id     uint64   global, monotonic across the whole capture (and slices)
//   interface_id  uint32   pcapng interface index (0 for classic pcap)
//   ts_raw        uint64   raw timestamp ticks at 10^-ts_resol (or 2^-n) resolution
//   caplen        uint32   captured bytes
//   origlen       uint32   original on-wire bytes
//   link_type     uint16   LINKTYPE_* of the capturing interface
//   ts_resol      uint8    timestamp resolution code (if_tsresol; default 6)
//   epb_flags     uint32   EPB epb_flags option (0 when absent)
//
// To bound memory on large captures, rows are flushed as one row group every
// --window-rows packets via the streaming writer (the Lance example's windowing,
// expressed in rows rather than bytes). Payloads are never buffered.
//
// Supported inputs: classic pcap (us/ns magic, either endianness) and pcapng
// (SHB/IDB/EPB/SPB blocks, either endianness). Self-contained -- no nanotins.

#include "nanoarrow2parquet/nanoarrow2parquet.h"

#include <nanoarrow/nanoarrow.h>

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <string>
#include <vector>

namespace {

// ---- L1 packet row (mirrors pcapng2lance PacketRow) -----------------------

struct PacketRow {
    std::uint64_t packet_id;
    std::uint32_t interface_id;
    std::uint64_t ts_raw;
    std::uint32_t caplen;
    std::uint32_t origlen;
    std::uint16_t link_type;
    std::uint8_t  ts_resol;
    std::uint32_t epb_flags;
};

struct Options {
    const char* input = nullptr;
    const char* output = nullptr;
    bool compress = true;
    std::uint64_t window_rows = 1u << 16;  // flush a row group every 65536 packets
    std::uint64_t drop = 0;                // skip the first N packets
    std::uint64_t count = UINT64_MAX;      // then emit at most N
};

[[noreturn]] void die(const std::string& msg) {
    std::fprintf(stderr, "pcapng2parquet: %s\n", msg.c_str());
    std::exit(1);
}

void usage(const char* argv0) {
    std::fprintf(stderr,
        "usage: %s [--no-compress] [--window-rows N] [-d|--drop N] [-c|--count N]\n"
        "          <input.pcap|pcapng> <output.parquet>\n"
        "  writes the all-scalar L1 packet table (metadata only; payloads skipped).\n"
        "  -d/-c select a packet slice: skip the first N, then emit at most N;\n"
        "  packet_id stays global so slices stitch into the full dataset.\n",
        argv0);
}

std::uint64_t parse_u64(const char* s, const char* flag) {
    char* end = nullptr;
    const unsigned long long v = std::strtoull(s, &end, 10);
    if (end == s || *end != '\0') die(std::string("invalid value for ") + flag + ": " + s);
    return static_cast<std::uint64_t>(v);
}

Options parse_args(int argc, char** argv) {
    Options o;
    std::vector<const char*> pos;
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        auto next = [&](const char* flag) -> const char* {
            if (i + 1 >= argc) die(std::string("missing argument for ") + flag);
            return argv[++i];
        };
        if (a == "--no-compress") o.compress = false;
        else if (a == "--window-rows") o.window_rows = parse_u64(next("--window-rows"), "--window-rows");
        else if (a == "-d" || a == "--drop") o.drop = parse_u64(next("--drop"), "--drop");
        else if (a == "-c" || a == "--count") o.count = parse_u64(next("--count"), "--count");
        else if (a == "-h" || a == "--help") { usage(argv[0]); std::exit(0); }
        else if (!a.empty() && a[0] == '-') die("unknown flag: " + a);
        else pos.push_back(argv[i]);
    }
    if (pos.size() != 2) { usage(argv[0]); std::exit(1); }
    if (o.window_rows == 0) die("--window-rows must be > 0");
    o.input = pos[0];
    o.output = pos[1];
    return o;
}

// ---- little/big-endian field readers --------------------------------------

std::uint16_t rd16(const std::uint8_t* p, bool swap) {
    std::uint16_t v = static_cast<std::uint16_t>(p[0]) | (static_cast<std::uint16_t>(p[1]) << 8);
    return swap ? static_cast<std::uint16_t>((v >> 8) | (v << 8)) : v;
}
std::uint32_t rd32(const std::uint8_t* p, bool swap) {
    std::uint32_t v = static_cast<std::uint32_t>(p[0]) | (static_cast<std::uint32_t>(p[1]) << 8) |
                      (static_cast<std::uint32_t>(p[2]) << 16) | (static_cast<std::uint32_t>(p[3]) << 24);
    if (!swap) return v;
    return (v >> 24) | ((v >> 8) & 0x0000FF00u) | ((v << 8) & 0x00FF0000u) | (v << 24);
}

// ---- a tiny buffered file reader (seek-past payloads, never buffer them) ---

class Reader {
public:
    explicit Reader(const char* path) : in_(path, std::ios::binary) {
        if (!in_) die(std::string("cannot open input: ") + path);
    }
    // Read exactly n bytes into buf; returns false at clean EOF before any byte.
    bool read_exact(std::uint8_t* buf, std::size_t n) {
        in_.read(reinterpret_cast<char*>(buf), static_cast<std::streamsize>(n));
        const std::streamsize got = in_.gcount();
        if (got == 0 && in_.eof()) return false;
        if (static_cast<std::size_t>(got) != n) die("unexpected end of file (truncated record)");
        return true;
    }
    void skip(std::uint64_t n) {
        in_.seekg(static_cast<std::streamoff>(n), std::ios::cur);
        if (!in_) die("seek past payload failed (truncated file?)");
    }
    bool peek32(std::uint32_t& out) {
        std::uint8_t b[4];
        in_.read(reinterpret_cast<char*>(b), 4);
        if (in_.gcount() == 0 && in_.eof()) return false;
        if (in_.gcount() != 4) die("unexpected end of file");
        in_.seekg(-4, std::ios::cur);
        out = static_cast<std::uint32_t>(b[0]) | (static_cast<std::uint32_t>(b[1]) << 8) |
              (static_cast<std::uint32_t>(b[2]) << 16) | (static_cast<std::uint32_t>(b[3]) << 24);
        return true;
    }

private:
    std::ifstream in_;
};

// ---- Arrow batch construction ---------------------------------------------

void init_schema(ArrowSchema& schema) {
    struct Col { const char* name; ArrowType type; };
    static const Col cols[8] = {
        {"packet_id",    NANOARROW_TYPE_UINT64},
        {"interface_id", NANOARROW_TYPE_UINT32},
        {"ts_raw",       NANOARROW_TYPE_UINT64},
        {"caplen",       NANOARROW_TYPE_UINT32},
        {"origlen",      NANOARROW_TYPE_UINT32},
        {"link_type",    NANOARROW_TYPE_UINT16},
        {"ts_resol",     NANOARROW_TYPE_UINT8},
        {"epb_flags",    NANOARROW_TYPE_UINT32},
    };
    ArrowSchemaInit(&schema);
    if (ArrowSchemaSetTypeStruct(&schema, 8) != NANOARROW_OK) die("schema alloc failed");
    for (int i = 0; i < 8; ++i) {
        ArrowSchema* c = schema.children[i];
        if (ArrowSchemaSetType(c, cols[i].type) != NANOARROW_OK) die("schema set type failed");
        if (ArrowSchemaSetName(c, cols[i].name) != NANOARROW_OK) die("schema set name failed");
        c->flags = 0;  // non-nullable (REQUIRED)
    }
    schema.flags = 0;
}

void build_batch(const std::vector<PacketRow>& rows, const ArrowSchema& schema, ArrowArray& array) {
    if (ArrowArrayInitFromSchema(&array, const_cast<ArrowSchema*>(&schema), nullptr) != NANOARROW_OK)
        die("array init failed");
    if (ArrowArrayStartAppending(&array) != NANOARROW_OK) die("array append start failed");
    for (const PacketRow& r : rows) {
        ArrowArrayAppendUInt(array.children[0], r.packet_id);
        ArrowArrayAppendUInt(array.children[1], r.interface_id);
        ArrowArrayAppendUInt(array.children[2], r.ts_raw);
        ArrowArrayAppendUInt(array.children[3], r.caplen);
        ArrowArrayAppendUInt(array.children[4], r.origlen);
        ArrowArrayAppendUInt(array.children[5], r.link_type);
        ArrowArrayAppendUInt(array.children[6], r.ts_resol);
        ArrowArrayAppendUInt(array.children[7], r.epb_flags);
        if (ArrowArrayFinishElement(&array) != NANOARROW_OK) die("array finish element failed");
    }
    if (ArrowArrayFinishBuildingDefault(&array, nullptr) != NANOARROW_OK) die("array build failed");
}

// ---- streaming sink: buffer rows, flush a row group per window ------------

class ParquetSink {
public:
    ParquetSink(const Options& opt) : opt_(opt) {
        init_schema(schema_);
        if (n2p_writer_open(&w_, opt.output) != N2P_OK) {
            std::string e = w_ ? n2p_writer_last_error(w_) : "open failed";
            n2p_writer_close(w_);
            die("cannot open output: " + e);
        }
        if (!opt.compress && n2p_writer_set_codec(w_, N2P_CODEC_UNCOMPRESSED) != N2P_OK)
            die("set codec failed");
        buf_.reserve(opt.window_rows);
    }

    void add(const PacketRow& r) {
        buf_.push_back(r);
        if (buf_.size() >= opt_.window_rows) flush();
    }

    // Returns total rows written.
    std::uint64_t finish() {
        flush();
        if (n2p_writer_close(w_) != N2P_OK) die("close/footer failed");
        w_ = nullptr;
        ArrowSchemaRelease(&schema_);
        return total_;
    }

private:
    void flush() {
        if (buf_.empty()) return;
        ArrowArray array{};
        build_batch(buf_, schema_, array);
        int status = n2p_writer_write_batch(w_, &schema_, &array);
        ArrowArrayRelease(&array);
        if (status != N2P_OK) die(std::string("write_batch failed: ") + n2p_writer_last_error(w_));
        total_ += buf_.size();
        ++row_groups_;
        buf_.clear();
    }

    const Options& opt_;
    ArrowSchema schema_{};
    N2PWriter* w_ = nullptr;
    std::vector<PacketRow> buf_;
    std::uint64_t total_ = 0;
public:
    std::uint64_t row_groups_ = 0;
};

// A slice-aware emitter: assigns global packet_ids, honours --drop/--count.
class Emitter {
public:
    Emitter(ParquetSink& sink, const Options& opt) : sink_(sink), opt_(opt) {}

    // Returns false once the slice is exhausted (caller may stop early).
    bool emit(PacketRow r) {
        const std::uint64_t id = next_id_++;       // global id, before slicing
        if (id < opt_.drop) return true;
        if (emitted_ >= opt_.count) return false;
        r.packet_id = id;
        sink_.add(r);
        ++emitted_;
        return true;
    }

private:
    ParquetSink& sink_;
    const Options& opt_;
    std::uint64_t next_id_ = 0;
    std::uint64_t emitted_ = 0;
};

// ---- classic pcap ----------------------------------------------------------

void parse_pcap(Reader& rd, std::uint32_t magic_raw, Emitter& emit) {
    // magic already consumed by caller as raw LE u32; decode endianness + resolution.
    bool swap;
    std::uint8_t ts_resol;
    switch (magic_raw) {
        case 0xa1b2c3d4: swap = false; ts_resol = 6; break;  // microsecond, host endian
        case 0xd4c3b2a1: swap = true;  ts_resol = 6; break;  // microsecond, swapped
        case 0xa1b23c4d: swap = false; ts_resol = 9; break;  // nanosecond, host endian
        case 0x4d3cb2a1: swap = true;  ts_resol = 9; break;  // nanosecond, swapped
        default: die("unrecognized pcap magic"); return;
    }
    // Rest of the 24-byte global header: ver(2+2), thiszone(4), sigfigs(4),
    // snaplen(4), network/linktype(4). We only need linktype.
    std::uint8_t hdr[20];
    if (!rd.read_exact(hdr, sizeof hdr)) die("truncated pcap global header");
    const std::uint16_t link_type = static_cast<std::uint16_t>(rd32(hdr + 16, swap));

    std::uint8_t rec[16];
    while (rd.read_exact(rec, sizeof rec)) {
        const std::uint32_t ts_sec  = rd32(rec + 0, swap);
        const std::uint32_t ts_frac = rd32(rec + 4, swap);
        const std::uint32_t caplen  = rd32(rec + 8, swap);
        const std::uint32_t origlen = rd32(rec + 12, swap);
        const std::uint64_t scale = (ts_resol == 9) ? 1000000000ull : 1000000ull;

        PacketRow r{};
        r.interface_id = 0;
        r.ts_raw = static_cast<std::uint64_t>(ts_sec) * scale + ts_frac;
        r.caplen = caplen;
        r.origlen = origlen;
        r.link_type = link_type;
        r.ts_resol = ts_resol;
        r.epb_flags = 0;
        if (!emit.emit(r)) return;       // slice exhausted
        rd.skip(caplen);                 // payload: skipped, never buffered
    }
}

// ---- pcapng ----------------------------------------------------------------

struct Iface { std::uint16_t link_type = 0; std::uint8_t ts_resol = 6; };

// Parse trailing options of a block body for a single u8/u32 option code.
// `body` points just past the fixed fields; `len` is the remaining body bytes.
template <typename T>
bool find_option(const std::uint8_t* body, std::size_t len, bool swap,
                 std::uint16_t want_code, T& out) {
    std::size_t off = 0;
    while (off + 4 <= len) {
        const std::uint16_t code = rd16(body + off, swap);
        const std::uint16_t olen = rd16(body + off + 2, swap);
        off += 4;
        if (code == 0) break;            // opt_endofopt
        if (off + olen > len) break;
        if (code == want_code) {
            if (sizeof(T) == 1 && olen >= 1) { out = static_cast<T>(body[off]); return true; }
            if (sizeof(T) == 4 && olen >= 4) { out = static_cast<T>(rd32(body + off, swap)); return true; }
        }
        off += (olen + 3u) & ~3u;        // options are 32-bit padded
    }
    return false;
}

void parse_pcapng(Reader& rd, const std::uint8_t first4[4], Emitter& emit) {
    std::vector<Iface> ifaces;
    bool have_endian = false;
    bool swap = false;

    bool first = true;
    std::uint8_t typ_buf[4];
    for (;;) {
        // Block type (4) + total length (4). The very first block type is the 4
        // bytes the caller already peeked to detect pcapng; read fresh each loop.
        if (first) { std::memcpy(typ_buf, first4, 4); first = false; }
        else if (!rd.read_exact(typ_buf, 4)) return;  // clean EOF between blocks

        std::uint8_t len_buf[4];
        if (!rd.read_exact(len_buf, 4)) die("truncated pcapng block length");

        // Block type is endian-neutral only for SHB detection; once we know the
        // byte order, interpret everything with `swap`.
        const std::uint32_t block_type =
            static_cast<std::uint32_t>(typ_buf[0]) | (static_cast<std::uint32_t>(typ_buf[1]) << 8) |
            (static_cast<std::uint32_t>(typ_buf[2]) << 16) | (static_cast<std::uint32_t>(typ_buf[3]) << 24);

        // Section Header Block: 0x0A0D0D0A. Read body to find the byte-order magic.
        if (block_type == 0x0A0D0D0Au) {
            // total length is byte-order-independent to read both ways; try host.
            std::uint32_t total = rd32(len_buf, false);
            std::uint8_t bom[4];
            if (!rd.read_exact(bom, 4)) die("truncated SHB");
            const std::uint32_t magic =
                static_cast<std::uint32_t>(bom[0]) | (static_cast<std::uint32_t>(bom[1]) << 8) |
                (static_cast<std::uint32_t>(bom[2]) << 16) | (static_cast<std::uint32_t>(bom[3]) << 24);
            if (magic == 0x1A2B3C4Du) swap = false;
            else if (magic == 0x4D3C2B1Au) swap = true;
            else die("bad SHB byte-order magic");
            have_endian = true;
            total = rd32(len_buf, swap);
            if (total < 16) die("bad SHB length");
            rd.skip(total - 12);   // rest of body + trailing total-length; we re-sync per block
            // The trailing block_total_length (4) was included in `total`; we've
            // skipped it. A new section resets interfaces.
            ifaces.clear();
            continue;
        }

        if (!have_endian) die("pcapng did not start with a Section Header Block");
        const std::uint32_t total = rd32(len_buf, swap);
        if (total < 12) die("bad pcapng block length");
        const std::uint32_t body_len = total - 12;  // minus type(4)+len(4)+trailing len(4)

        std::vector<std::uint8_t> body(body_len);
        if (body_len && !rd.read_exact(body.data(), body_len)) die("truncated pcapng block body");
        std::uint8_t trailer[4];
        if (!rd.read_exact(trailer, 4)) die("truncated pcapng block trailer");

        if (block_type == 0x00000001u) {            // Interface Description Block
            if (body_len < 8) die("bad IDB");
            Iface iface;
            iface.link_type = rd16(body.data(), swap);
            iface.ts_resol = 6;
            find_option<std::uint8_t>(body.data() + 8, body_len - 8, swap, 9, iface.ts_resol);
            ifaces.push_back(iface);
        } else if (block_type == 0x00000006u) {     // Enhanced Packet Block
            if (body_len < 20) die("bad EPB");
            const std::uint32_t if_id   = rd32(body.data() + 0, swap);
            const std::uint32_t ts_high = rd32(body.data() + 4, swap);
            const std::uint32_t ts_low  = rd32(body.data() + 8, swap);
            const std::uint32_t caplen  = rd32(body.data() + 12, swap);
            const std::uint32_t origlen = rd32(body.data() + 16, swap);
            const Iface iface = (if_id < ifaces.size()) ? ifaces[if_id] : Iface{};

            PacketRow r{};
            r.interface_id = if_id;
            r.ts_raw = (static_cast<std::uint64_t>(ts_high) << 32) | ts_low;
            r.caplen = caplen;
            r.origlen = origlen;
            r.link_type = iface.link_type;
            r.ts_resol = iface.ts_resol;
            r.epb_flags = 0;
            const std::size_t padded = (caplen + 3u) & ~3u;
            if (20 + padded <= body_len)
                find_option<std::uint32_t>(body.data() + 20 + padded, body_len - 20 - padded,
                                           swap, 2, r.epb_flags);   // opt code 2 = epb_flags
            if (!emit.emit(r)) return;
        } else if (block_type == 0x00000003u) {     // Simple Packet Block
            if (body_len < 4) die("bad SPB");
            const std::uint32_t origlen = rd32(body.data(), swap);
            const Iface iface = ifaces.empty() ? Iface{} : ifaces[0];
            // SPB has no caplen field; captured length is body minus the 4-byte
            // origlen field, but bounded by origlen.
            const std::uint32_t caplen =
                static_cast<std::uint32_t>(body_len - 4) < origlen ? body_len - 4 : origlen;
            PacketRow r{};
            r.interface_id = 0;
            r.ts_raw = 0;
            r.caplen = caplen;
            r.origlen = origlen;
            r.link_type = iface.link_type;
            r.ts_resol = iface.ts_resol;
            r.epb_flags = 0;
            if (!emit.emit(r)) return;
        }
        // Other block types (NRB, ISB, custom, ...) are skipped: body already read.
        (void)trailer;
    }
}

}  // namespace

int main(int argc, char** argv) {
    const Options opt = parse_args(argc, argv);

    Reader rd(opt.input);
    std::uint32_t first;
    if (!rd.peek32(first)) die("empty input file");

    ParquetSink sink(opt);
    Emitter emit(sink, opt);

    // Dispatch on the first 4 bytes: pcapng SHB type vs classic pcap magic.
    if (first == 0x0A0D0D0Au) {
        std::uint8_t first4[4] = {0x0A, 0x0D, 0x0D, 0x0A};
        rd.skip(4);  // consume the peeked block type; parse_pcapng expects it via first4
        parse_pcapng(rd, first4, emit);
    } else if (first == 0xa1b2c3d4u || first == 0xd4c3b2a1u ||
               first == 0xa1b23c4du || first == 0x4d3cb2a1u) {
        rd.skip(4);  // consume the peeked magic
        parse_pcap(rd, first, emit);
    } else {
        die("not a pcap or pcapng file (unrecognized magic)");
    }

    const std::uint64_t rows = sink.finish();
    std::printf("wrote %s (%llu packets, %llu row group%s, codec=%s)\n",
                opt.output, static_cast<unsigned long long>(rows),
                static_cast<unsigned long long>(sink.row_groups_),
                sink.row_groups_ == 1 ? "" : "s", opt.compress ? "zstd" : "none");
    return 0;
}
