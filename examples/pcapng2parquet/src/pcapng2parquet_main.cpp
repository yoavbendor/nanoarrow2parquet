// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Yoav Bendor

// pcapng / pcap -> Parquet converter. The Parquet-output counterpart to nanolance's pcapng2lance: it runs
// the SAME nanotins parsing stack (the pcap/pcapng block scanner + the wire_spec / spec_dag L2..L4 + PTP
// decoder) and writes one table per layer; only the OUTPUT ENDPOINT differs -- nanoarrow2parquet instead
// of nanolance. Both consume the identical soatins-built ArrowSchema/ArrowArray, so the per-PDU tables are
// byte-for-byte the same columns; the file format is the only difference.
//
// Pipeline (top to bottom):
//   parse args -> stream the capture in bounded windows -> scan blocks (SHB/IDB/EPB/SPB or classic pcap)
//   -> per packet: parse the L1 EPB into a scalar PacketRow (write a packets row group per window) and,
//   with --decode-l2l3, walk the spec/DAG to scatter each emitted PDU (Ethernet / VLAN / IPv4 / IPv6 /
//   TCP / UDP / PTP / IPv6 ext headers / SOME/IP) into that node's columns -> at EOF write one Parquet
//   file per non-empty PDU table.
//
// Differences from pcapng2lance (intentional, because Parquet has no external blob store):
//   * no payload_ref / remainder_after_l4 tables -- packet payloads are not stored (parse-only);
//   * no --stage staged enrichment (it is built on external-blob refetch);
//   * decode runs serially (no stdexec bulk path) -- serial == bulk output, so the tables are identical.

#include "packet_row.hpp"
#include "parquet_pdu_table_writer.hpp"
#include "streaming_reader.hpp"

#include "soatins/arrow_glue.hpp"  // soatins::soa, arrow_schema, to_arrow

#include "nanotins/dag_decode.hpp"   // dag_tables, dag_decode_packet
#include "nanotins/pcap_blocks.hpp"  // pcapblocks scan + parse
#include "nanotins/spec_dag.hpp"     // L2L3Graph + node ids

#include "lldp.hpp"  // LLDP via DPAR (the user-written palette from nanotins's lldp example)

#include "nanoarrow2parquet/nanoarrow2parquet.h"

#include <nanoarrow/nanoarrow.h>

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

namespace fs = std::filesystem;

namespace {

using pcapng2parquet::PacketRow;

// LINKTYPE_ETHERNET; the DAG root is Ethernet, so non-Ethernet packets emit no PDU rows (only L1).
constexpr std::uint32_t kLinkTypeEthernet = 1;

int fail(const std::string& msg) {
    std::fprintf(stderr, "pcapng2parquet: %s\n", msg.c_str());
    return 1;
}

// The two standard LLDP DPAR rules (from nanotins's lldp example): LLDP rides directly on Ethernet
// (untagged, EtherType 0x88CC) or inside a single VLAN tag. Both feed the one "lldp" table.
constexpr const char* kLldpRules =
    "eth.ethertype == 0x88CC        => lldp eth_payload  \"lldp\"\n"
    "vlan.inner_ethertype == 0x88CC => lldp vlan_payload \"lldp\"\n";

// ---- command line --------------------------------------------------------------------------------

struct Args {
    bool compress = true;
    bool decode_l2l3 = false;
    bool lldp = false;  // also run the LLDP DPAR parser -> <stem>_lldp.parquet
    std::size_t window_bytes = std::size_t{512} * 1024 * 1024;  // RAM budget per window
    std::uint64_t drop = 0;          // -d: skip the first N packets (their packet_id is preserved)
    std::uint64_t take = UINT64_MAX;  // -c: emit at most N packets after the drop (default: all)
    std::vector<std::string> pos;
};

bool parse_args(int argc, char** argv, Args& a, std::string& err) {
    const auto value = [&](int& i) -> const char* {
        if (i + 1 >= argc) {
            err = std::string(argv[i]) + " requires a value";
            return nullptr;
        }
        return argv[++i];
    };
    for (int i = 1; i < argc; ++i) {
        const std::string s = argv[i];
        if (s == "--no-compress") {
            a.compress = false;
        } else if (s == "--decode-l2l3") {
            a.decode_l2l3 = true;
        } else if (s == "--lldp") {
            a.lldp = true;
        } else if (s == "--window-bytes") {
            const char* v = value(i);
            if (!v) return false;
            a.window_bytes = static_cast<std::size_t>(std::stoull(v));
            if (a.window_bytes == 0) return (err = "--window-bytes must be > 0", false);
        } else if (s == "-d" || s == "--drop") {
            const char* v = value(i);
            if (!v) return false;
            a.drop = std::stoull(v);
        } else if (s == "-c" || s == "--count") {
            const char* v = value(i);
            if (!v) return false;
            a.take = std::stoull(v);
        } else {
            a.pos.push_back(s);
        }
    }
    return true;
}

// ---- the converter -------------------------------------------------------------------------------

// Streams a capture into Parquet: a `<stem>_packets.parquet` L1 table (one row group per window) plus, with
// --decode-l2l3, one `<stem>_<pdu>.parquet` per PDU type (accumulated across windows, one row group each).
class Converter {
public:
    Converter(Args args, fs::path stem) : args_(std::move(args)), stem_(std::move(stem)) {}

    int run(const fs::path& input) {
        streaming::FileSource source(input);
        if (!source.ok()) return fail("cannot open input file: " + input.string());
        streaming::Window<streaming::FileSource> win(source, args_.window_bytes);

        std::string err;
        if (!soatins::arrow_schema<PacketRow>(pkt_schema_, err)) return fail("packets schema: " + err);
        if (n2p_writer_open(&pkt_writer_, (stem_.string() + "_packets.parquet").c_str()) != N2P_OK) {
            ArrowSchemaRelease(&pkt_schema_);
            return fail("open packets writer");
        }
        n2p_writer_set_codec(pkt_writer_, args_.compress ? N2P_CODEC_ZSTD : N2P_CODEC_UNCOMPRESSED);

        if (args_.lldp) {
            lldp_engine_ = std::make_unique<lldp_example::Engine>();
            const auto compiled = lldp_engine_->load_rules(kLldpRules);
            if (!compiled.ok) {
                n2p_writer_close(pkt_writer_);
                ArrowSchemaRelease(&pkt_schema_);
                std::string msg = "LLDP rule compile failed:";
                for (const auto& e : compiled.errors) msg += " " + e;
                return fail(msg);
            }
        }

        win.fill();
        while (win.size() > 0) {
            std::vector<pcapblocks::BlockRef> refs;
            std::size_t consumed = 0;
            if (!pcapblocks::scan_window(st_, win.bytes(), refs, consumed, win.eof(), err)) {
                n2p_writer_close(pkt_writer_);
                ArrowSchemaRelease(&pkt_schema_);
                return fail("scan: " + err);
            }
            if (consumed == 0) {
                if (win.eof()) break;        // no complete block remains
                if (win.full()) win.grow();  // a single block larger than the window
                win.fill();
                continue;
            }
            if (const int rc = process_window(win.bytes(), refs)) {
                n2p_writer_close(pkt_writer_);
                ArrowSchemaRelease(&pkt_schema_);
                return rc;
            }
            win.consume(consumed);
            if (args_.take != UINT64_MAX && global_pid_ >= args_.drop + args_.take) break;
            win.fill();
        }

        if (n2p_writer_close(pkt_writer_) != N2P_OK) return fail("close packets writer");
        pkt_writer_ = nullptr;
        ArrowSchemaRelease(&pkt_schema_);

        if (args_.decode_l2l3 && emitted_ > 0) {
            if (const int rc = write_pdu_tables()) return rc;
        }
        if (lldp_engine_) {
            if (const int rc = write_lldp_table()) return rc;
        }
        print_summary();
        return 0;
    }

private:
    // Route one block: SHB opens a section, IDB extends the current section's interface table, EPB/record
    // is a packet (remembered with its section). Mirrors pcapng2lance's classify_block, minus KV metadata.
    void classify_block(const pcapblocks::Bytes& wbytes, const pcapblocks::BlockRef& r,
                        std::vector<pcapblocks::BlockRef>& packets, std::vector<std::size_t>& packet_section) {
        switch (r.kind) {
            case pcapblocks::Kind::Shb:
                sections_.emplace_back();
                cur_section_ = static_cast<std::ptrdiff_t>(sections_.size()) - 1;
                break;
            case pcapblocks::Kind::Idb: {
                if (cur_section_ < 0) {
                    sections_.emplace_back();
                    cur_section_ = 0;
                }
                pcapblocks::IdbView idb{};
                if (pcapblocks::parse_idb(wbytes, r, idb)) {
                    sections_[static_cast<std::size_t>(cur_section_)].push_back(idb);
                }
                break;
            }
            case pcapblocks::Kind::Epb:
            case pcapblocks::Kind::PcapRecord:
            case pcapblocks::Kind::SimplePacket:
                if (cur_section_ < 0) {
                    sections_.emplace_back();
                    cur_section_ = 0;
                }
                packets.push_back(r);
                packet_section.push_back(static_cast<std::size_t>(cur_section_));
                break;
            default:
                ++other_count_;
                break;
        }
    }

    // One window: classify its blocks, then for each kept packet build its L1 row (+ optional DAG decode).
    int process_window(const pcapblocks::Bytes& wbytes, const std::vector<pcapblocks::BlockRef>& refs) {
        std::vector<pcapblocks::BlockRef> packets;
        std::vector<std::size_t> packet_section;
        for (const auto& r : refs) {
            classify_block(wbytes, r, packets, packet_section);
        }
        const std::size_t n_total = packets.size();
        if (n_total == 0) return 0;

        // --drop/--count slicing over the GLOBAL packet index, so a kept packet keeps the packet_id it
        // would have in a full run (slices stitch into a bit-exact replica).
        const std::uint64_t g0 = global_pid_;
        global_pid_ += n_total;
        std::size_t lo = 0, hi = n_total;
        if (args_.drop > g0) lo = static_cast<std::size_t>(std::min<std::uint64_t>(args_.drop - g0, n_total));
        if (args_.take != UINT64_MAX) {
            const std::uint64_t end = args_.drop + args_.take;
            hi = end > g0 ? static_cast<std::size_t>(std::min<std::uint64_t>(end - g0, n_total)) : 0;
        }
        if (hi < lo) hi = lo;
        const std::size_t n = hi - lo;
        if (n == 0) return 0;
        emitted_ += n;

        soatins::soa<PacketRow> rows;
        rows.resize(n);
        for (std::size_t k = 0; k < n; ++k) {
            const std::size_t idx = lo + k;
            const pcapblocks::BlockRef& ref = packets[idx];
            pcapblocks::EpbView e{};
            pcapblocks::parse_epb(wbytes, ref, e);  // leaves e default-zeroed on parse failure

            std::uint16_t link = 0;
            std::uint8_t res = 6;  // default if_tsresol: microseconds
            if (ref.kind == pcapblocks::Kind::Epb || ref.kind == pcapblocks::Kind::SimplePacket) {
                const auto& table = sections_[packet_section[idx]];
                if (e.interface_id < table.size()) {
                    link = table[e.interface_id].link_type;
                    res = table[e.interface_id].ts_resol;
                }
            } else {  // classic pcap record: link type rides on the BlockRef, no interface table
                link = static_cast<std::uint16_t>(ref.type_or_link);
            }

            const std::uint64_t pid = g0 + idx;
            rows.store(k, PacketRow{pid, e.interface_id, e.ts_raw, e.caplen, e.origlen, link, res, e.epb_flags});

            if (link == kLinkTypeEthernet) {
                const std::size_t poff = static_cast<std::size_t>(e.payload_file_offset);
                if (poff + e.caplen <= wbytes.size()) {
                    const std::uint8_t* pkt = wbytes.data() + poff;
                    if (args_.decode_l2l3) {
                        nanotins::dag_decode_packet<nanotins::L2L3Graph>(pid, pkt, e.caplen, dag_,
                                                                         nanotins::kEthRoot);
                    }
                    if (lldp_engine_) {
                        lldp_engine_->run(pkt, e.caplen, pid);  // DPAR: matches eth/vlan ethertype 0x88CC
                    }
                }
            }
        }

        // L1 packets table: one row group per window.
        ArrowArray batch{};
        std::string err;
        if (!soatins::to_arrow(rows, batch, err)) return fail("packets to_arrow: " + err);
        const int wrote = n2p_writer_write_batch(pkt_writer_, &pkt_schema_, &batch);
        batch.release(&batch);
        if (wrote != N2P_OK) return fail(std::string("packets write_batch: ") + n2p_writer_last_error(pkt_writer_));
        ++pkt_row_groups_;
        return 0;
    }

    // At EOF: write one Parquet file per non-empty PDU table (accumulated across all windows). The output
    // is the spec/DAG's columns -- byte-identical to pcapng2lance's per-PDU tables, in Parquet.
    int write_pdu_tables() {
        using G = nanotins::L2L3Graph;
        const std::string stem = stem_.string();
        std::string err;
        bool ok = true;
        const auto write_one = [&](const char* suffix, auto Node) {
            using N = decltype(Node);
            const auto& table = std::get<nanotins::node_id_v<N, G>>(dag_);
            const fs::path p = stem + suffix;
            if (!pq_pdu_io::write_dag_pdu_table(p, table, args_.compress, err)) {
                std::fprintf(stderr, "pcapng2parquet: failed to write %s: %s\n", p.string().c_str(), err.c_str());
                ok = false;
            }
        };
        write_one("_ethernet.parquet", nanotins::EthNode{});
        write_one("_vlan.parquet", nanotins::VlanNode{});
        write_one("_ipv4.parquet", nanotins::Ipv4Node{});
        write_one("_ipv6.parquet", nanotins::Ipv6Node{});
        write_one("_tcp.parquet", nanotins::TcpNode{});
        write_one("_udp.parquet", nanotins::UdpNode{});
        // gPTP common header + per-message-type bodies (the GptpNode message_type sub-dispatch).
        write_one("_ptp.parquet", nanotins::GptpNode{});
        write_one("_ptp_timestamp.parquet", nanotins::PtpTimestampBody{});
        write_one("_ptp_ts_port.parquet", nanotins::PtpTsPortBody{});
        write_one("_ptp_announce.parquet", nanotins::PtpAnnounceBody{});
        write_one("_ptp_signaling.parquet", nanotins::PtpSignalingBody{});
        // IPv6 extension-header node tables (fixed fields; their variable parts -- SRv6 segments / options --
        // need the stdexec children-bulk pass and are out of scope here).
        write_one("_ipv6_hopbyhop.parquet", nanotins::Ipv6HopByHopNode{});
        write_one("_ipv6_routing.parquet", nanotins::Ipv6RoutingNode{});
        write_one("_ipv6_fragment.parquet", nanotins::Ipv6FragmentNode{});
        write_one("_ipv6_destopt.parquet", nanotins::Ipv6DestOptNode{});
        write_one("_ipv6_ah.parquet", nanotins::Ipv6AhNode{});
        write_one("_someip.parquet", nanotins::SomeipNode{});
        if (!ok) return 1;

        std::fprintf(stderr,
                     "pcapng2parquet: decoded -> eth %zu, vlan %zu, ipv4 %zu, ipv6 %zu, tcp %zu, udp %zu, ptp %zu\n",
                     std::get<nanotins::node_id_v<nanotins::EthNode, G>>(dag_).size(),
                     std::get<nanotins::node_id_v<nanotins::VlanNode, G>>(dag_).size(),
                     std::get<nanotins::node_id_v<nanotins::Ipv4Node, G>>(dag_).size(),
                     std::get<nanotins::node_id_v<nanotins::Ipv6Node, G>>(dag_).size(),
                     std::get<nanotins::node_id_v<nanotins::TcpNode, G>>(dag_).size(),
                     std::get<nanotins::node_id_v<nanotins::UdpNode, G>>(dag_).size(),
                     std::get<nanotins::node_id_v<nanotins::GptpNode, G>>(dag_).size());
        return 0;
    }

    // The LLDP table (one row per TLV), produced by the DPAR engine, written via the same
    // soatins-reflection -> Arrow -> Parquet path as the L1 packets table. value_head is fixed_size_binary.
    int write_lldp_table() {
        const auto& rows = lldp_engine_->table();
        if (rows.empty()) {
            std::fprintf(stderr, "pcapng2parquet: LLDP -> 0 TLV rows (no LLDP frames matched)\n");
            return 0;
        }
        std::string err;
        ArrowSchema schema{};
        if (!soatins::arrow_schema<lldp_example::LldpTlvRow>(schema, err)) return fail("lldp schema: " + err);
        soatins::soa<lldp_example::LldpTlvRow> soa;
        soa.resize(rows.size());
        for (std::size_t i = 0; i < rows.size(); ++i) soa.store(i, rows[i]);
        ArrowArray batch{};
        if (!soatins::to_arrow(soa, batch, err)) {
            ArrowSchemaRelease(&schema);
            return fail("lldp to_arrow: " + err);
        }
        N2PWriter* w = nullptr;
        if (n2p_writer_open(&w, (stem_.string() + "_lldp.parquet").c_str()) != N2P_OK) {
            batch.release(&batch);
            ArrowSchemaRelease(&schema);
            return fail("open lldp writer");
        }
        n2p_writer_set_codec(w, args_.compress ? N2P_CODEC_ZSTD : N2P_CODEC_UNCOMPRESSED);
        const int wrote = n2p_writer_write_batch(w, &schema, &batch);
        batch.release(&batch);
        n2p_writer_close(w);
        ArrowSchemaRelease(&schema);
        if (wrote != N2P_OK) return fail("lldp write_batch");
        std::fprintf(stderr, "pcapng2parquet: LLDP -> %zu TLV rows\n", rows.size());
        return 0;
    }

    void print_summary() const {
        if (emitted_ != global_pid_) {
            std::fprintf(stderr, "pcapng2parquet: emitted %llu of %llu packets (--drop %llu --count %s)\n",
                         static_cast<unsigned long long>(emitted_), static_cast<unsigned long long>(global_pid_),
                         static_cast<unsigned long long>(args_.drop),
                         args_.take == UINT64_MAX ? "all" : std::to_string(args_.take).c_str());
        }
        std::fprintf(stderr, "pcapng2parquet: %llu packets, %zu section(s), %zu skipped block(s) -> %s_packets.parquet\n",
                     static_cast<unsigned long long>(emitted_), sections_.size(), other_count_, stem_.string().c_str());
    }

    Args args_;
    fs::path stem_;

    ArrowSchema pkt_schema_{};
    N2PWriter* pkt_writer_ = nullptr;
    std::size_t pkt_row_groups_ = 0;

    pcapblocks::ScanState st_{};
    std::vector<std::vector<pcapblocks::IdbView>> sections_;  // per-section interface tables
    std::ptrdiff_t cur_section_ = -1;
    std::uint64_t global_pid_ = 0;  // packets SEEN (global index; spans --drop so packet_id stays global)
    std::uint64_t emitted_ = 0;     // packets actually written
    std::size_t other_count_ = 0;
    nanotins::dag_tables<nanotins::L2L3Graph> dag_;  // accumulated across windows when --decode-l2l3
    std::unique_ptr<lldp_example::Engine> lldp_engine_;  // non-null only with --lldp; accumulates TLV rows
};

}  // namespace

int main(int argc, char** argv) {
    Args args;
    std::string err;
    if (!parse_args(argc, argv, args, err)) return fail(err);

    if (args.pos.size() < 2) {
        std::fprintf(stderr,
                     "usage: %s [--no-compress] [--decode-l2l3] [--lldp] [--window-bytes N]\n"
                     "          [-d|--drop N] [-c|--count N] <input.pcap|pcapng> <output-stem>\n"
                     "  writes <output-stem>_packets.parquet (L1) and, with --decode-l2l3,\n"
                     "  <output-stem>_<pdu>.parquet per PDU type (ethernet/vlan/ipv4/ipv6/tcp/udp/ptp/...);\n"
                     "  with --lldp, also <output-stem>_lldp.parquet (one row per LLDP TLV)\n",
                     argv[0]);
        return 2;
    }

    const fs::path input = args.pos[0];
    // The output argument is a STEM: tables are <stem>_packets.parquet, <stem>_ipv4.parquet, ... Strip a
    // trailing .parquet/.lance if the user passed one, so `out.parquet` and `out` behave the same.
    fs::path stem = args.pos[1];
    if (stem.extension() == ".parquet" || stem.extension() == ".lance") {
        stem = stem.parent_path() / stem.stem();
    }
    return Converter(std::move(args), std::move(stem)).run(input);
}
