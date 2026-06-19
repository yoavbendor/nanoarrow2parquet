// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Yoav Bendor

#pragma once

// A tiny, write-only Thrift *compact protocol* encoder -- just enough to emit the
// Parquet FileMetaData and PageHeader structures. No Thrift library, no decode.
//
// Compact protocol essentials (see the Thrift spec):
//   * varint           : ULEB128, 7 bits/byte, low to high, 0x80 continuation bit.
//   * zigzag           : map signed -> unsigned so small magnitudes stay small.
//   * struct field hdr : if the field-id delta from the previous field is in
//                        1..15, one byte (delta << 4) | type; otherwise a type
//                        byte followed by the zigzag-varint i16 field id. Field
//                        ids within a struct MUST be written in ascending order.
//   * bool fields      : the value lives in the type nibble (TRUE=1 / FALSE=2).
//   * STOP             : a single 0x00 byte ends every struct.
//   * list header      : if size <= 14, one byte (size << 4) | elemType; else
//                        0xF0 | elemType followed by the varint size.

#include <cstdint>
#include <span>
#include <string_view>
#include <vector>

namespace n2p {

// Compact-protocol type ids (the wire nibble), distinct from Parquet's own enums.
enum class CType : std::uint8_t {
    BoolTrue = 1,
    BoolFalse = 2,
    Byte = 3,
    I16 = 4,
    I32 = 5,
    I64 = 6,
    Double = 7,
    Binary = 8,  // also used for string
    List = 9,
    Set = 10,
    Map = 11,
    Struct = 12,
};

class CompactWriter {
public:
    explicit CompactWriter(std::vector<std::uint8_t>& out) : out_(out) {}

    std::size_t size() const { return out_.size(); }

    // ---- low-level value writers (no field header) ----

    void put_byte(std::uint8_t b) { out_.push_back(b); }

    void put_varint(std::uint64_t v) {
        while (v >= 0x80) {
            out_.push_back(static_cast<std::uint8_t>(v) | 0x80);
            v >>= 7;
        }
        out_.push_back(static_cast<std::uint8_t>(v));
    }

    void put_zigzag_i32(std::int32_t v) {
        put_varint(static_cast<std::uint32_t>((v << 1) ^ (v >> 31)));
    }
    void put_zigzag_i64(std::int64_t v) {
        put_varint(static_cast<std::uint64_t>((v << 1) ^ (v >> 63)));
    }

    void put_binary(std::span<const std::uint8_t> bytes) {
        put_varint(bytes.size());
        out_.insert(out_.end(), bytes.begin(), bytes.end());
    }
    void put_string(std::string_view s) {
        put_varint(s.size());
        out_.insert(out_.end(), s.begin(), s.end());
    }

    // ---- field headers / typed fields ----

    void field_header(CType type, std::int16_t id) {
        const int delta = id - last_id_;
        if (delta > 0 && delta <= 15) {
            out_.push_back(static_cast<std::uint8_t>((delta << 4) |
                                                     static_cast<std::uint8_t>(type)));
        } else {
            out_.push_back(static_cast<std::uint8_t>(type));
            put_zigzag_i32(id);  // i16 zigzag fits in the i32 path
        }
        last_id_ = id;
    }

    void field_bool(std::int16_t id, bool value) {
        field_header(value ? CType::BoolTrue : CType::BoolFalse, id);
    }
    void field_i32(std::int16_t id, std::int32_t v) {
        field_header(CType::I32, id);
        put_zigzag_i32(v);
    }
    void field_i64(std::int16_t id, std::int64_t v) {
        field_header(CType::I64, id);
        put_zigzag_i64(v);
    }
    void field_string(std::int16_t id, std::string_view s) {
        field_header(CType::Binary, id);
        put_string(s);
    }
    void field_binary(std::int16_t id, std::span<const std::uint8_t> bytes) {
        field_header(CType::Binary, id);
        put_binary(bytes);
    }

    // List field: writes the field + list header. Caller then writes `size`
    // element values (for struct elements use begin_struct_element/end_struct).
    void field_list_header(std::int16_t id, CType elem, std::size_t size) {
        field_header(CType::List, id);
        list_header(elem, size);
    }

    void list_header(CType elem, std::size_t size) {
        if (size <= 14) {
            out_.push_back(static_cast<std::uint8_t>((size << 4) |
                                                     static_cast<std::uint8_t>(elem)));
        } else {
            out_.push_back(static_cast<std::uint8_t>(0xF0) |
                           static_cast<std::uint8_t>(elem));
            put_varint(size);
        }
    }

    // Begin a struct as a *field* of the current struct.
    void begin_struct_field(std::int16_t id) {
        field_header(CType::Struct, id);
        push_struct();
    }
    // Begin a struct that is an *element of a list* (no preceding field header).
    void begin_struct_element() { push_struct(); }

    // End the current struct: STOP byte, restore parent's last-id.
    void end_struct() {
        out_.push_back(0x00);
        last_id_ = id_stack_.back();
        id_stack_.pop_back();
    }

private:
    void push_struct() {
        id_stack_.push_back(last_id_);
        last_id_ = 0;
    }

    std::vector<std::uint8_t>& out_;
    std::vector<std::int16_t> id_stack_;
    std::int16_t last_id_ = 0;
};

}  // namespace n2p
