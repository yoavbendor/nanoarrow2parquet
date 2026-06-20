// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Yoav Bendor

#pragma once

// nanoarrow2parquet -- native struct-of-arrays (SoA) front-end.
//
// This is a *compile-time* entry point: you describe the schema as C++ types
// (Field<"name", T>...) and hand the writer one contiguous range per column.
// The schema is known to the compiler, so:
//   * unsupported field types and duplicate names are caught by static_assert,
//   * each column's element type is checked against its field at compile time,
//   * for fixed-width columns the Parquet data page is the column's storage --
//     no copy, and crucially no ArrowArray/ArrowSchema is built via nanoarrow.
//
// We bypass nanoarrow entirely: from the SoA spans we fill borrowed Arrow C Data
// Interface *views* (plain pointers into your storage) and feed them to the
// existing page/footer back-end through the C API. Each write_chunk() call emits
// exactly one row group, so batching policy stays with the caller (the producer
// knows its natural chunk size; the writer never buffers the whole dataset).
//
// Nullable columns: declare the field with Nullable<"name", T> and pass the
// column wrapped in present(values, mask) -- a per-element presence mask that is
// packed into an Arrow validity bitmap -- or valid_bits(values, bitmap, nulls)
// to alias a pre-packed LSB-first bitmap with no copy.
//
// Strings/binary: declare the field with the utf8 or binary tag and pass a range
// of string-like values. These are not zero-copy -- the offsets and data buffers
// are materialized per chunk -- but they need no Arrow structs from nanoarrow.
//
// Scope: REQUIRED or OPTIONAL fixed-width numeric columns (int8/16/32/64,
// uint8/16/32/64, float, double) and utf8/binary columns. Nested structs are not
// yet reachable from this path -- use the ArrowArray C API for those.

#include "nanoarrow2parquet/nanoarrow2parquet.h"

#include <array>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <ranges>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

// Arrow C Data Interface structs, defined under the standard guard so this header
// can coexist with nanoarrow.h (identical layout -> ABI-compatible). We only fill
// these as borrowed views; we never allocate or release them.
#ifndef ARROW_C_DATA_INTERFACE
#define ARROW_C_DATA_INTERFACE

#define ARROW_FLAG_DICTIONARY_ORDERED 1
#define ARROW_FLAG_NULLABLE 2
#define ARROW_FLAG_MAP_KEYS_SORTED 4

struct ArrowSchema {
    const char* format;
    const char* name;
    const char* metadata;
    int64_t flags;
    int64_t n_children;
    struct ArrowSchema** children;
    struct ArrowSchema* dictionary;
    void (*release)(struct ArrowSchema*);
    void* private_data;
};

struct ArrowArray {
    int64_t length;
    int64_t null_count;
    int64_t offset;
    int64_t n_buffers;
    int64_t n_children;
    const void** buffers;
    struct ArrowArray** children;
    struct ArrowArray* dictionary;
    void (*release)(struct ArrowArray*);
    void* private_data;
};

#endif  // ARROW_C_DATA_INTERFACE

namespace n2p::soa {

// ---- compile-time type mapping -------------------------------------------
//
// Each supported C++ element type maps to an Arrow C format string. The runtime
// writer already turns that format into the right Parquet physical/converted
// type, so choosing the format here is the whole of the static type mapping.

template <class T>
struct arrow_traits;  // undefined => unsupported type (compile error if used)

#define N2P_SOA_TRAIT(CppType, Fmt) \
    template <> struct arrow_traits<CppType> { static constexpr const char* format = Fmt; }
N2P_SOA_TRAIT(std::int8_t,   "c");
N2P_SOA_TRAIT(std::uint8_t,  "C");
N2P_SOA_TRAIT(std::int16_t,  "s");
N2P_SOA_TRAIT(std::uint16_t, "S");
N2P_SOA_TRAIT(std::int32_t,  "i");
N2P_SOA_TRAIT(std::uint32_t, "I");
N2P_SOA_TRAIT(std::int64_t,  "l");
N2P_SOA_TRAIT(std::uint64_t, "L");
N2P_SOA_TRAIT(float,         "f");
N2P_SOA_TRAIT(double,        "g");
#undef N2P_SOA_TRAIT

// Variable-length field tags. Unlike the numeric types these are not zero-copy:
// a SoA string column (range of string-like values) is materialized into Arrow
// offsets + data buffers per chunk.
struct utf8 {};    // -> BYTE_ARRAY annotated UTF8
struct binary {};  // -> BYTE_ARRAY, no annotation

template <class T>
inline constexpr bool is_string_field_v = std::is_same_v<T, utf8> || std::is_same_v<T, binary>;

template <class T>
consteval const char* string_format() {
    if constexpr (std::is_same_v<T, utf8>) return "u";
    else return "z";
}

template <class T>
concept SupportedField = (requires { arrow_traits<T>::format; }) || is_string_field_v<T>;

// ---- field / schema description ------------------------------------------

// A compile-time field name usable as a non-type template parameter.
template <std::size_t N>
struct fixed_string {
    char value[N]{};
    constexpr fixed_string(const char (&s)[N]) {
        for (std::size_t i = 0; i < N; ++i) value[i] = s[i];
    }
    constexpr const char* c_str() const { return value; }
    constexpr std::string_view view() const { return std::string_view(value, N - 1); }
};
template <std::size_t N>
fixed_string(const char (&)[N]) -> fixed_string<N>;

template <fixed_string Name, class T, bool Nullable = false>
struct Field {
    static constexpr auto name = Name;
    using type = T;
    static constexpr bool nullable = Nullable;
};

// Declares an OPTIONAL column; supply it via present(...) or valid_bits(...).
template <fixed_string Name, class T>
using Nullable = Field<Name, T, true>;

// ---- nullable column wrappers --------------------------------------------

// A values range paired with an Arrow validity bitmap. `owned_bitmap` holds a
// bitmap we packed (from a presence mask); when empty, `bitmap` aliases a caller
// buffer with no copy. A null `bitmap` means "all present".
template <class Values>
struct nullable_column {
    using soa_nullable_column = void;  // tag for is_nullable_col
    const Values& values;
    std::vector<std::uint8_t> owned_bitmap;
    const std::uint8_t* bitmap;
    std::int64_t null_count;

    const std::uint8_t* validity() const {
        return owned_bitmap.empty() ? bitmap : owned_bitmap.data();
    }
};

template <class T, class = void>
struct is_nullable_col : std::false_type {};
template <class T>
struct is_nullable_col<T, std::void_t<typename T::soa_nullable_column>> : std::true_type {};

// Wrap a column with a per-element presence mask (truthy element == present, not
// null). The mask is packed into an Arrow LSB-first validity bitmap.
template <class Values, class Mask>
nullable_column<Values> present(const Values& values, const Mask& mask) {
    const std::size_t n = std::ranges::size(values);
    std::vector<std::uint8_t> bits((n + 7) / 8, 0);
    std::int64_t nulls = 0;
    std::size_t i = 0;
    for (auto&& m : mask) {
        if (i >= n) break;
        if (static_cast<bool>(m)) bits[i >> 3] |= static_cast<std::uint8_t>(1u << (i & 7));
        else ++nulls;
        ++i;
    }
    return nullable_column<Values>{values, std::move(bits), nullptr, nulls};
}

// Wrap a column with a pre-packed LSB-first validity bitmap (1 == present). No
// copy: the bitmap is aliased. `bitmap == nullptr` means all present.
template <class Values>
nullable_column<Values> valid_bits(const Values& values, const std::uint8_t* bitmap,
                                   std::int64_t null_count) {
    return nullable_column<Values>{values, {}, bitmap, null_count};
}

// ---- contiguous-column helpers -------------------------------------------

template <class R>
using column_value_t = std::remove_cvref_t<std::ranges::range_value_t<std::remove_cvref_t<R>>>;

// ---- streaming SoA writer -------------------------------------------------

template <class... Fields>
class Writer {
    static_assert(sizeof...(Fields) > 0, "schema must have at least one field");
    static_assert((SupportedField<typename Fields::type> && ...),
                  "every field type must be a supported numeric type, utf8, or binary");

    static constexpr std::size_t K = sizeof...(Fields);
    template <std::size_t I>
    using field_at = std::tuple_element_t<I, std::tuple<Fields...>>;

    static constexpr bool names_unique() {
        std::array<std::string_view, K> ns{Fields::name.view()...};
        for (std::size_t i = 0; i < K; ++i)
            for (std::size_t j = 0; j < i; ++j)
                if (ns[i] == ns[j]) return false;
        return true;
    }
    static_assert(names_unique(), "duplicate field names in schema");

public:
    explicit Writer(const char* path) {
        status_ = static_cast<N2PStatus>(n2p_writer_open(&w_, path));
    }
    Writer(const Writer&) = delete;
    Writer& operator=(const Writer&) = delete;
    ~Writer() { if (w_) n2p_writer_close(w_); }

    bool ok() const { return status_ == N2P_OK; }
    N2PStatus status() const { return status_; }
    const char* last_error() const { return w_ ? n2p_writer_last_error(w_) : "writer not open"; }

    N2PStatus set_codec(N2PCodec codec) {
        return static_cast<N2PStatus>(n2p_writer_set_codec(w_, codec));
    }

    // Write one row group from one column per field, in field order. REQUIRED
    // fields take a plain contiguous range; Nullable fields take present(...) or
    // valid_bits(...). Element types are checked at compile time; equal row
    // counts are checked at runtime (a row group must be rectangular).
    template <class... Cols>
    N2PStatus write_chunk(const Cols&... cols) {
        static_assert(sizeof...(Cols) == K, "column count must match the schema");
        return write_chunk_impl(std::index_sequence_for<Fields...>{}, cols...);
    }

    N2PStatus close() {
        if (!w_) return status_;
        N2PStatus s = static_cast<N2PStatus>(n2p_writer_close(w_));
        w_ = nullptr;
        if (status_ == N2P_OK) status_ = s;
        return s;
    }

private:
    template <class Arg>
    static std::size_t row_count(const Arg& arg) {
        if constexpr (is_nullable_col<std::remove_cvref_t<Arg>>::value)
            return std::ranges::size(arg.values);
        else
            return std::ranges::size(arg);
    }

    template <std::size_t... I, class... Cols>
    N2PStatus write_chunk_impl(std::index_sequence<I...>, const Cols&... cols) {
        if (!w_) return status_;

        const std::size_t sizes[K] = {row_count(cols)...};
        for (std::size_t i = 1; i < K; ++i) {
            if (sizes[i] != sizes[0]) {
                status_ = N2P_INVALID_ARGUMENT;
                return status_;
            }
        }
        const auto n = static_cast<std::int64_t>(sizes[0]);

        // Borrowed Arrow C views over the SoA storage. Fixed-width columns alias
        // their storage (no copy); string columns materialize offsets + data into
        // the owned stores below, which outlive the write_batch call.
        ArrowSchema cs[K]{};
        ArrowArray ca[K]{};
        const void* cb[K][3]{};
        ArrowSchema* csp[K]{};
        ArrowArray* cap[K]{};
        std::array<std::vector<std::uint8_t>, K> off_store;   // string offsets (int32)
        std::array<std::vector<std::uint8_t>, K> data_store;  // string bytes

        auto setup = [&]<std::size_t J, class Col>(std::integral_constant<std::size_t, J>,
                                                   const Col& col) {
            using F = field_at<J>;
            using FT = typename F::type;
            constexpr bool arg_nullable = is_nullable_col<std::remove_cvref_t<Col>>::value;
            static_assert(F::nullable == arg_nullable,
                          "Nullable<> fields need present()/valid_bits(); "
                          "required fields take a plain range");

            const std::uint8_t* validity = nullptr;
            std::int64_t null_count = 0;
            auto get_values = [&]() -> const auto& {
                if constexpr (arg_nullable) return col.values;
                else return col;
            };
            const auto& values = get_values();
            if constexpr (arg_nullable) {
                validity = col.validity();
                null_count = col.null_count;
            }
            auto present_at = [&](std::size_t i) {
                return validity == nullptr || ((validity[i >> 3] >> (i & 7)) & 1);
            };

            cs[J].name = F::name.c_str();
            cs[J].flags = F::nullable ? ARROW_FLAG_NULLABLE : 0;
            cb[J][0] = validity;                                          // validity bitmap
            ca[J].length = n;
            ca[J].null_count = null_count;

            if constexpr (is_string_field_v<FT>) {
                static_assert(std::convertible_to<column_value_t<decltype(values)>, std::string_view>,
                              "string/binary column must be a range of string-like values");
                auto& off = off_store[J];
                auto& dat = data_store[J];
                off.resize(sizeof(std::int32_t) * (static_cast<std::size_t>(n) + 1));
                auto* o = reinterpret_cast<std::int32_t*>(off.data());
                std::int32_t cur = 0;
                o[0] = 0;
                std::size_t i = 0;
                for (auto&& e : values) {
                    if (present_at(i)) {
                        std::string_view sv(e);
                        dat.insert(dat.end(), sv.begin(), sv.end());
                        cur += static_cast<std::int32_t>(sv.size());
                    }
                    o[++i] = cur;
                }
                cs[J].format = string_format<FT>();
                cb[J][1] = off.data();                                    // offsets
                cb[J][2] = dat.data();                                    // data
                ca[J].n_buffers = 3;
            } else {
                static_assert(std::same_as<column_value_t<decltype(values)>, FT>,
                              "SoA column element type does not match its field type");
                cs[J].format = arrow_traits<FT>::format;
                cb[J][1] = static_cast<const void*>(std::ranges::data(values));
                ca[J].n_buffers = 2;
            }
            ca[J].buffers = cb[J];
            csp[J] = &cs[J];
            cap[J] = &ca[J];
        };
        (setup(std::integral_constant<std::size_t, I>{}, cols), ...);

        const void* root_buf[1] = {nullptr};
        ArrowSchema root_s{};
        root_s.format = "+s";
        root_s.name = "schema";
        root_s.n_children = static_cast<std::int64_t>(K);
        root_s.children = csp;
        ArrowArray root_a{};
        root_a.length = n;
        root_a.n_buffers = 1;
        root_a.buffers = root_buf;
        root_a.n_children = static_cast<std::int64_t>(K);
        root_a.children = cap;

        status_ = static_cast<N2PStatus>(n2p_writer_write_batch(w_, &root_s, &root_a));
        return status_;
    }

    N2PWriter* w_ = nullptr;
    N2PStatus status_ = N2P_OK;
};

// Convenience: open, write a single row group, close.
template <class... Fields, class... Cols>
N2PStatus write_file(const char* path, const Cols&... cols) {
    Writer<Fields...> w(path);
    if (!w.ok()) return w.status();
    if (N2PStatus s = w.write_chunk(cols...); s != N2P_OK) { w.close(); return s; }
    return w.close();
}

}  // namespace n2p::soa
