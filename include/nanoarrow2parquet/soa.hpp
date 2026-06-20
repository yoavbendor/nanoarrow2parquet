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
// Scope (v0): REQUIRED fixed-width numeric columns (int8/16/32/64, uint8/16/32/64,
// float, double). Nullable columns, strings/binary, and nested structs are not
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

template <class T>
concept SupportedField = requires { arrow_traits<T>::format; };

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

template <fixed_string Name, class T>
struct Field {
    static constexpr auto name = Name;
    using type = T;
};

// ---- contiguous-column helpers -------------------------------------------

template <class R>
using column_value_t = std::remove_cvref_t<std::ranges::range_value_t<std::remove_cvref_t<R>>>;

// ---- streaming SoA writer -------------------------------------------------

template <class... Fields>
class Writer {
    static_assert(sizeof...(Fields) > 0, "schema must have at least one field");
    static_assert((SupportedField<typename Fields::type> && ...),
                  "every field type must be a supported fixed-width numeric type");

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

    // Write one row group from one contiguous range per field, in field order.
    // Column element types are checked against the schema at compile time; equal
    // row counts are checked at runtime (a row group must be rectangular).
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
    template <std::size_t... I, class... Cols>
    N2PStatus write_chunk_impl(std::index_sequence<I...>, const Cols&... cols) {
        if (!w_) return status_;

        const std::size_t sizes[K] = {std::ranges::size(cols)...};
        for (std::size_t i = 1; i < K; ++i) {
            if (sizes[i] != sizes[0]) {
                status_ = N2P_INVALID_ARGUMENT;
                return status_;
            }
        }
        const auto n = static_cast<std::int64_t>(sizes[0]);

        // Borrowed Arrow C views over the SoA storage (no copies, no nanoarrow).
        ArrowSchema cs[K]{};
        ArrowArray ca[K]{};
        const void* cb[K][2]{};
        ArrowSchema* csp[K]{};
        ArrowArray* cap[K]{};

        auto setup = [&]<std::size_t J, class Col>(std::integral_constant<std::size_t, J>,
                                                   const Col& col) {
            using F = field_at<J>;
            static_assert(std::same_as<column_value_t<Col>, typename F::type>,
                          "SoA column element type does not match its field type");
            cs[J].format = arrow_traits<typename F::type>::format;
            cs[J].name = F::name.c_str();
            cb[J][0] = nullptr;                                        // validity (all valid)
            cb[J][1] = static_cast<const void*>(std::ranges::data(col));
            ca[J].length = n;
            ca[J].n_buffers = 2;
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
