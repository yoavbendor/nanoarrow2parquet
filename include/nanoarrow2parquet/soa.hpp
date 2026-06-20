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
// Bool: declare the field bool and pass a range of bool. Also not zero-copy --
// byte-per-bool storage is packed into Arrow's 1-bit-per-value bitmap per chunk.
//
// Nested structs: declare a Struct<"name", Children...> (or NullableStruct) field
// and supply it with group(child_args...) -- or group_present(mask, ...) /
// group_valid(bitmap, nulls, ...) for the OPTIONAL variant. Groups nest to any
// depth; each leaf still aliases or materializes exactly as it would at top level.
//
// Scope: REQUIRED or OPTIONAL fixed-width numeric columns (int8/16/32/64,
// uint8/16/32/64, float, double), bool, utf8/binary (incl. large_*), and nested
// struct groups.

#include "nanoarrow2parquet/nanoarrow2parquet.h"

#include <array>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <deque>
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
// offsets + data buffers per chunk. The large_* variants use 64-bit offsets.
struct utf8 {};          // -> BYTE_ARRAY annotated UTF8, 32-bit offsets
struct binary {};        // -> BYTE_ARRAY, 32-bit offsets
struct large_utf8 {};    // -> BYTE_ARRAY annotated UTF8, 64-bit offsets
struct large_binary {};  // -> BYTE_ARRAY, 64-bit offsets

template <class T>
inline constexpr bool is_large_string_v =
    std::is_same_v<T, large_utf8> || std::is_same_v<T, large_binary>;

template <class T>
inline constexpr bool is_string_field_v =
    std::is_same_v<T, utf8> || std::is_same_v<T, binary> || is_large_string_v<T>;

template <class T>
consteval const char* string_format() {
    if constexpr (std::is_same_v<T, utf8>) return "u";
    else if constexpr (std::is_same_v<T, binary>) return "z";
    else if constexpr (std::is_same_v<T, large_utf8>) return "U";
    else return "Z";
}

template <class T>
concept SupportedField = (requires { arrow_traits<T>::format; })
                      || is_string_field_v<T> || std::is_same_v<T, bool>;

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

// A nested struct group. Children are Field/Nullable/Struct/NullableStruct. At
// write time supply it with group(child_args...); the OPTIONAL variant takes
// group_present(mask, ...) or group_valid(bitmap, nulls, ...).
template <fixed_string Name, class... Children>
struct Struct {
    static constexpr auto name = Name;
    static constexpr bool nullable = false;
    static constexpr bool is_group = true;
    using children = std::tuple<Children...>;
};
template <fixed_string Name, class... Children>
struct NullableStruct {
    static constexpr auto name = Name;
    static constexpr bool nullable = true;
    static constexpr bool is_group = true;
    using children = std::tuple<Children...>;
};

template <class F, class = void>
struct is_group_field_t : std::false_type {};
template <class F>
struct is_group_field_t<F, std::void_t<typename F::children>> : std::true_type {};
template <class F>
inline constexpr bool is_group_field = is_group_field_t<F>::value;

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

// ---- struct group wrappers -----------------------------------------------

// Child arguments of a struct, held by reference (they are temporaries created
// within the single write_chunk(...) full-expression, so they outlive the call).
// HasValidity records whether a validity bitmap was supplied, checked against the
// field's nullability at compile time.
template <bool HasValidity, class... ChildArgs>
struct group_column {
    using soa_group_column = void;
    static constexpr bool has_validity = HasValidity;
    std::tuple<const ChildArgs&...> children;
    const std::uint8_t* bitmap = nullptr;
    std::vector<std::uint8_t> owned_bitmap;
    std::int64_t null_count = 0;
    std::size_t rows = static_cast<std::size_t>(-1);

    const std::uint8_t* validity() const {
        return owned_bitmap.empty() ? bitmap : owned_bitmap.data();
    }
};

template <class T, class = void>
struct is_group_col_t : std::false_type {};
template <class T>
struct is_group_col_t<T, std::void_t<typename T::soa_group_column>> : std::true_type {};
template <class T>
inline constexpr bool is_group_col = is_group_col_t<std::remove_cvref_t<T>>::value;

// REQUIRED struct: group its children's columns.
template <class... ChildArgs>
group_column<false, ChildArgs...> group(const ChildArgs&... cs) {
    return group_column<false, ChildArgs...>{std::tie(cs...)};
}

// OPTIONAL struct via a per-element presence mask (packed to a validity bitmap).
template <class Mask, class... ChildArgs>
group_column<true, ChildArgs...> group_present(const Mask& mask, const ChildArgs&... cs) {
    const std::size_t n = std::ranges::size(mask);
    std::vector<std::uint8_t> bits((n + 7) / 8, 0);
    std::int64_t nulls = 0;
    std::size_t i = 0;
    for (auto&& m : mask) {
        if (static_cast<bool>(m)) bits[i >> 3] |= static_cast<std::uint8_t>(1u << (i & 7));
        else ++nulls;
        ++i;
    }
    return group_column<true, ChildArgs...>{std::tie(cs...), nullptr, std::move(bits), nulls, n};
}

// OPTIONAL struct aliasing a pre-packed LSB-first validity bitmap (no copy).
template <class... ChildArgs>
group_column<true, ChildArgs...> group_valid(const std::uint8_t* bitmap,
                                             std::int64_t null_count, const ChildArgs&... cs) {
    return group_column<true, ChildArgs...>{std::tie(cs...), bitmap, {}, null_count};
}

// ---- contiguous-column helpers -------------------------------------------

template <class R>
using column_value_t = std::remove_cvref_t<std::ranges::range_value_t<std::remove_cvref_t<R>>>;

// Recursively validate a field: a group's children must all be valid (and it must
// be non-empty); a leaf must be a SupportedField.
template <class F>
consteval bool field_ok();
template <class Tuple, std::size_t... J>
consteval bool children_ok(std::index_sequence<J...>) {
    return (field_ok<std::tuple_element_t<J, Tuple>>() && ...);
}
template <class F>
consteval bool field_ok() {
    if constexpr (is_group_field<F>)
        return std::tuple_size_v<typename F::children> > 0 &&
               children_ok<typename F::children>(
                   std::make_index_sequence<std::tuple_size_v<typename F::children>>{});
    else
        return SupportedField<typename F::type>;
}

// ---- streaming SoA writer -------------------------------------------------

template <class... Fields>
class Writer {
    static_assert(sizeof...(Fields) > 0, "schema must have at least one field");
    static_assert((field_ok<Fields>() && ...),
                  "every leaf field must be a supported numeric type, bool, utf8, "
                  "or binary; struct groups must be non-empty");

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
    // Pointer-stable storage for the borrowed Arrow tree built per chunk. deque
    // never invalidates element pointers on push_back, so nodes and child-pointer
    // arrays stay valid for the duration of the write_batch call.
    struct Pools {
        std::deque<ArrowSchema> schemas;
        std::deque<ArrowArray> arrays;
        std::deque<std::array<const void*, 3>> bufs;
        std::deque<std::vector<ArrowSchema*>> schildren;
        std::deque<std::vector<ArrowArray*>> achildren;
        std::deque<std::vector<std::uint8_t>> bytes;  // string offsets/data, packed bools
    };

    // Row count of a field's first leaf, used to fix the row-group length.
    template <class F, class Arg>
    static std::size_t first_leaf_size(const Arg& arg) {
        if constexpr (is_group_field<F>) {
            using Ch = typename F::children;
            return first_leaf_size<std::tuple_element_t<0, Ch>>(std::get<0>(arg.children));
        } else if constexpr (is_nullable_col<std::remove_cvref_t<Arg>>::value) {
            return std::ranges::size(arg.values);
        } else {
            return std::ranges::size(arg);
        }
    }

    // Build one Arrow schema/array node (recursing into struct groups) from field
    // F and its argument. `ok` is cleared on any runtime shape mismatch.
    template <class F, class Arg>
    static std::pair<ArrowSchema*, ArrowArray*> build_node(const Arg& arg, std::int64_t n,
                                                           Pools& p, bool& ok) {
        ArrowSchema& s = p.schemas.emplace_back();
        ArrowArray& a = p.arrays.emplace_back();
        std::array<const void*, 3>& vb = p.bufs.emplace_back();
        s.name = F::name.c_str();
        s.flags = F::nullable ? ARROW_FLAG_NULLABLE : 0;
        a.length = n;

        if constexpr (is_group_field<F>) {
            static_assert(is_group_col<Arg>,
                          "struct field needs group()/group_present()/group_valid()");
            static_assert(F::nullable == std::remove_cvref_t<Arg>::has_validity,
                          "NullableStruct needs group_present()/group_valid(); "
                          "Struct needs group()");
            if constexpr (F::nullable) {
                vb[0] = arg.validity();
                a.null_count = arg.null_count;
                if (arg.rows != static_cast<std::size_t>(-1) && arg.rows != static_cast<std::size_t>(n))
                    ok = false;
            }
            using Ch = typename F::children;
            constexpr std::size_t Nc = std::tuple_size_v<Ch>;
            std::vector<ArrowSchema*>& sc = p.schildren.emplace_back();
            std::vector<ArrowArray*>& ac = p.achildren.emplace_back();
            [&]<std::size_t... J>(std::index_sequence<J...>) {
                ([&] {
                    auto pr = build_node<std::tuple_element_t<J, Ch>>(
                        std::get<J>(arg.children), n, p, ok);
                    sc.push_back(pr.first);
                    ac.push_back(pr.second);
                }(), ...);
            }(std::make_index_sequence<Nc>{});
            s.format = "+s";
            s.n_children = static_cast<std::int64_t>(Nc);
            s.children = sc.data();
            a.n_buffers = 1;
            a.n_children = static_cast<std::int64_t>(Nc);
            a.children = ac.data();
            a.buffers = vb.data();
            return {&s, &a};
        } else {
            using FT = typename F::type;
            constexpr bool arg_nullable = is_nullable_col<std::remove_cvref_t<Arg>>::value;
            static_assert(F::nullable == arg_nullable,
                          "Nullable<> fields need present()/valid_bits(); "
                          "required fields take a plain range");
            const std::uint8_t* validity = nullptr;
            std::int64_t null_count = 0;
            const auto& values = [&]() -> const auto& {
                if constexpr (arg_nullable) return arg.values;
                else return arg;
            }();
            if constexpr (arg_nullable) {
                validity = arg.validity();
                null_count = arg.null_count;
            }
            if (std::ranges::size(values) != static_cast<std::size_t>(n)) ok = false;
            auto present_at = [&](std::size_t i) {
                return validity == nullptr || ((validity[i >> 3] >> (i & 7)) & 1);
            };
            vb[0] = validity;
            a.null_count = null_count;

            if constexpr (is_string_field_v<FT>) {
                static_assert(std::convertible_to<column_value_t<decltype(values)>, std::string_view>,
                              "string/binary column must be a range of string-like values");
                using off_t = std::conditional_t<is_large_string_v<FT>, std::int64_t, std::int32_t>;
                std::vector<std::uint8_t>& off = p.bytes.emplace_back();
                std::vector<std::uint8_t>& dat = p.bytes.emplace_back();
                off.resize(sizeof(off_t) * (static_cast<std::size_t>(n) + 1));
                auto* o = reinterpret_cast<off_t*>(off.data());
                off_t cur = 0;
                o[0] = 0;
                std::size_t i = 0;
                for (auto&& e : values) {
                    if (present_at(i)) {
                        std::string_view sv(e);
                        dat.insert(dat.end(), sv.begin(), sv.end());
                        cur += static_cast<off_t>(sv.size());
                    }
                    o[++i] = cur;
                }
                s.format = string_format<FT>();
                vb[1] = off.data();
                vb[2] = dat.data();
                a.n_buffers = 3;
            } else if constexpr (std::is_same_v<FT, bool>) {
                static_assert(std::same_as<column_value_t<decltype(values)>, bool>,
                              "bool column must be a range of bool");
                std::vector<std::uint8_t>& packed = p.bytes.emplace_back();
                packed.assign((static_cast<std::size_t>(n) + 7) / 8, 0);
                std::size_t i = 0;
                for (auto&& v : values) {
                    if (static_cast<bool>(v)) packed[i >> 3] |= static_cast<std::uint8_t>(1u << (i & 7));
                    ++i;
                }
                s.format = "b";
                vb[1] = packed.data();
                a.n_buffers = 2;
            } else {
                static_assert(std::same_as<column_value_t<decltype(values)>, FT>,
                              "SoA column element type does not match its field type");
                s.format = arrow_traits<FT>::format;
                vb[1] = static_cast<const void*>(std::ranges::data(values));
                a.n_buffers = 2;
            }
            a.buffers = vb.data();
            return {&s, &a};
        }
    }

    template <std::size_t... I, class... Cols>
    N2PStatus write_chunk_impl(std::index_sequence<I...>, const Cols&... cols) {
        if (!w_) return status_;

        const auto& first = std::get<0>(std::forward_as_tuple(cols...));
        const auto n = static_cast<std::int64_t>(first_leaf_size<field_at<0>>(first));

        Pools p;
        bool ok = true;
        std::vector<ArrowSchema*>& tops = p.schildren.emplace_back();
        std::vector<ArrowArray*>& topa = p.achildren.emplace_back();
        ([&] {
            auto pr = build_node<field_at<I>>(cols, n, p, ok);
            tops.push_back(pr.first);
            topa.push_back(pr.second);
        }(), ...);
        if (!ok) {
            status_ = N2P_INVALID_ARGUMENT;
            return status_;
        }

        const void* root_buf[1] = {nullptr};
        ArrowSchema root_s{};
        root_s.format = "+s";
        root_s.name = "schema";
        root_s.n_children = static_cast<std::int64_t>(K);
        root_s.children = tops.data();
        ArrowArray root_a{};
        root_a.length = n;
        root_a.n_buffers = 1;
        root_a.buffers = root_buf;
        root_a.n_children = static_cast<std::int64_t>(K);
        root_a.children = topa.data();

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
