// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Yoav Bendor
//
// Zero-copy NumPy column dict -> Arrow struct record batch export.

#pragma once

#include "arrow_capsule.hpp"

#include <nanoarrow/hpp/buffer.hpp>
#include <nanoarrow/nanoarrow.h>

#include <nanobind/nanobind.h>
#include <nanobind/stl/map.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/vector.h>

#include <cstdint>
#include <cstring>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace nb = nanobind;

namespace nanoarrow_io::numpy_bridge {

namespace detail {

struct ColumnBacking {
    nb::object array;
};

struct RecordBatchBacking {
    std::vector<std::string> names;
    std::vector<ColumnBacking> columns;
    std::unique_ptr<ArrowSchema, void (*)(ArrowSchema*)> schema{nullptr, ArrowSchemaRelease};
    std::unique_ptr<ArrowArray, void (*)(ArrowArray*)> array{nullptr, ArrowArrayRelease};
};

inline void require_c_contiguous(const nb::object& arr) {
    if (!nb::cast<bool>(arr.attr("flags").attr("c_contiguous"))) {
        throw std::invalid_argument("numpy columns must be C-contiguous (use numpy.ascontiguousarray)");
    }
    if (nb::cast<int>(arr.attr("ndim")) != 1) {
        throw std::invalid_argument("numpy columns must be one-dimensional");
    }
}

inline std::pair<uint8_t*, int64_t> numpy_column_bytes(const nb::object& arr) {
    nb::object iface = arr.attr("__array_interface__");
    const uintptr_t addr = nb::cast<uintptr_t>(iface["data"][0]);
    const int itemsize = nb::cast<int>(arr.attr("dtype").attr("itemsize"));
    const int64_t length = nb::cast<int64_t>(arr.attr("shape").attr("__getitem__")(0));
    return {reinterpret_cast<uint8_t*>(addr), length * itemsize};
}

inline ArrowType map_numeric_dtype(const nb::object& dtype) {
    const std::string kind = nb::cast<std::string>(dtype.attr("kind"));
    const int itemsize = nb::cast<int>(dtype.attr("itemsize"));
    if (kind == "b") {
        return NANOARROW_TYPE_BOOL;
    }
    if (kind == "i") {
        switch (itemsize) {
            case 1:
                return NANOARROW_TYPE_INT8;
            case 2:
                return NANOARROW_TYPE_INT16;
            case 4:
                return NANOARROW_TYPE_INT32;
            case 8:
                return NANOARROW_TYPE_INT64;
            default:
                break;
        }
    }
    if (kind == "u") {
        switch (itemsize) {
            case 1:
                return NANOARROW_TYPE_UINT8;
            case 2:
                return NANOARROW_TYPE_UINT16;
            case 4:
                return NANOARROW_TYPE_UINT32;
            case 8:
                return NANOARROW_TYPE_UINT64;
            default:
                break;
        }
    }
    if (kind == "f") {
        if (itemsize == 4) {
            return NANOARROW_TYPE_FLOAT;
        }
        if (itemsize == 8) {
            return NANOARROW_TYPE_DOUBLE;
        }
    }
    throw std::invalid_argument(
        "unsupported numpy dtype for zero-copy export (supported: bool, int/uint/float widths 1/2/4/8 bytes)");
}

inline void attach_numpy_column(ColumnBacking& col, ArrowArray* child_array, int64_t length) {
    nb::object dtype = col.array.attr("dtype");
    (void)map_numeric_dtype(dtype);

    const auto [ptr, nbytes] = numpy_column_bytes(col.array);
    (void)nbytes;
    (void)length;

    ArrowBuffer values{};
    ArrowBufferInit(&values);
    nanoarrow::BufferInitWrapped(&values, nb::object(col.array), ptr, nbytes);
    if (ArrowArraySetBuffer(child_array, 1, &values) != NANOARROW_OK) {
        ArrowBufferReset(&values);
        throw std::runtime_error("failed to attach numpy buffer to Arrow array");
    }
    ArrowBufferReset(&values);

    child_array->length = length;
    child_array->null_count = 0;
}

inline std::shared_ptr<RecordBatchBacking> build_record_batch(
    const std::vector<std::pair<std::string, nb::object>>& columns) {
    if (columns.empty()) {
        throw std::invalid_argument("record_batch requires at least one column");
    }

    auto backing = std::make_shared<RecordBatchBacking>();
    backing->names.reserve(columns.size());
    backing->columns.reserve(columns.size());

    int64_t length = -1;
    for (const auto& [name, arr] : columns) {
        (void)name;
        nb::module_ np = nb::module_::import_("numpy");
        if (!nb::isinstance(arr, np.attr("ndarray"))) {
            throw std::invalid_argument("all columns must be numpy.ndarray instances");
        }
        require_c_contiguous(arr);
        const int64_t col_len = nb::cast<int64_t>(arr.attr("shape").attr("__getitem__")(0));
        if (length < 0) {
            length = col_len;
        } else if (length != col_len) {
            throw std::invalid_argument("all numpy columns must have the same length");
        }
        ColumnBacking col_backing;
        col_backing.array = arr;
        backing->names.push_back(name);
        backing->columns.push_back(std::move(col_backing));
    }

    backing->schema.reset(new ArrowSchema());
    ArrowSchemaInit(backing->schema.get());
    if (ArrowSchemaSetTypeStruct(backing->schema.get(), static_cast<int64_t>(columns.size())) !=
        NANOARROW_OK) {
        throw std::runtime_error("failed to initialize struct schema");
    }

    for (std::size_t i = 0; i < columns.size(); ++i) {
        const ArrowType type = map_numeric_dtype(backing->columns[i].array.attr("dtype"));
        if (ArrowSchemaSetType(backing->schema->children[i], type) != NANOARROW_OK) {
            throw std::runtime_error("failed to set struct child type");
        }
        if (ArrowSchemaSetName(backing->schema->children[i], columns[i].first.c_str()) !=
            NANOARROW_OK) {
            throw std::runtime_error("failed to set struct child name");
        }
    }

    backing->array.reset(new ArrowArray());
    std::memset(backing->array.get(), 0, sizeof(ArrowArray));
    if (ArrowArrayInitFromSchema(backing->array.get(), backing->schema.get(), nullptr) !=
        NANOARROW_OK) {
        throw std::runtime_error("failed to initialize struct array");
    }

    for (std::size_t i = 0; i < columns.size(); ++i) {
        attach_numpy_column(backing->columns[i], backing->array->children[i], length);
    }

    backing->array->length = length;
    backing->array->null_count = 0;
    return backing;
}

}  // namespace detail

class NumpyRecordBatch {
public:
    explicit NumpyRecordBatch(const std::map<std::string, nb::object>& columns) {
        std::vector<std::pair<std::string, nb::object>> ordered;
        ordered.reserve(columns.size());
        for (const auto& [name, arr] : columns) {
            ordered.emplace_back(name, arr);
        }
        backing_ = detail::build_record_batch(ordered);
    }

    explicit NumpyRecordBatch(const nb::dict& columns) {
        std::vector<std::pair<std::string, nb::object>> ordered;
        ordered.reserve(nb::len(columns));
        for (auto item : columns) {
            ordered.emplace_back(nb::cast<std::string>(nb::str(item.first)),
                                 nb::cast<nb::object>(item.second));
        }
        backing_ = detail::build_record_batch(ordered);
    }

    nb::tuple arrow_c_array(nb::object /*requested_schema*/) {
        if (!backing_ || !backing_->schema || !backing_->array) {
            throw std::runtime_error("NumpyRecordBatch was already exported");
        }
        auto* schema = static_cast<ArrowSchema*>(std::malloc(sizeof(ArrowSchema)));
        std::memset(schema, 0, sizeof(ArrowSchema));
        ArrowSchemaMove(backing_->schema.get(), schema);

        auto* array = static_cast<ArrowArray*>(std::malloc(sizeof(ArrowArray)));
        std::memset(array, 0, sizeof(ArrowArray));
        ArrowArrayMove(backing_->array.get(), array);

        backing_->schema.reset();
        backing_->array.reset();

        return nb::make_tuple(arrow_capsule::detail::make_schema_capsule(schema),
                              arrow_capsule::detail::make_array_capsule(array));
    }

    nb::capsule arrow_c_stream(nb::object /*requested_schema*/) {
        if (!backing_ || !backing_->schema || !backing_->array) {
            throw std::runtime_error("NumpyRecordBatch was already exported");
        }
        auto state = backing_;
        auto* stream = static_cast<ArrowArrayStream*>(std::malloc(sizeof(ArrowArrayStream)));
        std::memset(stream, 0, sizeof(ArrowArrayStream));
        arrow_capsule::nanoarrow_check(
            "ArrowBasicArrayStreamInit",
            ArrowBasicArrayStreamInit(stream, state->schema.get(), 1));
        state->schema.release();
        ArrowBasicArrayStreamSetArray(stream, 0, state->array.get());
        state->array.release();
        return arrow_capsule::detail::make_stream_capsule(stream);
    }

    int64_t length() const {
        return backing_ && backing_->array ? backing_->array->length : 0;
    }

    std::uintptr_t column_address(const std::string& name) const {
        if (!backing_) {
            throw std::runtime_error("empty NumpyRecordBatch");
        }
        for (std::size_t i = 0; i < backing_->names.size(); ++i) {
            if (backing_->names[i] == name) {
                nb::object iface = backing_->columns[i].array.attr("__array_interface__");
                return nb::cast<std::uintptr_t>(iface["data"][0]);
            }
        }
        throw std::invalid_argument("unknown column: " + name);
    }

private:
    std::shared_ptr<detail::RecordBatchBacking> backing_;
};

}  // namespace nanoarrow_io::numpy_bridge
