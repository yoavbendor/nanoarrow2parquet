// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Yoav Bendor

#include "numpy_bridge.hpp"

#include <nanobind/nanobind.h>
#include <nanobind/stl/map.h>

namespace nb = nanobind;
using nanoarrow_io::numpy_bridge::NumpyRecordBatch;

NB_MODULE(_numpy, m) {
    m.doc() = "zero-copy NumPy -> Arrow bridge for nanoarrow-io";

    nb::class_<NumpyRecordBatch>(m, "RecordBatch")
        .def(nb::init<const std::map<std::string, nb::object>&>(), nb::arg("columns"))
        .def(nb::init<const nb::dict&>(), nb::arg("columns"))
        .def_prop_ro("length", &NumpyRecordBatch::length)
        .def("column_address", &NumpyRecordBatch::column_address, nb::arg("name"),
             "Return the data pointer for a column's numpy buffer (before export).")
        .def("__arrow_c_array__", &NumpyRecordBatch::arrow_c_array, nb::arg("requested_schema") = nb::none())
        .def("__arrow_c_stream__", &NumpyRecordBatch::arrow_c_stream,
             nb::arg("requested_schema") = nb::none());
}
