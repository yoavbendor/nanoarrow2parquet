// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Yoav Bendor

// Second translation unit for the single-header build: includes the amalgamated
// header WITHOUT NANOARROW2PARQUET_IMPLEMENTATION. It must see the public
// declarations and link cleanly against the implementation compiled in
// test_single_header.cpp -- this is what catches accidental external linkage
// (missing inline / anonymous-namespace) in the amalgamation.

#include "nanoarrow2parquet.h"

// Referencing a public symbol forces the linker to resolve it from the other TU.
const char* single_header_path();

const char* (*kForceLink)() = &single_header_path;
