# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Yoav Bendor

cmake_minimum_required(VERSION 3.24)
project(nanoarrow2parquet LANGUAGES C CXX VERSION 0.1.0)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_C_STANDARD 11)
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "" FORCE)
endif()

option(N2P_BUILD_TESTS "Build nanoarrow2parquet tests" OFF)
option(N2P_BUILD_EXAMPLES "Build nanoarrow2parquet examples" ON)

# Detect whether we are the top-level project (standalone) so an embedding parent
# can supply nanoarrow / zstd targets itself.
if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
    set(N2P_STANDALONE TRUE)
else()
    set(N2P_STANDALONE FALSE)
endif()

# Park FetchContent downloads in a stable, gitignored ./.deps that survives
# `rm -rf build` (honour a caller-provided base dir, e.g. a CI cache).
if(NOT DEFINED FETCHCONTENT_BASE_DIR)
    set(FETCHCONTENT_BASE_DIR "${CMAKE_SOURCE_DIR}/.deps" CACHE PATH "Fetched dependency sources")
endif()
include(FetchContent)

# --- nanoarrow (pinned to the same commit nanolance uses) ---
if(NOT TARGET nanoarrow_static)
    set(NANOARROW_IPC OFF CACHE BOOL "" FORCE)
    set(NANOARROW_BUILD_TESTS OFF CACHE BOOL "" FORCE)
    set(NANOARROW_BUILD_APPS OFF CACHE BOOL "" FORCE)
    FetchContent_Declare(nanoarrow
        GIT_REPOSITORY https://github.com/apache/arrow-nanoarrow.git
        GIT_TAG ffe61d3cd2d02da9e60cfc405cd6c50c512f64b9
    )
    FetchContent_MakeAvailable(nanoarrow)
endif()

# --- zstd (page compression) ---
set(N2P_ZSTD_TARGET "")
if(TARGET libzstd_static)
    set(N2P_ZSTD_TARGET libzstd_static)
elseif(TARGET zstd::libzstd_static)
    set(N2P_ZSTD_TARGET zstd::libzstd_static)
elseif(TARGET zstd::libzstd)
    set(N2P_ZSTD_TARGET zstd::libzstd)
else()
    find_package(zstd QUIET)
    if(TARGET zstd::libzstd_static)
        set(N2P_ZSTD_TARGET zstd::libzstd_static)
    elseif(TARGET zstd::libzstd)
        set(N2P_ZSTD_TARGET zstd::libzstd)
    endif()
endif()
if(NOT N2P_ZSTD_TARGET)
    set(ZSTD_BUILD_PROGRAMS OFF CACHE BOOL "" FORCE)
    set(ZSTD_BUILD_TESTS OFF CACHE BOOL "" FORCE)
    set(ZSTD_BUILD_SHARED OFF CACHE BOOL "" FORCE)
    set(ZSTD_BUILD_STATIC ON CACHE BOOL "" FORCE)
    FetchContent_Declare(_n2p_zstd
        GIT_REPOSITORY https://github.com/facebook/zstd.git
        GIT_TAG v1.5.6
        SOURCE_SUBDIR build/cmake
    )
    FetchContent_MakeAvailable(_n2p_zstd)
    if(TARGET libzstd_static)
        set(N2P_ZSTD_TARGET libzstd_static)
        # zstd's CMake doesn't always export usable include dirs for in-tree use.
        target_include_directories(libzstd_static INTERFACE
            $<BUILD_INTERFACE:${_n2p_zstd_SOURCE_DIR}/lib>)
    endif()
endif()

# --- library ---
add_library(nanoarrow2parquet STATIC src/writer.cpp)
target_include_directories(nanoarrow2parquet PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>)
target_include_directories(nanoarrow2parquet PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(nanoarrow2parquet PUBLIC nanoarrow_static PRIVATE ${N2P_ZSTD_TARGET})
add_library(nanoarrow2parquet::nanoarrow2parquet ALIAS nanoarrow2parquet)

# --- single-header amalgamation ---
# The split sources above are the source of truth; single_include/ is generated
# from them by scripts/amalgamate.py and committed so consumers can vendor one
# file. `make n2p_amalgamate` regenerates it; the single-header test below keeps
# it honest at build time.
find_package(Python3 COMPONENTS Interpreter QUIET)
set(N2P_SINGLE_HEADER ${CMAKE_CURRENT_SOURCE_DIR}/single_include/nanoarrow2parquet.h)
if(Python3_Interpreter_FOUND)
    add_custom_target(n2p_amalgamate
        COMMAND ${Python3_EXECUTABLE}
                ${CMAKE_CURRENT_SOURCE_DIR}/scripts/amalgamate.py
                -o ${N2P_SINGLE_HEADER}
        BYPRODUCTS ${N2P_SINGLE_HEADER}
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        COMMENT "Regenerating single_include/nanoarrow2parquet.h")
endif()

# Header-only interface target for embedders who prefer the single header. They
# still link nanoarrow + zstd (the implementation depends on both).
add_library(nanoarrow2parquet_single INTERFACE)
target_include_directories(nanoarrow2parquet_single INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/single_include>)
add_library(nanoarrow2parquet::single ALIAS nanoarrow2parquet_single)

if(N2P_BUILD_EXAMPLES)
    add_executable(arrow2parquet_demo examples/arrow2parquet_demo.cpp)
    target_link_libraries(arrow2parquet_demo PRIVATE nanoarrow2parquet nanoarrow_static)

    add_executable(pcapng2parquet examples/pcapng2parquet.cpp)
    target_link_libraries(pcapng2parquet PRIVATE nanoarrow2parquet nanoarrow_static)
endif()

# --- benchmarks (vs Apache Parquet C++) ---
option(N2P_BUILD_BENCHMARKS "Build write benchmarks vs Apache Parquet C++" OFF)
if(N2P_BUILD_BENCHMARKS)
    add_executable(bench_n2p bench/bench_n2p.cpp)
    target_include_directories(bench_n2p PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/bench)
    target_link_libraries(bench_n2p PRIVATE nanoarrow2parquet nanoarrow_static)

    # The Apache baseline links the Parquet C++ libraries. By default we discover
    # the ones bundled in an installed pyarrow wheel (headers + libarrow/libparquet
    # shared objects), which avoids building Arrow from source. Override with
    # -DARROW_INCLUDE_DIR=... -DARROW_LIB_DIR=... to use a system/conda Arrow.
    if(NOT DEFINED ARROW_INCLUDE_DIR OR NOT DEFINED ARROW_LIB_DIR)
        find_package(Python3 COMPONENTS Interpreter QUIET)
        if(Python3_Interpreter_FOUND)
            execute_process(
                COMMAND ${Python3_EXECUTABLE} -c
                    "import pyarrow,os;print(pyarrow.get_include());print(os.path.dirname(pyarrow.__file__))"
                OUTPUT_VARIABLE _pa_out OUTPUT_STRIP_TRAILING_WHITESPACE
                RESULT_VARIABLE _pa_rc ERROR_QUIET)
            if(_pa_rc EQUAL 0)
                string(REGEX MATCHALL "[^\n]+" _pa_lines "${_pa_out}")
                list(GET _pa_lines 0 _pa_inc)
                list(GET _pa_lines 1 _pa_lib)
                if(NOT DEFINED ARROW_INCLUDE_DIR)
                    set(ARROW_INCLUDE_DIR "${_pa_inc}")
                endif()
                if(NOT DEFINED ARROW_LIB_DIR)
                    set(ARROW_LIB_DIR "${_pa_lib}")
                endif()
            endif()
        endif()
    endif()

    if(DEFINED ARROW_INCLUDE_DIR AND DEFINED ARROW_LIB_DIR)
        file(GLOB _arrow_so "${ARROW_LIB_DIR}/libarrow.so*")
        file(GLOB _parquet_so "${ARROW_LIB_DIR}/libparquet.so*")
        if(_arrow_so AND _parquet_so)
            list(GET _arrow_so 0 ARROW_SO)
            list(GET _parquet_so 0 PARQUET_SO)
            add_executable(bench_arrow bench/bench_arrow.cpp)
            target_include_directories(bench_arrow PRIVATE
                ${CMAKE_CURRENT_SOURCE_DIR}/bench ${ARROW_INCLUDE_DIR})
            target_link_libraries(bench_arrow PRIVATE ${PARQUET_SO} ${ARROW_SO})
            set_target_properties(bench_arrow PROPERTIES
                BUILD_RPATH "${ARROW_LIB_DIR}" INSTALL_RPATH "${ARROW_LIB_DIR}")
            message(STATUS "bench_arrow: using Arrow at ${ARROW_LIB_DIR}")
        else()
            message(WARNING "bench_arrow skipped: no libarrow/libparquet in ${ARROW_LIB_DIR}")
        endif()
    else()
        message(WARNING "bench_arrow skipped: Arrow C++ not found (install pyarrow "
                        "or set -DARROW_INCLUDE_DIR / -DARROW_LIB_DIR). bench_n2p still builds.")
    endif()
endif()

if(N2P_BUILD_TESTS)
    enable_testing()
    add_executable(test_roundtrip tests/test_roundtrip.cpp)
    target_include_directories(test_roundtrip PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
    target_link_libraries(test_roundtrip PRIVATE nanoarrow2parquet nanoarrow_static)
    add_test(NAME test_roundtrip COMMAND test_roundtrip)

    # Single-header build: two TUs share the amalgamated header (one defines the
    # implementation macro), proving it compiles and links with no ODR clashes.
    add_executable(test_single_header
        tests/test_single_header.cpp
        tests/test_single_header_decls.cpp)
    target_link_libraries(test_single_header PRIVATE
        nanoarrow2parquet::single nanoarrow_static ${N2P_ZSTD_TARGET})
    add_test(NAME test_single_header COMMAND test_single_header)

    # Keep the committed amalgamation in sync with the split sources.
    if(Python3_Interpreter_FOUND)
        add_test(NAME amalgamation_up_to_date
                 COMMAND ${Python3_EXECUTABLE}
                         ${CMAKE_CURRENT_SOURCE_DIR}/scripts/amalgamate.py
                         -o ${N2P_SINGLE_HEADER} --check)
    endif()

    # Nullable (OPTIONAL columns / definition levels) + Arrow null type roundtrip.
    add_executable(test_nullable tests/test_nullable.cpp)
    target_link_libraries(test_nullable PRIVATE nanoarrow2parquet nanoarrow_static)

    # Nested struct column roundtrip (multi-level definition levels).
    add_executable(test_struct tests/test_struct.cpp)
    target_link_libraries(test_struct PRIVATE nanoarrow2parquet nanoarrow_static)

    # BYTE_ARRAY encoding chooser: PLAIN fallback vs RLE_DICTIONARY.
    add_executable(test_strings tests/test_strings.cpp)
    target_link_libraries(test_strings PRIVATE nanoarrow2parquet nanoarrow_static)

    # Native struct-of-arrays path. Links only nanoarrow2parquet -- NOT nanoarrow --
    # to prove the SoA front-end reaches Parquet without the Arrow C builders.
    add_executable(test_soa tests/test_soa.cpp)
    target_link_libraries(test_soa PRIVATE nanoarrow2parquet)

    add_executable(test_soa_nullable tests/test_soa_nullable.cpp)
    target_link_libraries(test_soa_nullable PRIVATE nanoarrow2parquet)

    add_executable(test_soa_strings tests/test_soa_strings.cpp)
    target_link_libraries(test_soa_strings PRIVATE nanoarrow2parquet)

    find_package(Python3 COMPONENTS Interpreter)
    if(Python3_Interpreter_FOUND)
        add_test(NAME smoke_pyarrow
                 COMMAND ${CMAKE_COMMAND} -E env
                         N2P_DEMO=$<TARGET_FILE:arrow2parquet_demo>
                         PYTHON=${Python3_EXECUTABLE}
                         bash ${CMAKE_CURRENT_SOURCE_DIR}/tests/smoke_pyarrow_roundtrip.sh)

        add_test(NAME nullable_roundtrip
                 COMMAND bash -c
                     "$<TARGET_FILE:test_nullable> ${CMAKE_CURRENT_BINARY_DIR}/n2p_nulls.parquet && \
                      ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/tests/check_nullable.py \
                      ${CMAKE_CURRENT_BINARY_DIR}/n2p_nulls.parquet")

        add_test(NAME struct_roundtrip
                 COMMAND bash -c
                     "$<TARGET_FILE:test_struct> ${CMAKE_CURRENT_BINARY_DIR}/n2p_structs.parquet && \
                      ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/tests/check_struct.py \
                      ${CMAKE_CURRENT_BINARY_DIR}/n2p_structs.parquet")

        add_test(NAME strings_roundtrip
                 COMMAND bash -c
                     "$<TARGET_FILE:test_strings> ${CMAKE_CURRENT_BINARY_DIR}/n2p_strings.parquet && \
                      ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/tests/check_strings.py \
                      ${CMAKE_CURRENT_BINARY_DIR}/n2p_strings.parquet")

        add_test(NAME soa_roundtrip
                 COMMAND bash -c
                     "$<TARGET_FILE:test_soa> ${CMAKE_CURRENT_BINARY_DIR}/n2p_soa.parquet && \
                      ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/tests/check_soa.py \
                      ${CMAKE_CURRENT_BINARY_DIR}/n2p_soa.parquet")

        add_test(NAME soa_nullable_roundtrip
                 COMMAND bash -c
                     "$<TARGET_FILE:test_soa_nullable> ${CMAKE_CURRENT_BINARY_DIR}/n2p_soa_nullable.parquet && \
                      ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/tests/check_soa_nullable.py \
                      ${CMAKE_CURRENT_BINARY_DIR}/n2p_soa_nullable.parquet")

        add_test(NAME soa_strings_roundtrip
                 COMMAND bash -c
                     "$<TARGET_FILE:test_soa_strings> ${CMAKE_CURRENT_BINARY_DIR}/n2p_soa_strings.parquet && \
                      ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/tests/check_soa_strings.py \
                      ${CMAKE_CURRENT_BINARY_DIR}/n2p_soa_strings.parquet")
    endif()

    # Performance/size regression gate vs Apache Parquet C++. Only registered when
    # the benchmarks are built (-DN2P_BUILD_BENCHMARKS=ON, which needs the Apache
    # baseline). Compares the two writers on the same machine, so the ratios are
    # stable under CI noise; a small fast matrix keeps it to a few seconds.
    if(Python3_Interpreter_FOUND AND TARGET bench_n2p AND TARGET bench_arrow)
        add_test(NAME bench_regression
                 COMMAND ${Python3_EXECUTABLE}
                         ${CMAKE_CURRENT_SOURCE_DIR}/bench/run_bench.py --check
                         --bin-dir ${CMAKE_CURRENT_BINARY_DIR}
                         --totals 128 --chunks 64 --codecs zstd,uncompressed --repeat 3)
    endif()
endif()
