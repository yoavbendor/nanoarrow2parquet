# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Yoav Bendor

cmake_minimum_required(VERSION 3.24)
project(nanoarrow_io LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_C_STANDARD 11)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "" FORCE)
endif()

set(N2P_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../..")

if(NOT DEFINED FETCHCONTENT_BASE_DIR)
    set(FETCHCONTENT_BASE_DIR "${N2P_ROOT}/.deps" CACHE PATH "Fetched dependency sources")
endif()

# --- nanoarrow with IPC (required by nanolance) ---
include(FetchContent)
set(NANOARROW_IPC ON CACHE BOOL "" FORCE)
set(NANOARROW_IPC_WITH_ZSTD OFF CACHE BOOL "" FORCE)
set(NANOARROW_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(NANOARROW_BUILD_APPS OFF CACHE BOOL "" FORCE)
FetchContent_Declare(
    nanoarrow
    GIT_REPOSITORY https://github.com/apache/arrow-nanoarrow.git
    GIT_TAG ffe61d3cd2d02da9e60cfc405cd6c50c512f64b9
)
FetchContent_MakeAvailable(nanoarrow)

# --- zstd (page compression; shared by n2p + nanolance) ---
if(NOT TARGET libzstd_static)
    set(ZSTD_BUILD_PROGRAMS OFF CACHE BOOL "" FORCE)
    set(ZSTD_BUILD_TESTS OFF CACHE BOOL "" FORCE)
    set(ZSTD_BUILD_SHARED OFF CACHE BOOL "" FORCE)
    set(ZSTD_BUILD_STATIC ON CACHE BOOL "" FORCE)
    FetchContent_Declare(
        _nanoarrow_io_zstd
        GIT_REPOSITORY https://github.com/facebook/zstd.git
        GIT_TAG v1.5.6
        SOURCE_SUBDIR build/cmake
    )
    FetchContent_MakeAvailable(_nanoarrow_io_zstd)
endif()

# --- nanolance (pinned sibling project) ---
set(NANOLANCE_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(NANOLANCE_BUILD_TOOLS OFF CACHE BOOL "" FORCE)
set(NANOLANCE_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(NANOLANCE_BUILD_FUSE_EXAMPLE OFF CACHE BOOL "" FORCE)
set(NANOLANCE_ENABLE_S3 OFF CACHE BOOL "" FORCE)
FetchContent_Declare(
    nanolance
    GIT_REPOSITORY https://github.com/yoavbendor/nanolance.git
    GIT_TAG 08e1bd4ee8060f4a9aea473b1b80e8e3246a342b
    GIT_SHALLOW FALSE
)
FetchContent_MakeAvailable(nanolance)

# --- nanoarrow2parquet (reuses nanoarrow from nanolance) ---
set(N2P_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(N2P_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(N2P_BUILD_BENCHMARKS OFF CACHE BOOL "" FORCE)
add_subdirectory(${N2P_ROOT} ${CMAKE_BINARY_DIR}/n2p EXCLUDE_FROM_ALL)

# --- nanobind modules ---
find_package(Python 3.9 REQUIRED COMPONENTS Interpreter Development.Module)
execute_process(
    COMMAND "${Python_EXECUTABLE}" -m nanobind --cmake_dir
    OUTPUT_STRIP_TRAILING_WHITESPACE
    OUTPUT_VARIABLE nanobind_ROOT
)
find_package(nanobind CONFIG REQUIRED)

nanobind_add_module(_n2p STABLE_ABI NB_STATIC src/n2p_bindings.cpp)
target_include_directories(_n2p PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(_n2p PRIVATE nanoarrow2parquet::nanoarrow2parquet nanoarrow_static)

nanobind_add_module(_nanolance STABLE_ABI NB_STATIC src/nanolance_bindings.cpp)
target_include_directories(_nanolance PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(_nanolance PRIVATE nanolance::nanolance nanoarrow_static)

nanobind_add_module(_numpy STABLE_ABI NB_STATIC src/numpy_bindings.cpp)
target_include_directories(_numpy PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(_numpy PRIVATE nanoarrow_static)

install(TARGETS _n2p _nanolance _numpy LIBRARY DESTINATION nanoarrow_io)
