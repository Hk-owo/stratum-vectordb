# Top-level CMake entry point for the Stratum project's C++ components.
#
# Stratum is split between a Go side (api/, internal/, service/, cmd/) built
# with `go build`, and a C++ side (vecstore/) built with CMake. This file
# only drives the C++ side; see go.mod for the Go module.
cmake_minimum_required(VERSION 3.16)
project(stratum_vecstore CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

enable_testing()

add_subdirectory(vecstore)
