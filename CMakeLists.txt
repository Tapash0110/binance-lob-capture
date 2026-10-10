cmake_minimum_required(VERSION 3.16)
project(binance_lob_capture VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Build flags
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    add_compile_options(-Wall -Wextra -Wpedantic)
endif()

# Find dependencies
find_package(Threads REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(Boost REQUIRED)

find_path(RAPIDJSON_INCLUDE_DIRS "rapidjson/rapidjson.h"
    PATHS /usr/include /usr/local/include
)
if(NOT RAPIDJSON_INCLUDE_DIRS)
    message(WARNING "RapidJSON system path not found; using default include paths")
endif()

# Common includes
include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${OPENSSL_INCLUDE_DIR}
    ${Boost_INCLUDE_DIRS}
    ${RAPIDJSON_INCLUDE_DIRS}
)

# Output directory settings to match document specifications:
# ./build/binance_capture
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}")

# Core library
add_library(lob_core STATIC
    src/lob.cpp
    src/csv_writer.cpp
    src/ws_client.cpp
    src/replay.cpp
    src/app.cpp
)

target_link_libraries(lob_core PUBLIC
    Threads::Threads
    OpenSSL::SSL
    OpenSSL::Crypto
    ${Boost_LIBRARIES}
)

# Main executable: binance_capture
add_executable(binance_capture src/main.cpp)
target_link_libraries(binance_capture PRIVATE lob_core)

# Tests
enable_testing()
add_subdirectory(tests)

# Convenience target 'tests' per assignment specification
add_custom_target(tests DEPENDS unit_tests)
