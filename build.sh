#!/usr/bin/env bash
set -e

cd /mnt/c/Users/tejas/Downloads/iRage
cmake -B build -G Ninja -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="-Wall -Wextra -O2"
cmake --build build --parallel
cmake --build build --target tests
./build/tests/unit_tests
