set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/tests")

add_executable(unit_tests unit_tests.cpp)
target_link_libraries(unit_tests PRIVATE lob_core)

add_test(NAME unit_tests COMMAND unit_tests)
