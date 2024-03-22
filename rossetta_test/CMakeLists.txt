set(CMAKE_BUILD_TYPE Debug)
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0 -fsanitize=undefined -fsanitize=address")

add_executable(rossetta_test "")

target_sources(rossetta_test PRIVATE rossetta_test.cpp)

target_include_directories(rossetta_test PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_include_directories(rossetta_test PRIVATE ${CMAKE_SOURCE_DIR}/db/)


