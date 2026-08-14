cmake_minimum_required(VERSION 3.16)
project(market_data_feed_handler CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -march=native")

include_directories(include)

add_executable(demo src/main.cpp)
add_executable(bench_run bench/benchmark.cpp)
add_executable(test_feed_handler tests/test_feed_handler.cpp)

enable_testing()
add_test(NAME feed_handler_correctness COMMAND test_feed_handler)
