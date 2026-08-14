CXX      ?= g++
CXXFLAGS ?= -O3 -march=native -std=c++20 -Wall -Wextra -Iinclude

.PHONY: all demo bench test clean

all: demo bench test

demo: bin/demo
bench: bin/bench_run
test: bin/test_feed_handler
	./bin/test_feed_handler

bin:
	mkdir -p bin

bin/demo: src/main.cpp include/*.hpp | bin
	$(CXX) $(CXXFLAGS) src/main.cpp -o bin/demo

bin/bench_run: bench/benchmark.cpp include/*.hpp | bin
	$(CXX) $(CXXFLAGS) bench/benchmark.cpp -o bin/bench_run

bin/test_feed_handler: tests/test_feed_handler.cpp include/*.hpp | bin
	$(CXX) $(CXXFLAGS) tests/test_feed_handler.cpp -o bin/test_feed_handler

clean:
	rm -rf bin
