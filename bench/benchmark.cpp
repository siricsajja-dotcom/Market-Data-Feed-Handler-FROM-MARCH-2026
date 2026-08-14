// Benchmark: end-to-end throughput and per-message latency of
// decode -> sequence -> book-apply, under a clean feed (no loss/reorder)
// and under a lossy/reordering one, so the overhead the Sequencer adds
// for out-of-order handling is visible on its own.
//
// Build: g++ -O3 -march=native -std=c++20 -Iinclude bench/benchmark.cpp -o bench_run

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <vector>
#include "protocol.hpp"
#include "sequencer.hpp"
#include "book_builder.hpp"
#include "feed_source.hpp"

using namespace feed;
using Clock = std::chrono::steady_clock;

struct LatencyStats {
    double p50_ns = 0, p99_ns = 0, p999_ns = 0, mean_ns = 0;
    std::uint64_t ops = 0;
    double ops_per_sec = 0;
};

static LatencyStats summarize(std::vector<std::int64_t>& samples, double wall_seconds) {
    LatencyStats st;
    st.ops = samples.size();
    if (st.ops == 0) return st;
    std::sort(samples.begin(), samples.end());
    auto pct = [&](double p) {
        return static_cast<double>(samples[static_cast<std::size_t>(p * (samples.size() - 1))]);
    };
    st.p50_ns = pct(0.50);
    st.p99_ns = pct(0.99);
    st.p999_ns = pct(0.999);
    double sum = 0;
    for (auto v : samples) sum += static_cast<double>(v);
    st.mean_ns = sum / static_cast<double>(samples.size());
    st.ops_per_sec = wall_seconds > 0 ? static_cast<double>(st.ops) / wall_seconds : 0;
    return st;
}

static void print_row(const char* name, const LatencyStats& st) {
    std::printf("%-32s %10llu %14.0f %10.0f %10.0f %10.0f\n",
                name, (unsigned long long)st.ops, st.ops_per_sec, st.mean_ns, st.p50_ns, st.p99_ns);
}

// Encodes every message to wire bytes up front (so decode cost is
// measured, not generation cost), then times decode+sequence+book-apply
// per message.
static LatencyStats run_pipeline_bench(const std::vector<Message>& delivered) {
    std::vector<std::vector<std::uint8_t>> encoded;
    encoded.reserve(delivered.size());
    for (const auto& m : delivered) {
        std::vector<std::uint8_t> buf;
        encode(m, buf);
        encoded.push_back(std::move(buf));
    }

    BookBuilder book;
    Sequencer seq(
        [&](const Message& m) { book.apply(m); },
        [&](SeqNum, SeqNum) {},
        1, 30
    );

    std::vector<std::int64_t> samples;
    samples.reserve(encoded.size());
    auto wall_start = Clock::now();
    for (const auto& buf : encoded) {
        auto t0 = Clock::now();
        Message decoded;
        decode(buf.data(), buf.size(), 0, decoded);
        seq.feed(decoded);
        auto t1 = Clock::now();
        samples.push_back(std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());
    }
    auto wall_end = Clock::now();
    return summarize(samples, std::chrono::duration<double>(wall_end - wall_start).count());
}

int main(int argc, char** argv) {
    std::size_t n = 500'000;
    if (argc > 1) n = static_cast<std::size_t>(std::stoull(argv[1]));

    std::printf("Generating %zu synthetic messages...\n\n", n);
    std::printf("%-32s %10s %14s %10s %10s %10s\n",
                "scenario", "ops", "ops/sec", "mean_ns", "p50_ns", "p99_ns");
    std::printf("%s\n", std::string(100, '-').c_str());

    {
        SimulatedChannel channel(1, /*drop=*/0.0, /*window=*/1);
        auto generated = channel.generate(n);
        auto delivered = channel.deliver(generated); // no loss, no reorder -- best case
        auto st = run_pipeline_bench(delivered);
        print_row("clean feed (no loss/reorder)", st);
    }
    {
        SimulatedChannel channel(1, /*drop=*/0.02, /*window=*/8);
        auto generated = channel.generate(n);
        auto delivered = channel.deliver(generated);
        auto st = run_pipeline_bench(delivered);
        print_row("lossy + reordering feed (2% drop)", st);
    }
    {
        SimulatedChannel channel(1, /*drop=*/0.10, /*window=*/20);
        auto generated = channel.generate(n);
        auto delivered = channel.deliver(generated);
        auto st = run_pipeline_bench(delivered);
        print_row("heavily degraded feed (10% drop)", st);
    }

    std::printf("\nNote: 'ops' counts messages actually delivered to the pipeline "
                "(post-drop), timing decode+sequence+book-apply per message.\n");
    return 0;
}
