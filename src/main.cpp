// Small runnable demo: simulate a lossy/reordering feed, run it through
// the Sequencer into a BookBuilder, and print what the sequencer had to
// deal with plus the resulting top-of-book.
#include <cstdio>
#include "protocol.hpp"
#include "sequencer.hpp"
#include "book_builder.hpp"
#include "feed_source.hpp"

using namespace feed;

int main() {
    SimulatedChannel channel(/*seed=*/42, /*drop_prob=*/0.02, /*reorder_window=*/4);
    auto generated = channel.generate(3000);
    auto delivered = channel.deliver(generated);

    std::printf("Generated %zu messages, %zu survived the simulated lossy/reordering channel.\n",
                generated.size(), delivered.size());

    BookBuilder book;
    int gap_events = 0;
    Sequencer sequencer(
        [&](const Message& m) { book.apply(m); },
        [&](SeqNum from, SeqNum to) {
            gap_events++;
            std::printf("  [gap] declared unrecoverable: seq [%u, %u] (%u message%s) lost\n",
                        from, to, to - from + 1, (to - from) ? "s" : "");
        },
        /*start_seq=*/1, /*gap_timeout_msgs=*/15
    );

    for (const auto& m : delivered) sequencer.feed(m);

    const auto& stats = sequencer.stats();
    std::printf("\nSequencer stats:\n");
    std::printf("  dispatched:            %llu\n", (unsigned long long)stats.dispatched);
    std::printf("  duplicates dropped:    %llu\n", (unsigned long long)stats.duplicates_dropped);
    std::printf("  out-of-order buffered: %llu\n", (unsigned long long)stats.out_of_order_buffered);
    std::printf("  gaps declared:         %llu\n", (unsigned long long)stats.gaps_declared);
    std::printf("  messages lost to gaps: %llu\n", (unsigned long long)stats.messages_lost_to_gaps);
    std::printf("  still buffered (unresolved at end of stream): %zu\n", sequencer.buffered_count());

    Price bid, ask;
    bool has_bid = book.best_bid(bid), has_ask = book.best_ask(ask);
    std::printf("\nReconstructed book: bid=%s ask=%s (%zu resting orders)\n",
                has_bid ? std::to_string(bid).c_str() : "-",
                has_ask ? std::to_string(ask).c_str() : "-",
                book.order_count());
    return 0;
}
