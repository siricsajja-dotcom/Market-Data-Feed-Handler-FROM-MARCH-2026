#include <cassert>
#include <cstdio>
#include <vector>
#include "protocol.hpp"
#include "sequencer.hpp"
#include "book_builder.hpp"
#include "feed_source.hpp"

using namespace feed;

static int g_failures = 0;
#define CHECK(cond) do { \
    if (!(cond)) { std::printf("FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond); g_failures++; } \
} while (0)

static void test_encode_decode_roundtrip() {
    std::vector<Message> originals = {
        Message{1, MsgType::Add,    100, Side::Buy,  9'990, 50},
        Message{2, MsgType::Add,    101, Side::Sell, 10'010, 30},
        Message{3, MsgType::Modify, 100, Side::Buy,  0,      20},
        Message{4, MsgType::Trade,  101, Side::Sell, 10'010, 15},
        Message{5, MsgType::Delete, 100, Side::Buy,  0,      0},
    };
    std::vector<std::uint8_t> buf;
    for (const auto& m : originals) encode(m, buf);

    std::size_t offset = 0;
    for (std::size_t i = 0; i < originals.size(); ++i) {
        Message decoded;
        std::size_t consumed = decode(buf.data(), buf.size(), offset, decoded);
        CHECK(consumed > 0);
        CHECK(decoded.seq == originals[i].seq);
        CHECK(decoded.type == originals[i].type);
        CHECK(decoded.order_id == originals[i].order_id);
        if (originals[i].type == MsgType::Add) {
            CHECK(decoded.side == originals[i].side);
            CHECK(decoded.price == originals[i].price);
            CHECK(decoded.qty == originals[i].qty);
        }
        offset += consumed;
    }
    CHECK(offset == buf.size());
}

static void test_decode_returns_zero_on_incomplete_buffer() {
    Message m{1, MsgType::Add, 1, Side::Buy, 100, 10};
    std::vector<std::uint8_t> buf;
    encode(m, buf);
    Message out;
    // truncate the buffer -- decode must report "not enough bytes" (0),
    // not read past the end.
    std::size_t consumed = decode(buf.data(), buf.size() - 1, 0, out);
    CHECK(consumed == 0);
}

static void test_sequencer_in_order_dispatches_immediately() {
    std::vector<SeqNum> received;
    Sequencer seq(
        [&](const Message& m) { received.push_back(m.seq); },
        [&](SeqNum, SeqNum) { CHECK(false && "no gap expected"); }
    );
    for (SeqNum s = 1; s <= 5; ++s) {
        seq.feed(Message{s, MsgType::Add, s, Side::Buy, 100, 1});
    }
    CHECK((received == std::vector<SeqNum>{1, 2, 3, 4, 5}));
    CHECK(seq.stats().dispatched == 5);
    CHECK(seq.stats().gaps_declared == 0);
}

static void test_sequencer_buffers_and_drains_out_of_order() {
    std::vector<SeqNum> received;
    Sequencer seq(
        [&](const Message& m) { received.push_back(m.seq); },
        [&](SeqNum, SeqNum) { CHECK(false && "no gap expected"); }
    );
    // deliver 1, 3, 2, 4 -- sequencer must still dispatch in order 1,2,3,4
    seq.feed(Message{1, MsgType::Add, 1, Side::Buy, 100, 1});
    seq.feed(Message{3, MsgType::Add, 3, Side::Buy, 100, 1});
    CHECK(seq.buffered_count() == 1); // 3 is waiting on 2
    seq.feed(Message{2, MsgType::Add, 2, Side::Buy, 100, 1});
    seq.feed(Message{4, MsgType::Add, 4, Side::Buy, 100, 1});
    CHECK((received == std::vector<SeqNum>{1, 2, 3, 4}));
    CHECK(seq.buffered_count() == 0);
}

static void test_sequencer_declares_unrecoverable_gap_after_timeout() {
    std::vector<SeqNum> received;
    std::vector<std::pair<SeqNum, SeqNum>> gaps;
    Sequencer seq(
        [&](const Message& m) { received.push_back(m.seq); },
        [&](SeqNum from, SeqNum to) { gaps.push_back({from, to}); },
        /*start_seq=*/1, /*gap_timeout_msgs=*/3
    );
    seq.feed(Message{1, MsgType::Add, 1, Side::Buy, 100, 1});
    // seq 2 is permanently missing; feed enough later messages to exceed
    // the gap timeout and force a declared, unrecoverable gap.
    seq.feed(Message{3, MsgType::Add, 3, Side::Buy, 100, 1});
    seq.feed(Message{4, MsgType::Add, 4, Side::Buy, 100, 1});
    seq.feed(Message{5, MsgType::Add, 5, Side::Buy, 100, 1}); // 3rd early arrival -> timeout fires

    CHECK(gaps.size() == 1);
    CHECK(gaps[0].first == 2 && gaps[0].second == 2);
    CHECK(seq.stats().messages_lost_to_gaps == 1);
    // after the gap is declared, expected_ resyncs to 5 and drains buffered 3,4,5
    CHECK((received == std::vector<SeqNum>{1, 3, 4, 5}));
}

static void test_sequencer_drops_duplicates() {
    std::vector<SeqNum> received;
    Sequencer seq(
        [&](const Message& m) { received.push_back(m.seq); },
        [&](SeqNum, SeqNum) {}
    );
    seq.feed(Message{1, MsgType::Add, 1, Side::Buy, 100, 1});
    seq.feed(Message{1, MsgType::Add, 1, Side::Buy, 100, 1}); // duplicate
    seq.feed(Message{2, MsgType::Add, 2, Side::Buy, 100, 1});
    CHECK((received == std::vector<SeqNum>{1, 2}));
    CHECK(seq.stats().duplicates_dropped == 1);
}

static void test_book_builder_add_modify_delete() {
    BookBuilder book;
    book.apply(Message{1, MsgType::Add, 1, Side::Buy, 9'990, 100});
    CHECK(book.order_count() == 1);
    Price bb;
    CHECK(book.best_bid(bb) && bb == 9'990);
    CHECK(book.qty_at_price(Side::Buy, 9'990) == 100);

    book.apply(Message{2, MsgType::Modify, 1, Side::Buy, 0, 40});
    CHECK(book.qty_at_price(Side::Buy, 9'990) == 40);
    CHECK(book.order_qty(1) == 40);

    book.apply(Message{3, MsgType::Delete, 1, Side::Buy, 0, 0});
    CHECK(book.order_count() == 0);
    CHECK(!book.best_bid(bb));
}

static void test_book_builder_trade_partial_and_full() {
    BookBuilder book;
    book.apply(Message{1, MsgType::Add, 1, Side::Sell, 10'010, 50});
    book.apply(Message{2, MsgType::Trade, 1, Side::Sell, 10'010, 20}); // partial
    CHECK(book.order_qty(1) == 30);
    CHECK(book.qty_at_price(Side::Sell, 10'010) == 30);

    book.apply(Message{3, MsgType::Trade, 1, Side::Sell, 10'010, 30}); // fully fills
    CHECK(!book.has_order(1));
    Price ba;
    CHECK(!book.best_ask(ba));
}

static void test_book_builder_ignores_ops_on_unknown_order() {
    BookBuilder book;
    // Modify/Delete/Trade for an order id the book never saw an Add for
    // (e.g. because it was lost in an unrecoverable gap) must not crash
    // or corrupt state -- just be ignored defensively.
    book.apply(Message{1, MsgType::Modify, 999, Side::Buy, 0, 10});
    book.apply(Message{2, MsgType::Delete, 999, Side::Buy, 0, 0});
    book.apply(Message{3, MsgType::Trade, 999, Side::Buy, 100, 5});
    CHECK(book.order_count() == 0);
}

// End-to-end: run the simulated lossy/reordering channel through the
// Sequencer into a BookBuilder, and cross-check against a BookBuilder
// fed the perfectly-ordered original stream directly. Any messages lost
// to an unrecoverable gap are also applied to a THIRD "gap-aware
// reference" book that skips exactly those messages -- that's the book
// the sequenced pipeline should match, since it genuinely never saw them.
static void test_end_to_end_matches_reference_after_loss_and_reorder() {
    SimulatedChannel channel(/*seed=*/7, /*drop_prob=*/0.02, /*reorder_window=*/5);
    auto generated = channel.generate(5000);
    auto delivered = channel.deliver(generated);

    BookBuilder pipeline_book;
    std::vector<std::pair<SeqNum, SeqNum>> gaps;
    Sequencer seq(
        [&](const Message& m) { pipeline_book.apply(m); },
        [&](SeqNum from, SeqNum to) { gaps.push_back({from, to}); },
        /*start_seq=*/1, /*gap_timeout_msgs=*/20
    );
    for (const auto& m : delivered) seq.feed(m);

    // Build the reference book: apply every generated message EXCEPT
    // those whose seq falls inside a declared-lost gap range.
    BookBuilder reference_book;
    auto in_a_gap = [&](SeqNum s) {
        for (auto& g : gaps) if (s >= g.first && s <= g.second) return true;
        return false;
    };
    for (const auto& m : generated) {
        if (!in_a_gap(m.seq)) reference_book.apply(m);
    }

    // Correctness invariant 1: declared gap ranges must never overlap —
    // an overlap would mean some already-resolved range got folded into
    // a later gap declaration.
    for (std::size_t i = 1; i < gaps.size(); ++i) {
        CHECK(gaps[i].first > gaps[i - 1].second); // gap ranges must never overlap
    }

    // Correctness invariant 2: dispatched + declared-lost + still-buffered
    // (anything left mid-recovery when the synthetic stream simply ends)
    // must never EXCEED what was generated -- exceeding it would mean a
    // message got attributed to more than one bucket.
    std::uint64_t accounted = seq.stats().dispatched + seq.stats().messages_lost_to_gaps
                             + seq.buffered_count();
    CHECK(accounted <= generated.size());

    // Correctness invariant 3: the reconstructed book still agrees with a
    // reference book built by skipping exactly the declared-lost ranges --
    // i.e. gap accounting is precise enough that nothing else diverged.
    Price pb = 0, rb = 0, pa = 0, ra = 0;
    bool p_has_bid = pipeline_book.best_bid(pb), r_has_bid = reference_book.best_bid(rb);
    bool p_has_ask = pipeline_book.best_ask(pa), r_has_ask = reference_book.best_ask(ra);
    CHECK(p_has_bid == r_has_bid);
    if (p_has_bid) CHECK(pb == rb);
    CHECK(p_has_ask == r_has_ask);
    if (p_has_ask) CHECK(pa == ra);

    std::printf("  (end-to-end: %zu generated, %zu delivered, %llu dispatched, "
                "%llu duplicates, %llu gaps declared, %llu msgs lost to gaps, %zu still buffered)\n",
                generated.size(), delivered.size(),
                (unsigned long long)seq.stats().dispatched,
                (unsigned long long)seq.stats().duplicates_dropped,
                (unsigned long long)seq.stats().gaps_declared,
                (unsigned long long)seq.stats().messages_lost_to_gaps,
                seq.buffered_count());
}

int main() {
    test_encode_decode_roundtrip();
    test_decode_returns_zero_on_incomplete_buffer();
    test_sequencer_in_order_dispatches_immediately();
    test_sequencer_buffers_and_drains_out_of_order();
    test_sequencer_declares_unrecoverable_gap_after_timeout();
    test_sequencer_drops_duplicates();
    test_book_builder_add_modify_delete();
    test_book_builder_trade_partial_and_full();
    test_book_builder_ignores_ops_on_unknown_order();
    test_end_to_end_matches_reference_after_loss_and_reorder();

    if (g_failures == 0) {
        std::printf("All tests passed.\n");
        return 0;
    }
    std::printf("%d test(s) FAILED.\n", g_failures);
    return 1;
}
