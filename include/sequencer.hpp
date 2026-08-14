#pragma once
#include <cstdint>
#include <functional>
#include <map>
#include "protocol.hpp"

namespace feed {

// --------------------------------------------------------------------------
// Sequencer
//
// Sits between the raw (possibly lossy, possibly reordering) feed and the
// book builder. Real exchange multicast feeds arrive over UDP: packets can
// be dropped, duplicated, or reordered, and the feed handler's job is to
// present a single ordered, gap-free stream of application messages to
// everything downstream — while telling you clearly when a gap could NOT
// be filled in time, since silently skipping a gap produces a corrupt book.
//
// Behavior:
//   - in-order arrival -> dispatched immediately.
//   - early arrival (seq > expected) -> buffered; a gap timer starts.
//   - late/duplicate arrival (seq < expected) -> dropped, counted.
//   - a message that fills the buffered range -> drains everything now
//     contiguous, in order.
//   - if the gap isn't filled within `gap_timeout_msgs` further arrivals,
//     it's declared unrecoverable: on_gap fires with the missing range,
//     the sequencer resyncs forward to the earliest still-useful seq, and
//     draining continues from there. This models requesting/waiting on a
//     retransmission channel and giving up after a bounded window rather
//     than blocking the whole feed forever on one lost packet.
// --------------------------------------------------------------------------
class Sequencer {
public:
    using OnMessage = std::function<void(const Message&)>;
    using OnGap     = std::function<void(SeqNum missing_from, SeqNum missing_to)>;

    struct Stats {
        std::uint64_t dispatched = 0;
        std::uint64_t duplicates_dropped = 0;
        std::uint64_t out_of_order_buffered = 0;
        std::uint64_t gaps_declared = 0;
        std::uint64_t messages_lost_to_gaps = 0;
    };

    Sequencer(OnMessage on_message, OnGap on_gap,
              SeqNum start_seq = 1, std::size_t gap_timeout_msgs = 50)
        : expected_(start_seq), on_message_(std::move(on_message)),
          on_gap_(std::move(on_gap)), gap_timeout_msgs_(gap_timeout_msgs) {}

    // Feed one message, whether it arrived on the live/primary channel or
    // as an out-of-band retransmission — both go through the same logic.
    void feed(const Message& m) {
        if (m.seq < expected_) {
            stats_.duplicates_dropped++;
            return;
        }
        if (m.seq == expected_) {
            dispatch(m);
            drain();
            return;
        }
        // m.seq > expected_: out-of-order / early arrival.
        pending_[m.seq] = m;
        stats_.out_of_order_buffered++;
        if (!gap_pending_) {
            gap_pending_ = true;
            gap_start_ = expected_;
            msgs_since_gap_started_ = 0;
        }
        msgs_since_gap_started_++;
        if (msgs_since_gap_started_ >= gap_timeout_msgs_) {
            declare_unrecoverable_gap();
        }
    }

    const Stats& stats() const { return stats_; }
    SeqNum expected() const { return expected_; }
    std::size_t buffered_count() const { return pending_.size(); }

private:
    SeqNum expected_;
    std::map<SeqNum, Message> pending_;
    OnMessage on_message_;
    OnGap on_gap_;
    std::size_t gap_timeout_msgs_;
    std::size_t msgs_since_gap_started_ = 0;
    bool gap_pending_ = false;
    SeqNum gap_start_ = 0;
    Stats stats_;

    void dispatch(const Message& m) {
        on_message_(m);
        expected_ = m.seq + 1;
        stats_.dispatched++;
    }

    void drain() {
        while (true) {
            auto it = pending_.find(expected_);
            if (it == pending_.end()) break;
            Message m = it->second;
            pending_.erase(it);
            dispatch(m);
        }
        // Any drain (even one that resolves nothing new) reflects the
        // current state of forward progress: if nothing is buffered
        // anymore, the gap is fully closed. If something is still
        // buffered, we've made whatever progress was possible right now,
        // so the "no progress" timeout window restarts from the current
        // frontier -- an old, already-resolved stretch must never be
        // folded into a gap declared much later.
        if (pending_.empty()) {
            gap_pending_ = false;
        } else {
            gap_pending_ = true;
            gap_start_ = expected_;
        }
        msgs_since_gap_started_ = 0;
    }

    // Give up waiting at the current frontier (gap_start_ == expected_,
    // kept in sync by drain()). The earliest seq we actually have
    // buffered tells us exactly how much was lost — everything from
    // gap_start_ up to (but not including) that seq is permanently
    // missing; everything at or after it just needs draining in order.
    // (A real system would also flag the book as suspect / trigger a
    // snapshot resync here; this harness reports it via on_gap.)
    void declare_unrecoverable_gap() {
        if (pending_.empty()) return; // nothing buffered -- nothing to resync to
        SeqNum smallest_buffered = pending_.begin()->first;
        SeqNum lost_from = gap_start_;
        SeqNum lost_to = smallest_buffered - 1;
        if (lost_to >= lost_from) {
            stats_.gaps_declared++;
            stats_.messages_lost_to_gaps += (lost_to - lost_from + 1);
            on_gap_(lost_from, lost_to);
        }
        expected_ = smallest_buffered;
        drain(); // dispatches smallest_buffered and any now-contiguous run after it
    }
};

} // namespace feed
