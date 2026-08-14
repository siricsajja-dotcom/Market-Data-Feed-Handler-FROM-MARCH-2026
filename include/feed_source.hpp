#pragma once
#include <algorithm>
#include <random>
#include <vector>
#include "protocol.hpp"

namespace feed {

// --------------------------------------------------------------------------
// SimulatedChannel
//
// Generates a synthetic, internally-consistent sequence of Add/Modify/
// Delete/Trade messages (a plausible order book's worth of traffic), then
// delivers them to a callback in an order that mimics a real lossy UDP
// multicast feed: some messages get reordered within a small jitter
// window, and some are dropped entirely (never delivered live — a real
// gap that only a retransmission/recovery channel could fill).
//
// This exists purely so the Sequencer and BookBuilder can be exercised
// and benchmarked against real disorder/loss without needing an actual
// network — deterministic given the same seed, so tests are reproducible.
// --------------------------------------------------------------------------
class SimulatedChannel {
public:
    SimulatedChannel(std::uint32_t seed, double drop_prob, std::size_t reorder_window)
        : rng_(seed), drop_prob_(drop_prob), reorder_window_(reorder_window) {}

    // Generates `n` sequential messages representing plausible order book
    // activity (mostly adds, some modifies/deletes/trades against
    // previously-added order ids) starting at seq `start_seq`. Buy/sell
    // adds are clamped against the running best opposite price so the
    // synthetic book never crosses -- a real exchange feed never shows a
    // crossed book either, since the exchange's own matching engine
    // resolves any cross before anything reaches the feed.
    std::vector<Message> generate(std::size_t n, SeqNum start_seq = 1) {
        std::vector<Message> out;
        out.reserve(n);
        std::vector<OrderId> live_ids;
        OrderId next_id = 1;
        std::uniform_real_distribution<double> action(0.0, 1.0);
        std::uniform_int_distribution<Quantity> qty_dist(1, 500);
        std::normal_distribution<double> price_offset(0.0, 20.0);
        std::uniform_int_distribution<int> side_dist(0, 1);
        const Price mid = 100'000;
        Price running_best_bid = mid - 10'000; // monotonic lower bound tracker
        Price running_best_ask = mid + 10'000; // monotonic upper bound tracker

        for (std::size_t i = 0; i < n; ++i) {
            Message m;
            m.seq = start_seq + static_cast<SeqNum>(i);
            double roll = action(rng_);
            if (roll < 0.55 || live_ids.empty()) {
                m.type = MsgType::Add;
                m.order_id = next_id++;
                m.side = side_dist(rng_) == 0 ? Side::Buy : Side::Sell;
                Price raw_price = mid + static_cast<Price>(price_offset(rng_));
                if (m.side == Side::Buy) {
                    m.price = std::min(raw_price, running_best_ask - 1);
                    running_best_bid = std::max(running_best_bid, m.price);
                } else {
                    m.price = std::max(raw_price, running_best_bid + 1);
                    running_best_ask = std::min(running_best_ask, m.price);
                }
                m.qty = qty_dist(rng_);
                live_ids.push_back(m.order_id);
            } else if (roll < 0.75) {
                m.type = MsgType::Modify;
                m.order_id = pick_live(live_ids);
                m.qty = qty_dist(rng_);
            } else if (roll < 0.90) {
                m.type = MsgType::Trade;
                m.order_id = pick_live(live_ids);
                m.qty = qty_dist(rng_) % 100 + 1;
                m.price = mid;
            } else {
                m.type = MsgType::Delete;
                std::uniform_int_distribution<std::size_t> pick(0, live_ids.size() - 1);
                std::size_t idx = pick(rng_);
                m.order_id = live_ids[idx];
                live_ids[idx] = live_ids.back();
                live_ids.pop_back();
            }
            out.push_back(m);
        }
        return out;
    }

    // Reorders within a small jitter window and drops some messages
    // entirely, simulating what actually arrives on the wire. The
    // returned vector is what a live feed handler would see; messages
    // dropped here are NOT included at all (they represent packet loss),
    // separate from `dropped_out` if the caller wants to inspect exactly
    // which sequence numbers were lost (e.g. to simulate a recovery feed
    // later filling some of them back in).
    std::vector<Message> deliver(const std::vector<Message>& generated, std::vector<Message>* dropped_out = nullptr) {
        std::vector<Message> survivors;
        survivors.reserve(generated.size());
        std::uniform_real_distribution<double> drop_roll(0.0, 1.0);

        for (const auto& m : generated) {
            if (drop_roll(rng_) < drop_prob_) {
                if (dropped_out) dropped_out->push_back(m);
                continue;
            }
            survivors.push_back(m);
        }
        // Shuffle within a bounded window to simulate reordering without
        // producing wildly out-of-order (unrealistic) delivery.
        for (std::size_t i = 0; i + 1 < survivors.size(); ++i) {
            std::size_t window_end = std::min(survivors.size(), i + reorder_window_);
            std::uniform_int_distribution<std::size_t> pick(i, window_end - 1);
            std::size_t j = pick(rng_);
            std::swap(survivors[i], survivors[j]);
        }
        return survivors;
    }

private:
    std::mt19937_64 rng_;
    double drop_prob_;
    std::size_t reorder_window_;

    OrderId pick_live(std::vector<OrderId>& live_ids) {
        std::uniform_int_distribution<std::size_t> pick(0, live_ids.size() - 1);
        return live_ids[pick(rng_)];
    }
};

} // namespace feed
