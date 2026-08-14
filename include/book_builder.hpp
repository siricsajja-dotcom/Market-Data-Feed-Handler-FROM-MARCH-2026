#pragma once
#include <map>
#include <unordered_map>
#include <functional>
#include "protocol.hpp"

namespace feed {

// --------------------------------------------------------------------------
// BookBuilder
//
// Applies a gap-free, in-order stream of decoded messages (as produced by
// Sequencer) to reconstruct a price-level-aggregated order book. This is
// deliberately a plain std::map-based structure — the systems-performance
// story for order books lives in the companion `matching-engine` project;
// this project's job is correctly and robustly turning a raw, unreliable
// wire feed into a trustworthy book, which is a different (and equally
// real) piece of market-data infrastructure.
// --------------------------------------------------------------------------
class BookBuilder {
public:
    void apply(const Message& m) {
        switch (m.type) {
            case MsgType::Add:    apply_add(m); break;
            case MsgType::Modify: apply_modify(m); break;
            case MsgType::Delete: apply_delete(m); break;
            case MsgType::Trade:  apply_trade(m); break;
        }
    }

    bool best_bid(Price& out) const {
        if (bids_.empty()) return false;
        out = bids_.begin()->first;
        return true;
    }
    bool best_ask(Price& out) const {
        if (asks_.empty()) return false;
        out = asks_.begin()->first;
        return true;
    }
    Quantity qty_at_price(Side side, Price price) const {
        if (side == Side::Buy) {
            auto it = bids_.find(price);
            return it == bids_.end() ? 0 : it->second;
        }
        auto it = asks_.find(price);
        return it == asks_.end() ? 0 : it->second;
    }
    std::size_t order_count() const { return orders_.size(); }
    bool has_order(OrderId id) const { return orders_.count(id) != 0; }
    Quantity order_qty(OrderId id) const {
        auto it = orders_.find(id);
        return it == orders_.end() ? 0 : it->second.qty;
    }

private:
    struct RestingOrder { Side side; Price price; Quantity qty; };

    std::unordered_map<OrderId, RestingOrder> orders_;
    std::map<Price, Quantity, std::greater<Price>> bids_; // best bid = highest price, first
    std::map<Price, Quantity, std::less<Price>>    asks_; // best ask = lowest price, first

    void adjust_level(Side side, Price price, std::int64_t delta_qty) {
        if (side == Side::Buy) {
            auto& q = bids_[price];
            q = static_cast<Quantity>(static_cast<std::int64_t>(q) + delta_qty);
            if (q == 0) bids_.erase(price);
        } else {
            auto& q = asks_[price];
            q = static_cast<Quantity>(static_cast<std::int64_t>(q) + delta_qty);
            if (q == 0) asks_.erase(price);
        }
    }

    void apply_add(const Message& m) {
        // A duplicate Add for an already-known order id is ignored rather
        // than corrupting state — defensive against a retransmitted
        // message slipping through as a "new" one.
        if (orders_.count(m.order_id)) return;
        orders_[m.order_id] = RestingOrder{m.side, m.price, m.qty};
        adjust_level(m.side, m.price, static_cast<std::int64_t>(m.qty));
    }

    void apply_modify(const Message& m) {
        auto it = orders_.find(m.order_id);
        if (it == orders_.end()) return; // modify for unknown order: ignore defensively
        std::int64_t delta = static_cast<std::int64_t>(m.qty) - static_cast<std::int64_t>(it->second.qty);
        adjust_level(it->second.side, it->second.price, delta);
        it->second.qty = m.qty;
        if (it->second.qty == 0) orders_.erase(it);
    }

    void apply_delete(const Message& m) {
        auto it = orders_.find(m.order_id);
        if (it == orders_.end()) return;
        adjust_level(it->second.side, it->second.price, -static_cast<std::int64_t>(it->second.qty));
        orders_.erase(it);
    }

    void apply_trade(const Message& m) {
        // A trade reduces the resting order's remaining quantity by the
        // traded amount (partial fill), removing it entirely if fully
        // filled — same effect as a Modify/Delete but semantically an
        // execution rather than a cancel/requote.
        auto it = orders_.find(m.order_id);
        if (it == orders_.end()) return;
        Quantity new_qty = (m.qty >= it->second.qty) ? 0 : (it->second.qty - m.qty);
        std::int64_t delta = static_cast<std::int64_t>(new_qty) - static_cast<std::int64_t>(it->second.qty);
        adjust_level(it->second.side, it->second.price, delta);
        if (new_qty == 0) {
            orders_.erase(it);
        } else {
            it->second.qty = new_qty;
        }
    }
};

} // namespace feed
