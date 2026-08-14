#pragma once
#include <cstdint>
#include <cstring>
#include <vector>

// --------------------------------------------------------------------------
// A simplified ITCH/FIX-style binary market data protocol.
//
// Every message starts with a common header (sequence number + type +
// payload length), so a feed handler can walk a byte stream without
// knowing message-specific layouts in advance — exactly how real
// exchange feeds (Nasdaq ITCH, CME MDP3, etc.) are structured, just
// without their bit-packing tricks.
//
// All multi-byte fields are written in the host's native byte order via
// memcpy (this is a simulator/demo, not a wire-compatible implementation
// of any real exchange protocol — see README for that caveat).
// --------------------------------------------------------------------------
namespace feed {

using SeqNum   = std::uint32_t;
using OrderId  = std::uint64_t;
using Price    = std::int64_t;   // integer ticks
using Quantity = std::uint32_t;

enum class MsgType : std::uint8_t {
    Add    = 1,
    Modify = 2,   // change resting quantity (partial fill or requote)
    Delete = 3,
    Trade  = 4,   // execution against a resting order
};

enum class Side : std::uint8_t { Buy = 0, Sell = 1 };

#pragma pack(push, 1)
struct MsgHeader {
    SeqNum  seq;
    MsgType type;
    std::uint16_t payload_len;
};

struct AddPayload {
    OrderId  order_id;
    Side     side;
    Price    price;
    Quantity qty;
};

struct ModifyPayload {
    OrderId  order_id;
    Quantity new_qty;
};

struct DeletePayload {
    OrderId order_id;
};

struct TradePayload {
    OrderId  order_id;
    Price    price;
    Quantity qty;
};
#pragma pack(pop)

// A decoded, in-memory representation of any message type — what the
// rest of the pipeline (Sequencer, BookBuilder) actually works with,
// as opposed to raw bytes.
struct Message {
    SeqNum  seq = 0;
    MsgType type = MsgType::Add;
    OrderId order_id = 0;
    Side    side = Side::Buy;
    Price   price = 0;
    Quantity qty = 0;
};

// Encodes one Message into `out`, appending bytes (header + payload).
inline void encode(const Message& m, std::vector<std::uint8_t>& out) {
    MsgHeader hdr{};
    hdr.seq = m.seq;
    hdr.type = m.type;

    switch (m.type) {
        case MsgType::Add: {
            AddPayload p{m.order_id, m.side, m.price, m.qty};
            hdr.payload_len = sizeof(p);
            out.resize(out.size() + sizeof(hdr) + sizeof(p));
            std::memcpy(out.data() + out.size() - sizeof(hdr) - sizeof(p), &hdr, sizeof(hdr));
            std::memcpy(out.data() + out.size() - sizeof(p), &p, sizeof(p));
            break;
        }
        case MsgType::Modify: {
            ModifyPayload p{m.order_id, m.qty};
            hdr.payload_len = sizeof(p);
            out.resize(out.size() + sizeof(hdr) + sizeof(p));
            std::memcpy(out.data() + out.size() - sizeof(hdr) - sizeof(p), &hdr, sizeof(hdr));
            std::memcpy(out.data() + out.size() - sizeof(p), &p, sizeof(p));
            break;
        }
        case MsgType::Delete: {
            DeletePayload p{m.order_id};
            hdr.payload_len = sizeof(p);
            out.resize(out.size() + sizeof(hdr) + sizeof(p));
            std::memcpy(out.data() + out.size() - sizeof(hdr) - sizeof(p), &hdr, sizeof(hdr));
            std::memcpy(out.data() + out.size() - sizeof(p), &p, sizeof(p));
            break;
        }
        case MsgType::Trade: {
            TradePayload p{m.order_id, m.price, m.qty};
            hdr.payload_len = sizeof(p);
            out.resize(out.size() + sizeof(hdr) + sizeof(p));
            std::memcpy(out.data() + out.size() - sizeof(hdr) - sizeof(p), &hdr, sizeof(hdr));
            std::memcpy(out.data() + out.size() - sizeof(p), &p, sizeof(p));
            break;
        }
    }
}

// Decodes exactly one message starting at `data[offset]`. Returns the
// number of bytes consumed (header + payload), or 0 if there isn't a
// full message available yet (caller should wait for more bytes).
inline std::size_t decode(const std::uint8_t* data, std::size_t available, std::size_t offset, Message& out) {
    if (available < offset + sizeof(MsgHeader)) return 0;
    MsgHeader hdr{};
    std::memcpy(&hdr, data + offset, sizeof(hdr));
    std::size_t total = sizeof(MsgHeader) + hdr.payload_len;
    if (available < offset + total) return 0;

    out.seq = hdr.seq;
    out.type = hdr.type;
    const std::uint8_t* payload = data + offset + sizeof(MsgHeader);

    switch (hdr.type) {
        case MsgType::Add: {
            AddPayload p{};
            std::memcpy(&p, payload, sizeof(p));
            out.order_id = p.order_id; out.side = p.side; out.price = p.price; out.qty = p.qty;
            break;
        }
        case MsgType::Modify: {
            ModifyPayload p{};
            std::memcpy(&p, payload, sizeof(p));
            out.order_id = p.order_id; out.qty = p.new_qty;
            break;
        }
        case MsgType::Delete: {
            DeletePayload p{};
            std::memcpy(&p, payload, sizeof(p));
            out.order_id = p.order_id;
            break;
        }
        case MsgType::Trade: {
            TradePayload p{};
            std::memcpy(&p, payload, sizeof(p));
            out.order_id = p.order_id; out.price = p.price; out.qty = p.qty;
            break;
        }
    }
    return total;
}

} // namespace feed
