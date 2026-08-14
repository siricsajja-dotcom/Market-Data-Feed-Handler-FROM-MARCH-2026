# Market Data Feed Handler - July 2026

The "upstream half" of a trading system's book: turns a raw, unreliable
binary market-data feed into a trustworthy, gap-aware order book. Real
exchange multicast feeds arrive over UDP — packets get dropped,
duplicated, and reordered — and a feed handler's actual job is making
that mess look like a clean, ordered stream to everything downstream,
while being honest about exactly what it couldn't recover.

Pairs naturally with [`matching-engine`](../matching-engine): that
project is the "downstream half" — what happens once a book exists.
This one is what builds the book from the wire in the first place.

## What's in here

- **`protocol.hpp`** — a simplified ITCH/FIX-style binary protocol:
  Add/Modify/Delete/Trade messages, each with a sequence number, packed
  and unpacked from raw bytes.
- **`sequencer.hpp`** — the core piece. Buffers early/out-of-order
  arrivals, drops duplicates, and — if a gap isn't filled within a
  configurable number of further arrivals — declares it **unrecoverable**,
  reports exactly which sequence range was lost, and resyncs forward
  rather than blocking the entire feed on one dropped packet forever.
- **`book_builder.hpp`** — applies the sequenced, gap-free message stream
  to reconstruct a price-aggregated order book.
- **`feed_source.hpp`** — a deterministic, seeded simulator that
  generates plausible book activity and delivers it through a lossy,
  reordering channel, so the pipeline can be tested and benchmarked
  against real disorder without needing an actual network.

## Build & run

Requires only a C++20 compiler, no external dependencies.

```bash
make            # builds demo, bench_run, and the test binary; runs tests
./bin/demo      # feeds a simulated lossy channel through the pipeline
./bin/bench_run 500000
```

Or with CMake: `mkdir build && cd build && cmake .. && cmake --build . && ctest`.

### Sample demo output

```
Generated 3000 messages, 2947 survived the simulated lossy/reordering channel.
  [gap] declared unrecoverable: seq [11, 11] (1 message) lost
  [gap] declared unrecoverable: seq [56, 56] (1 message) lost
  ...
  [gap] declared unrecoverable: seq [1303, 1304] (2 messages) lost
  ...

Sequencer stats:
  dispatched:            2938
  duplicates dropped:    9
  out-of-order buffered: 2369
  gaps declared:         61
  messages lost to gaps: 62
  still buffered (unresolved at end of stream): 0

Reconstructed book: bid=99988 ask=99989 (1240 resting orders)
```

### Sample benchmark output

Measured on this machine (`g++ -O3 -march=native`), decode + sequence +
book-apply per message under three feed conditions:

```
scenario                                ops        ops/sec    mean_ns     p50_ns     p99_ns
----------------------------------------------------------------------------------------------
clean feed (no loss/reorder)         300000        5291291        163         89        667
lossy + reordering feed (2% drop)     293997        3334103        274         80       4893
heavily degraded feed (10% drop)     269904        3807351        237        127       2934
```

The p50 barely moves — most messages are still in-order and dispatch
immediately — but p99 blows out under a degraded feed, which is exactly
where the out-of-order buffering (a `std::map` insert per early arrival)
shows up. That's the real cost of loss/reorder tolerance, made visible
rather than hidden in an average.

## Correctness testing

`tests/test_feed_handler.cpp` covers protocol round-tripping, sequencer
gap/duplicate/reorder handling in isolation, book-builder message
semantics, and — the test that actually matters — an **end-to-end
reference check**: run the same synthetic order flow through the full
lossy-channel → sequencer → book pipeline, and separately build a
"reference" book by applying the original flow directly but skipping
exactly the sequence ranges the sequencer declared lost. The two books'
best bid/ask must agree, and no sequence number may appear both
dispatched and inside a declared-lost range.

I also ran this as a stress sweep — 30 random seeds × 4 drop rates
(0%–15%) × 4 reorder-window sizes, 480 combinations total — checking
that invariant plus "gap ranges never overlap." Two real bugs surfaced
and got fixed this way before landing here:

1. **Stale gap boundaries.** The sequencer originally didn't reset its
   "how long have we been stuck" window when partial forward progress
   happened mid-gap, so a gap declared much later could wrongly absorb
   an already-dispatched range into its reported loss. Fixed by
   re-anchoring the window on every drain, not just a full one.
2. **A crossed synthetic book.** The traffic generator initially placed
   buy/sell orders independently at random offsets from mid, which can
   (correctly, by construction) cross — but a real exchange feed never
   shows a crossed book, since the exchange's own matching engine
   resolves any cross before it reaches the feed. Fixed by clamping
   generated prices against a running best-opposite-price tracker.

Both are called out here rather than quietly fixed, because "I found
this with a stress sweep and fixed it before shipping" is a more
useful signal than a repo that never admits it needed one.

## Design notes / what's deliberately simplified

- **Not wire-compatible with any real exchange protocol.** The message
  format is deliberately simplified (native byte order, no bit-packing)
  — the point is the sequencing/recovery logic, not implementing
  Nasdaq ITCH or CME MDP3 byte-for-byte.
- **`BookBuilder` is a plain `std::map`, not the cache-friendly array
  book from `matching-engine`.** That performance story already exists
  in the companion project; this one's job is correctness and
  robustness of reconstruction, not book-access latency.
- **Gap recovery here is "give up and report," not "request a
  retransmission and wait."** A production feed handler would pair this
  with an actual recovery/snapshot channel; `Sequencer::feed()` accepts
  messages from any source uniformly, so wiring in real retransmission
  traffic is a matter of calling `feed()` from that channel too — the
  logic for merging it in is already there.
