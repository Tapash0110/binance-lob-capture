#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <array>
#include <atomic>

namespace lob {

enum class Venue : uint8_t {
    Spot,
    Usdm
};

inline const char* venue_to_string(Venue v) noexcept {
    switch (v) {
        case Venue::Spot: return "spot";
        case Venue::Usdm: return "usdm";
    }
    return "spot";
}

inline Venue string_to_venue(std::string_view sv) noexcept {
    if (sv == "usdm" || sv == "futures") return Venue::Usdm;
    return Venue::Spot;
}

enum class StreamKind : uint8_t {
    DepthDiff,
    Depth5,
    Trade
};

inline const char* stream_kind_to_string(StreamKind k) noexcept {
    switch (k) {
        case StreamKind::DepthDiff: return "depth_diff";
        case StreamKind::Depth5:    return "depth5";
        case StreamKind::Trade:     return "trade";
    }
    return "unknown";
}

inline StreamKind string_to_stream_kind(std::string_view sv) noexcept {
    if (sv == "depth_diff" || sv == "0") return StreamKind::DepthDiff;
    if (sv == "depth5" || sv == "1")     return StreamKind::Depth5;
    if (sv == "trade" || sv == "2")      return StreamKind::Trade;
    return StreamKind::DepthDiff;
}

struct PriceLevel {
    int64_t price{0};
    int64_t size{0};
};

struct MarketDataEvent {
    int64_t recv_tsec{0};
    int32_t recv_tnsec{0};
    Venue venue{Venue::Spot};
    StreamKind stream_kind{StreamKind::DepthDiff};
    uint32_t shard_id{0};
    uint32_t conn_epoch{0};
    uint64_t conn_seq{0};
    std::string symbol;
    std::string payload_json; // Minified one-line JSON payload
};

struct OrderBookSnapshot {
    int64_t tsec{0};
    int32_t tnsec{0};
    uint64_t seqNo{0};
    int32_t id{0};
    char type{'D'}; // 'D' = differential, 'S' = depth5 snapshot, 'T' = trade
    char side{'N'}; // 'B' = bid, 'S' = ask, 'N' = symmetric / both
    std::array<int64_t, 5> bids{0, 0, 0, 0, 0};
    std::array<int64_t, 5> bid_sizes{0, 0, 0, 0, 0};
    std::array<int64_t, 5> asks{0, 0, 0, 0, 0};
    std::array<int64_t, 5> ask_sizes{0, 0, 0, 0, 0};
};

struct AppMetrics {
    std::atomic<uint64_t> messages_received{0};
    std::atomic<uint64_t> messages_applied{0};
    std::atomic<uint64_t> parse_errors{0};
    std::atomic<uint64_t> reconnect_count{0};
    std::atomic<uint64_t> sequence_gaps{0};
    std::atomic<uint64_t> market_dump_rows{0};
    std::atomic<uint64_t> orderbook_rows{0};
};

} // namespace lob
