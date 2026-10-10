#pragma once

#include "types.hpp"
#include "json_utils.hpp"

#include <map>
#include <vector>
#include <array>
#include <string>
#include <cstdint>

namespace lob {

class LocalOrderBook {
public:
    explicit LocalOrderBook(std::string symbol, int32_t instrument_id, Venue venue);

    const std::string& symbol() const noexcept { return symbol_; }
    int32_t instrument_id() const noexcept { return instrument_id_; }
    Venue venue() const noexcept { return venue_; }
    bool is_initialized() const noexcept { return is_initialized_; }

    /**
     * @brief Resets order book state upon reconnect (new conn_epoch).
     */
    void on_reconnect(uint32_t new_conn_epoch);

    /**
     * @brief Applies a parsed delta (depth_diff, depth5, or trade) to the order book.
     * @param delta Parsed event details
     * @param tsec Event timestamp seconds
     * @param tnsec Event timestamp nanoseconds
     * @param conn_epoch Current connection epoch
     * @param snapshot_out Populated snapshot row if book emitted an update
     * @param metrics Application metrics to update
     * @param include_trades Whether trades emit an orderbook snapshot
     * @return true if an order book snapshot should be emitted to CSV
     */
    bool apply_event(
        const ParsedDelta& delta,
        int64_t tsec,
        int32_t tnsec,
        uint32_t conn_epoch,
        OrderBookSnapshot& snapshot_out,
        AppMetrics& metrics,
        bool include_trades = false
    );

    /**
     * @brief Populate top 5 bids and asks into arrays.
     */
    void get_top5(
        std::array<int64_t, 5>& bids,
        std::array<int64_t, 5>& bid_sizes,
        std::array<int64_t, 5>& asks,
        std::array<int64_t, 5>& ask_sizes
    ) const;

    size_t bid_depth() const noexcept { return bids_.size(); }
    size_t ask_depth() const noexcept { return asks_.size(); }

    void clear();

private:
    void apply_depth_diff(const ParsedDelta& delta, char& out_side);
    void apply_depth5_replace(const ParsedDelta& delta);
    bool check_sequence(const ParsedDelta& delta, uint32_t conn_epoch, AppMetrics& metrics);

    std::string symbol_;
    int32_t instrument_id_{0};
    Venue venue_{Venue::Spot};

    bool is_initialized_{false};
    uint32_t current_conn_epoch_{0};
    uint64_t last_diff_u_{0};    // u on depth_diff stream
    uint64_t last_depth5_u_{0};  // u / lastUpdateId on depth5 stream
    uint64_t seq_no_{0};         // Monotonic sequence per symbol orderbook rows

    // Price -> Size maps
    // Bids sorted descending (highest price first)
    std::map<int64_t, int64_t, std::greater<int64_t>> bids_;
    // Asks sorted ascending (lowest price first)
    std::map<int64_t, int64_t, std::less<int64_t>> asks_;
};

} // namespace lob
