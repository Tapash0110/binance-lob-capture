#include "lob.hpp"

namespace lob {

LocalOrderBook::LocalOrderBook(std::string symbol, int32_t instrument_id, Venue venue)
    : symbol_(std::move(symbol)),
      instrument_id_(instrument_id),
      venue_(venue) {}

void LocalOrderBook::on_reconnect(uint32_t new_conn_epoch) {
    current_conn_epoch_ = new_conn_epoch;
    last_diff_u_ = 0;
    last_depth5_u_ = 0;
    is_initialized_ = false;
    // On reconnect, we keep the book but await the next depth5 to resync top-of-book
}

void LocalOrderBook::clear() {
    bids_.clear();
    asks_.clear();
    last_diff_u_ = 0;
    last_depth5_u_ = 0;
    is_initialized_ = false;
}

bool LocalOrderBook::check_sequence(const ParsedDelta& delta, uint32_t conn_epoch, AppMetrics& metrics) {
    if (conn_epoch != current_conn_epoch_) {
        on_reconnect(conn_epoch);
        metrics.sequence_gaps.fetch_add(1, std::memory_order_relaxed);
    }

    if (delta.stream_kind == StreamKind::Depth5) {
        if (delta.final_update_id > 0) {
            last_depth5_u_ = delta.final_update_id;
        }
        is_initialized_ = true;
        return true;
    }

    if (delta.stream_kind == StreamKind::DepthDiff) {
        if (venue_ == Venue::Usdm) {
            // USD-M Futures: diff stream tracks prev_update_id (pu == last_diff_u_)
            if (last_diff_u_ > 0 && delta.prev_update_id > 0) {
                if (delta.prev_update_id != last_diff_u_) {
                    metrics.sequence_gaps.fetch_add(1, std::memory_order_relaxed);
                }
            }
        } else {
            // Spot: delta has first_update_id (U) and final_update_id (u)
            if (last_diff_u_ > 0 && delta.first_update_id > 0) {
                if (delta.first_update_id > last_diff_u_ + 1) {
                    metrics.sequence_gaps.fetch_add(1, std::memory_order_relaxed);
                }
            }
        }
        if (delta.final_update_id > 0) {
            last_diff_u_ = delta.final_update_id;
        }
        is_initialized_ = true;
        return true;
    }

    return true;
}

void LocalOrderBook::apply_depth_diff(const ParsedDelta& delta, char& out_side) {
    bool bid_changed = false;
    for (const auto& [price, size] : delta.bids) {
        if (size == 0) {
            bids_.erase(price);
        } else {
            bids_[price] = size;
        }
        bid_changed = true;
    }

    bool ask_changed = false;
    for (const auto& [price, size] : delta.asks) {
        if (size == 0) {
            asks_.erase(price);
        } else {
            asks_[price] = size;
        }
        ask_changed = true;
    }

    if (bid_changed && !ask_changed) {
        out_side = 'B';
    } else if (!bid_changed && ask_changed) {
        out_side = 'S';
    } else {
        out_side = 'N';
    }
}

void LocalOrderBook::apply_depth5_replace(const ParsedDelta& delta) {
    if (!delta.bids.empty()) {
        int64_t lowest_depth5_bid = delta.bids.back().price;
        // Erase any existing bids at or above lowest depth5 bid that are stale
        for (auto it = bids_.begin(); it != bids_.end();) {
            if (it->first >= lowest_depth5_bid) {
                it = bids_.erase(it);
            } else {
                break;
            }
        }
        // Insert new depth5 bids
        for (const auto& [price, size] : delta.bids) {
            if (size > 0) {
                bids_[price] = size;
            }
        }
    }

    if (!delta.asks.empty()) {
        int64_t highest_depth5_ask = delta.asks.back().price;
        // Erase any existing asks at or below highest depth5 ask that are stale
        for (auto it = asks_.begin(); it != asks_.end();) {
            if (it->first <= highest_depth5_ask) {
                it = asks_.erase(it);
            } else {
                break;
            }
        }
        // Insert new depth5 asks
        for (const auto& [price, size] : delta.asks) {
            if (size > 0) {
                asks_[price] = size;
            }
        }
    }
}

bool LocalOrderBook::apply_event(
    const ParsedDelta& delta,
    int64_t tsec,
    int32_t tnsec,
    uint32_t conn_epoch,
    OrderBookSnapshot& snapshot_out,
    AppMetrics& metrics,
    bool include_trades
) {
    check_sequence(delta, conn_epoch, metrics);

    char type_char = 'D';
    char side_char = 'N';

    if (delta.stream_kind == StreamKind::DepthDiff) {
        type_char = 'D';
        apply_depth_diff(delta, side_char);
    } else if (delta.stream_kind == StreamKind::Depth5) {
        type_char = 'S';
        side_char = 'N';
        apply_depth5_replace(delta);
    } else if (delta.stream_kind == StreamKind::Trade) {
        if (!include_trades) {
            return false;
        }
        type_char = 'T';
        side_char = delta.trade_buyer_maker ? 'S' : 'B';
    } else {
        return false;
    }

    snapshot_out.tsec = tsec;
    snapshot_out.tnsec = tnsec;
    snapshot_out.seqNo = seq_no_++;
    snapshot_out.id = instrument_id_;
    snapshot_out.type = type_char;
    snapshot_out.side = side_char;

    get_top5(snapshot_out.bids, snapshot_out.bid_sizes, snapshot_out.asks, snapshot_out.ask_sizes);
    return true;
}

void LocalOrderBook::get_top5(
    std::array<int64_t, 5>& bids,
    std::array<int64_t, 5>& bid_sizes,
    std::array<int64_t, 5>& asks,
    std::array<int64_t, 5>& ask_sizes
) const {
    bids.fill(0);
    bid_sizes.fill(0);
    asks.fill(0);
    ask_sizes.fill(0);

    size_t i = 0;
    for (auto it = bids_.begin(); it != bids_.end() && i < 5; ++it, ++i) {
        bids[i] = it->first;
        bid_sizes[i] = it->second;
    }

    size_t j = 0;
    for (auto it = asks_.begin(); it != asks_.end() && j < 5; ++it, ++j) {
        asks[j] = it->first;
        ask_sizes[j] = it->second;
    }
}

} // namespace lob
