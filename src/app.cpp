#include "app.hpp"
#include "replay.hpp"
#include "json_utils.hpp"

#include <iostream>
#include <iomanip>
#include <ctime>

namespace lob {

namespace {

std::string get_current_utc_date() {
    std::time_t now = std::time(nullptr);
    std::tm tm_buf{};
#if defined(_WIN32)
    gmtime_s(&tm_buf, &now);
#else
    gmtime_r(&now, &tm_buf);
#endif
    char buf[32];
    std::strftime(buf, sizeof(buf), "%Y-%m-%d", &tm_buf);
    return std::string(buf);
}

} // namespace

Application::Application(AppConfig config)
    : config_(std::move(config)),
      utc_date_(get_current_utc_date()) {}

Application::~Application() {
    stop();
}

void Application::stop() {
    if (ws_client_) {
        ws_client_->stop();
    }
    if (csv_writer_) {
        csv_writer_->stop();
    }
}

LocalOrderBook& Application::get_or_create_book(const std::string& symbol) {
    std::lock_guard<std::mutex> lock(books_mutex_);
    auto it = books_.find(symbol);
    if (it != books_.end()) {
        return *it->second;
    }
    auto book = std::make_unique<LocalOrderBook>(symbol, next_instrument_id_++, config_.venue);
    auto& ref = *book;
    books_[symbol] = std::move(book);
    return ref;
}

void Application::handle_message(
    const char* data,
    size_t length,
    int64_t recv_tsec,
    int32_t recv_tnsec,
    uint32_t conn_epoch,
    uint64_t conn_seq
) {
    std::string symbol;
    StreamKind kind;
    std::string minified_payload;
    ParsedDelta delta;

    if (!parse_binance_message(data, length, config_.venue, symbol, kind, minified_payload, delta)) {
        metrics_.parse_errors.fetch_add(1, std::memory_order_relaxed);
        return;
    }

    // Deliverable A: Write to market data CSV dump
    MarketDataEvent md_event;
    md_event.recv_tsec = recv_tsec;
    md_event.recv_tnsec = recv_tnsec;
    md_event.venue = config_.venue;
    md_event.stream_kind = kind;
    md_event.shard_id = config_.shard_id;
    md_event.conn_epoch = conn_epoch;
    md_event.conn_seq = conn_seq;
    md_event.symbol = symbol;
    md_event.payload_json = std::move(minified_payload);

    csv_writer_->write_market_data(md_event);
    metrics_.market_dump_rows.fetch_add(1, std::memory_order_relaxed);

    // Deliverable B: Apply to local order book and emit snapshot
    auto& book = get_or_create_book(symbol);
    OrderBookSnapshot snapshot;
    bool emitted = book.apply_event(
        delta,
        recv_tsec,
        recv_tnsec,
        conn_epoch,
        snapshot,
        metrics_,
        config_.include_trades_in_lob
    );

    if (emitted) {
        csv_writer_->write_orderbook_snapshot(symbol, snapshot);
        metrics_.orderbook_rows.fetch_add(1, std::memory_order_relaxed);
    }
    metrics_.messages_applied.fetch_add(1, std::memory_order_relaxed);
}

int Application::run() {
    // If replay mode specified, run offline replay without network
    if (!config_.replay_file.empty()) {
        std::cout << "========================================" << std::endl;
        std::cout << "Starting Binance LOB Replay Engine" << std::endl;
        std::cout << "Input CSV: " << config_.replay_file << std::endl;
        std::cout << "Output Dir: " << config_.output_dir << std::endl;
        std::cout << "========================================" << std::endl;
        ReplayEngine replay(config_.replay_file, config_.output_dir, config_.include_trades_in_lob);
        return replay.run() ? 0 : 1;
    }

    std::cout << "========================================" << std::endl;
    std::cout << "Starting Binance WebSocket Capture + LOB" << std::endl;
    std::cout << "Venue: " << venue_to_string(config_.venue) << std::endl;
    std::cout << "Symbols: ";
    for (size_t i = 0; i < config_.symbols.size(); ++i) {
        std::cout << (i > 0 ? ", " : "") << config_.symbols[i];
    }
    std::cout << std::endl;
    std::cout << "Output Dir: " << config_.output_dir << std::endl;
    std::cout << "Duration: " << (config_.duration_seconds > 0 ? std::to_string(config_.duration_seconds) + "s" : "indefinite") << std::endl;
    std::cout << "Shard ID: " << config_.shard_id << std::endl;
    std::cout << "UTC Date: " << utc_date_ << std::endl;
    std::cout << "========================================" << std::endl;

    // Initialize CSV writer
    csv_writer_ = std::make_unique<CsvWriter>(config_.output_dir, config_.venue, utc_date_);

    // Pre-create books for configured symbols
    for (const auto& sym : config_.symbols) {
        get_or_create_book(sym);
    }

    // Initialize WebSocket client
    ws_client_ = std::make_unique<BinanceWebSocketClient>(
        config_.venue,
        config_.symbols,
        config_.shard_id,
        config_.port,
        [this](const char* data, size_t len, int64_t tsec, int32_t tnsec, uint32_t epoch, uint64_t seq) {
            handle_message(data, len, tsec, tnsec, epoch, seq);
        },
        metrics_
    );

    ws_client_->run(config_.duration_seconds);

    std::cout << "\n[Application] Shutting down..." << std::endl;
    csv_writer_->flush();
    csv_writer_->stop();

    std::cout << "========================================" << std::endl;
    std::cout << "Session Metrics Summary:" << std::endl;
    std::cout << "  Messages Received:      " << metrics_.messages_received.load() << std::endl;
    std::cout << "  Messages Applied:       " << metrics_.messages_applied.load() << std::endl;
    std::cout << "  Parse Errors:           " << metrics_.parse_errors.load() << std::endl;
    std::cout << "  Reconnects:             " << metrics_.reconnect_count.load() << std::endl;
    std::cout << "  Sequence Gaps:          " << metrics_.sequence_gaps.load() << std::endl;
    std::cout << "  Market Dump Rows:       " << metrics_.market_dump_rows.load() << std::endl;
    std::cout << "  Orderbook Snapshot Rows:" << metrics_.orderbook_rows.load() << std::endl;
    std::cout << "========================================" << std::endl;

    return 0;
}

} // namespace lob
