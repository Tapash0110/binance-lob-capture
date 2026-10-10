#include "replay.hpp"
#include "lob.hpp"
#include "csv_writer.hpp"
#include "json_utils.hpp"

#include <fstream>
#include <iostream>
#include <sstream>
#include <filesystem>

namespace lob {

namespace fs = std::filesystem;

ReplayEngine::ReplayEngine(std::string input_csv, std::string output_dir, bool include_trades)
    : input_csv_(std::move(input_csv)),
      output_dir_(std::move(output_dir)),
      include_trades_(include_trades) {}

bool ReplayEngine::run() {
    std::ifstream file(input_csv_);
    if (!file.is_open()) {
        std::cerr << "[Replay] Failed to open input file: " << input_csv_ << std::endl;
        return false;
    }

    std::string line;
    // Read header line
    if (!std::getline(file, line)) {
        std::cerr << "[Replay] Empty input file: " << input_csv_ << std::endl;
        return false;
    }

    // Verify header starts with recv_tsec
    if (line.rfind("recv_tsec", 0) != 0) {
        std::cerr << "[Replay] Warning: unexpected header: " << line << std::endl;
    }

    // Parse filename to extract venue, symbol, and date if possible
    // e.g., market_data_spot_BTCUSDT_2025-01-15.csv
    fs::path input_path(input_csv_);
    std::string stem = input_path.stem().string();

    Venue detected_venue = Venue::Spot;
    if (stem.find("_usdm_") != std::string::npos) {
        detected_venue = Venue::Usdm;
    }

    std::string target_dir = output_dir_.empty() ? input_path.parent_path().string() : output_dir_;
    if (target_dir.empty()) target_dir = ".";

    // Extract date from stem: tokens split by '_'
    std::string date_str = "replay";
    auto last_underscore = stem.rfind('_');
    if (last_underscore != std::string::npos && last_underscore + 1 < stem.size()) {
        date_str = stem.substr(last_underscore + 1);
    }

    CsvWriter writer(target_dir, detected_venue, date_str);
    AppMetrics metrics;

    std::unordered_map<std::string, std::unique_ptr<LocalOrderBook>> books;
    int32_t next_instrument_id = 0;

    uint64_t rows_read = 0;
    uint64_t snapshots_emitted = 0;

    while (std::getline(file, line)) {
        if (line.empty()) continue;

        // Split first 8 commas
        size_t pos = 0;
        size_t prev = 0;
        std::vector<std::string_view> tokens;
        tokens.reserve(9);

        for (int i = 0; i < 8; ++i) {
            pos = line.find(',', prev);
            if (pos == std::string::npos) break;
            tokens.push_back(std::string_view(line.data() + prev, pos - prev));
            prev = pos + 1;
        }

        if (tokens.size() < 8) {
            std::cerr << "[Replay] Malformed row at line " << (rows_read + 2) << std::endl;
            continue;
        }

        // Remaining is payload_json
        std::string_view payload_csv(line.data() + prev, line.size() - prev);
        std::string unescaped_json = unescape_rfc4180(payload_csv);

        int64_t recv_tsec = std::stoll(std::string(tokens[0]));
        int32_t recv_tnsec = std::stoi(std::string(tokens[1]));
        Venue venue = string_to_venue(tokens[2]);
        StreamKind kind = string_to_stream_kind(tokens[3]);
        (void)tokens[4]; // shard_id
        uint32_t conn_epoch = static_cast<uint32_t>(std::stoul(std::string(tokens[5])));
        (void)tokens[6]; // conn_seq
        std::string symbol(tokens[7]);

        auto b_it = books.find(symbol);
        if (b_it == books.end()) {
            auto new_book = std::make_unique<LocalOrderBook>(symbol, next_instrument_id++, venue);
            b_it = books.emplace(symbol, std::move(new_book)).first;
        }

        ParsedDelta delta;
        std::string parsed_symbol = symbol;
        StreamKind parsed_kind = kind;
        std::string minified;

        if (!parse_binance_message(
            unescaped_json.c_str(),
            unescaped_json.size(),
            venue,
            parsed_symbol,
            parsed_kind,
            minified,
            delta
        )) {
            metrics.parse_errors.fetch_add(1, std::memory_order_relaxed);
            continue;
        }

        delta.stream_kind = kind; // Preserve the original stream_kind
        OrderBookSnapshot snapshot;
        bool emitted = b_it->second->apply_event(
            delta,
            recv_tsec,
            recv_tnsec,
            conn_epoch,
            snapshot,
            metrics,
            include_trades_
        );

        if (emitted) {
            writer.write_orderbook_snapshot(symbol, snapshot);
            snapshots_emitted++;
        }
        rows_read++;
    }

    writer.flush();
    writer.stop();

    std::cout << "[Replay Complete] Read " << rows_read << " events, emitted "
              << snapshots_emitted << " orderbook snapshots. Sequence gaps: "
              << metrics.sequence_gaps.load() << std::endl;

    return true;
}

} // namespace lob
