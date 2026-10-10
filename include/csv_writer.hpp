#pragma once

#include "types.hpp"
#include "json_utils.hpp"

#include <string>
#include <fstream>
#include <memory>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <atomic>
#include <unordered_map>

namespace lob {

class CsvWriter {
public:
    CsvWriter(std::string output_dir, Venue venue, std::string utc_date);
    ~CsvWriter();

    // Disable copy
    CsvWriter(const CsvWriter&) = delete;
    CsvWriter& operator=(const CsvWriter&) = delete;

    /**
     * @brief Writes a market data dump row (Deliverable A).
     */
    void write_market_data(const MarketDataEvent& event);

    /**
     * @brief Writes an order book snapshot row (Deliverable B).
     */
    void write_orderbook_snapshot(const std::string& symbol, const OrderBookSnapshot& snapshot);

    /**
     * @brief Flushes all buffered writes to disk and ensures files are synchronized.
     */
    void flush();

    /**
     * @brief Stops background worker thread and closes files cleanly.
     */
    void stop();

    std::string get_market_data_filepath(const std::string& symbol) const;
    std::string get_orderbook_filepath(const std::string& symbol) const;

private:
    struct SymbolFiles {
        std::ofstream market_data_file;
        std::ofstream orderbook_file;
        std::string market_data_path;
        std::string orderbook_path;
    };

    void worker_loop();
    SymbolFiles& get_or_create_files(const std::string& symbol);

    std::string output_dir_;
    Venue venue_;
    std::string utc_date_;

    std::unordered_map<std::string, std::unique_ptr<SymbolFiles>> files_;
    std::mutex files_mutex_;

    // Asynchronous write queue
    struct WriteTask {
        std::string symbol;
        bool is_market_data{true};
        std::string row_content;
    };

    std::queue<WriteTask> queue_;
    std::mutex queue_mutex_;
    std::condition_variable cv_;
    std::condition_variable cv_flush_;
    std::atomic<bool> running_{true};
    std::thread worker_thread_;
};

} // namespace lob
