#pragma once

#include "types.hpp"
#include "lob.hpp"
#include "csv_writer.hpp"
#include "ws_client.hpp"

#include <vector>
#include <string>
#include <memory>
#include <unordered_map>
#include <mutex>

namespace lob {

struct AppConfig {
    Venue venue{Venue::Spot};
    std::vector<std::string> symbols{"BTCUSDT"};
    std::string output_dir{"./output"};
    uint32_t duration_seconds{0};
    uint32_t shard_id{0};
    std::string port{"443"};
    bool include_trades_in_lob{false};
    std::string replay_file;
};

class Application {
public:
    explicit Application(AppConfig config);
    ~Application();

    int run();
    void stop();

    const AppMetrics& metrics() const noexcept { return metrics_; }

private:
    void handle_message(
        const char* data,
        size_t length,
        int64_t recv_tsec,
        int32_t recv_tnsec,
        uint32_t conn_epoch,
        uint64_t conn_seq
    );

    LocalOrderBook& get_or_create_book(const std::string& symbol);

    AppConfig config_;
    AppMetrics metrics_;
    std::string utc_date_;

    std::unique_ptr<CsvWriter> csv_writer_;
    std::unique_ptr<BinanceWebSocketClient> ws_client_;

    std::unordered_map<std::string, std::unique_ptr<LocalOrderBook>> books_;
    std::mutex books_mutex_;
    int32_t next_instrument_id_{0};
};

} // namespace lob
