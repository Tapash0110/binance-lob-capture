#pragma once

#include "types.hpp"

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast.hpp>
#include <boost/beast/ssl.hpp>

#include <string>
#include <vector>
#include <functional>
#include <atomic>
#include <memory>
#include <chrono>

namespace lob {

using MessageCallback = std::function<void(
    const char* data,
    size_t length,
    int64_t recv_tsec,
    int32_t recv_tnsec,
    uint32_t conn_epoch,
    uint64_t conn_seq
)>;

class BinanceWebSocketClient {
public:
    BinanceWebSocketClient(
        Venue venue,
        std::vector<std::string> symbols,
        uint32_t shard_id,
        std::string port,
        MessageCallback callback,
        AppMetrics& metrics
    );
    ~BinanceWebSocketClient();

    // Disable copy
    BinanceWebSocketClient(const BinanceWebSocketClient&) = delete;
    BinanceWebSocketClient& operator=(const BinanceWebSocketClient&) = delete;

    /**
     * @brief Runs the WebSocket event loop (blocks until stopped or duration expires).
     * @param duration_seconds 0 = run indefinitely until stop()
     */
    void run(uint32_t duration_seconds = 0);

    /**
     * @brief Requests graceful shutdown of WebSocket connection.
     */
    void stop();

    uint32_t conn_epoch() const noexcept { return conn_epoch_; }
    uint32_t shard_id() const noexcept { return shard_id_; }

private:
    std::string build_stream_path() const;
    bool connect_and_read();

    Venue venue_;
    std::vector<std::string> symbols_;
    uint32_t shard_id_{0};
    std::string port_;
    MessageCallback callback_;
    AppMetrics& metrics_;

    std::atomic<bool> stop_requested_{false};
    uint32_t conn_epoch_{0};
    uint64_t conn_seq_{0};

    std::string host_;
    std::string target_path_;

    boost::asio::ip::tcp::socket* current_sock_{nullptr};
    std::mutex sock_mutex_;
};

} // namespace lob
