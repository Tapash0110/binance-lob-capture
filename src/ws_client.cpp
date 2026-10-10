#include "ws_client.hpp"

#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/ssl.hpp>

#include <iostream>
#include <algorithm>
#include <ctime>
#include <thread>

namespace lob {

namespace net = boost::asio;
namespace ssl = boost::asio::ssl;
namespace websocket = boost::beast::websocket;
using tcp = boost::asio::ip::tcp;

BinanceWebSocketClient::BinanceWebSocketClient(
    Venue venue,
    std::vector<std::string> symbols,
    uint32_t shard_id,
    std::string port,
    MessageCallback callback,
    AppMetrics& metrics
) : venue_(venue),
    symbols_(std::move(symbols)),
    shard_id_(shard_id),
    port_(std::move(port)),
    callback_(std::move(callback)),
    metrics_(metrics) {

    if (venue_ == Venue::Spot) {
        host_ = "stream.binance.com";
        if (port_.empty()) port_ = "443";
        target_path_ = "/stream?streams=" + build_stream_path();
    } else {
        host_ = "fstream.binance.com";
        if (port_.empty()) port_ = "443";
        target_path_ = "/public/stream?streams=" + build_stream_path();
    }
}

BinanceWebSocketClient::~BinanceWebSocketClient() {
    stop();
}

std::string BinanceWebSocketClient::build_stream_path() const {
    std::string path;
    for (size_t i = 0; i < symbols_.size(); ++i) {
        std::string sym = symbols_[i];
        std::transform(sym.begin(), sym.end(), sym.begin(), [](unsigned char c) {
            return static_cast<char>(std::tolower(c));
        });

        if (i > 0) path += "/";
        path += sym + "@depth@100ms/" + sym + "@depth5@100ms/" + sym + "@trade";
    }
    return path;
}

void BinanceWebSocketClient::stop() {
    stop_requested_.store(true, std::memory_order_release);
    std::lock_guard<std::mutex> lock(sock_mutex_);
    if (current_sock_ && current_sock_->is_open()) {
        boost::system::error_code ec;
        current_sock_->cancel(ec);
        current_sock_->close(ec);
    }
}

void BinanceWebSocketClient::run(uint32_t duration_seconds) {
    std::thread timer_thread;
    if (duration_seconds > 0) {
        timer_thread = std::thread([this, duration_seconds]() {
            for (uint32_t s = 0; s < duration_seconds; ++s) {
                if (stop_requested_.load(std::memory_order_acquire)) break;
                std::this_thread::sleep_for(std::chrono::seconds(1));
            }
            if (!stop_requested_.load(std::memory_order_acquire)) {
                std::cout << "[WebSocket] Duration of " << duration_seconds << "s reached. Stopping..." << std::endl;
                stop();
            }
        });
    }

    while (!stop_requested_.load(std::memory_order_acquire)) {
        bool ok = connect_and_read();
        if (!ok && !stop_requested_.load(std::memory_order_acquire)) {
            conn_epoch_++;
            metrics_.reconnect_count.fetch_add(1, std::memory_order_relaxed);
            std::cerr << "[WebSocket] Disconnected. Reconnecting (epoch=" << conn_epoch_ << ")..." << std::endl;
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    }

    if (timer_thread.joinable()) {
        timer_thread.join();
    }
}

bool BinanceWebSocketClient::connect_and_read() {
    try {
        net::io_context ioc;
        ssl::context ctx{ssl::context::tlsv12_client};
        ctx.set_default_verify_paths();
        ctx.set_verify_mode(ssl::verify_peer);

        tcp::resolver resolver{ioc};
        websocket::stream<boost::beast::ssl_stream<boost::beast::tcp_stream>> ws{ioc, ctx};

        auto const results = resolver.resolve(host_, port_);

        // Make connection on IP
        auto ep = boost::beast::get_lowest_layer(ws).connect(results);
        (void)ep;

        {
            std::lock_guard<std::mutex> lock(sock_mutex_);
            current_sock_ = &boost::beast::get_lowest_layer(ws).socket();
        }

        // Set SNI Hostname (critical for TLS handshake)
        if (!SSL_set_tlsext_host_name(ws.next_layer().native_handle(), host_.c_str())) {
            boost::beast::error_code ec{static_cast<int>(::ERR_get_error()), boost::asio::error::get_ssl_category()};
            std::cerr << "[WebSocket] TLS SNI error: " << ec.message() << std::endl;
            return false;
        }

        // Perform SSL handshake
        ws.next_layer().handshake(ssl::stream_base::client);

        // Configure WebSocket timeouts and User-Agent
        ws.set_option(websocket::stream_base::timeout::suggested(boost::beast::role_type::client));
        ws.set_option(websocket::stream_base::decorator([](websocket::request_type& req) {
            req.set(boost::beast::http::field::user_agent, "binance-lob-capture/1.0.0");
        }));

        std::cout << "[WebSocket] Connecting to wss://" << host_ << ":" << port_ << target_path_ << " ..." << std::endl;
        ws.handshake(host_ + ":" + port_, target_path_);
        std::cout << "[WebSocket] Connected successfully! conn_epoch=" << conn_epoch_ << std::endl;

        conn_seq_ = 0;
        boost::beast::flat_buffer buffer;

        while (!stop_requested_.load(std::memory_order_acquire)) {
            buffer.clear();
            boost::beast::error_code ec;
            ws.read(buffer, ec);

            if (ec) {
                if (ec == websocket::error::closed) {
                    std::cout << "[WebSocket] Connection closed cleanly by peer." << std::endl;
                } else {
                    std::cerr << "[WebSocket] Read error: " << ec.message() << std::endl;
                }
                return false;
            }

            // High resolution receive wall-clock timestamp per spec
            struct timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);
            int64_t recv_tsec = static_cast<int64_t>(ts.tv_sec);
            int32_t recv_tnsec = static_cast<int32_t>(ts.tv_nsec);

            conn_seq_++;
            metrics_.messages_received.fetch_add(1, std::memory_order_relaxed);

            const char* data_ptr = static_cast<const char*>(buffer.data().data());
            size_t data_len = buffer.size();

            callback_(data_ptr, data_len, recv_tsec, recv_tnsec, conn_epoch_, conn_seq_);
        }

        // Graceful close
        boost::beast::error_code close_ec;
        ws.close(websocket::close_code::normal, close_ec);

        {
            std::lock_guard<std::mutex> lock(sock_mutex_);
            current_sock_ = nullptr;
        }
        return true;
    } catch (const std::exception& e) {
        {
            std::lock_guard<std::mutex> lock(sock_mutex_);
            current_sock_ = nullptr;
        }
        std::cerr << "[WebSocket] Exception: " << e.what() << std::endl;
        return false;
    }
}

} // namespace lob
