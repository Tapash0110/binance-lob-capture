#include "app.hpp"

#include <iostream>
#include <string>
#include <vector>
#include <sstream>
#include <csignal>
#include <memory>

namespace {

std::unique_ptr<lob::Application> g_app;

void signal_handler(int signum) {
    std::cout << "\n[Signal] Caught signal " << signum << ". Initiating graceful teardown..." << std::endl;
    if (g_app) {
        g_app->stop();
    }
}

void print_usage(const char* prog_name) {
    std::cout << "Usage: " << prog_name << " [options]\n\n"
              << "Options:\n"
              << "  --venue <spot|usdm>          Binance venue to capture (default: spot)\n"
              << "  --symbols <SYM1,SYM2,...>    Comma-separated list of symbols (default: BTCUSDT)\n"
              << "  --output-dir <path>          Directory to write CSV files (default: ./output)\n"
              << "  --duration <seconds>         Capture duration in seconds (default: 0 = indefinite)\n"
              << "  --shard-id <id>              Shard index for multi-connection setups (default: 0)\n"
              << "  --replay <path_to_csv>       Replay mode: regenerate orderbook CSV from market data CSV\n"
              << "  --include-trades-in-lob      Emit orderbook snapshot on trade events as well\n"
              << "  -h, --help                   Display this help message\n"
              << std::endl;
}

std::vector<std::string> split_symbols(const std::string& str) {
    std::vector<std::string> tokens;
    std::stringstream ss(str);
    std::string item;
    while (std::getline(ss, item, ',')) {
        if (!item.empty()) {
            tokens.push_back(item);
        }
    }
    return tokens;
}

} // namespace

int main(int argc, char* argv[]) {
    lob::AppConfig config;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--venue" && i + 1 < argc) {
            config.venue = lob::string_to_venue(argv[++i]);
        } else if (arg == "--symbols" && i + 1 < argc) {
            config.symbols = split_symbols(argv[++i]);
        } else if (arg == "--output-dir" && i + 1 < argc) {
            config.output_dir = argv[++i];
        } else if (arg == "--duration" && i + 1 < argc) {
            config.duration_seconds = static_cast<uint32_t>(std::stoul(argv[++i]));
        } else if (arg == "--shard-id" && i + 1 < argc) {
            config.shard_id = static_cast<uint32_t>(std::stoul(argv[++i]));
        } else if (arg == "--port" && i + 1 < argc) {
            config.port = argv[++i];
        } else if (arg == "--replay" && i + 1 < argc) {
            config.replay_file = argv[++i];
        } else if (arg == "--include-trades-in-lob") {
            config.include_trades_in_lob = true;
        } else if (arg == "-h" || arg == "--help") {
            print_usage(argv[0]);
            return 0;
        } else {
            std::cerr << "Unknown or incomplete argument: " << arg << "\n";
            print_usage(argv[0]);
            return 1;
        }
    }

    if (config.symbols.empty()) {
        config.symbols.push_back("BTCUSDT");
    }

    // Register signal handlers for clean RAII teardown
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);

    g_app = std::make_unique<lob::Application>(config);
    int rc = g_app->run();
    g_app.reset();

    return rc;
}
