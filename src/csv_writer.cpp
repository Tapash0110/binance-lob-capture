#include "csv_writer.hpp"
#include <filesystem>
#include <iostream>

namespace lob {

namespace fs = std::filesystem;

CsvWriter::CsvWriter(std::string output_dir, Venue venue, std::string utc_date)
    : output_dir_(std::move(output_dir)),
      venue_(venue),
      utc_date_(std::move(utc_date)) {
    
    // Ensure output directory exists
    if (!output_dir_.empty()) {
        fs::create_directories(output_dir_);
    }

    // Start background writer thread
    worker_thread_ = std::thread(&CsvWriter::worker_loop, this);
}

CsvWriter::~CsvWriter() {
    stop();
}

std::string CsvWriter::get_market_data_filepath(const std::string& symbol) const {
    fs::path dir(output_dir_);
    std::string fname = "market_data_" + std::string(venue_to_string(venue_)) + "_" + symbol + "_" + utc_date_ + ".csv";
    return (dir / fname).string();
}

std::string CsvWriter::get_orderbook_filepath(const std::string& symbol) const {
    fs::path dir(output_dir_);
    std::string fname = "market_data_" + std::string(venue_to_string(venue_)) + "_" + symbol + "_" + utc_date_ + "_orderbook.csv";
    return (dir / fname).string();
}

CsvWriter::SymbolFiles& CsvWriter::get_or_create_files(const std::string& symbol) {
    auto it = files_.find(symbol);
    if (it != files_.end()) {
        return *it->second;
    }

    auto files = std::make_unique<SymbolFiles>();
    files->market_data_path = get_market_data_filepath(symbol);
    files->orderbook_path = get_orderbook_filepath(symbol);

    bool md_exists = fs::exists(files->market_data_path) && fs::file_size(files->market_data_path) > 0;
    files->market_data_file.open(files->market_data_path, std::ios::out | std::ios::app);
    if (!md_exists) {
        // Write header for Deliverable A
        files->market_data_file << "recv_tsec,recv_tnsec,venue,stream_kind,shard_id,conn_epoch,conn_seq,symbol,payload_json\n";
    }

    bool ob_exists = fs::exists(files->orderbook_path) && fs::file_size(files->orderbook_path) > 0;
    files->orderbook_file.open(files->orderbook_path, std::ios::out | std::ios::app);
    if (!ob_exists) {
        // Write header for Deliverable B (exact single line, 26 columns)
        files->orderbook_file << "tsec,tnsec,seqNo,id,type,side,bid0,bid1,bid2,bid3,bid4,bid_size0,bid_size1,bid_size2,bid_size3,bid_size4,ask0,ask1,ask2,ask3,ask4,ask_size0,ask_size1,ask_size2,ask_size3,ask_size4\n";
    }

    auto& ref = *files;
    files_[symbol] = std::move(files);
    return ref;
}

void CsvWriter::write_market_data(const MarketDataEvent& event) {
    // Format row RFC 4180 style
    std::string row;
    row.reserve(256 + event.payload_json.size());

    row += std::to_string(event.recv_tsec);
    row += ',';
    row += std::to_string(event.recv_tnsec);
    row += ',';
    row += venue_to_string(event.venue);
    row += ',';
    row += stream_kind_to_string(event.stream_kind);
    row += ',';
    row += std::to_string(event.shard_id);
    row += ',';
    row += std::to_string(event.conn_epoch);
    row += ',';
    row += std::to_string(event.conn_seq);
    row += ',';
    row += event.symbol;
    row += ',';
    row += escape_rfc4180(event.payload_json);
    row += '\n';

    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        queue_.push(WriteTask{event.symbol, true, std::move(row)});
    }
    cv_.notify_one();
}

void CsvWriter::write_orderbook_snapshot(const std::string& symbol, const OrderBookSnapshot& snapshot) {
    // Format row: exactly 26 columns
    std::string row;
    row.reserve(320);

    row += std::to_string(snapshot.tsec);
    row += ',';
    row += std::to_string(snapshot.tnsec);
    row += ',';
    row += std::to_string(snapshot.seqNo);
    row += ',';
    row += std::to_string(snapshot.id);
    row += ',';
    row += snapshot.type;
    row += ',';
    row += snapshot.side;
    row += ',';

    // bid0 .. bid4
    for (int i = 0; i < 5; ++i) {
        row += std::to_string(snapshot.bids[i]);
        row += ',';
    }

    // bid_size0 .. bid_size4
    for (int i = 0; i < 5; ++i) {
        row += std::to_string(snapshot.bid_sizes[i]);
        row += ',';
    }

    // ask0 .. ask4
    for (int i = 0; i < 5; ++i) {
        row += std::to_string(snapshot.asks[i]);
        row += ',';
    }

    // ask_size0 .. ask_size4
    for (int i = 0; i < 5; ++i) {
        row += std::to_string(snapshot.ask_sizes[i]);
        if (i < 4) row += ',';
    }
    row += '\n';

    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        queue_.push(WriteTask{symbol, false, std::move(row)});
    }
    cv_.notify_one();
}

void CsvWriter::worker_loop() {
    std::vector<WriteTask> batch;
    batch.reserve(256);

    while (running_) {
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            cv_.wait(lock, [this]() {
                return !queue_.empty() || !running_;
            });

            while (!queue_.empty() && batch.size() < 1024) {
                batch.push_back(std::move(queue_.front()));
                queue_.pop();
            }
        }

        if (batch.empty()) {
            continue;
        }

        {
            std::lock_guard<std::mutex> f_lock(files_mutex_);
            for (const auto& task : batch) {
                auto& f = get_or_create_files(task.symbol);
                if (task.is_market_data) {
                    f.market_data_file.write(task.row_content.data(), task.row_content.size());
                } else {
                    f.orderbook_file.write(task.row_content.data(), task.row_content.size());
                }
            }
        }
        batch.clear();
        cv_flush_.notify_all();
    }

    // Drain remaining tasks on shutdown
    std::lock_guard<std::mutex> lock(queue_mutex_);
    std::lock_guard<std::mutex> f_lock(files_mutex_);
    while (!queue_.empty()) {
        const auto& task = queue_.front();
        auto& f = get_or_create_files(task.symbol);
        if (task.is_market_data) {
            f.market_data_file.write(task.row_content.data(), task.row_content.size());
        } else {
            f.orderbook_file.write(task.row_content.data(), task.row_content.size());
        }
        queue_.pop();
    }

    for (auto& [_, f] : files_) {
        f->market_data_file.flush();
        f->orderbook_file.flush();
        f->market_data_file.close();
        f->orderbook_file.close();
    }
    cv_flush_.notify_all();
}

void CsvWriter::flush() {
    std::unique_lock<std::mutex> lock(queue_mutex_);
    cv_flush_.wait(lock, [this]() { return queue_.empty(); });

    std::lock_guard<std::mutex> f_lock(files_mutex_);
    for (auto& [_, f] : files_) {
        f->market_data_file.flush();
        f->orderbook_file.flush();
    }
}

void CsvWriter::stop() {
    if (running_.exchange(false)) {
        cv_.notify_all();
        if (worker_thread_.joinable()) {
            worker_thread_.join();
        }
    }
}

} // namespace lob
