#include "fixed_point.hpp"
#include "types.hpp"
#include "lob.hpp"
#include "json_utils.hpp"
#include "csv_writer.hpp"
#include "replay.hpp"

#include <iostream>
#include <cassert>
#include <sstream>
#include <filesystem>

namespace fs = std::filesystem;

void test_fixed_point_parser() {
    std::cout << "[Test] Running test_fixed_point_parser..." << std::endl;
    using namespace lob;

    assert(parse_fixed8("0") == 0);
    assert(parse_fixed8("0.0") == 0);
    assert(parse_fixed8("1") == 100'000'000LL);
    assert(parse_fixed8("100") == 10'000'000'000LL);
    assert(parse_fixed8("83040.5") == 8'304'050'000'000LL);
    assert(parse_fixed8("83040.01000000") == 8'304'001'000'000LL);
    assert(parse_fixed8("0.00013000") == 13'000LL);
    assert(parse_fixed8("0.00000001") == 1LL);
    assert(parse_fixed8("1.123456789") == 112'345'678LL); // Truncates beyond 8th decimal
    assert(parse_fixed8("-50.25") == -5'025'000'000LL);

    assert(format_fixed8(100'000'000LL) == "1.00000000");
    assert(format_fixed8(8'304'001'000'000LL) == "83040.01000000");

    std::cout << "  -> test_fixed_point_parser PASSED" << std::endl;
}

void test_lob_diff_semantics() {
    std::cout << "[Test] Running test_lob_diff_semantics (qty 0 removes level)..." << std::endl;
    using namespace lob;

    LocalOrderBook book("BTCUSDT", 0, Venue::Spot);
    AppMetrics metrics;

    // 1. Initial snapshot / depth
    ParsedDelta d1;
    d1.stream_kind = StreamKind::DepthDiff;
    d1.final_update_id = 100;
    d1.bids = {{parse_fixed8("80000"), parse_fixed8("1.0")},
               {parse_fixed8("79990"), parse_fixed8("2.0")},
               {parse_fixed8("79980"), parse_fixed8("3.0")}};
    d1.asks = {{parse_fixed8("80010"), parse_fixed8("1.5")},
               {parse_fixed8("80020"), parse_fixed8("2.5")}};

    OrderBookSnapshot s1;
    assert(book.apply_event(d1, 1000, 0, 0, s1, metrics));
    assert(s1.bids[0] == parse_fixed8("80000"));
    assert(s1.bids[1] == parse_fixed8("79990"));
    assert(s1.bids[2] == parse_fixed8("79980"));
    assert(s1.bid_sizes[0] == parse_fixed8("1.0"));
    assert(s1.asks[0] == parse_fixed8("80010"));

    // 2. Remove best bid with size = 0
    ParsedDelta d2;
    d2.stream_kind = StreamKind::DepthDiff;
    d2.first_update_id = 101;
    d2.final_update_id = 101;
    d2.bids = {{parse_fixed8("80000"), 0}}; // QTY 0 MUST REMOVE LEVEL

    OrderBookSnapshot s2;
    assert(book.apply_event(d2, 1001, 0, 0, s2, metrics));
    // The previous 2nd bid (79990) should now become the best bid!
    assert(s2.bids[0] == parse_fixed8("79990"));
    assert(s2.bid_sizes[0] == parse_fixed8("2.0"));
    assert(s2.bids[1] == parse_fixed8("79980"));
    assert(s2.bids[2] == 0); // No 3rd bid left
    assert(s2.side == 'B');  // Only bid side updated
    assert(s2.type == 'D');

    std::cout << "  -> test_lob_diff_semantics PASSED" << std::endl;
}

void test_lob_top5_ordering() {
    std::cout << "[Test] Running test_lob_top5_ordering..." << std::endl;
    using namespace lob;

    LocalOrderBook book("BTCUSDT", 0, Venue::Spot);
    AppMetrics metrics;

    ParsedDelta d;
    d.stream_kind = StreamKind::DepthDiff;
    d.final_update_id = 10;
    // Insert 6 bids in scrambled order
    d.bids = {
        {parse_fixed8("100"), parse_fixed8("1")},
        {parse_fixed8("500"), parse_fixed8("5")},
        {parse_fixed8("200"), parse_fixed8("2")},
        {parse_fixed8("400"), parse_fixed8("4")},
        {parse_fixed8("300"), parse_fixed8("3")},
        {parse_fixed8("50"),  parse_fixed8("0.5")}
    };
    // Insert 6 asks in scrambled order
    d.asks = {
        {parse_fixed8("900"), parse_fixed8("9")},
        {parse_fixed8("600"), parse_fixed8("6")},
        {parse_fixed8("800"), parse_fixed8("8")},
        {parse_fixed8("700"), parse_fixed8("7")},
        {parse_fixed8("650"), parse_fixed8("6.5")},
        {parse_fixed8("1000"), parse_fixed8("10")}
    };

    OrderBookSnapshot s;
    assert(book.apply_event(d, 1000, 0, 0, s, metrics));

    // Bids must be highest first: 500, 400, 300, 200, 100
    assert(s.bids[0] == parse_fixed8("500"));
    assert(s.bids[1] == parse_fixed8("400"));
    assert(s.bids[2] == parse_fixed8("300"));
    assert(s.bids[3] == parse_fixed8("200"));
    assert(s.bids[4] == parse_fixed8("100"));

    // Asks must be lowest first: 600, 650, 700, 800, 900
    assert(s.asks[0] == parse_fixed8("600"));
    assert(s.asks[1] == parse_fixed8("650"));
    assert(s.asks[2] == parse_fixed8("700"));
    assert(s.asks[3] == parse_fixed8("800"));
    assert(s.asks[4] == parse_fixed8("900"));

    std::cout << "  -> test_lob_top5_ordering PASSED" << std::endl;
}

void test_lob_depth5_replace() {
    std::cout << "[Test] Running test_lob_depth5_replace..." << std::endl;
    using namespace lob;

    LocalOrderBook book("BTCUSDT", 0, Venue::Spot);
    AppMetrics metrics;

    ParsedDelta d5;
    d5.stream_kind = StreamKind::Depth5;
    d5.final_update_id = 50;
    d5.bids = {
        {parse_fixed8("50000"), parse_fixed8("1")},
        {parse_fixed8("49900"), parse_fixed8("2")},
        {parse_fixed8("49800"), parse_fixed8("3")},
        {parse_fixed8("49700"), parse_fixed8("4")},
        {parse_fixed8("49600"), parse_fixed8("5")}
    };
    d5.asks = {
        {parse_fixed8("50100"), parse_fixed8("1")},
        {parse_fixed8("50200"), parse_fixed8("2")},
        {parse_fixed8("50300"), parse_fixed8("3")},
        {parse_fixed8("50400"), parse_fixed8("4")},
        {parse_fixed8("50500"), parse_fixed8("5")}
    };

    OrderBookSnapshot s;
    assert(book.apply_event(d5, 2000, 0, 0, s, metrics));
    assert(s.type == 'S');
    assert(s.side == 'N');
    assert(s.bids[0] == parse_fixed8("50000"));
    assert(s.asks[0] == parse_fixed8("50100"));

    std::cout << "  -> test_lob_depth5_replace PASSED" << std::endl;
}

void test_sequence_gap_detection() {
    std::cout << "[Test] Running test_sequence_gap_detection..." << std::endl;
    using namespace lob;

    // Spot gap detection
    {
        LocalOrderBook book("BTCUSDT", 0, Venue::Spot);
        AppMetrics metrics;

        ParsedDelta d1;
        d1.stream_kind = StreamKind::DepthDiff;
        d1.first_update_id = 100;
        d1.final_update_id = 105;
        OrderBookSnapshot s1;
        book.apply_event(d1, 1000, 0, 0, s1, metrics);
        assert(metrics.sequence_gaps == 0);

        // Continuous update: U=106, u=110 -> no gap
        ParsedDelta d2;
        d2.stream_kind = StreamKind::DepthDiff;
        d2.first_update_id = 106;
        d2.final_update_id = 110;
        OrderBookSnapshot s2;
        book.apply_event(d2, 1001, 0, 0, s2, metrics);
        assert(metrics.sequence_gaps == 0);

        // Gap update: U=120, u=125 -> gap!
        ParsedDelta d3;
        d3.stream_kind = StreamKind::DepthDiff;
        d3.first_update_id = 120;
        d3.final_update_id = 125;
        OrderBookSnapshot s3;
        book.apply_event(d3, 1002, 0, 0, s3, metrics);
        assert(metrics.sequence_gaps == 1);
    }

    // USD-M Futures gap detection
    {
        LocalOrderBook book("BTCUSDT", 0, Venue::Usdm);
        AppMetrics metrics;

        ParsedDelta d1;
        d1.stream_kind = StreamKind::DepthDiff;
        d1.prev_update_id = 0;
        d1.final_update_id = 100;
        OrderBookSnapshot s1;
        book.apply_event(d1, 1000, 0, 0, s1, metrics);
        assert(metrics.sequence_gaps == 0);

        // Continuous update: pu=100, u=105 -> no gap
        ParsedDelta d2;
        d2.stream_kind = StreamKind::DepthDiff;
        d2.prev_update_id = 100;
        d2.final_update_id = 105;
        OrderBookSnapshot s2;
        book.apply_event(d2, 1001, 0, 0, s2, metrics);
        assert(metrics.sequence_gaps == 0);

        // Gap update: pu=110, u=115 (expected pu=105) -> gap!
        ParsedDelta d3;
        d3.stream_kind = StreamKind::DepthDiff;
        d3.prev_update_id = 110;
        d3.final_update_id = 115;
        OrderBookSnapshot s3;
        book.apply_event(d3, 1002, 0, 0, s3, metrics);
        assert(metrics.sequence_gaps == 1);
    }

    std::cout << "  -> test_sequence_gap_detection PASSED" << std::endl;
}

void test_rfc4180_escaping() {
    std::cout << "[Test] Running test_rfc4180_escaping..." << std::endl;
    using namespace lob;

    std::string simple = "simple_text";
    assert(escape_rfc4180(simple) == "simple_text");

    std::string with_comma = "hello,world";
    assert(escape_rfc4180(with_comma) == "\"hello,world\"");

    std::string with_quote = "quote \"me\" please";
    assert(escape_rfc4180(with_quote) == "\"quote \"\"me\"\" please\"");

    std::string json_str = "{\"e\":\"depthUpdate\",\"U\":100}";
    std::string escaped = escape_rfc4180(json_str);
    assert(escaped == "\"{\"\"e\"\":\"\"depthUpdate\"\",\"\"U\"\":100}\"");

    std::string unescaped = unescape_rfc4180(escaped);
    assert(unescaped == json_str);

    std::cout << "  -> test_rfc4180_escaping PASSED" << std::endl;
}

void test_orderbook_csv_schema() {
    std::cout << "[Test] Running test_orderbook_csv_schema (confirm 26 data columns)..." << std::endl;
    using namespace lob;

    std::string test_dir = "./test_output";
    fs::create_directories(test_dir);

    CsvWriter writer(test_dir, Venue::Spot, "2025-01-15");

    OrderBookSnapshot s;
    s.tsec = 1736932400;
    s.tnsec = 123456789;
    s.seqNo = 1;
    s.id = 0;
    s.type = 'D';
    s.side = 'B';
    s.bids = {8000000000000LL, 7999000000000LL, 7998000000000LL, 7997000000000LL, 7996000000000LL};
    s.bid_sizes = {100000000LL, 200000000LL, 300000000LL, 400000000LL, 500000000LL};
    s.asks = {8001000000000LL, 8002000000000LL, 8003000000000LL, 8004000000000LL, 8005000000000LL};
    s.ask_sizes = {150000000LL, 250000000LL, 350000000LL, 450000000LL, 550000000LL};

    writer.write_orderbook_snapshot("BTCUSDT", s);
    writer.flush();
    writer.stop();

    std::string path = writer.get_orderbook_filepath("BTCUSDT");
    std::ifstream in(path);
    assert(in.is_open());

    std::string header;
    std::getline(in, header);
    // Verify exact header string
    std::string expected_header = "tsec,tnsec,seqNo,id,type,side,bid0,bid1,bid2,bid3,bid4,bid_size0,bid_size1,bid_size2,bid_size3,bid_size4,ask0,ask1,ask2,ask3,ask4,ask_size0,ask_size1,ask_size2,ask_size3,ask_size4";
    assert(header == expected_header);

    // Verify row column count = 26
    std::string row;
    std::getline(in, row);
    size_t comma_count = 0;
    for (char c : row) {
        if (c == ',') comma_count++;
    }
    // 25 commas = 26 columns
    assert(comma_count == 25);

    fs::remove_all(test_dir);
    std::cout << "  -> test_orderbook_csv_schema PASSED (exact 26 columns verified)" << std::endl;
}

int main() {
    std::cout << "========================================" << std::endl;
    std::cout << "Running Binance LOB Capture Unit Tests" << std::endl;
    std::cout << "========================================" << std::endl;

    test_fixed_point_parser();
    test_lob_diff_semantics();
    test_lob_top5_ordering();
    test_lob_depth5_replace();
    test_sequence_gap_detection();
    test_rfc4180_escaping();
    test_orderbook_csv_schema();

    std::cout << "========================================" << std::endl;
    std::cout << "All Unit Tests Passed Successfully! (7/7)" << std::endl;
    std::cout << "========================================" << std::endl;
    return 0;
}
