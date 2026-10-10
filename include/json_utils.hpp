#pragma once

#include "types.hpp"
#include "fixed_point.hpp"

#include <rapidjson/document.h>
#include <rapidjson/writer.h>
#include <rapidjson/stringbuffer.h>

#include <string>
#include <string_view>
#include <vector>
#include <algorithm>
#include <cctype>

namespace lob {

/**
 * @brief RFC 4180 CSV field escaper.
 * If field contains comma, quote, or newline, wraps in double-quotes and escapes internal quotes as "".
 */
inline std::string escape_rfc4180(std::string_view in) {
    bool needs_quoting = false;
    for (char c : in) {
        if (c == ',' || c == '"' || c == '\n' || c == '\r') {
            needs_quoting = true;
            break;
        }
    }
    if (!needs_quoting) {
        return std::string(in);
    }

    std::string out;
    out.reserve(in.size() + 16);
    out.push_back('"');
    for (char c : in) {
        if (c == '"') {
            out.push_back('"');
            out.push_back('"');
        } else {
            out.push_back(c);
        }
    }
    out.push_back('"');
    return out;
}

/**
 * @brief RFC 4180 CSV unescaper for replay mode.
 */
inline std::string unescape_rfc4180(std::string_view in) {
    if (in.size() >= 2 && in.front() == '"' && in.back() == '"') {
        std::string out;
        out.reserve(in.size());
        for (size_t i = 1; i + 1 < in.size(); ++i) {
            if (in[i] == '"' && in[i + 1] == '"') {
                out.push_back('"');
                ++i;
            } else {
                out.push_back(in[i]);
            }
        }
        return out;
    }
    return std::string(in);
}

struct ParsedDelta {
    std::string symbol;
    StreamKind stream_kind{StreamKind::DepthDiff};
    uint64_t first_update_id{0}; // U
    uint64_t final_update_id{0}; // u
    uint64_t prev_update_id{0};  // pu (Futures)
    std::vector<PriceLevel> bids;
    std::vector<PriceLevel> asks;
    
    // Trade details (if applicable)
    int64_t trade_price{0};
    int64_t trade_size{0};
    bool trade_buyer_maker{false};
};

/**
 * @brief Parse Binance stream name into symbol (uppercase) and stream kind.
 * Examples:
 * "btcusdt@depth@100ms" -> symbol="BTCUSDT", kind=DepthDiff
 * "btcusdt@depth5@100ms" -> symbol="BTCUSDT", kind=Depth5
 * "btcusdt@trade" -> symbol="BTCUSDT", kind=Trade
 */
inline bool parse_stream_name(std::string_view stream_name, std::string& symbol, StreamKind& kind) {
    auto at_pos = stream_name.find('@');
    if (at_pos == std::string_view::npos) return false;

    std::string sym(stream_name.substr(0, at_pos));
    for (char& c : sym) {
        c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    }
    symbol = std::move(sym);

    std::string_view rest = stream_name.substr(at_pos + 1);
    if (rest.rfind("depth5", 0) == 0) {
        kind = StreamKind::Depth5;
    } else if (rest.rfind("depth", 0) == 0) {
        kind = StreamKind::DepthDiff;
    } else if (rest.rfind("trade", 0) == 0) {
        kind = StreamKind::Trade;
    } else {
        return false;
    }
    return true;
}

/**
 * @brief Parses raw WebSocket message or replay JSON payload.
 * Extracts:
 * 1. Stream kind & symbol
 * 2. Minified payload_json of the inner 'data' object
 * 3. Structured order book delta or trade
 */
inline bool parse_binance_message(
    const char* json_str,
    size_t length,
    Venue venue,
    std::string& out_symbol,
    StreamKind& out_kind,
    std::string& out_minified_payload,
    ParsedDelta& out_delta
) {
    (void)venue;
    rapidjson::Document doc;
    if (doc.Parse(json_str, length).HasParseError()) {
        return false;
    }

    const rapidjson::Value* data_val = nullptr;

    if (doc.IsObject() && doc.HasMember("stream") && doc.HasMember("data")) {
        std::string_view stream_name = doc["stream"].GetString();
        if (!parse_stream_name(stream_name, out_symbol, out_kind)) {
            return false;
        }
        data_val = &doc["data"];
    } else if (doc.IsObject()) {
        // Direct event payload (e.g. from replay of inner object)
        data_val = &doc;
    } else {
        return false;
    }

    // Minify the inner data object into a single-line JSON string
    rapidjson::StringBuffer sb;
    rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
    data_val->Accept(writer);
    out_minified_payload.assign(sb.GetString(), sb.GetSize());

    // Populate ParsedDelta
    out_delta.symbol = out_symbol;
    out_delta.stream_kind = out_kind;
    out_delta.bids.clear();
    out_delta.asks.clear();

    if (!data_val->IsObject()) return false;

    // Check for symbol if not already set or override from payload
    if (data_val->HasMember("s") && (*data_val)["s"].IsString()) {
        out_symbol = (*data_val)["s"].GetString();
        out_delta.symbol = out_symbol;
    }

    if (out_kind == StreamKind::DepthDiff) {
        if (data_val->HasMember("U") && (*data_val)["U"].IsUint64()) {
            out_delta.first_update_id = (*data_val)["U"].GetUint64();
        }
        if (data_val->HasMember("u") && (*data_val)["u"].IsUint64()) {
            out_delta.final_update_id = (*data_val)["u"].GetUint64();
        }
        if (data_val->HasMember("pu") && (*data_val)["pu"].IsUint64()) {
            out_delta.prev_update_id = (*data_val)["pu"].GetUint64();
        }

        // Bids array: "b": [["price", "qty"], ...]
        if (data_val->HasMember("b") && (*data_val)["b"].IsArray()) {
            const auto& b_arr = (*data_val)["b"];
            out_delta.bids.reserve(b_arr.Size());
            for (rapidjson::SizeType i = 0; i < b_arr.Size(); ++i) {
                const auto& item = b_arr[i];
                if (item.IsArray() && item.Size() >= 2 && item[0].IsString() && item[1].IsString()) {
                    int64_t p = parse_fixed8(item[0].GetString());
                    int64_t q = parse_fixed8(item[1].GetString());
                    out_delta.bids.push_back({p, q});
                }
            }
        }

        // Asks array: "a": [["price", "qty"], ...]
        if (data_val->HasMember("a") && (*data_val)["a"].IsArray()) {
            const auto& a_arr = (*data_val)["a"];
            out_delta.asks.reserve(a_arr.Size());
            for (rapidjson::SizeType i = 0; i < a_arr.Size(); ++i) {
                const auto& item = a_arr[i];
                if (item.IsArray() && item.Size() >= 2 && item[0].IsString() && item[1].IsString()) {
                    int64_t p = parse_fixed8(item[0].GetString());
                    int64_t q = parse_fixed8(item[1].GetString());
                    out_delta.asks.push_back({p, q});
                }
            }
        }
    } else if (out_kind == StreamKind::Depth5) {
        // Can have lastUpdateId or u
        if (data_val->HasMember("lastUpdateId") && (*data_val)["lastUpdateId"].IsUint64()) {
            out_delta.final_update_id = (*data_val)["lastUpdateId"].GetUint64();
        } else if (data_val->HasMember("u") && (*data_val)["u"].IsUint64()) {
            out_delta.final_update_id = (*data_val)["u"].GetUint64();
        }

        // Bids array: "bids" or "b"
        const rapidjson::Value* b_arr = nullptr;
        if (data_val->HasMember("bids") && (*data_val)["bids"].IsArray()) {
            b_arr = &(*data_val)["bids"];
        } else if (data_val->HasMember("b") && (*data_val)["b"].IsArray()) {
            b_arr = &(*data_val)["b"];
        }

        if (b_arr) {
            out_delta.bids.reserve(b_arr->Size());
            for (rapidjson::SizeType i = 0; i < b_arr->Size(); ++i) {
                const auto& item = (*b_arr)[i];
                if (item.IsArray() && item.Size() >= 2 && item[0].IsString() && item[1].IsString()) {
                    int64_t p = parse_fixed8(item[0].GetString());
                    int64_t q = parse_fixed8(item[1].GetString());
                    out_delta.bids.push_back({p, q});
                }
            }
        }

        // Asks array: "asks" or "a"
        const rapidjson::Value* a_arr = nullptr;
        if (data_val->HasMember("asks") && (*data_val)["asks"].IsArray()) {
            a_arr = &(*data_val)["asks"];
        } else if (data_val->HasMember("a") && (*data_val)["a"].IsArray()) {
            a_arr = &(*data_val)["a"];
        }

        if (a_arr) {
            out_delta.asks.reserve(a_arr->Size());
            for (rapidjson::SizeType i = 0; i < a_arr->Size(); ++i) {
                const auto& item = (*a_arr)[i];
                if (item.IsArray() && item.Size() >= 2 && item[0].IsString() && item[1].IsString()) {
                    int64_t p = parse_fixed8(item[0].GetString());
                    int64_t q = parse_fixed8(item[1].GetString());
                    out_delta.asks.push_back({p, q});
                }
            }
        }
    } else if (out_kind == StreamKind::Trade) {
        if (data_val->HasMember("p") && (*data_val)["p"].IsString()) {
            out_delta.trade_price = parse_fixed8((*data_val)["p"].GetString());
        }
        if (data_val->HasMember("q") && (*data_val)["q"].IsString()) {
            out_delta.trade_size = parse_fixed8((*data_val)["q"].GetString());
        }
        if (data_val->HasMember("m") && (*data_val)["m"].IsBool()) {
            out_delta.trade_buyer_maker = (*data_val)["m"].GetBool();
        }
    }

    return true;
}

} // namespace lob
