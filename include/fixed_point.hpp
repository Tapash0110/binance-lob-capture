#pragma once

#include <cstdint>
#include <string_view>
#include <string>
#include <charconv>

namespace lob {

// Fixed scaling factor: 10^8 ticks (8 decimal places)
// Documented per assignment requirement: deterministic integer scaling for all price and quantity values.
// Ensures 0 binary floats (double/float) in the hot path.
inline constexpr int64_t FIXED_SCALE = 100'000'000LL;
inline constexpr int FIXED_DECIMALS = 8;

/**
 * @brief Parse a decimal string into a 64-bit fixed-point integer scaled by 10^8.
 * Deterministic integer arithmetic, zero binary float conversion.
 */
inline int64_t parse_fixed8(std::string_view sv) noexcept {
    if (sv.empty()) return 0;

    size_t idx = 0;
    bool negative = false;
    if (sv[idx] == '-') {
        negative = true;
        idx++;
    } else if (sv[idx] == '+') {
        idx++;
    }

    int64_t int_part = 0;
    while (idx < sv.size() && sv[idx] >= '0' && sv[idx] <= '9') {
        int_part = int_part * 10 + (sv[idx] - '0');
        idx++;
    }

    int64_t frac_part = 0;
    int frac_digits = 0;

    if (idx < sv.size() && sv[idx] == '.') {
        idx++;
        while (idx < sv.size() && sv[idx] >= '0' && sv[idx] <= '9') {
            if (frac_digits < FIXED_DECIMALS) {
                frac_part = frac_part * 10 + (sv[idx] - '0');
                frac_digits++;
            }
            idx++;
        }
    }

    // Pad remaining fractional digits up to FIXED_DECIMALS
    while (frac_digits < FIXED_DECIMALS) {
        frac_part *= 10;
        frac_digits++;
    }

    int64_t total = int_part * FIXED_SCALE + frac_part;
    return negative ? -total : total;
}

/**
 * @brief Formats a scaled 64-bit integer into a decimal string with up to 8 decimal places.
 */
inline std::string format_fixed8(int64_t val) {
    if (val == 0) return "0.00000000";
    std::string s;
    if (val < 0) {
        s += '-';
        val = -val;
    }
    int64_t int_part = val / FIXED_SCALE;
    int64_t frac_part = val % FIXED_SCALE;
    s += std::to_string(int_part);
    s += '.';
    std::string frac_str = std::to_string(frac_part);
    while (frac_str.size() < static_cast<size_t>(FIXED_DECIMALS)) {
        frac_str = "0" + frac_str;
    }
    s += frac_str;
    return s;
}

} // namespace lob
