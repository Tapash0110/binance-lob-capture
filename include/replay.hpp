#pragma once

#include "types.hpp"
#include <string>

namespace lob {

class ReplayEngine {
public:
    ReplayEngine(std::string input_csv, std::string output_dir, bool include_trades = false);

    /**
     * @brief Executes offline replay from market_data CSV to reproduce the exact order book CSV.
     * @return true on success
     */
    bool run();

private:
    std::string input_csv_;
    std::string output_dir_;
    bool include_trades_{false};
};

} // namespace lob
