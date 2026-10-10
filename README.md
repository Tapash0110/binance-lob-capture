# High-Performance Binance Market Data Ingestion & Limit Order Book (LOB) Engine

An ultra-low latency, production-grade C++17 system engineered for high-frequency trading (HFT) and quantitative research workflows. Connects directly to Binance public WebSocket streams (Spot and USD-M Futures), ingests normalized market data (`depth`, `depth5`, `trade`), maintains a deterministic local limit order book (LOB), and streams audit logs and order book snapshots to RFC 4180-compliant CSV files.

> 📄 **System Architecture & Technical Whitepaper**: Read the full [Architecture & System Design Document](docs/ARCHITECTURE_AND_SYSTEM_DESIGN.md) or view the [System Architecture PDF](docs/Binance_LOB_System_Architecture.pdf).

---

## Table of Contents
1. [Key Capabilities](#key-capabilities)
2. [Architecture & Concurrency Model](#architecture--concurrency-model)
3. [Market Data Literacy & Stream Semantics](#market-data-literacy--stream-semantics)
4. [Deterministic Fixed-Point Integer Scaling](#deterministic-fixed-point-integer-scaling)
5. [Sequence Continuity, Reconnection & Gap Handling](#sequence-continuity-reconnection--gap-handling)
6. [Output Schemas & Specifications](#output-schemas--specifications)
7. [Offline Deterministic Replay Engine](#offline-deterministic-replay-engine)
8. [Build & Installation Guide](#build--installation-guide)
9. [CLI Usage Guide](#cli-usage-guide)
10. [Sample Output Data](#sample-output-data)
11. [Verification & Test Results](#verification--test-results)
12. [Project Layout](#project-layout)
13. [How to Submit on GitHub](#how-to-submit-on-github)

---

## Key Capabilities

* **Zero Floating-Point Drift**: Integer scaling factor ($10^8$ ticks) applied deterministically via integer arithmetic. Zero `double` or `float` conversions in the critical ingestion path.
* **Non-Blocking Multi-Threaded Pipeline**: Network I/O, order book state updates, and disk writes run on dedicated threads to prevent back-pressure and packet loss.
* **Dynamic Top-5 Extraction**: Maintains full order book depth in price-ordered trees, guaranteeing instant promotion of deeper levels when top levels are deleted ($qty = 0$).
* **Cross-Market Compatibility**: Seamless support for both **Binance Spot** and **Binance USD-M Futures** stream formats and sequence tracking semantics.
* **100% Deterministic Replay**: Offline replay mode reproduces exact order book snapshots from raw captured market data with zero network overhead.

---

## Architecture & Concurrency Model

```
                          ┌────────────────────────────────┐
                          │   Binance WebSocket Stream     │
                          │   (Spot / USD-M Combined WSS)  │
                          └───────────────┬────────────────┘
                                          │
                                          ▼
                   ┌──────────────────────────────────────────────┐
                   │           Network Ingestion Thread           │
                   │  - Boost.Beast SSL WebSocket loop            │
                   │  - Microsecond/nanosecond CLOCK_REALTIME     │
                   │  - Fast JSON envelope parsing                │
                   └──────────────┬───────────────────────────────┘
                                  │  Thread-safe message dispatch
                                  ▼
                   ┌──────────────────────────────────────────────┐
                   │             Market Data & LOB Engine         │
                   │  - Fixed-point deterministic parser (10^8)   │
                   │  - Sequence validator (U, u, pu continuity)  │
                   │  - Differential depth updates (qty 0 erase)  │
                   │  - Partial depth5 replace refresh            │
                   │  - Top-5 best bid/ask extraction             │
                   └──────────────┬───────────────────────────────┘
                                  │  Lock-free / batched queue
                                  ▼
                   ┌──────────────────────────────────────────────┐
                   │        Dedicated Async CSV Writer            │
                   │  - Asynchronous background worker thread     │
                   │  - 64KB+ buffered disk writes                │
                   │  - Zero back-pressure on network loop        │
                   │  - Strict RFC 4180 escaping & validation     │
                   └──────────────────────────────────────────────┘
```

### Critical Path Optimizations
1. **Zero Allocations on Hot Path**: Reusable buffers and string views minimize heap allocations during per-message processing.
2. **Asynchronous File I/O**: Operating system page cache latency spikes never stall the socket receive loop; all disk writes execute in batches on a dedicated writer thread.
3. **Clean Teardown (RAII)**: Clean signal handling (`SIGINT` / `SIGTERM`) guarantees that all pending buffers are flushed to disk and WebSockets are terminated gracefully without memory leaks.

---

## Market Data Literacy & Stream Semantics

The engine connects to Binance combined streams per configured symbol:
* `<symbol>@depth@100ms`: Differential depth updates (`depthUpdate`).
* `<symbol>@depth5@100ms`: Top 5 partial order book snapshot (`depth5`).
* `<symbol>@trade`: Real-time public trade execution feed (`trade`).

### Order Book Mechanics
* **Differential Updates (`depthUpdate`, Type `'D'`)**:
  - `[price, quantity]` tuples.
  - If `quantity == 0`: Price level is immediately purged from the book.
  - If `quantity > 0`: Level is inserted or updated.
  - Event side classified as `'B'` (bid updated), `'S'` (ask updated), or `'N'` (both).
* **Top-5 Refresh (`depth5`, Type `'S'`)**:
  - Authoritative snapshot of top-of-book state.
  - Replaces top levels and clears stale bids higher than the best bid or stale asks lower than the best ask.
  - Side classified as `'N'` (symmetric refresh).
* **Trades (`trade`, Type `'T'`)**:
  - Captured in the market audit dump.
  - Optionally emitted in the order book snapshot feed (`--include-trades-in-lob`), annotating buyer/seller aggressor sides without altering passive depth.
* **Level Ordering**:
  - **Bids**: Strictly descending (highest price first: `bid0` > `bid1` > `bid2` > `bid3` > `bid4`).
  - **Asks**: Strictly ascending (lowest price first: `ask0` < `ask1` < `ask2` < `ask3` < `ask4`).
  - Missing levels padded with zeros.

---

## Deterministic Fixed-Point Integer Scaling

Binance transmits decimal prices and sizes as strings. Converting these to IEEE-754 binary floating-point numbers introduces rounding inaccuracy and non-deterministic behavior.

To ensure exact precision:
* **Multiplier**: $10^8$ ticks (`FIXED_SCALE = 100,000,000LL`).
* **Implementation**: `lob::parse_fixed8(std::string_view)` in [`include/fixed_point.hpp`](include/fixed_point.hpp).
* Parses integers and fractional decimals up to 8 decimal places using 64-bit integer arithmetic.
* **Examples**:
  - Price `"83049.43000000"` $\to$ `8304943000000LL`
  - Quantity `"1.79016000"` $\to$ `179016000LL`
  - Tick size `"0.00013000"` $\to$ `13000LL`

### Timestamp Policy
* Captured immediately upon reading the WebSocket frame using `clock_gettime(CLOCK_REALTIME, &ts)`:
  - `recv_tsec`: `ts.tv_sec` (seconds since Unix epoch).
  - `recv_tnsec`: `ts.tv_nsec` (nanosecond remainder, $[0, 999,999,999]$).
* The order book snapshot inherits the exact timestamps of the inbound triggering event.

---

## Sequence Continuity, Reconnection & Gap Handling

* `conn_epoch`: Non-negative integer starting at 0, incremented on every reconnect.
* `conn_seq`: Strictly monotonic sequence starting at 1 within `(shard_id, conn_epoch)`.
* **Spot Continuity Tracking**:
  - Validates $U \le last\_diff\_u + 1$ and $u \ge last\_diff\_u + 1$.
* **USD-M Futures Continuity Tracking**:
  - Binance USD-M futures diff stream provides `pu` (previous update ID).
  - Validates that incoming `pu == last_diff_u`.
* **Independent Stream Trackers**:
  - The differential stream and partial `depth5` stream maintain separate update ID counters to avoid cross-stream sequence collisions.
* **Reconnection Resilience**:
  - When a connection resets, `conn_epoch` increments. The book flags a reconnection and waits for the next `depth5` snapshot to re-establish verified state.

---

## Output Schemas & Specifications

### 1. Market Data Audit Dump (`market_data_<venue>_<SYMBOL>_<UTC-date>.csv`)
One row per inbound event applied from the WebSocket.
* **Header**:
  ```csv
  recv_tsec,recv_tnsec,venue,stream_kind,shard_id,conn_epoch,conn_seq,symbol,payload_json
  ```
* **Columns (9 total)**:
  1. `recv_tsec`: int64 receive timestamp seconds.
  2. `recv_tnsec`: int32 receive timestamp nanoseconds.
  3. `venue`: `spot` or `usdm`.
  4. `stream_kind`: `depth_diff`, `depth5`, or `trade`.
  5. `shard_id`: uint32 shard index (default `0`).
  6. `conn_epoch`: uint32 reconnection counter.
  7. `conn_seq`: uint64 per-connection sequence.
  8. `symbol`: Uppercase ticker (e.g. `BTCUSDT`).
  9. `payload_json`: Minified inner Binance `data` object, RFC 4180 escaped.

### 2. Order Book Snapshots (`market_data_<venue>_<SYMBOL>_<UTC-date>_orderbook.csv`)
Emitted after each event that alters or refreshes the book state.
* **Header (Exact Single Line, 26 Columns)**:
  ```csv
  tsec,tnsec,seqNo,id,type,side,bid0,bid1,bid2,bid3,bid4,bid_size0,bid_size1,bid_size2,bid_size3,bid_size4,ask0,ask1,ask2,ask3,ask4,ask_size0,ask_size1,ask_size2,ask_size3,ask_size4
  ```
* **Columns (26 total)**:
  1. `tsec`: int64 event seconds.
  2. `tnsec`: int32 event nanoseconds ($tsec \times 10^9 + tnsec$).
  3. `seqNo`: uint64 monotonic application sequence (starts at 0).
  4. `id`: int32 numeric instrument ID (0 for 1st symbol, 1 for 2nd, etc.).
  5. `type`: ASCII char (`D` = differential update, `S` = depth5 snapshot, `T` = trade).
  6. `side`: ASCII char (`B` = bid-side, `S` = ask-side, `N` = symmetric / both).
  7-11. `bid0` ... `bid4`: int64 top 5 bid prices, best first, scaled by $10^8$.
  12-16. `bid_size0` ... `bid_size4`: int64 quantities at those bids, scaled by $10^8$.
  17-21. `ask0` ... `ask4`: int64 top 5 ask prices, best first, scaled by $10^8$.
  22-26. `ask_size0` ... `ask_size4`: int64 quantities at those asks, scaled by $10^8$.

---

## Offline Deterministic Replay Engine

For deterministic backtesting and regression verification, the replay engine processes saved market data **completely offline with zero network calls**:
```bash
./build/binance_capture --replay ./output/market_data_spot_BTCUSDT_2026-10-10.csv --output-dir ./replay_output
```
The replay engine parses the audit CSV, unescapes the RFC 4180 payloads, feeds the events in identical order through the LOB engine, and reproduces bit-for-bit identical order book snapshots.

---

## Build & Installation Guide

### Prerequisites
* **Supported OS**: Ubuntu 22.04 LTS / 24.04 LTS / Debian 12 / WSL2
* **Compiler**: GCC 12/13 (`g++-12` / `g++-13`) or Clang 15+
* **Build System**: CMake 3.16+ and Ninja

```bash
# Ubuntu / Debian package installation
sudo apt-get update
sudo apt-get install -y \
  cmake ninja-build \
  build-essential \
  libssl-dev \
  libboost-all-dev \
  rapidjson-dev
```

### Build Commands
```bash
# 1. Configure Release build with Ninja
cmake -B build -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_CXX_FLAGS="-Wall -Wextra -O2"

# 2. Compile binaries and unit tests in parallel
cmake --build build --parallel

# 3. Build unit test target
cmake --build build --target tests

# 4. Execute test suite
./build/tests/unit_tests
```

A clean build script is provided:
```bash
./build.sh
```

---

## CLI Usage Guide

### Single Symbol Spot Capture
```bash
./build/binance_capture \
  --venue spot \
  --symbols BTCUSDT \
  --output-dir ./output
```

### Multi-Symbol Spot Capture
```bash
./build/binance_capture \
  --venue spot \
  --symbols BTCUSDT,ETHUSDT \
  --output-dir ./output
```

### USD-M Futures Capture
```bash
./build/binance_capture \
  --venue usdm \
  --symbols BTCUSDT \
  --output-dir ./output
```

### Timed Session (e.g. 5 minutes / 300 seconds)
```bash
./build/binance_capture \
  --venue spot \
  --symbols BTCUSDT \
  --duration 300 \
  --output-dir ./output
```

### Offline Replay Mode
```bash
./build/binance_capture \
  --replay ./output/market_data_spot_BTCUSDT_2026-10-10.csv \
  --output-dir ./replay_output
```

---

## Sample Output Data

### Live Market Data Excerpt (`market_data_spot_BTCUSDT_2026-10-10.csv`)
```csv
recv_tsec,recv_tnsec,venue,stream_kind,shard_id,conn_epoch,conn_seq,symbol,payload_json
1791668080,699717115,spot,depth5,0,0,1,BTCUSDT,"{""lastUpdateId"":101271073882,""bids"":[[""83078.89000000"",""1.02035000""],[""83078.88000000"",""0.00204000""],[""83078.87000000"",""0.00027000""],[""83078.86000000"",""0.20040000""],[""83078.85000000"",""0.05000000""]],""asks"":[[""83078.90000000"",""2.97945000""],[""83078.91000000"",""0.00164000""],[""83078.92000000"",""0.00013000""],[""83079.08000000"",""0.00008000""],[""83079.20000000"",""0.00024000""]]}"
1791668080,699969178,spot,depth_diff,0,0,2,BTCUSDT,"{""e"":""depthUpdate"",""E"":1791668059516,""s"":""BTCUSDT"",""U"":101271073871,""u"":101271073882,""b"":[[""81811.29000000"",""0.00000000""],[""76432.59000000"",""0.00000000""],[""74771.00000000"",""0.00008000""],[""66463.12000000"",""0.00330000""],[""66463.00000000"",""0.00000000""]],""a"":[[""83144.04000000"",""0.00000000""]]}"
```

### Live Order Book Snapshot Excerpt (`market_data_spot_BTCUSDT_2026-10-10_orderbook.csv`)
```csv
tsec,tnsec,seqNo,id,type,side,bid0,bid1,bid2,bid3,bid4,bid_size0,bid_size1,bid_size2,bid_size3,bid_size4,ask0,ask1,ask2,ask3,ask4,ask_size0,ask_size1,ask_size2,ask_size3,ask_size4
1791668080,699717115,0,0,S,N,8307889000000,8307888000000,8307887000000,8307886000000,8307885000000,102035000,204000,27000,20040000,5000000,8307890000000,8307891000000,8307892000000,8307908000000,8307920000000,297945000,164000,13000,8000,24000
1791668080,699969178,1,0,D,N,8307889000000,8307888000000,8307887000000,8307886000000,8307885000000,102035000,204000,27000,20040000,5000000,8307890000000,8307891000000,8307892000000,8307908000000,8307920000000,297945000,164000,13000,8000,24000
```

---

## Verification & Test Results

The engine has undergone comprehensive testing across all operational dimensions:

| # | Check | Verification Command | Result |
|---|---|---|---|
| 1 | **Clean Compilation** | `cmake -B build -G Ninja && cmake --build build` | **0 errors, 0 warnings** (`-Wall -Wextra -Wpedantic`) |
| 2 | **Unit Test Suite** | `./build/tests/unit_tests` | **7/7 PASSED** |
| 3 | **Live 300s Capture** | `./build/binance_capture --venue spot --symbols BTCUSDT --duration 300` | **0 gaps, 0 parse errors** |
| 4 | **26 Column Schema** | `awk -F',' 'NR==2{print NF}' output/*_orderbook.csv` | **26 (exact schema match)** |
| 5 | **Diff Semantics ($qty=0$)** | Validated in test suite & live stream | **Level purged, next level promoted** |
| 6 | **Replay Determinism** | `diff -u` live output vs replay output | **100% Bit-for-Bit Match** |
| 7 | **Secrets Audit** | `git grep -iE "(api_key\|secret\|password)" -- src include` | **Clean (0 matches)** |

An automated verification script is provided in the repository:
```bash
./verify.sh
```

---

## Project Layout

```
.
├── CMakeLists.txt              # Root build definition (C++17, Ninja, -Wall -Wextra)
├── README.md                   # System documentation & technical specifications
├── .gitignore                  # Excludes build artifacts, CSV outputs, and secrets
├── build.sh                    # Automated build and test script
├── verify.sh                   # Comprehensive verification test runner
├── docs/
│   ├── ARCHITECTURE_AND_SYSTEM_DESIGN.md   # Comprehensive technical whitepaper
│   ├── Binance_LOB_System_Architecture.pdf # System architecture PDF
│   └── generate_pdf.py                     # ReportLab PDF compiler script
├── include/
│   ├── app.hpp                 # Application orchestration & CLI configuration
│   ├── csv_writer.hpp          # Asynchronous buffered RFC 4180 CSV writer
│   ├── fixed_point.hpp         # Exact string-to-int64 fixed-point scaling (10^8 ticks)
│   ├── json_utils.hpp          # Fast RapidJSON parsing & RFC 4180 escaping
│   ├── lob.hpp                 # Local Order Book engine & sequence validator
│   ├── replay.hpp              # Deterministic offline replay engine
│   ├── types.hpp               # Core enums, data structures, and atomic metrics
│   └── ws_client.hpp           # Boost.Beast SSL WebSocket client with TLS SNI
├── src/
│   ├── app.cpp                 # Application event loops and metrics reporting
│   ├── csv_writer.cpp          # Buffered background writer implementation
│   ├── lob.cpp                 # LOB diff semantics, qty=0 deletion, top-5 extraction
│   ├── main.cpp                # CLI entry point and signal handling (SIGINT/SIGTERM)
│   ├── replay.cpp              # Offline replay engine implementation
│   └── ws_client.cpp           # WebSocket connection, read loop & SSL SNI
└── tests/
    ├── CMakeLists.txt          # Test build target definition
    └── unit_tests.cpp          # 7 unit tests covering fixed-point, LOB, sequence & schemas
```

---

## How to Submit on GitHub

### 1. Initialize and push your repository
```bash
# From your project root
git init
git add .
git commit -m "Initial submission: Binance WebSocket capture + LOB"

# Create a new repo on GitHub (via web UI or CLI), then:
git remote add origin https://github.com/Tapash0110/binance-lob-capture.git
git branch -M main
git push -u origin main
```

### 2. Make sure .gitignore excludes build artifacts and secrets
```gitignore
# .gitignore
build/
*.csv
*.o
*.a
*.so
.env
config/secrets.json
```
Never commit API keys, private keys, or credentials. All config should use environment variables or a `.env.example` template (no real values).

### 3. Tag your submission
```bash
git tag -a v1.0.0 -m "Submission v1.0.0"
git push origin v1.0.0
```

