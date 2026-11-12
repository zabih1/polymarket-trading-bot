# Polymarket Trading Bot (Python)

A Python research and trading system for **Polymarket 5-Minute and 15-Minute Crypto Binary (Up/Down) Markets** (BTC, ETH, SOL, etc.).

---

## 📁 Project Architecture & Workflow

The project follows a phased step-by-step roadmap. Each step has its own dedicated Jupyter Notebook for rapid experimentation, analysis, and visualization. Later, verified logic is modularized into `src/` for live automated execution.

```
polymarket_trading_bot/
│
├── notebooks/                              # Phase-by-phase interactive notebooks
│   ├── 01_data_collection.ipynb            # [STEP 1] Live data collection pipeline
│   ├── 02_feature_engineering.ipynb        # [STEP 2] Orderbook imbalance, spread, latency signals
│   └── 03_backtesting.ipynb                # [STEP 3] Fill simulation & P&L backtesting
│
├── data/                                   # Data storage
│   ├── raw/                                # Raw live market snapshots (CSV/JSON)
│   └── processed/                          # Enriched datasets with engineered features
│
├── src/                                    # Modular Python engine (collectors, utils, engine)
│   ├── config.py                           # System settings, endpoints & asset maps
│   ├── continuous_collector.py             # Continuous multi-slot data collector daemon
│   ├── collectors/                         # Dedicated collection connectors
│   │   ├── gamma_client.py                 # Market discovery & condition/token IDs
│   │   ├── clob_client.py                  # L2 Order book (REST + WS)
│   │   ├── oracle_client.py                # Chainlink settlement open/close prices
│   │   └── binance_client.py               # Live proxy reference feed
│   └── utils/                              # Helpers & timing calculators
│       └── slot.py                         # 5m/15m rolling slot math
│
├── run_collector.py                        # Standalone collector launcher
├── config/                                 # Runtime configurations
├── docs/                                   # Reference documents & requirements
├── .env.example                            # Sample environment variables
└── requirements.txt                        # Python dependencies
```

---

## 🚀 Step-by-Step Roadmap

| Step | Status | Target | Description |
| :--- | :---: | :--- | :--- |
| **Step 1: Data Collection** | **Completed** | `notebooks/01_data_collection.ipynb` & `run_collector.py` | Captures full L2 order books (bids/asks with depth), Gamma market metadata, Chainlink baseline (`openPrice`) & settlement (`closePrice`), and Binance proxy prices. Includes continuous multi-slot collector. |
| **Step 2: Signal & Indicator Engineering** | **Completed** | `notebooks/02_feature_engineering.ipynb` | Computes the streamlined indicator suite: Price Gap, Wilder RSI(14) on gap, Wilder ATR(14) on price, RTV(30), Gap Safety, Peak Gap Ratio, and Order Book Imbalance (OBI). |
| **Step 3: Strategy & Backtesting** | **Completed** | `notebooks/03_backtesting.ipynb` | Simulates realistic L2 book depth fills (volume-weighted slippage) and evaluates the two primary strategy models: Late-Entry Strategy and Limit Order Simulation Strategy with full terminal settlement accounting. |
| **Step 4: Production Engine** | Up Next | `src/` | Packages verified strategies and live order book trackers into an automated async Python execution engine. |

---

## 🛠️ Quickstart

### 1. Continuous Multi-Slot Data Collection
To run the automated multi-slot collector across consecutive 5-minute rolling windows:

```bash
# Collect consecutive 5-minute BTC slots continuously
python run_collector.py --rounds 12 --window 5m --asset btc

# Run indefinitely in the background
python run_collector.py --rounds 0
```

### 2. Interactive Notebook Research Pipeline
1. **[01 Data Collection](file:///D:/Companies/chaolin/Research/polymarket_trading_bot/notebooks/01_data_collection.ipynb)**: Connects to Polymarket CLOB WebSocket, Gamma API, and Binance WebSocket for single-slot inspection.
2. **[02 Feature Engineering](file:///D:/Companies/chaolin/Research/polymarket_trading_bot/notebooks/02_feature_engineering.ipynb)**: Calculates indicators, spreads, and order book imbalances.
3. **[03 Backtesting](file:///D:/Companies/chaolin/Research/polymarket_trading_bot/notebooks/03_backtesting.ipynb)**: Simulates order fills on full L2 depth and evaluates strategy P&L against Chainlink settlement.
