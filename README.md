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
│   ├── 03_backtesting.ipynb                # [STEP 3] Fill simulation & P&L backtesting
│   └── 04_production_engine.ipynb          # [STEP 4] Production execution & lifecycle engine
│
├── data/                                   # Data storage
│   ├── raw/                                # Raw live market snapshots (CSV/JSON)
│   └── processed/                          # Enriched datasets with engineered features
│
├── src/                                    # Modular Python engine (collectors, engine, utils)
│   ├── config.py                           # System settings, endpoints & asset maps
│   ├── continuous_collector.py             # Continuous multi-slot data collector daemon
│   ├── collectors/                         # Dedicated collection connectors
│   │   ├── gamma_client.py                 # Market discovery & condition/token IDs
│   │   ├── clob_client.py                  # L2 Order book (REST + WS)
│   │   ├── oracle_client.py                # Chainlink settlement open/close prices
│   │   └── binance_client.py               # Live proxy reference feed
│   ├── engine/                             # Core execution engine
│   │   ├── runner.py                       # Top-level TradingEngine orchestrator
│   │   ├── market_lifecycle.py             # Single-slot state machine (INIT->RUNNING->STOPPING->DONE)
│   │   ├── wallet.py                       # Cash reservations, share tracking & settlement
│   │   ├── client.py                       # SimTradingClient & ProdTradingClient
│   │   ├── indicators.py                   # Real-time streaming RSI, ATR, RTV, Gap Safety
│   │   ├── user_channel.py                 # Asynchronous fill, expiry, and failure event router
│   │   ├── state.py                        # Crash-resilient state persistence & recovery
│   │   ├── types.py                        # Dataclasses, orders, and lifecycle enums
│   │   └── strategies/                     # Event-driven strategies (Late-Entry, Simulation)
│   └── utils/                              # Helpers & timing calculators
│       └── slot.py                         # 5m/15m rolling slot math
│
├── run_collector.py                        # Standalone collector launcher
├── run_engine.py                           # Standalone production engine launcher
├── state/                                  # Persisted session state (engine_sim.json / engine_prod.json)
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
| **Step 4: Production Engine** | **Completed** | `notebooks/04_production_engine.ipynb` & `run_engine.py` | Full event-driven async execution engine (`TradingEngine` & `MarketLifecycle`), wallet capital tracking, streaming indicators, circuit breakers (`MAX_SESSION_LOSS`), and state persistence. |

---

## 🛠️ Quickstart

### 1. Automated Production Trading Engine
To run automated multi-slot trading with real-time order books, streaming indicators, and risk management:

```bash
# Run paper-trading simulation with the Late-Entry Strategy for 5 rounds:
python run_engine.py --strategy late-entry --rounds 5 --slot-offset 1

# Run the Limit Order Simulation Strategy with custom initial cash balance & loss limit:
python run_engine.py --strategy simulation --wallet-balance 100 --max-session-loss 5

# Run indefinitely in continuous simulation (Ctrl+C gracefully exits & flushes state):
python run_engine.py --strategy late-entry --rounds 0

# Run in production mode with real funds on Polygon (requires PRIVATE_KEY):
python run_engine.py --strategy late-entry --prod
```

### 2. Continuous Multi-Slot Data Collection
To collect raw Level-2 snapshots continuously in the background across consecutive slots:

```bash
# Collect consecutive 5-minute BTC slots continuously
python run_collector.py --rounds 12 --window 5m --asset btc

# Run indefinitely in the background
python run_collector.py --rounds 0
```

### 3. Interactive Notebook Research Pipeline
1. **[01 Data Collection](file:///D:/Companies/chaolin/Research/polymarket_trading_bot/notebooks/01_data_collection.ipynb)**: Connects to Polymarket CLOB WebSocket, Gamma API, and Binance WebSocket for single-slot inspection.
2. **[02 Feature Engineering](file:///D:/Companies/chaolin/Research/polymarket_trading_bot/notebooks/02_feature_engineering.ipynb)**: Calculates indicators, spreads, and order book imbalances.
3. **[03 Backtesting](file:///D:/Companies/chaolin/Research/polymarket_trading_bot/notebooks/03_backtesting.ipynb)**: Simulates order fills on full L2 depth and evaluates strategy P&L against Chainlink settlement.
4. **[04 Production Engine](file:///D:/Companies/chaolin/Research/polymarket_trading_bot/notebooks/04_production_engine.ipynb)**: Live paper-trading execution, wallet tracking demonstration, and strategy execution state machine.
