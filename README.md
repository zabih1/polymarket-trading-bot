# Polymarket Trading Bot

A Python application for collecting Polymarket market data, executing automated
paper-trading strategies, and validating strategy performance against historical
captures.

The application is designed around short-duration crypto Up/Down markets. It
maintains synchronized order books and reference prices, applies deterministic
execution rules, tracks wallet state, and settles positions using authoritative
market results.

## Features

- Discovers upcoming 5-minute and 15-minute Polymarket markets.
- Maintains live UP and DOWN order books from the Polymarket CLOB.
- Aggregates Polymarket, Binance, and Coinbase reference prices.
- Rejects trading signals when required feeds are missing or stale.
- Supports deterministic paper execution with realistic placement and
  settlement delays.
- Tracks cash, reserved funds, token positions, fills, fees, and market payouts.
- Persists wallet state and session results across controlled restarts.
- Collects one-second market snapshots for offline validation.
- Validates captured data before feature calculation or backtesting.
- Provides an event-ordered backtester using the same indicators, entry rules,
  fill checks, stop-loss behavior, and settlement logic as the runtime engine.
- Includes a production client adapter for the official Polymarket Python SDK.

## Application Components

| Component | Responsibility |
| --- | --- |
| Trading engine | Coordinates market lifecycles, strategies, orders, wallet state, and settlement. |
| Market collector | Records order books, reference prices, oracle prices, and completed outcomes. |
| Data processor | Validates capture integrity and calculates deterministic strategy features. |
| Backtester | Replays captured events through the paper-execution and strategy rules. |
| Production adapter | Connects reviewed live strategies to the official authenticated Polymarket client. |

## Requirements

- Python 3.12 or 3.13
- Network access to Polymarket, Binance, and Coinbase
- A funded Polygon wallet only when operating a separately reviewed live strategy

The included `late-entry` and `simulation` strategies are intentionally restricted
to paper trading.

## Installation

Create an isolated environment and install the application with its development
tools:

```powershell
py -3.13 -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
Copy-Item .env.example .env
```

Notebook dependencies are optional:

```powershell
python -m pip install -e ".[notebooks]"
```

## Configuration

Runtime settings are read from `.env`. The committed `.env.example` contains safe
defaults for paper trading.

| Variable | Default | Description |
| --- | --- | --- |
| `MARKET_ASSET` | `btc` | Asset used for market discovery. |
| `MARKET_WINDOW` | `5m` | Market duration: `5m` or `15m`. |
| `PRICE_FEEDS` | `polymarket,binance,coinbase` | Enabled public reference-price streams. |
| `PRICE_FEED_PRIORITY` | `polymarket,binance,coinbase` | Price source selection order. |
| `MAX_PRICE_STALENESS_SECONDS` | `2.0` | Maximum age of a price before it is unavailable. |
| `SIM_DELAY_MS` | `150` | Base simulated API placement delay. |
| `SIM_SETTLEMENT_DELAY_MS` | `4000` | Delay between simulated match and settlement. |
| `BUY_MAX_RETRIES` | `30` | Maximum buy retries after balance-related rejection. |
| `BUY_RETRY_DELAY_MS` | `500` | Delay between buy retries. |

Production credentials are not required for collection, backtesting, or paper
trading. If a reviewed live strategy is added, configure:

```dotenv
POLYMARKET_PRIVATE_KEY=0x...
POLYMARKET_DEPOSIT_WALLET=0x...
```

Never commit `.env`, private keys, wallet secrets, or API credentials.

## Running the Application

### Paper-trading engine

Run the late-entry strategy for five consecutive markets:

```powershell
python run_engine.py --strategy late-entry --rounds 5
```

Run the example simulation strategy with a custom starting balance and session
loss limit:

```powershell
python run_engine.py --strategy simulation --wallet-balance 100 --max-session-loss 5
```

Omit `--rounds` to keep the engine running continuously. Use `Ctrl+C` to request
a graceful shutdown; active lifecycles are allowed to cancel or settle before the
process exits.

### Market-data collector

Collect twelve consecutive BTC five-minute markets:

```powershell
python run_collector.py --asset btc --window 5m --rounds 12
```

Raw captures are partitioned by the market's UTC date under
`data/market_data/raw/YYYYMMDD/`.
For example, a market opening on November 14, 2026 UTC is stored in
`data/market_data/raw/20261114/`. The collector subscribes before each market
opens and updates the capture after the completed result is available.

### Data processing

Validate raw captures and write enriched files to `data/market_data/processed/`.
Date partitions from the raw directory are preserved automatically:

```powershell
python run_pipeline.py
```

### Backtesting

Run the backtester over captured markets:

```powershell
python run_backtest.py
```

Results are always written to `data/backtests/backtest_results.csv`.

Use `--backfill-resolutions` to request authoritative historical open and close
prices when a capture does not contain a completed result:

```powershell
python run_backtest.py --backfill-resolutions
```

The backtester reports incomplete inputs explicitly. It does not infer missing
price-feed divergence or guess a market winner.

## Data Integrity and Execution Rules

- Market slugs, slot boundaries, and expected durations must agree.
- Liquidity is measured in USDC at the single best price level.
- A simulated order fills only when its price crosses the book and best-level
  liquidity is strictly greater than twice the order notional.
- GTC orders rest on the book before they can match.
- Matched simulated orders settle four seconds later.
- A matched order cannot be canceled while settlement is pending.
- Oracle timestamps use Unix milliseconds, and equal open and close prices resolve
  to DOWN.
- Positions are paid out only after the market reports a completed result.

These rules are shared by the runtime engine and the backtester to avoid different
execution assumptions between live-book paper trading and historical validation.

## Project Structure

```text
polymarket_trading_bot/
|-- src/
|   |-- collectors/       # Market metadata, order books, oracle, and price feeds
|   |-- engine/           # Execution clients, lifecycle, wallet, and strategies
|   `-- backtesting.py    # Event-ordered strategy replay
|-- tests/                # Execution, market-data, and pipeline tests
|-- data/
|   |-- market_data/
|   |   |-- raw/YYYYMMDD/       # Collected snapshots by UTC market date
|   |   `-- processed/YYYYMMDD/ # Validated data with matching partitions
|   `-- backtests/               # Backtest result files
|-- run_engine.py
|-- run_collector.py
|-- run_pipeline.py
`-- run_backtest.py
```

## Quality Checks

Run the automated test and code-quality suites before deployment:

```powershell
python -m pytest
python -m ruff check .
python -m ruff format --check .
```

## Operational Safety

- Paper mode is the default.
- Built-in strategies refuse to operate in production mode.
- Production mode requires explicit confirmation and wallet credentials.
- Missing or stale safety feeds prevent late-entry signals.
- The session-loss circuit breaker stops new market creation after the configured
  loss threshold is reached.
- Runtime state is stored locally under `state/`; credentials remain outside
  source control.

Automated trading involves financial risk. Review and test every strategy,
credential path, and order rule before enabling real-money execution.
