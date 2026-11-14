# Trading System Signal & Indicator Specifications

## 1. Overview

This document defines the quantitative signals and indicators calculated for Polymarket's 5-minute crypto Up/Down markets (for example `btc-updown-5m-<t>`): what each signal looks like, how it is calculated from raw market snapshots, and why it is needed.

- **Purpose**: Raw market data (1-second prices and order books) contains too much noise to trade on directly. Indicators transform raw prices and book depth into clean, reliable decision metrics that tell the trading bot **when to enter**, **which side to choose (UP vs DOWN)**, and **when to exit**.
- **Streamlined Design**: We focus exclusively on core signals directly utilized in execution rules:
  1. **Price Gap** (directional lead)
  2. **Wilder RSI on Gap** (directional momentum & stop-loss suppression)
  3. **Wilder ATR on BTC** (volatility regime filter)
  4. **Real-Time Volatility (RTV)** (micro-tick noise filter)
  5. **Gap Safety Ratio** (directional cushion relative to volatility)
  6. **Peak Gap Ratio** (trend retention metric)
  7. **Order Book Imbalance (OBI) & Midpoint** (liquidity pressure and implied probability)

---

## 2. Indicator Inventory

| Indicator | Input Data | Output Range / Unit | Role in Trading Decisions |
| :--- | :--- | :--- | :--- |
| **`price_gap`** | Binance BTC price, Chainlink open price | Dollar amount ($\pm \$X$) | Core directional lead signal (is BTC above or below the target price?) |
| **`rsi_gap`** | Historical `price_gap` sequence | 0 to 100 | Momentum confirmation & stop-loss suppression |
| **`atr_btc`** | Historical Binance BTC prices | Dollar amount ($\$X$) | Volatility regime filter ($\text{ATR} \le 2.0$ required for late entry) |
| **`rtv_btc`** | Rolling 30-tick BTC prices | Dollar amount ($\$X$) | Micro-volatility noise filter |
| **`gap_safety`** | `price_gap`, `atr_btc` | Unitless ratio ($X$) | High-confidence entry cushion ($\text{Gap Safety} \ge 40.0$) |
| **`peak_gap_ratio`** | Current gap, session peak gap | 0.0 to 1.0 (ratio) | Trend retention filter ($\text{Peak Gap Ratio} \ge 0.75$) |
| **`up_obi` / `down_obi`** | Top-5 order book depth | -1.0 to +1.0 | Liquidity pressure (buying vs selling dominance) |
| **`up_midpoint`** | UP token best bid and ask | 0.00 to 1.00 ($) | Market implied probability of UP winning |

---

## 3. Signal Details

### 3.1 Price Gap (`price_gap`)

```python
# Formula
price_gap = binance_current_price - chainlink_price_to_beat
price_gap_abs = abs(price_gap)
price_gap_pct = (price_gap / chainlink_price_to_beat) * 100
```

```json
{
  "chainlink_price_to_beat": 75468.25,
  "binance_current_price": 75585.40,
  "price_gap": 117.15,
  "price_gap_abs": 117.15,
  "price_gap_pct": 0.155,
  "gap_direction": "UP"
}
```

| Field | Meaning |
| :--- | :--- |
| `chainlink_price_to_beat` | The Chainlink price recorded at the exact moment the 5-minute window opens. This is the target price that determines the winning outcome. |
| `binance_current_price` | The current live price of BTC on Binance. |
| `price_gap` | The dollar difference ($P_{\text{current}} - P_{\text{open}}$). Positive means BTC is currently higher than the open price; negative means lower. |
| `price_gap_abs` | The absolute dollar size of the lead, regardless of direction. |
| `gap_direction` | Direction of the current lead: `"UP"` if positive, `"DOWN"` if negative. |

**Why we need it**: The price gap is the primary driver of market resolution. If the gap is positive at window close, UP wins; if negative, DOWN wins. Monitoring the real-time gap gives the bot an early read on which outcome is likely to win.

---

### 3.2 Wilder RSI on Price Gap (`rsi_gap`)

```python
# 14-period Wilder Smoothed Relative Strength Index on price_gap
delta = gap_t - gap_{t-1}
avg_gain = (prev_avg_gain * 13 + gain) / 14
avg_loss = (prev_avg_loss * 13 + loss) / 14
rs = avg_gain / avg_loss
rsi_gap = 100 - (100 / (1 + rs))
```

```json
{
  "period": 14,
  "rsi_gap": 62.45,
  "momentum": "BULLISH_EXPANSION"
}
```

| Field | Meaning |
| :--- | :--- |
| `period` | 14 ticks (14 seconds of snapshots). Standard Wilder smoothing window. |
| `rsi_gap` | Momentum oscillator bounded between 0 and 100. Values above 50 mean the gap is actively expanding; values below 50 mean the gap is contracting. |

**Why we need it**:
1. **Entry Confirmation**: Confirms that a price gap is expanding with momentum rather than decaying.
2. **Stop-Loss Suppression**: If the bot enters a position and the price temporarily pulls back, but `rsi_gap` remains strong ($> 50$ for UP, $< 50$ for DOWN), the bot suppresses premature panic sells, preventing false stop-outs during normal market fluctuations.

---

### 3.3 Wilder ATR on BTC Price (`atr_btc`)

```python
# 14-period Wilder Average True Range on 1-second BTC spot prices
true_range = abs(price_t - price_{t-1})
atr_btc = (prev_atr * 13 + true_range) / 14
```

```json
{
  "period": 14,
  "atr_btc": 1.45,
  "regime": "LOW_VOLATILITY"
}
```

| Field | Meaning |
| :--- | :--- |
| `period` | 14 ticks (14 seconds of price updates). |
| `atr_btc` | Average dollar movement of BTC per second over the last 14 seconds. |
| `regime` | Market volatility classification: `"LOW_VOLATILITY"` ($\le 2.0$) vs `"HIGH_VOLATILITY"` ($> 2.0$). |

**Why we need it**: A large price gap is only safe if the underlying asset is calm. If BTC is swinging by \$20 every second ($\text{ATR} = 20$), a \$40 gap can evaporate in two seconds. The bot requires $\text{ATR} \le 2.0$ before considering late-window entries, protecting capital from sudden volatility spikes.

---

### 3.4 Real-Time Volatility (`rtv_btc`)

```python
# 30-tick rolling Mean Absolute Return
rtv_btc = sum(abs(price_i - price_{i-1}) for i in range(1, 31)) / 30
```

```json
{
  "window": 30,
  "rtv_btc": 1.12
}
```

| Field | Meaning |
| :--- | :--- |
| `window` | 30 ticks (30 seconds). |
| `rtv_btc` | Short-term tick-level noise measurement. |

**Why we need it**: Complements the 14-period ATR by measuring micro-volatility noise over a wider 30-second horizon, catching erratic tick anomalies.

---

### 3.5 Gap Safety Ratio (`gap_safety`)

```python
# Ratio of directional lead to current market volatility
gap_safety = abs(price_gap) / atr_btc
```

```json
{
  "price_gap_abs": 92.50,
  "atr_btc": 1.45,
  "gap_safety": 63.79,
  "entry_eligible": true
}
```

| Field | Meaning |
| :--- | :--- |
| `gap_safety` | How many "volatility units" the current gap represents. |
| `entry_eligible` | `true` if `gap_safety >= 40.0`. |

**Why we need it**: This is our primary safety filter. A gap of \$40 with an ATR of \$1 gives a gap safety of 40 (the price lead is 40x the typical per-second move). When `gap_safety >= 40.0`, the probability of the lead reversing before market close is low, providing a mathematical cushion for high-conviction trades.

---

### 3.6 Peak Gap Ratio (`peak_gap_ratio`)

```python
# Ratio of current lead to the highest lead achieved in this session
peak_gap_ratio = abs(price_gap) / peak_abs_gap
```

```json
{
  "current_gap": 85.00,
  "peak_abs_gap": 95.00,
  "peak_gap_ratio": 0.895,
  "trend_retained": true
}
```

| Field | Meaning |
| :--- | :--- |
| `peak_abs_gap` | The largest absolute gap observed since the market opened. |
| `peak_gap_ratio` | Ratio between 0.0 and 1.0. High values mean the lead is holding strong near its high. |
| `trend_retained` | `true` if `peak_gap_ratio >= 0.75`. |

**Why we need it**: Protects against decaying trends. If a lead reached \$100 earlier but has shriveled to \$25, the trend is collapsing. Requiring `peak_gap_ratio >= 0.75` ensures the bot only enters when the trend is actively maintaining at least 75% of its peak strength.

---

### 3.7 Order Book Imbalance (OBI) & Implied Probability

```python
# Order Book Imbalance from Level-2 top-5 bids and asks
total_bid_liq = sum(bid['size'] for bid in top5_bids)
total_ask_liq = sum(ask['size'] for ask in top5_asks)
obi = (total_bid_liq - total_ask_liq) / (total_bid_liq + total_ask_liq)

# Token Midpoint Implied Probability
midpoint = (best_bid + best_ask) / 2.0
```

```json
{
  "up_bid_liquidity": 12450.0,
  "up_ask_liquidity": 3200.0,
  "up_obi": 0.591,
  "up_best_bid": 0.88,
  "up_best_ask": 0.90,
  "up_midpoint": 0.89,
  "up_spread": 0.02
}
```

| Field | Meaning |
| :--- | :--- |
| `up_obi` / `down_obi` | Imbalance value from -1.0 (pure selling pressure) to +1.0 (pure buying pressure). |
| `up_midpoint` | The midpoint price of the UP token (0.00 to 1.00), representing the market's real-time implied probability of UP winning. |
| `up_spread` | Difference between lowest ask and highest bid (`best_ask - best_bid`). |

**Why we need it**: Order book imbalance reveals institutional buying or selling pressure before prices move. A positive OBI with deep bid liquidity confirms that market makers are supporting the position.

---

## 4. Processed Feature Dataset Schema

Each processed dataset is saved under `data/market_data/processed/YYYYMMDD/` and contains the following standardized columns:

```
Timestamp & Market IDs:
  current_time, market_slug, condition_id, slot_start, slot_end, elapsed_seconds, remaining_seconds

Core Price & Baseline:
  chainlink_price_to_beat, binance_current_price

Price Gap Indicators:
  price_gap, price_gap_pct, price_gap_abs, gap_direction, peak_gap_abs, peak_gap_ratio

Volatility & Momentum Indicators:
  rsi_gap, atr_btc, rtv_btc, gap_safety

Order Book Microstructure & Imbalance:
  polymarket_up_best_bid, polymarket_up_best_ask, polymarket_up_spread,
  polymarket_up_bid_liquidity, polymarket_up_ask_liquidity, up_obi, up_midpoint,
  polymarket_down_best_bid, polymarket_down_best_ask, polymarket_down_spread,
  polymarket_down_bid_liquidity, polymarket_down_ask_liquidity, down_obi, down_midpoint
```

---

## 5. Usage in Trading Strategy Execution

These indicators feed directly into our trading strategies:

### 1. Late-Entry Strategy Execution Rules
- **Timing Window**: Remaining time $\le 90$ seconds.
- **Calm Market Filter**: `atr_btc <= 2.0`.
- **Directional Safety Filter**: `gap_safety >= 40.0`.
- **Trend Retention Filter**: `peak_gap_ratio >= 0.75`.
- **Confidence Pricing Filter**: Target token `best_ask > 0.85` and available liquidity $\ge \$20$.
- **Stop-Loss Rule**: Initial stop-loss set at $0.48$, but suppressed if `rsi_gap` and gap direction continue to confirm the trade.

### 2. Limit Order Simulation Strategy Execution Rules
- **Discount Maker Entry**: Enters limit buy at $0.49$ on window open.
- **Take-Profit Exit**: Places limit sell at $0.70$ upon fill.
- **Emergency Exit**: Emergency market sell at $T - 30$ seconds if unfilled.

---

## 6. Conclusion & Next Step

The signal and indicator engineering pipeline transforms raw 1-second snapshots into a clean, actionable feature suite. By eliminating extraneous noise and keeping only signals that directly govern entry, sizing, and risk exits, we ensure fast calculation and reliable execution.

- **Completed**: Step 1 (Data Collection) & Step 2 (Signal & Indicator Engineering).
- **Next Step**: Step 3 (Backtesting with realistic Level-2 order book depth and Chainlink settlement validation).
