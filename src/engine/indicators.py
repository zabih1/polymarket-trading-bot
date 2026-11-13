"""
Real-time streaming indicators for the trading engine.
Implements Wilder RSI, Wilder ATR, RTV, Gap Safety, and Peak Gap Ratio.
"""

import time


class RSI:
    """
    Wilder Relative Strength Index (RSI) on price gap.
    """

    def __init__(self, period: int = 14):
        self.period = period
        self._prev: float | None = None
        self._avg_gain: float | None = None
        self._avg_loss: float | None = None
        self._seed_gains: list[float] = []
        self._seed_losses: list[float] = []
        self._value: float | None = None

    def update(self, value: float) -> float | None:
        if self._prev is None:
            self._prev = value
            return None

        delta = value - self._prev
        self._prev = value

        gain = delta if delta > 0 else 0.0
        loss = -delta if delta < 0 else 0.0

        if self._avg_gain is None:
            self._seed_gains.append(gain)
            self._seed_losses.append(loss)

            if len(self._seed_gains) >= self.period:
                self._avg_gain = sum(self._seed_gains) / self.period
                self._avg_loss = sum(self._seed_losses) / self.period
                self._value = self._compute_rsi(self._avg_gain, self._avg_loss)
            return self._value

        self._avg_gain = (self._avg_gain * (self.period - 1) + gain) / self.period
        self._avg_loss = (self._avg_loss * (self.period - 1) + loss) / self.period
        self._value = self._compute_rsi(self._avg_gain, self._avg_loss)
        return self._value

    @property
    def value(self) -> float | None:
        return self._value

    @staticmethod
    def _compute_rsi(avg_gain: float, avg_loss: float) -> float:
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return 100.0 - (100.0 / (1.0 + rs))


class ATR:
    """
    Wilder Average True Range (ATR) on underlying asset price.
    """

    def __init__(self, period: int = 14):
        self.period = period
        self._prev: float | None = None
        self._avg_tr: float | None = None
        self._seed_trs: list[float] = []
        self._value: float | None = None

    def update(self, price: float) -> float | None:
        if self._prev is None:
            self._prev = price
            return None

        tr = abs(price - self._prev)
        self._prev = price

        if self._avg_tr is None:
            self._seed_trs.append(tr)
            if len(self._seed_trs) >= self.period:
                self._avg_tr = sum(self._seed_trs) / self.period
                self._value = self._avg_tr
            return self._value

        self._avg_tr = (self._avg_tr * (self.period - 1) + tr) / self.period
        self._value = self._avg_tr
        return self._value

    @property
    def value(self) -> float | None:
        return self._value

    def gap_safety(self, gap: float) -> float | None:
        if not self._value or self._value == 0:
            return None
        return abs(gap) / self._value


class RTV:
    """
    Real-Time Volatility (mean absolute tick return over N updates).
    """

    def __init__(self, window: int = 30):
        self.window = window
        self._prices: list[float] = []
        self._value: float | None = None

    def update(self, price: float) -> float | None:
        self._prices.append(price)

        if len(self._prices) > self.window + 1:
            self._prices.pop(0)

        if len(self._prices) < 3:
            self._value = None
            return None

        total_abs_diff = sum(
            abs(self._prices[i] - self._prices[i - 1]) for i in range(1, len(self._prices))
        )
        self._value = total_abs_diff / (len(self._prices) - 1)
        return self._value

    @property
    def value(self) -> float | None:
        return self._value


class Indicators:
    """
    Unified indicator suite updated each market tick.
    """

    def __init__(self):
        self._rsi = RSI(14)
        self._atr = ATR(14)
        self._rtv = RTV(30)
        self._peak_abs_gap = 0.0
        self._last_update_ms = 0.0

    def tick(self, gap: float | None, btc_price: float | None):
        now_ms = time.time() * 1000
        if now_ms - self._last_update_ms < 1000:
            return
        self._last_update_ms = now_ms

        if gap is not None:
            self._rsi.update(gap)
            if self._atr.value is not None:
                abs_gap = abs(gap)
                if abs_gap > self._peak_abs_gap:
                    self._peak_abs_gap = abs_gap

        if btc_price is not None:
            self._atr.update(btc_price)
            self._rtv.update(btc_price)

    @property
    def rsi(self) -> float | None:
        return self._rsi.value

    @property
    def atr(self) -> float | None:
        return self._atr.value

    @property
    def rtv(self) -> float | None:
        return self._rtv.value

    def peak_gap_ratio(self, gap: float | None) -> float | None:
        if gap is None or self._peak_abs_gap == 0.0:
            return None
        return abs(gap) / self._peak_abs_gap

    def gap_safety(self, gap: float | None) -> float | None:
        if gap is None:
            return None
        return self._atr.gap_safety(gap)
