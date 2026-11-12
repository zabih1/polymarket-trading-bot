"""
Wallet and balance tracking for paper trading and execution.
Tracks cash, reservations, share holdings, and settlement credits.
"""

import logging
from typing import Dict, Optional, Callable

logger = logging.getLogger(__name__)


class WalletTracker:
    """
    Tracks cash balance, in-flight reservations, and token share positions.
    Ensures orders cannot exceed available capital or share inventory.
    """

    def __init__(self, initial_balance: float = 50.0, log_fn: Optional[Callable[[str], None]] = None):
        self._balance = float(initial_balance)
        self._reserved_for_buys: Dict[str, float] = {}  # order_id -> reserved_cash
        self._shares: Dict[str, float] = {}             # token_id -> total_shares
        self._reserved_for_sells: Dict[str, float] = {} # order_id -> reserved_shares
        self._sell_token_map: Dict[str, str] = {}       # order_id -> token_id
        self._log = log_fn or logger.debug
        self._log(f"[Wallet] Initialized with balance: ${self._balance:.2f}")

    @property
    def balance(self) -> float:
        """Total cash balance (including reserved funds)."""
        return self._balance

    @property
    def available_cash(self) -> float:
        """Unreserved cash available for new buy orders."""
        reserved = sum(self._reserved_for_buys.values())
        return max(0.0, self._balance - reserved)

    def total_shares(self, token_id: str) -> float:
        """Total shares owned for a token ID."""
        return self._shares.get(token_id, 0.0)

    def available_shares(self, token_id: str) -> float:
        """Unreserved shares available for new sell orders."""
        total = self._shares.get(token_id, 0.0)
        reserved = sum(
            qty for oid, qty in self._reserved_for_sells.items()
            if self._sell_token_map.get(oid) == token_id
        )
        return max(0.0, total - reserved)

    def can_place_buy(self, price: float, shares: float) -> bool:
        """Checks if enough unreserved cash exists to fund the buy."""
        cost = price * shares
        return self.available_cash >= cost

    def can_place_sell(self, token_id: str, shares: float) -> bool:
        """Checks if enough unreserved shares exist to sell."""
        return self.available_shares(token_id) >= shares

    # --- Buy Lifecycle ---

    def lock_for_buy(self, order_id: str, price: float, shares: float, label: str = ""):
        cost = price * shares
        self._reserved_for_buys[order_id] = cost
        self._log(f"[Wallet] Lock Buy ({label}): -${cost:.2f} | Avail Cash: ${self.available_cash:.2f}")

    def unlock_buy(self, order_id: str, label: str = ""):
        cost = self._reserved_for_buys.pop(order_id, None)
        if cost is not None:
            self._log(f"[Wallet] Unlock Buy ({label}): +${cost:.2f} | Avail Cash: ${self.available_cash:.2f}")

    def on_buy_filled(self, order_id: str, token_id: str, price: float, filled_shares: float):
        """Deducts cash and credits purchased shares to holdings."""
        cost = self._reserved_for_buys.pop(order_id, None)
        actual_cost = price * filled_shares

        # Deduct actual cash spent
        self._balance -= actual_cost
        self._shares[token_id] = self._shares.get(token_id, 0.0) + filled_shares

        self._log(
            f"[Wallet] BUY Filled: -${actual_cost:.2f} cash, +{filled_shares:.2f} shares | "
            f"Cash: ${self._balance:.2f} | Holding: {self._shares[token_id]:.2f} shares"
        )

    # --- Sell Lifecycle ---

    def lock_for_sell(self, order_id: str, token_id: str, shares: float, label: str = ""):
        self._reserved_for_sells[order_id] = shares
        self._sell_token_map[order_id] = token_id
        self._log(f"[Wallet] Lock Sell ({label}): -{shares:.2f} shares | Avail Shares: {self.available_shares(token_id):.2f}")

    def unlock_sell(self, order_id: str, label: str = ""):
        shares = self._reserved_for_sells.pop(order_id, None)
        tid = self._sell_token_map.pop(order_id, None)
        if shares is not None and tid is not None:
            self._log(f"[Wallet] Unlock Sell ({label}): +{shares:.2f} shares | Avail: {self.available_shares(tid):.2f}")

    def on_sell_filled(self, order_id: str, token_id: str, price: float, filled_shares: float, fee: float = 0.0):
        """Deducts sold shares and credits net sale proceeds to cash balance."""
        self._reserved_for_sells.pop(order_id, None)
        self._sell_token_map.pop(order_id, None)

        gross_proceeds = price * filled_shares
        net_proceeds = max(0.0, gross_proceeds - fee)

        self._balance += net_proceeds
        current = self._shares.get(token_id, 0.0)
        self._shares[token_id] = max(0.0, current - filled_shares)

        self._log(
            f"[Wallet] SELL Filled: +${net_proceeds:.2f} cash (fee ${fee:.4f}), -{filled_shares:.2f} shares | "
            f"Cash: ${self._balance:.2f} | Holding: {self._shares[token_id]:.2f} shares"
        )

    # --- Settlement Resolution ---

    def credit_settlement(self, token_id: str, payout_per_share: float, label: str = "") -> float:
        """
        Credits settlement payout for remaining unclosed shares at market resolution.
        payout_per_share: $1.00 for winning side, $0.00 for losing side.
        """
        shares = self._shares.pop(token_id, 0.0)
        if shares <= 0:
            return 0.0

        payout = shares * payout_per_share
        self._balance += payout
        self._log(
            f"[Wallet] Resolution ({label}): Settled {shares:.2f} shares @ ${payout_per_share:.2f} = "
            f"+${payout:.2f} | New Cash: ${self._balance:.2f}"
        )
        return payout
