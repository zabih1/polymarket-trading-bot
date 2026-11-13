"""
Strategy registry and definitions.
"""

from collections.abc import Callable
from typing import Any

from .base import StrategyContext
from .late_entry import late_entry_strategy
from .simulation import simulation_strategy

STRATEGIES: dict[str, Callable[[StrategyContext], Any]] = {
    "late-entry": late_entry_strategy,
    "simulation": simulation_strategy,
}

DEFAULT_STRATEGY = "simulation"

__all__ = [
    "StrategyContext",
    "STRATEGIES",
    "DEFAULT_STRATEGY",
    "late_entry_strategy",
    "simulation_strategy",
]
