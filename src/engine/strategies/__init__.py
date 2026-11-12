"""
Strategy registry and definitions.
"""

from typing import Dict, Callable, Any
from .base import StrategyContext
from .late_entry import late_entry_strategy
from .simulation import simulation_strategy

STRATEGIES: Dict[str, Callable[[StrategyContext], Any]] = {
    "late-entry": late_entry_strategy,
    "simulation": simulation_strategy
}

DEFAULT_STRATEGY = "late-entry"

__all__ = [
    "StrategyContext",
    "STRATEGIES",
    "DEFAULT_STRATEGY",
    "late_entry_strategy",
    "simulation_strategy",
]
