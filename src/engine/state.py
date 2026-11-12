"""
State persistence and recovery for the trading engine.
Enables crash-resilient session tracking and state replay.
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any

logger = logging.getLogger(__name__)


def load_state(filepath: Path) -> Optional[Dict[str, Any]]:
    """Loads saved engine state from JSON file."""
    path = Path(filepath)
    if not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            data.setdefault("completed_markets", [])
            data.setdefault("active_markets", [])
            return data
    except Exception as e:
        logger.warning(f"Failed to load state from {path}: {e}")
        return None


def save_state(filepath: Path, state: Dict[str, Any]):
    """Atomic write of engine state using a temporary file."""
    path = Path(filepath)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(".tmp")
    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2)
        tmp_path.replace(path)
    except Exception as e:
        logger.error(f"Failed to save state to {path}: {e}")
