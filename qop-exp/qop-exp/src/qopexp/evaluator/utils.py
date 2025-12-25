# src/qopexp/evaluator/utils.py
from __future__ import annotations

import os
import re
from typing import Any, Dict, List, Optional, Tuple

_ENV_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


def expand_env_vars(obj: Any) -> Any:
    if isinstance(obj, str):
        def _repl(m: re.Match[str]) -> str:
            k = m.group(1)
            return os.environ.get(k, m.group(0))
        return _ENV_PATTERN.sub(_repl, obj)

    if isinstance(obj, dict):
        return {k: expand_env_vars(v) for k, v in obj.items()}

    if isinstance(obj, list):
        return [expand_env_vars(x) for x in obj]

    return obj


def require(cfg: Dict[str, Any], keys: List[str], *, where: str) -> None:
    missing = [k for k in keys if k not in cfg]
    if missing:
        raise ValueError(f"Missing keys at {where}: {missing}")


def get_nested(cfg: Dict[str, Any], path: str, default: Any = None) -> Any:
    cur: Any = cfg
    for part in path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return default
        cur = cur[part]
    return cur


def safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def safe_int(x: Any) -> Optional[int]:
    try:
        if x is None:
            return None
        return int(x)
    except Exception:
        return None


def counts_success_rate(
    counts: Dict[str, int],
    shots: int,
    *,
    success_bit_index: Optional[int] = None,
) -> Optional[float]:
    """
    Assumes a 1-bit outcome model: success = '1'.
    For multi-bit outcomes, treat success as the specified bit (default: c[0] -> rightmost bit).
    """
    if shots <= 0:
        return None

    # Fast path for 1-bit counts
    if all(str(k) in ("0", "1") for k in counts.keys()):
        one = counts.get("1", 0)
        return float(one) / float(shots)

    idx = 0 if success_bit_index is None else int(success_bit_index)
    ones = 0
    total = 0
    for k, v in counts.items():
        s = str(k).strip().replace(" ", "")
        if not s:
            continue
        count = int(v)
        total += count
        pos = len(s) - 1 - idx  # rightmost bit is c[0]
        if pos < 0:
            continue
        if s[pos] == "1":
            ones += count

    denom = shots if shots > 0 else total
    if denom <= 0:
        return None
    return float(ones) / float(denom)


def abs_rel_error(p_hat: Optional[float], p_true: Optional[float]) -> Tuple[Optional[float], Optional[float]]:
    if p_hat is None or p_true is None:
        return None, None
    ae = abs(p_hat - p_true)
    re = ae / abs(p_true) if p_true != 0 else None
    return ae, re
