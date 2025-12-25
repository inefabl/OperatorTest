# src/qopexp/workloads/utils.py
from __future__ import annotations

import hashlib
import os
import re
from datetime import datetime
from typing import Any, Dict, List, Tuple


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


def stable_id(*parts: Any) -> str:
    """
    Stable id for query instances based on content (not time).
    """
    s = "|".join(str(p) for p in parts)
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:16]


def parse_datetime(s: str) -> datetime:
    """
    Best-effort parse for dates emitted by dataset adapter (ISO-like).
    """
    # pandas often emits "YYYY-MM-DD HH:MM:SS"
    s2 = s.strip()
    try:
        return datetime.fromisoformat(s2)
    except Exception:
        # fallback: date-only
        try:
            return datetime.strptime(s2[:10], "%Y-%m-%d")
        except Exception as e:
            raise ValueError(f"Unable to parse datetime: {s}") from e


def percentile_to_M(p: float, N: int) -> int:
    """
    Convert selectivity percentile p into integer M (~ number of matches).
    Guarantee 0 <= M <= N.
    """
    if N <= 0:
        return 0
    if p <= 0.0:
        return 0
    if p >= 1.0:
        return N
    # Conservative: ceil so that very small p still yields at least 1 match when p>0
    import math
    return max(1, min(N, int(math.ceil(p * N))))
