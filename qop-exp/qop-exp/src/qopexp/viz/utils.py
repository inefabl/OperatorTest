# src/qopexp/viz/utils.py
from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, Dict, List

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


def ensure_dir(p: str | Path) -> Path:
    d = Path(p)
    d.mkdir(parents=True, exist_ok=True)
    return d
