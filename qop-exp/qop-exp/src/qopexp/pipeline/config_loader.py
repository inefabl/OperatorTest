# src/qopexp/pipeline/config_loader.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

import yaml


@dataclass(frozen=True)
class LoadedConfig:
    path: Path
    data: Dict[str, Any]


def load_yaml_config(path: str | Path) -> LoadedConfig:
    p = Path(path).expanduser().resolve()
    if not p.exists():
        raise FileNotFoundError(f"Config not found: {p}")
    with p.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Config root must be a mapping/dict: {p}")
    # Inject path for downstream hashing/lineage (adapters already look for __config_path__)
    data["__config_path__"] = str(p)
    return LoadedConfig(path=p, data=data)


def maybe_load(path: Optional[str | Path]) -> Optional[LoadedConfig]:
    if not path:
        return None
    return load_yaml_config(path)
