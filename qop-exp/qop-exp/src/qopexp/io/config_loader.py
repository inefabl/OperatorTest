# src/qopexp/io/config_loader.py
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Tuple

from .serializers import read_json, read_yaml
from .hashing import sha256_file


def load_yaml(path: str | Path) -> Dict[str, Any]:
    return read_yaml(path)


def load_json(path: str | Path) -> Dict[str, Any]:
    return read_json(path)


def load_config_with_sha256(path: str | Path) -> Tuple[Dict[str, Any], str]:
    """
    Loads a YAML/JSON config and returns (config_obj, sha256_of_file_bytes).
    The sha256 is recorded in ArtifactManifest.config_refs for reproducibility.
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Config not found: {p}")

    if p.suffix.lower() in (".yaml", ".yml"):
        cfg = read_yaml(p)
    elif p.suffix.lower() == ".json":
        cfg = read_json(p)
    else:
        raise ValueError(f"Unsupported config extension: {p.suffix} (expected .yaml/.yml/.json)")

    return cfg, sha256_file(p)
