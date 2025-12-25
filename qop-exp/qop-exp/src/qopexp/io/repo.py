# src/qopexp/io/repo.py
from __future__ import annotations

from pathlib import Path


def find_repo_root(start: str | Path | None = None) -> Path:
    """
    Find repo root by searching upwards for 'pyproject.toml' and 'artifacts/' or 'configs/'.
    """
    p = Path(start or Path.cwd()).resolve()
    for cur in [p] + list(p.parents):
        pyproj = cur / "pyproject.toml"
        configs = cur / "configs"
        artifacts = cur / "artifacts"
        if pyproj.exists() and (configs.exists() or artifacts.exists()):
            return cur
    # fallback: current dir
    return p
