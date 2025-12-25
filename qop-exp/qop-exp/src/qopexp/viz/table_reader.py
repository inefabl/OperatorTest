# src/qopexp/viz/table_reader.py
from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd


def read_curated_table(curated_artifact_dir: Path, table_rel_path: str) -> pd.DataFrame:
    p = curated_artifact_dir / table_rel_path
    if not p.exists():
        raise FileNotFoundError(f"Curated table not found: {p}")
    df = pd.read_csv(p)
    return df
