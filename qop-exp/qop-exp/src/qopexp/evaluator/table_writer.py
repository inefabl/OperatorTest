# src/qopexp/evaluator/table_writer.py
from __future__ import annotations

import csv
from pathlib import Path
from typing import Any, Dict, Iterable, List


def write_csv(path: str | Path, rows: List[Dict[str, Any]], columns: List[str]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            out = {k: r.get(k, None) for k in columns}
            w.writerow(out)
