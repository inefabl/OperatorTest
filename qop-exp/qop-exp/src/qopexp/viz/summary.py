# src/qopexp/viz/summary.py
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd


@dataclass
class ReportSummary:
    row_count: int
    variants: list[str]
    backend_name: Optional[str]
    dataset_name: Optional[str]
    workload_name: Optional[str]
    kernel_name: Optional[str]

    # Aggregates (best-effort)
    mean_abs_error_by_variant: Dict[str, float]
    mean_success_rate_by_variant: Dict[str, float]
    mean_compile_depth_by_variant: Dict[str, float]
    mean_2q_gates_by_variant: Dict[str, float]
    mean_walltime_total_by_variant: Dict[str, float]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _mean_or_nan(s: pd.Series) -> float:
    s2 = pd.to_numeric(s, errors="coerce")
    if s2.dropna().empty:
        return float("nan")
    return float(s2.dropna().mean())


def build_summary(df: pd.DataFrame) -> ReportSummary:
    row_count = int(len(df))
    variants = sorted([str(x) for x in df.get("variant", pd.Series([], dtype=str)).dropna().unique().tolist()])

    backend_name = df.get("backend_name", pd.Series([None])).dropna().astype(str).unique()
    dataset_name = df.get("dataset_name", pd.Series([None])).dropna().astype(str).unique()
    workload_name = df.get("workload_name", pd.Series([None])).dropna().astype(str).unique()
    kernel_name = df.get("kernel_name", pd.Series([None])).dropna().astype(str).unique()

    bname = str(backend_name[0]) if len(backend_name) > 0 else None
    dname = str(dataset_name[0]) if len(dataset_name) > 0 else None
    wname = str(workload_name[0]) if len(workload_name) > 0 else None
    kname = str(kernel_name[0]) if len(kernel_name) > 0 else None

    mean_abs_error_by_variant: Dict[str, float] = {}
    mean_success_rate_by_variant: Dict[str, float] = {}
    mean_compile_depth_by_variant: Dict[str, float] = {}
    mean_2q_gates_by_variant: Dict[str, float] = {}
    mean_walltime_total_by_variant: Dict[str, float] = {}

    if "variant" not in df.columns:
        df = df.assign(variant="unknown")

    for v, g in df.groupby("variant"):
        v = str(v)
        mean_abs_error_by_variant[v] = _mean_or_nan(g.get("abs_error", pd.Series([], dtype=float)))
        mean_success_rate_by_variant[v] = _mean_or_nan(g.get("success_rate", pd.Series([], dtype=float)))
        mean_compile_depth_by_variant[v] = _mean_or_nan(g.get("compile_depth", pd.Series([], dtype=float)))
        mean_2q_gates_by_variant[v] = _mean_or_nan(g.get("compile_2q_gates", pd.Series([], dtype=float)))
        mean_walltime_total_by_variant[v] = _mean_or_nan(g.get("walltime_sec_total", pd.Series([], dtype=float)))

    return ReportSummary(
        row_count=row_count,
        variants=variants,
        backend_name=bname,
        dataset_name=dname,
        workload_name=wname,
        kernel_name=kname,
        mean_abs_error_by_variant=mean_abs_error_by_variant,
        mean_success_rate_by_variant=mean_success_rate_by_variant,
        mean_compile_depth_by_variant=mean_compile_depth_by_variant,
        mean_2q_gates_by_variant=mean_2q_gates_by_variant,
        mean_walltime_total_by_variant=mean_walltime_total_by_variant,
    )
