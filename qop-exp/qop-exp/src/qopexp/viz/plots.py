# src/qopexp/viz/plots.py
from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional, Sequence, Tuple

import matplotlib.pyplot as plt
import pandas as pd


def _safe_numeric(df: pd.DataFrame, col: str) -> pd.Series:
    return pd.to_numeric(df.get(col, pd.Series([], dtype=float)), errors="coerce")


def plot_error_vs_shots(df: pd.DataFrame, out: Path, *, title: str = "Abs error vs shots") -> None:
    if "shots" not in df.columns or "abs_error" not in df.columns:
        return

    d = df.copy()
    d["shots"] = _safe_numeric(d, "shots")
    d["abs_error"] = _safe_numeric(d, "abs_error")
    d = d.dropna(subset=["shots", "abs_error"])

    if d.empty:
        return

    plt.figure()
    for v, g in d.groupby(d.get("variant", "unknown")):
        gg = g.sort_values("shots")
        plt.plot(gg["shots"], gg["abs_error"], marker="o", linestyle="-", label=str(v))
    plt.xscale("log")
    plt.xlabel("shots (log)")
    plt.ylabel("abs_error")
    plt.title(title)
    plt.legend()
    plt.tight_layout()
    out.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out, dpi=200)
    plt.close()


def plot_success_vs_shots(df: pd.DataFrame, out: Path, *, title: str = "Success rate vs shots") -> None:
    if "shots" not in df.columns or "success_rate" not in df.columns:
        return

    d = df.copy()
    d["shots"] = _safe_numeric(d, "shots")
    d["success_rate"] = _safe_numeric(d, "success_rate")
    d = d.dropna(subset=["shots", "success_rate"])
    if d.empty:
        return

    plt.figure()
    for v, g in d.groupby(d.get("variant", "unknown")):
        gg = g.sort_values("shots")
        plt.plot(gg["shots"], gg["success_rate"], marker="o", linestyle="-", label=str(v))
    plt.xscale("log")
    plt.xlabel("shots (log)")
    plt.ylabel("success_rate")
    plt.title(title)
    plt.legend()
    plt.tight_layout()
    out.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out, dpi=200)
    plt.close()


def plot_walltime_by_variant(df: pd.DataFrame, out: Path, *, title: str = "Walltime by variant") -> None:
    if "variant" not in df.columns or "walltime_sec_total" not in df.columns:
        return

    d = df.copy()
    d["walltime_sec_total"] = _safe_numeric(d, "walltime_sec_total")
    d = d.dropna(subset=["variant", "walltime_sec_total"])
    if d.empty:
        return

    agg = d.groupby("variant")["walltime_sec_total"].mean().sort_values()
    plt.figure()
    plt.bar(agg.index.astype(str), agg.values)
    plt.xlabel("variant")
    plt.ylabel("mean walltime_sec_total")
    plt.title(title)
    plt.tight_layout()
    out.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out, dpi=200)
    plt.close()


def plot_compile_cost_vs_selectivity(df: pd.DataFrame, out: Path, *, title: str = "Compile depth vs selectivity") -> None:
    if "selectivity" not in df.columns or "compile_depth" not in df.columns:
        return

    d = df.copy()
    d["selectivity"] = _safe_numeric(d, "selectivity")
    d["compile_depth"] = _safe_numeric(d, "compile_depth")
    d = d.dropna(subset=["selectivity", "compile_depth"])
    if d.empty:
        return

    plt.figure()
    for v, g in d.groupby(d.get("variant", "unknown")):
        plt.scatter(g["selectivity"], g["compile_depth"], label=str(v), alpha=0.7)
    plt.xscale("log")
    plt.xlabel("selectivity (log)")
    plt.ylabel("compile_depth (est)")
    plt.title(title)
    plt.legend()
    plt.tight_layout()
    out.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out, dpi=200)
    plt.close()
