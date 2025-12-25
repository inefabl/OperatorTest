# src/qopexp/baselines/tpch_parquet_scan_filter.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from qopexp.contracts import ArtifactStage, ArtifactRef, CodeRef
from qopexp.io.artifact_store import ArtifactStore
from qopexp.baselines.utils import expand_env_vars, require, Stopwatch


def _resolve_tpch_lineitem_path(store: ArtifactStore, dataset_env) -> Path:
    """
    Resolve a Parquet path for TPC-H lineitem.
    We try multiple payload keys to avoid coupling to a single dataset adapter.
    The path is interpreted as artifact-local relative path by default.
    """
    dp = dataset_env.payload or {}
    # Candidate keys (choose the first that exists)
    candidates = [
        dp.get("lineitem_path"),
        dp.get("parquet_path"),
        dp.get("data_path"),
        dp.get("base_path"),
    ]
    candidates = [c for c in candidates if isinstance(c, str) and c]

    if not candidates:
        raise ValueError(
            "TPC-H dataset artifact must provide a Parquet path in payload under one of keys: "
            "lineitem_path / parquet_path / data_path / base_path"
        )

    art_dir = store.paths.artifacts_root / ArtifactStage.DATASETS.value / dataset_env.manifest.artifact_id
    p0 = (art_dir / candidates[0]).resolve()
    if not p0.exists():
        # allow absolute path
        p0 = Path(candidates[0]).expanduser().resolve()
    if not p0.exists():
        raise FileNotFoundError(f"Cannot find TPC-H lineitem parquet path: {p0}")
    return p0


def _arrow_count_rows(parquet_path: Path, shipdate_col: str, cutoff_date: str) -> int:
    """
    Fast path using pyarrow.dataset.
    cutoff_date is 'YYYY-MM-DD' string.
    """
    import pyarrow.dataset as ds

    dataset = ds.dataset(str(parquet_path), format="parquet")
    # shipdate <= cutoff_date
    expr = (ds.field(shipdate_col) <= cutoff_date)
    try:
        # newer pyarrow provides count_rows
        return int(dataset.count_rows(filter=expr))
    except Exception:
        # fallback: materialize minimal table and count
        tab = dataset.to_table(columns=[], filter=expr)
        return int(tab.num_rows)


def _pandas_count_rows(parquet_path: Path, shipdate_col: str, cutoff_date: str) -> int:
    """
    Slow fallback. Use only if pyarrow.dataset is unavailable.
    """
    import pandas as pd

    df = pd.read_parquet(parquet_path, columns=[shipdate_col])
    # pandas may parse date types; compare via string works for ISO format if dtype is string
    try:
        return int((df[shipdate_col] <= cutoff_date).sum())
    except Exception:
        # attempt datetime conversion
        s = pd.to_datetime(df[shipdate_col])
        return int((s <= pd.to_datetime(cutoff_date)).sum())


@dataclass
class TPCHParquetScanFilterBaseline:
    """
    Real baseline for TPC-H style filter selectivity:
      - executes a Parquet scan count_rows with predicate l_shipdate <= cutoff
      - records per-query scan time and aggregates breakdown
    """
    store: ArtifactStore

    def run(self, baseline_cfg: Dict[str, Any], experiment_cfg: Dict[str, Any], workload, dataset_env):
        cfg = expand_env_vars(baseline_cfg)
        name = str(cfg.get("name", "tpch_parquet_scan_filter"))
        btype = str(cfg.get("type", "scan_filter"))

        shipdate_col = str(cfg.get("shipdate_column", "l_shipdate"))

        parquet_path = _resolve_tpch_lineitem_path(self.store, dataset_env)

        instances = list((workload.payload.get("instances") or []))
        results: List[Dict[str, Any]] = []

        total_scan = 0.0

        # Optional: warmup scan to stabilize filesystem cache
        warmup = bool(cfg.get("warmup", True))
        if warmup and instances:
            cutoff = str((instances[0].get("params") or {}).get("shipdate_cutoff", "1992-01-01"))
            try:
                _ = _arrow_count_rows(parquet_path, shipdate_col, cutoff)
            except Exception:
                _ = _pandas_count_rows(parquet_path, shipdate_col, cutoff)

        with Stopwatch() as sw_total:
            for inst in instances:
                qid = str(inst.get("query_id"))
                params = inst.get("params", {}) or {}
                cutoff = str(params.get("shipdate_cutoff", ""))  # YYYY-MM-DD
                tags = inst.get("tags", {}) or {}

                # Index/scan breakdown: for Parquet scan, we attribute to scan_time.
                with Stopwatch() as sw_scan:
                    try:
                        M = _arrow_count_rows(parquet_path, shipdate_col, cutoff)
                        engine = "pyarrow.dataset"
                    except Exception:
                        M = _pandas_count_rows(parquet_path, shipdate_col, cutoff)
                        engine = "pandas.read_parquet"

                scan_t = sw_scan.elapsed
                total_scan += scan_t

                # Need N for selectivity. Prefer tags/predicate N, otherwise None.
                pred = inst.get("predicate", {}) or {}
                N = int(pred.get("N", tags.get("N", 0)) or 0) or None
                p_hat = (float(M) / float(N)) if (N and N > 0) else None
                p_true = tags.get("selectivity_effective", pred.get("selectivity", None))
                if p_true is not None:
                    try:
                        p_true = float(p_true)
                    except Exception:
                        p_true = None

                abs_error = (abs(p_hat - p_true) if (p_hat is not None and p_true is not None) else None)
                rel_error = (abs_error / abs(p_true) if (abs_error is not None and p_true not in (None, 0.0)) else None)

                results.append(
                    {
                        "query_id": qid,
                        "variant": "classical",
                        "baseline_name": name,
                        "baseline_type": btype,
                        "shots": None,

                        "p_hat": p_hat,
                        "p_true": p_true,
                        "abs_error": abs_error,
                        "rel_error": rel_error,

                        "metadata": {
                            "engine": engine,
                            "cutoff": cutoff,
                            "count_M": int(M),
                            "count_N": int(N) if N is not None else None,
                            # Cost breakdown (paper-friendly)
                            "walltime_sec_scan": scan_t,
                            "walltime_sec_index": 0.0,
                            "walltime_sec_verify": 0.0,
                            "walltime_sec_post": 0.0,
                            "walltime_sec_total": scan_t,
                        },
                        "tags": tags,
                    }
                )

        payload = {
            "baseline_name": name,
            "baseline_type": btype,
            "engine": "parquet_scan",
            "parquet_path": str(parquet_path),
            "shipdate_column": shipdate_col,
            "results": results,
            "breakdown": {
                "walltime_sec_scan_total": total_scan,
                "walltime_sec_index_total": 0.0,
                "walltime_sec_verify_total": 0.0,
                "walltime_sec_post_total": 0.0,
                "walltime_sec_total": sw_total.elapsed,
            },
            "notes": "Real Parquet scan baseline. Use this for credible classical timing in the paper.",
        }

        env = self.store.create(
            stage=ArtifactStage.BASELINE,
            kind="BaselineResultArtifact",
            name=f"baseline_{name}",
            description="TPC-H Parquet scan filter baseline (real timing)",
            payload=payload,
            metrics={"result_count": len(results), "walltime_sec_total": sw_total.elapsed},
            inputs=[
                ArtifactRef(stage=ArtifactStage.WORKLOAD_INSTANCES, artifact_id=workload.manifest.artifact_id),
                ArtifactRef(stage=ArtifactStage.DATASETS, artifact_id=dataset_env.manifest.artifact_id),
            ],
            code_ref=CodeRef(),
            config_refs=[],
            seed=workload.manifest.seed,
            backend_name=None,
            backend_profile_sha256=None,
            extra_manifest={"baseline_impl": "TPCHParquetScanFilterBaseline"},
        )
        return env
