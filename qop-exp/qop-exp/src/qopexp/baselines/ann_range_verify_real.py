# src/qopexp/baselines/ann_range_verify_real.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List

import numpy as np

from qopexp.contracts import ArtifactStage, ArtifactRef, CodeRef
from qopexp.io.artifact_store import ArtifactStore
from qopexp.baselines.utils import expand_env_vars, Stopwatch


def _load_npz(path: Path) -> Dict[str, np.ndarray]:
    with np.load(path, allow_pickle=False) as z:
        return {k: z[k] for k in z.files}


@dataclass
class ANNRangeVerifyBaselineReal:
    """
    Real timing baseline for ANN candidate generation + (light) verification.

    Assumes dataset artifact contains:
      payload.base_path  -> base npz with 'proj_0' (sorted)
      payload.query_path -> query npz with 'proj_0'
    Workload instances contain predicate with:
      type=qid_range, lo/hi/N/M, query_index, q_proj0, delta
    """
    store: ArtifactStore

    def run(self, baseline_cfg: Dict[str, Any], experiment_cfg: Dict[str, Any], workload, dataset_env):
        cfg = expand_env_vars(baseline_cfg)
        name = str(cfg.get("name", "ann_range_verify_real"))
        btype = str(cfg.get("type", "range_verify"))

        dp = dataset_env.payload
        base_path = dp.get("base_path")
        query_path = dp.get("query_path")
        if not base_path or not query_path:
            raise ValueError("ANN dataset artifact must include payload.base_path and payload.query_path")

        art_dir = self.store.paths.artifacts_root / ArtifactStage.DATASETS.value / dataset_env.manifest.artifact_id
        base_npz = (art_dir / str(base_path)).resolve()
        query_npz = (art_dir / str(query_path)).resolve()

        base = _load_npz(base_npz)
        qry = _load_npz(query_npz)

        proj0_sorted = base["proj_0"].astype(np.float64)
        q_proj0 = qry["proj_0"].astype(np.float64)

        N = int(proj0_sorted.shape[0])
        results: List[Dict[str, Any]] = []

        total_index = 0.0
        total_verify = 0.0

        # Optional warmup
        if bool(cfg.get("warmup", True)) and N > 0:
            _ = np.searchsorted(proj0_sorted, float(proj0_sorted[N // 2]), side="left")

        with Stopwatch() as sw_total:
            for inst in (workload.payload.get("instances") or []):
                qid = str(inst.get("query_id"))
                pred = inst.get("predicate", {}) or {}
                tags = inst.get("tags", {}) or {}

                qi = int(pred.get("query_index", 0))
                delta = float(pred.get("delta", 0.0))
                qv = float(pred.get("q_proj0", float(q_proj0[qi])))

                # Index time: binary searches to determine candidate range
                with Stopwatch() as sw_index:
                    lo = int(np.searchsorted(proj0_sorted, qv - delta, side="left"))
                    hi = int(np.searchsorted(proj0_sorted, qv + delta, side="right"))
                idx_t = sw_index.elapsed
                total_index += idx_t

                # Verify time: emulate a lightweight verification cost by touching the candidate slice
                with Stopwatch() as sw_verify:
                    # Touch slice to avoid being optimized away
                    cand = proj0_sorted[lo:hi]
                    # simple "verify": ensure within range (boolean check)
                    _ = np.count_nonzero((cand >= (qv - delta)) & (cand <= (qv + delta)))
                ver_t = sw_verify.elapsed
                total_verify += ver_t

                M = max(0, hi - lo)
                p_hat = float(M) / float(N) if N > 0 else None
                p_true = tags.get("selectivity", pred.get("selectivity", None))
                if p_true is not None:
                    try:
                        p_true = float(p_true)
                    except Exception:
                        p_true = None

                abs_error = (abs(p_hat - p_true) if (p_hat is not None and p_true is not None) else None)
                rel_error = (abs_error / abs(p_true) if (abs_error is not None and p_true not in (None, 0.0)) else None)

                total_t = idx_t + ver_t

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
                            "engine": "numpy_searchsorted",
                            "query_index": qi,
                            "delta": delta,
                            "lo": lo,
                            "hi": hi,
                            "count_M": int(M),
                            "count_N": int(N),

                            # Cost breakdown (paper-friendly)
                            "walltime_sec_scan": 0.0,
                            "walltime_sec_index": idx_t,
                            "walltime_sec_verify": ver_t,
                            "walltime_sec_post": 0.0,
                            "walltime_sec_total": total_t,
                        },
                        "tags": tags,
                    }
                )

        payload = {
            "baseline_name": name,
            "baseline_type": btype,
            "engine": "ann_range_verify",
            "results": results,
            "breakdown": {
                "walltime_sec_scan_total": 0.0,
                "walltime_sec_index_total": total_index,
                "walltime_sec_verify_total": total_verify,
                "walltime_sec_post_total": 0.0,
                "walltime_sec_total": sw_total.elapsed,
            },
            "notes": "Real ANN baseline using numpy searchsorted + slice-touch verification timing.",
        }

        env = self.store.create(
            stage=ArtifactStage.BASELINE,
            kind="BaselineResultArtifact",
            name=f"baseline_{name}",
            description="ANN range verify baseline (real timing)",
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
            extra_manifest={"baseline_impl": "ANNRangeVerifyBaselineReal"},
        )
        return env
