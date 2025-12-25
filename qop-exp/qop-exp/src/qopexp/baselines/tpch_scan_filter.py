# src/qopexp/baselines/tpch_scan_filter.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List

from qopexp.contracts import ArtifactStage, ArtifactRef, CodeRef
from qopexp.io.artifact_store import ArtifactStore
from qopexp.baselines.utils import expand_env_vars, require, Stopwatch


@dataclass
class TPCHScanFilterBaseline:
    store: ArtifactStore

    def run(self, baseline_cfg: Dict[str, Any], experiment_cfg: Dict[str, Any], workload):
        cfg = expand_env_vars(baseline_cfg)
        name = str(cfg.get("name", "tpch_scan_filter"))
        btype = str(cfg.get("type", "scan_filter"))

        instances = list((workload.payload.get("instances") or []))

        results: List[Dict[str, Any]] = []
        with Stopwatch() as sw:
            for inst in instances:
                qid = str(inst.get("query_id"))
                pred = inst.get("predicate", {}) or {}
                tags = inst.get("tags", {}) or {}

                N = int(pred.get("N", tags.get("N", 0)) or 0)
                M = int(pred.get("M", tags.get("M", 0)) or 0)
                p_true = float(M) / float(N) if N > 0 else None

                # Minimal baseline: p_hat == p_true, and "time" is amortized later.
                results.append(
                    {
                        "query_id": qid,
                        "variant": "classical",
                        "baseline_name": name,
                        "baseline_type": btype,
                        "shots": None,
                        "p_hat": p_true,
                        "p_true": p_true,
                        "abs_error": 0.0 if p_true is not None else None,
                        "rel_error": 0.0 if (p_true is not None and p_true != 0) else None,
                        "metadata": {
                            "method": "qid_lt_closed_form",
                        },
                        "tags": tags,
                    }
                )

        payload = {
            "baseline_name": name,
            "baseline_type": btype,
            "results": results,
            "walltime_sec_total": sw.elapsed,
            "notes": "Minimal baseline (closed-form via workload predicate). Replace with parquet scan timing later.",
        }

        env = self.store.create(
            stage=ArtifactStage.BASELINE,
            kind="BaselineResultArtifact",
            name=f"baseline_{name}",
            description="Classical baseline results",
            payload=payload,
            metrics={"result_count": len(results), "walltime_sec_total": sw.elapsed},
            inputs=[ArtifactRef(stage=ArtifactStage.WORKLOAD_INSTANCES, artifact_id=workload.manifest.artifact_id)],
            code_ref=CodeRef(),
            config_refs=[],
            seed=workload.manifest.seed,
            backend_name=None,
            backend_profile_sha256=None,
            extra_manifest={"baseline_impl": "TPCHScanFilterBaseline"},
        )
        return env
