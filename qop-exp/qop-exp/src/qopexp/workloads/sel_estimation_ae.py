# src/qopexp/workloads/sel_estimation_ae.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from qopexp.contracts import ArtifactStage, ArtifactRef, CodeRef, ConfigRef
from qopexp.io.artifact_store import ArtifactStore
from qopexp.workloads.utils import expand_env_vars, require, stable_id


@dataclass
class SelectivityEstimationWorkload:
    store: ArtifactStore

    def instantiate(self, workload_cfg: Dict[str, Any], dataset):
        cfg = expand_env_vars(workload_cfg)
        require(cfg, ["name", "params"], where="workload_cfg")

        wname = str(cfg["name"])
        params = cfg["params"]
        require(params, ["predicate_ref", "classical_baseline", "evaluation"], where="workload_cfg.params")

        predicate_ref = params["predicate_ref"]
        source = predicate_ref.get("source", None)

        # Try to resolve predicate family from an upstream WorkloadInstance artifact
        upstream_instances: Optional[List[Dict[str, Any]]] = None
        if isinstance(source, dict) and "stage" in source and "artifact_id" in source:
            stage = ArtifactStage(str(source["stage"]))
            aid = str(source["artifact_id"])
            up = self.store.load(stage, aid)
            upstream_instances = list((up.payload.get("instances") or []))
            # optional selector filtering (e.g., only one percentile regime)
            selector = source.get("selector", None)
            if isinstance(selector, dict) and upstream_instances:
                def _match(inst: Dict[str, Any]) -> bool:
                    tags = inst.get("tags", {}) or {}
                    for k, v in selector.items():
                        if tags.get(k) != v:
                            return False
                    return True
                upstream_instances = [x for x in upstream_instances if _match(x)]

        # Fallback: synthesize a small predicate family from current dataset stats
        if upstream_instances is None or len(upstream_instances) == 0:
            upstream_instances = self._fallback_predicates_from_dataset(dataset, max_predicates=20)

        # Classical baseline sweeps (MC sample sizes)
        cb = params["classical_baseline"]
        sweeps = cb.get("sweeps", {}) or {}
        samples_list = list(sweeps.get("samples", []))
        repeats = int(sweeps.get("repeats", 10))
        seed = int(sweeps.get("seed", 2025))

        instances: List[Dict[str, Any]] = []
        for pred_i, inst in enumerate(upstream_instances):
            predicate = inst.get("predicate", {}) or {}
            tags0 = inst.get("tags", {}) or {}

            # Expand MC sample sweeps as instances (Evaluator can later compare with AE results)
            for s in samples_list:
                s = int(s)
                for r in range(repeats):
                    qid = stable_id(wname, "pred", pred_i, "s", s, "r", r)
                    instances.append(
                        {
                            "query_id": qid,
                            "sql": "",  # estimation workload; defined by predicate
                            "params": {"mc_samples": s},
                            "predicate": predicate,
                            "tags": {
                                "workload": wname,
                                "baseline": "monte_carlo",
                                "mc_samples": s,
                                "repeat_id": r,
                                "seed": seed,
                                # propagate useful regime fields
                                "N": tags0.get("N", predicate.get("N")),
                                "selectivity": tags0.get("selectivity_effective", predicate.get("selectivity")),
                            },
                        }
                    )

        batching = params.get("batching", {"batch_size": 50, "shuffle": True})
        payload = {
            "workload_name": wname,
            "dataset_artifact_id": dataset.manifest.artifact_id,
            "instances": instances,
            "batching": batching,
        }

        config_refs: List[ConfigRef] = []
        cfg_path = cfg.get("__config_path__")
        if isinstance(cfg_path, str) and cfg_path:
            from qopexp.io.hashing import sha256_file
            try:
                config_refs.append(ConfigRef(path=cfg_path, sha256=sha256_file(cfg_path)))
            except Exception:
                pass

        env = self.store.create(
            stage=ArtifactStage.WORKLOAD_INSTANCES,
            kind="WorkloadInstanceArtifact",
            name=wname,
            description=str(cfg.get("description", "")),
            payload=payload,
            metrics={"instance_count": len(instances), "predicate_count": len(upstream_instances)},
            inputs=[ArtifactRef(stage=ArtifactStage.DATASETS, artifact_id=dataset.manifest.artifact_id)],
            code_ref=CodeRef(),
            config_refs=config_refs,
            seed=seed,
            backend_name=None,
            backend_profile_sha256=None,
            extra_manifest={"source": "sel_estimation_ae"},
        )
        return env

    def _fallback_predicates_from_dataset(self, dataset, max_predicates: int = 20) -> List[Dict[str, Any]]:
        """
        Conservative fallback: create a small family of oracle-friendly predicates.
        Priority:
          1) TPC-H style: qid_lt using row_count (requires dataset.stats.row_count)
          2) ANN style: pick a few qid_range predicates based on dataset views if available
        """
        dp = dataset.payload
        stats = dp.get("stats", {}) or {}

        preds: List[Dict[str, Any]] = []

        # TPC-H-like fallback
        if "row_count" in stats:
            N = int(stats["row_count"])
            # simple selectivity points
            points = [0.001, 0.002, 0.005, 0.01, 0.02]
            for p in points[:max_predicates]:
                M = max(1, int(p * N))
                preds.append(
                    {
                        "query_id": stable_id("fallback_tpch", p),
                        "predicate": {"type": "qid_lt", "column": "qid", "M": M, "N": N, "selectivity": float(M) / float(N)},
                        "tags": {"N": N, "selectivity_effective": float(M) / float(N)},
                    }
                )
            return preds

        # ANN-like fallback (if dataset has views/subsets)
        views = dp.get("views", []) or []
        if views:
            # pick smallest view N and craft a few fixed-width ranges
            v0 = views[0]
            N = int(v0.get("N", 0))
            if N > 0:
                widths = [max(1, N // 1000), max(1, N // 500), max(1, N // 200)]
                for w in widths[:max_predicates]:
                    lo = 0
                    hi = min(N, w)
                    M = hi - lo
                    preds.append(
                        {
                            "query_id": stable_id("fallback_ann", N, w),
                            "predicate": {"type": "qid_range", "column": "qid", "lo": lo, "hi": hi, "N": N, "M": M, "selectivity": float(M) / float(N)},
                            "tags": {"N": N, "selectivity_effective": float(M) / float(N)},
                        }
                    )
            return preds

        # Final fallback: empty (caller should bind properly)
        return preds
