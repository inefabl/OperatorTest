# src/qopexp/workloads/ann_candidate_range.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List

import numpy as np

from qopexp.contracts import ArtifactStage, ArtifactRef, CodeRef, ConfigRef
from qopexp.io.artifact_store import ArtifactStore
from qopexp.workloads.utils import expand_env_vars, require, stable_id


def _load_npz(path: Path) -> Dict[str, np.ndarray]:
    with np.load(path, allow_pickle=False) as z:
        return {k: z[k] for k in z.files}


@dataclass
class ANNCandidateRangeWorkload:
    store: ArtifactStore

    def instantiate(self, workload_cfg: Dict[str, Any], dataset):
        cfg = expand_env_vars(workload_cfg)
        require(cfg, ["name", "params"], where="workload_cfg")

        wname = str(cfg["name"])
        params = cfg["params"]

        require(params, ["dataset_view", "candidate_predicate", "query_selection"], where="workload_cfg.params")
        cand = params["candidate_predicate"]
        qsel = params["query_selection"]

        delta_list = list(cand.get("sweeps", {}).get("delta", []))
        if not delta_list:
            raise ValueError("ann_candidate_range requires params.candidate_predicate.sweeps.delta")

        queries_per_run = int(qsel.get("queries_per_run", 200))
        repeats = int(qsel.get("repeats", 1))
        seed = int(qsel.get("seed", 2025))

        dp = dataset.payload
        base_path = dp.get("base_path")
        query_path = dp.get("query_path")
        if not base_path or not query_path:
            raise ValueError("ANN dataset artifact must include payload.base_path and payload.query_path")

        # Resolve artifact-local paths
        art_dir = self.store.paths.artifacts_root / ArtifactStage.DATASETS.value / dataset.manifest.artifact_id
        base_npz = art_dir / str(base_path)
        query_npz = art_dir / str(query_path)

        base = _load_npz(base_npz)
        qry = _load_npz(query_npz)

        proj0_sorted = base["proj_0"].astype(np.float64)
        N = int(proj0_sorted.shape[0])

        q_proj0 = qry["proj_0"].astype(np.float64)
        q_count = int(q_proj0.shape[0])

        rng = np.random.default_rng(seed)

        instances: List[Dict[str, Any]] = []
        for rep in range(repeats):
            # sample queries
            q_idx = rng.choice(q_count, size=min(queries_per_run, q_count), replace=False)

            for qi in q_idx.tolist():
                qv = float(q_proj0[int(qi)])

                for delta in delta_list:
                    delta = float(delta)
                    lo = int(np.searchsorted(proj0_sorted, qv - delta, side="left"))
                    hi = int(np.searchsorted(proj0_sorted, qv + delta, side="right"))
                    M = max(0, hi - lo)
                    sel = float(M) / float(N) if N > 0 else 0.0

                    predicate = {
                        "type": "qid_range",
                        "column": "qid",
                        "lo": lo,
                        "hi": hi,
                        "N": N,
                        "M": M,
                        "selectivity": sel,
                        "query_index": int(qi),
                        "q_proj0": qv,
                        "delta": delta,
                    }

                    qid = stable_id(wname, "rep", rep, "qi", qi, "d", delta)
                    instances.append(
                        {
                            "query_id": qid,
                            "sql": "",  # optional; for ANN workloads, logic is defined by predicate+verification
                            "params": {"query_index": int(qi), "delta": delta},
                            "predicate": predicate,
                            "tags": {
                                "workload": wname,
                                "N": N,
                                "M": M,
                                "selectivity": sel,
                                "delta": delta,
                                "repeat_id": rep,
                                "seed": seed,
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
            metrics={"instance_count": len(instances), "N": N, "query_count": q_count},
            inputs=[ArtifactRef(stage=ArtifactStage.DATASETS, artifact_id=dataset.manifest.artifact_id)],
            code_ref=CodeRef(),
            config_refs=config_refs,
            seed=seed,
            backend_name=None,
            backend_profile_sha256=None,
            extra_manifest={"source": "ann_candidate_range"},
        )
        return env
