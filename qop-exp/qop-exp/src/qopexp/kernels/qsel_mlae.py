# src/qopexp/kernels/qsel_mlae.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List

from qopexp.contracts import ArtifactStage, ArtifactRef, CodeRef, ConfigRef
from qopexp.io.artifact_store import ArtifactStore
from qopexp.kernels.utils import expand_env_vars, require, get_nested
from qopexp.kernels.qasm_primitives import build_qasm2_mlae_schedule_element


@dataclass
class MLAESelectivityKernel:
    """
    Real MLAE schedule generator:
      - for each query instance, generate circuits for k in k_list
      - each circuit measures 1-bit success flag after Q^k
    """
    store: ArtifactStore

    def build(self, kernel_cfg: Dict[str, Any], plan):
        cfg = expand_env_vars(kernel_cfg)
        require(cfg, ["name", "params"], where="kernel_cfg")

        kname = str(cfg["name"])
        params = cfg["params"]
        require(params, ["kernel_type", "register", "ae"], where="kernel_cfg.params")

        n_index = int(get_nested(params, "register.index_qubits", 18))
        k_list = list(get_nested(params, "ae.schedule.k_list", [0, 1, 2, 4]))
        shots_per_k = int(get_nested(params, "ae.schedule.shots_per_k", 2048))

        p = plan.payload
        workload_aid = str(p.get("workload_artifact_id", ""))
        if not workload_aid:
            raise ValueError("Plan payload missing workload_artifact_id")

        workload = self.store.load(ArtifactStage.WORKLOAD_INSTANCES, workload_aid)
        instances = list((workload.payload.get("instances") or []))

        circuits: List[Dict[str, Any]] = []

        for inst in instances:
            qid = str(inst.get("query_id"))
            pred = inst.get("predicate", {}) or {}
            tags0 = inst.get("tags", {}) or {}

            pred_type = str(pred.get("type", tags0.get("type", "qid_lt")))
            if pred_type not in ("qid_lt", "qid_range"):
                pred_type = "qid_lt"

            if pred_type == "qid_lt":
                hi = int(pred.get("hi", pred.get("M", tags0.get("M", 0)) or 0))
                lo = 0
            else:
                lo = int(pred.get("lo", 0) or 0)
                hi = int(pred.get("hi", 0) or 0)

            dom = 1 << n_index
            lo = max(0, min(dom, lo))
            hi = max(0, min(dom, hi))

            N = int(pred.get("N", tags0.get("N", 0)) or 0)
            M = int(pred.get("M", tags0.get("M", 0)) or 0)

            for k in k_list:
                k_int = int(k)
                qasm = build_qasm2_mlae_schedule_element(
                    n_index=n_index,
                    k=k_int,
                    pred_type=pred_type,
                    lo=lo,
                    hi=hi,
                    flags_count=3,
                )
                circuits.append(
                    {
                        "circuit_id": f"{qid}__k{k_int}",
                        "qasm": qasm,
                        "tags": {
                            "query_id": qid,
                            "variant": "quantum",
                            "kernel": kname,
                            "ae_k": k_int,
                            "shots": shots_per_k,
                            "predicate_type": pred_type,
                            "lo": lo,
                            "hi": hi,
                            "N": N,
                            "M": M,
                            "selectivity": float(M) / float(N) if N > 0 else None,
                        },
                        "logical_metrics": {
                            "index_qubits": n_index,
                            "ae_k": k_int,
                            "shots": shots_per_k,
                            "domain_size": 1 << n_index,
                            "logical_depth_est": int(max(1, k_int) * max(20, 8 * n_index)),
                        },
                    }
                )

        payload: Dict[str, Any] = {
            "kernel_name": kname,
            "circuit_format": "qasm2",
            "circuits": circuits,
            "logical_metrics": {
                "index_qubits": n_index,
                "k_list": [int(x) for x in k_list],
                "shots_per_k": shots_per_k,
                "query_count": len(instances),
                "circuit_count": len(circuits),
            },
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
            stage=ArtifactStage.CIRCUITS,
            kind="CircuitArtifact",
            name=f"{kname}",
            description=str(cfg.get("description", "")),
            payload=payload,
            metrics={"circuit_count": len(circuits), "shots_per_k": shots_per_k},
            inputs=[
                ArtifactRef(stage=ArtifactStage.PLANS, artifact_id=plan.manifest.artifact_id),
                ArtifactRef(stage=ArtifactStage.WORKLOAD_INSTANCES, artifact_id=workload_aid),
            ],
            code_ref=CodeRef(),
            config_refs=config_refs,
            seed=None,
            backend_name=None,
            backend_profile_sha256=None,
            extra_manifest={"kernel_type": "selectivity_mlae_real"},
        )
        return env
