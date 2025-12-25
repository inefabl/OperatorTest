# src/qopexp/planner/simple_planner.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from qopexp.contracts import ArtifactStage, ArtifactRef, CodeRef, ConfigRef
from qopexp.io.artifact_store import ArtifactStore
from qopexp.planner.utils import expand_env_vars, require, get_nested


@dataclass
class SimpleHybridPlanner:
    """
    Minimal, backend-agnostic planner.

    Input:
      - experiment_cfg: configs/experiments/*.yaml (already loaded)
      - workload: WorkloadInstanceArtifact

    Output:
      - PlanArtifact with a small operator DAG that is intentionally abstract:
        * does not encode physical execution details
        * does not generate circuits
        * defines qkernel hook points and fallback paths
    """
    store: ArtifactStore

    def build(self, experiment_cfg: Dict[str, Any], workload):
        cfg = expand_env_vars(experiment_cfg)
        require(cfg, ["name", "kind", "refs"], where="experiment_cfg")

        exp_name = str(cfg["name"])
        refs = cfg["refs"]
        require(refs, ["dataset", "workload", "kernel", "backend"], where="experiment_cfg.refs")

        # Basic workload shape
        w_payload = workload.payload
        instances = list(w_payload.get("instances") or [])
        instance_count = len(instances)

        # Baselines are declared in experiment config
        baselines = list(cfg.get("baselines", []) or [])
        if not baselines:
            # Conservative default baseline
            baselines = [{"name": "classical_default", "type": "scan_filter"}]

        # Policies
        verification_enabled = bool(get_nested(cfg, "policies.verification.enabled", True))
        verification_method = str(get_nested(cfg, "policies.verification.method", "classical_predicate_verify"))

        # Kernel hook (only references here; kernel builder will load kernel cfg by ref)
        kernel_ref_path = str(refs["kernel"])
        backend_ref_path = str(refs["backend"])

        # Binding rule: how downstream stages locate predicates / tags in workload instances
        binding = {
            "workload_instances_field": "instances",
            "predicate_field": "predicate",
            "tags_field": "tags",
            "query_id_field": "query_id",
        }

        # Operator graph: keep it minimal and explicit
        operators: List[Dict[str, Any]] = []

        # 0) Workload source
        operators.append(
            {
                "op_id": "workload_source",
                "op_type": "workload_source",
                "inputs": [],
                "params": {
                    "workload_artifact_id": workload.manifest.artifact_id,
                    "instance_count": instance_count,
                },
            }
        )

        # 1) Classical baseline branch (can represent multiple baselines)
        operators.append(
            {
                "op_id": "classic_baseline",
                "op_type": "classical_baseline",
                "inputs": ["workload_source"],
                "params": {
                    "baselines": baselines,  # list of baseline descriptors
                    "notes": "Baselines are executed by evaluator/runner; planner only declares them.",
                },
            }
        )

        # 2) Hybrid / quantum branch
        # 2.1 qkernel hook
        qkernel_op: Dict[str, Any] = {
            "op_id": "qkernel_hook",
            "op_type": "qkernel_hook",
            "inputs": ["workload_source"],
            "params": {
                "kernel_config_ref": kernel_ref_path,
                "backend_config_ref": backend_ref_path,
                "binding": binding,
                # Optional overrides are carried as metadata; kernel builder may apply them
                "kernel_overrides": cfg.get("overrides", {}).get("kernel", {}),
            },
            # Fallback declared at operator level
            "fallback": {
                "op_type": "classical_fallback",
                "params": {
                    "fallback_to": "classic_baseline",
                    "reason": "qkernel failure / low confidence / runtime policy",
                },
            },
        }
        operators.append(qkernel_op)

        # 2.2 verification / post-check
        if verification_enabled:
            operators.append(
                {
                    "op_id": "verify",
                    "op_type": "verify",
                    "inputs": ["qkernel_hook"],
                    "params": {
                        "method": verification_method,
                        "binding": binding,
                        "notes": "Verification happens classically; may trigger fallback if needed.",
                    },
                    "fallback": {
                        "op_type": "classical_fallback",
                        "params": {
                            "fallback_to": "classic_baseline",
                            "reason": "verification failed or confidence too low",
                        },
                    },
                }
            )
            hybrid_tail_input = "verify"
        else:
            hybrid_tail_input = "qkernel_hook"

        # 2.3 output (logical sink)
        operators.append(
            {
                "op_id": "output",
                "op_type": "output",
                "inputs": ["classic_baseline", hybrid_tail_input],
                "params": {
                    "outputs": [
                        {"name": "classic_results", "from": "classic_baseline"},
                        {"name": "hybrid_results", "from": hybrid_tail_input},
                    ]
                },
            }
        )

        payload: Dict[str, Any] = {
            "experiment_name": exp_name,
            "workload_artifact_id": workload.manifest.artifact_id,
            "operators": operators,
            "plan_metadata": {
                "binding": binding,
                "refs": {
                    "dataset": refs["dataset"],
                    "workload": refs["workload"],
                    "kernel": refs["kernel"],
                    "backend": refs["backend"],
                },
                "overrides": cfg.get("overrides", {}),
                "policies": cfg.get("policies", {}),
            },
        }

        # Lineage: include workload ref + (optionally) propagate upstream dataset ref for convenience
        inputs: List[ArtifactRef] = [ArtifactRef(stage=ArtifactStage.WORKLOAD_INSTANCES, artifact_id=workload.manifest.artifact_id)]
        for up in workload.manifest.inputs or []:
            if up.stage == ArtifactStage.DATASETS:
                inputs.append(up)

        # Optional config ref for experiment config file if injected
        config_refs: List[ConfigRef] = []
        cfg_path = cfg.get("__config_path__")
        if isinstance(cfg_path, str) and cfg_path:
            from qopexp.io.hashing import sha256_file
            try:
                config_refs.append(ConfigRef(path=cfg_path, sha256=sha256_file(cfg_path)))
            except Exception:
                pass

        metrics = {
            "instance_count": instance_count,
            "baseline_count": len(baselines),
            "verification_enabled": verification_enabled,
        }

        env = self.store.create(
            stage=ArtifactStage.PLANS,
            kind="PlanArtifact",
            name=exp_name,
            description=str(cfg.get("description", "")),
            payload=payload,
            metrics=metrics,
            inputs=inputs,
            code_ref=CodeRef(),
            config_refs=config_refs,
            seed=None,
            backend_name=None,
            backend_profile_sha256=None,
            extra_manifest={"planner": "SimpleHybridPlanner"},
        )
        return env
