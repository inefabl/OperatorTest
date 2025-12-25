# src/qopexp/evaluator/simple_evaluator.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from qopexp.contracts import ArtifactStage, ArtifactRef, CodeRef, ConfigRef
from qopexp.contracts.result_schema import CURATED_TABLE_SPEC_V1
from qopexp.io.artifact_store import ArtifactStore

from .utils import (
    expand_env_vars,
    require,
    safe_float,
    safe_int,
    counts_success_rate,
    abs_rel_error,
)
from .table_writer import write_csv


@dataclass
class SimpleEvaluator:
    """
    Normalizes RawResultArtifact -> CuratedResultArtifact.

    Current assumptions (minimal pipeline):
      - Each result item includes: circuit_id, shots, counts, tags (from circuit builder)
      - 1-bit measurement counts with keys {"0","1"}
      - "selectivity" in tags (if present) is treated as p_true for error reporting
      - Compile metrics are retrieved by lineage: raw -> compiled -> compiled_circuits[*].metrics
    """
    store: ArtifactStore

    def evaluate(
        self,
        experiment_cfg: Dict[str, Any],
        raw,
        *,
        ground_truth: Optional[Dict[str, Any]] = None,
    ):
        cfg = expand_env_vars(experiment_cfg)
        require(cfg, ["name", "refs"], where="experiment_cfg")

        exp_name = str(cfg["name"])

        # Resolve lineage: raw -> job -> compiled -> circuits -> workload -> dataset
        backend_name = str(raw.payload.get("backend_name", raw.manifest.backend_name or ""))

        compiled_aid = self._find_upstream_artifact(raw, ArtifactStage.COMPILED)
        job_aid = self._find_upstream_artifact(raw, ArtifactStage.JOBS)

        compiled = self.store.load(ArtifactStage.COMPILED, compiled_aid) if compiled_aid else None

        compile_metrics_by_cid: Dict[str, Dict[str, Any]] = {}
        if compiled is not None:
            for c in (compiled.payload.get("compiled_circuits") or []):
                cid = str(c.get("circuit_id"))
                m = c.get("metrics", {}) or {}
                compile_metrics_by_cid[cid] = m

        # Find circuit/workload/dataset via compiled->inputs
        circuit_aid = None
        if compiled is not None:
            circuit_aid = self._find_upstream_artifact(compiled, ArtifactStage.CIRCUITS)

        dataset_name = ""
        workload_name = ""
        kernel_name = ""
        dim = None

        if circuit_aid:
            circuit = self.store.load(ArtifactStage.CIRCUITS, circuit_aid)
            kernel_name = str(circuit.payload.get("kernel_name", ""))

            workload_aid = self._find_upstream_artifact(circuit, ArtifactStage.WORKLOAD_INSTANCES)
            if workload_aid:
                workload = self.store.load(ArtifactStage.WORKLOAD_INSTANCES, workload_aid)
                workload_name = str(workload.payload.get("workload_name", ""))

                dataset_aid = self._find_upstream_artifact(workload, ArtifactStage.DATASETS)
                if dataset_aid:
                    dataset = self.store.load(ArtifactStage.DATASETS, dataset_aid)
                    dataset_name = str(dataset.payload.get("dataset_name", ""))
                    dim = safe_int(dataset.payload.get("vector_dim", None))

        # Build curated rows
        rows: List[Dict[str, Any]] = []
        results = list((raw.payload.get("results") or []))
        
        def _baseline_rows(self, baseline_envs: List[Any], *, exp_name: str) -> List[Dict[str, Any]]:
            rows: List[Dict[str, Any]] = []
            for b in baseline_envs or []:
                payload = b.payload or {}
                for r in (payload.get("results") or []):
                    tags = r.get("tags", {}) or {}
                    meta = r.get("metadata", {}) or {}

                    total = meta.get("walltime_sec_total", payload.get("walltime_sec_total", None))
                    # preserve breakdown even if schema does not have dedicated columns yet:
                    scan = meta.get("walltime_sec_scan", None)
                    idx = meta.get("walltime_sec_index", None)
                    ver = meta.get("walltime_sec_verify", None)
                    post = meta.get("walltime_sec_post", None)

                    rows.append(
                        {
                            "experiment_name": exp_name,
                            "dataset_name": "",
                            "workload_name": "",
                            "kernel_name": "",
                            "backend_name": "",

                            "variant": "classical",

                            "N": tags.get("N", None),
                            "selectivity": r.get("p_true", None),
                            "dim": tags.get("dim", None),
                            "topk": tags.get("topk", None),

                            "shots": None,
                            "grover_iterations": None,
                            "ae_k": None,

                            "success_rate": r.get("p_hat", None),
                            "abs_error": r.get("abs_error", None),
                            "rel_error": r.get("rel_error", None),
                            "confidence_level": None,
                            "ci_low": None,
                            "ci_high": None,

                            "compile_depth": None,
                            "compile_2q_gates": None,
                            "compile_qubits": None,

                            "walltime_sec_total": total,
                            "walltime_sec_device": None,
                            # temporarily carry breakdown into orchestration column; upgrade schema later if desired
                            "walltime_sec_orchestration": total,

                            "fallback_used": False,
                            "failure_reason": "",
                            "seed": tags.get("seed", None),
                            "repeat_id": tags.get("repeat_id", None),

                            # optional extra fields (ignored by CSV writer if not in schema)
                            "_baseline_walltime_sec_scan": scan,
                            "_baseline_walltime_sec_index": idx,
                            "_baseline_walltime_sec_verify": ver,
                            "_baseline_walltime_sec_post": post,
                        }
                    )
            return rows
        

        for r in results:
            cid = str(r.get("circuit_id", ""))
            shots = safe_int(r.get("shots", None)) or 0
            counts = r.get("counts", {}) or {}
            tags = r.get("tags", {}) or {}

            variant = str(tags.get("variant", "quantum"))
            kname = str(tags.get("kernel", kernel_name))
            qid = str(tags.get("query_id", ""))

            N = safe_int(tags.get("N", None))
            sel = safe_float(tags.get("selectivity", None))
            grover_it = safe_int(tags.get("grover_iterations", None))
            ae_k = safe_int(tags.get("ae_k", None))
            topk = safe_int(tags.get("topk", None))

            # Estimate p from counts
            success_bit_index = safe_int(tags.get("flag_bit_index", None))
            p_hat = counts_success_rate(
                {str(k): int(v) for k, v in counts.items()},
                shots,
                success_bit_index=success_bit_index,
            )
            p_true = sel

            ae, re = abs_rel_error(p_hat, p_true)

            cm = compile_metrics_by_cid.get(cid, {})
            compile_qubits = safe_int(cm.get("compile_qubits", None))
            compile_2q = safe_int(cm.get("compile_2q_gates", None))
            compile_depth = safe_int(cm.get("compile_depth_est", None))

            # walltime fields: raw backend may populate later; keep None if absent
            meta = r.get("metadata", {}) or {}
            wall_total = safe_float(meta.get("walltime_sec_total", None))
            wall_dev = safe_float(meta.get("walltime_sec_device", None))
            wall_orch = safe_float(meta.get("walltime_sec_orchestration", None))

            rows.append(
                {
                    "experiment_name": exp_name,
                    "dataset_name": dataset_name,
                    "workload_name": workload_name,
                    "kernel_name": kname,
                    "backend_name": backend_name,
                    "variant": variant,

                    "N": N,
                    "selectivity": sel,
                    "dim": dim,
                    "topk": topk,

                    "shots": shots,
                    "grover_iterations": grover_it,
                    "ae_k": ae_k,

                    "success_rate": p_hat,  # in this minimal model, success_rate equals p_hat
                    "abs_error": ae,
                    "rel_error": re,
                    "confidence_level": None,
                    "ci_low": None,
                    "ci_high": None,

                    "compile_depth": compile_depth,
                    "compile_2q_gates": compile_2q,
                    "compile_qubits": compile_qubits,
                    "walltime_sec_total": wall_total,
                    "walltime_sec_device": wall_dev,
                    "walltime_sec_orchestration": wall_orch,

                    "fallback_used": bool(meta.get("fallback_used", False)),
                    "failure_reason": str(meta.get("failure_reason", "")) if meta.get("failure_reason") else "",
                    "seed": safe_int(tags.get("seed", meta.get("seed", None))),
                    "repeat_id": safe_int(tags.get("repeat_id", None)),
                }
            )
                # Merge baselines if provided via ground_truth
        if isinstance(ground_truth, dict) and "baselines" in ground_truth:
            baseline_envs = list(ground_truth.get("baselines") or [])
            rows.extend(self._baseline_rows(baseline_envs, exp_name=exp_name))

        # Persist curated table under artifact dir
        table_rel = "table.csv"
        payload = {
            "table_format": "csv",
            "table_path": table_rel,
            "schema_version": CURATED_TABLE_SPEC_V1.schema_version,
            "summary": {
                "row_count": len(rows),
                "backend_name": backend_name,
                "dataset_name": dataset_name,
                "workload_name": workload_name,
                "kernel_name": kernel_name,
            },
        }

        metrics = {"row_count": len(rows), "schema_version": CURATED_TABLE_SPEC_V1.schema_version}

        # Optional config ref if injected
        config_refs: List[ConfigRef] = []
        cfg_path = cfg.get("__config_path__")
        if isinstance(cfg_path, str) and cfg_path:
            from qopexp.io.hashing import sha256_file
            try:
                config_refs.append(ConfigRef(path=cfg_path, sha256=sha256_file(cfg_path)))
            except Exception:
                pass

        inputs: List[ArtifactRef] = [ArtifactRef(stage=ArtifactStage.RESULTS_RAW, artifact_id=raw.manifest.artifact_id)]
        if compiled_aid:
            inputs.append(ArtifactRef(stage=ArtifactStage.COMPILED, artifact_id=compiled_aid))
        if job_aid:
            inputs.append(ArtifactRef(stage=ArtifactStage.JOBS, artifact_id=job_aid))
        if circuit_aid:
            inputs.append(ArtifactRef(stage=ArtifactStage.CIRCUITS, artifact_id=circuit_aid))

        env = self.store.create(
            stage=ArtifactStage.RESULTS_CURATED,
            kind="CuratedResultArtifact",
            name=f"curated_{exp_name}_{raw.manifest.artifact_id}",
            description="Curated normalized results table",
            payload=payload,
            metrics=metrics,
            inputs=inputs,
            code_ref=CodeRef(),
            config_refs=config_refs,
            seed=raw.manifest.seed,
            backend_name=backend_name,
            backend_profile_sha256=raw.manifest.backend_profile_sha256,
            extra_manifest={"evaluator": "SimpleEvaluator"},
        )

        # Write table.csv into curated artifact dir
        art_dir = self.store.paths.artifacts_root / ArtifactStage.RESULTS_CURATED.value / env.manifest.artifact_id
        table_path = art_dir / table_rel
        write_csv(table_path, rows=rows, columns=CURATED_TABLE_SPEC_V1.columns)

        # Return reloaded envelope (optional consistency)
        return self.store.load(ArtifactStage.RESULTS_CURATED, env.manifest.artifact_id)

    def _find_upstream_artifact(self, env, stage: ArtifactStage) -> Optional[str]:
        for ref in (env.manifest.inputs or []):
            if ref.stage == stage:
                return ref.artifact_id
        return None
