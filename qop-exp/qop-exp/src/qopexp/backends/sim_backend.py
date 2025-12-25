# src/qopexp/backends/sim_backend.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import numpy as np

from qopexp.contracts import ArtifactStage, ArtifactRef, CodeRef, ConfigRef
from qopexp.contracts.result_schema import RAW_SCHEMA_VERSION
from qopexp.io.artifact_store import ArtifactStore
from .utils import expand_env_vars, require, get_nested


@dataclass
class SimBackendAdapter:
    store: ArtifactStore

    def submit(self, backend_cfg: Dict[str, Any], compiled):
        cfg = expand_env_vars(backend_cfg)
        require(cfg, ["name", "params"], where="backend_cfg")

        backend_name = str(cfg["name"])
        params = cfg["params"]

        # Determine job partitioning policy
        shots_cap = int(get_nested(params, "runtime.shots_cap_per_job", 200000))
        max_walltime = int(get_nested(params, "runtime.max_walltime_sec", 600))

        compiled_circuits = list((compiled.payload.get("compiled_circuits") or []))
        jobs = []

        # Simple policy: one job per circuit (can be optimized later)
        for c in compiled_circuits:
            cid = str(c.get("circuit_id"))
            tags = c.get("tags", {}) or {}
            shots = int(tags.get("shots", 4096) or 4096)
            jobs.append(
                {
                    "job_id": f"sim::{compiled.manifest.artifact_id}::{cid}",
                    "circuit_ids": [cid],
                    "shots": min(shots, shots_cap),
                    "status": "SUBMITTED",
                    "extra": {"max_walltime_sec": max_walltime},
                }
            )

        payload = {
            "backend_name": backend_name,
            "submission": {"mode": "sim", "jobs_policy": "one_circuit_per_job"},
            "jobs": jobs,
        }

        env = self.store.create(
            stage=ArtifactStage.JOBS,
            kind="JobArtifact",
            name=f"sim_jobs_{compiled.manifest.artifact_id}",
            description="Sim backend job descriptors",
            payload=payload,
            metrics={"job_count": len(jobs)},
            inputs=[ArtifactRef(stage=ArtifactStage.COMPILED, artifact_id=compiled.manifest.artifact_id)],
            code_ref=CodeRef(),
            config_refs=[],
            seed=compiled.manifest.seed,
            backend_name=backend_name,
            backend_profile_sha256=compiled.manifest.backend_profile_sha256,
            extra_manifest={"backend_type": "simulator"},
        )
        return env

    def ingest(self, backend_cfg: Dict[str, Any], job):
        """
        Generates synthetic measurement counts for each circuit.
        Model:
          - 1-bit measurement with success probability p derived from selectivity / M/N
          - counts: {"0": shots*(1-p), "1": shots*p}
        """
        cfg = expand_env_vars(backend_cfg)
        require(cfg, ["name", "params"], where="backend_cfg")

        backend_name = str(cfg["name"])
        params = cfg["params"]
        seed = int(get_nested(params, "runtime.seed", 2025))

        # Load compiled artifact (lineage)
        compiled_aid = str((job.manifest.inputs[0].artifact_id) if job.manifest.inputs else "")
        if not compiled_aid:
            raise ValueError("JobArtifact missing compiled artifact input")
        compiled = self.store.load(ArtifactStage.COMPILED, compiled_aid)

        compiled_circuits = {str(c["circuit_id"]): c for c in (compiled.payload.get("compiled_circuits") or [])}

        rng = np.random.default_rng(seed)

        def _selectivity_from_tags(tags: Dict[str, Any]) -> float:
            """
            Prefer theoretical selectivity from predicate bounds (lo, hi, N).
            Fallback to explicit selectivity or M/N when bounds are missing.
            """
            pred_type = str(tags.get("predicate_type", "")).lower()
            lo = tags.get("lo", None)
            hi = tags.get("hi", None)
            N = tags.get("N", None)

            if isinstance(N, (int, float)) and N:
                if isinstance(lo, (int, float)) and isinstance(hi, (int, float)):
                    if pred_type == "qid_range":
                        return max(0.0, min(1.0, float(hi - lo) / float(N)))
                    if pred_type == "qid_lt":
                        return max(0.0, min(1.0, float(hi) / float(N)))

            # Fallbacks
            sel = tags.get("selectivity", None)
            if isinstance(sel, (int, float)):
                return max(0.0, min(1.0, float(sel)))
            M = tags.get("M", None)
            if isinstance(N, (int, float)) and N and isinstance(M, (int, float)):
                return max(0.0, min(1.0, float(M) / float(N)))
            return 0.5

        results: List[Dict[str, Any]] = []
        for j in job.payload.get("jobs", []) or []:
            shots = int(j.get("shots") or 4096)
            for cid in j.get("circuit_ids", []) or []:
                c = compiled_circuits.get(str(cid))
                if c is None:
                    continue
                tags = c.get("tags", {}) or {}

                # Derive p from predicate bounds (lo, hi, N) when available
                p = _selectivity_from_tags(tags)

                # binomial sample
                ones = int(rng.binomial(n=shots, p=p))
                zeros = int(shots - ones)
                counts = {"0": zeros, "1": ones}

                results.append(
                    {
                        "job_id": str(j.get("job_id")),
                        "circuit_id": str(cid),
                        "shots": shots,
                        "counts": counts,
                        "metadata": {
                            "synthetic": True,
                            "p_used": p,
                            "seed": seed,
                        },
                        "tags": tags,
                    }
                )

        payload = {
            "backend_name": backend_name,
            "results": results,
            "backend_metadata": {
                "mode": "sim_synthetic_counts",
                "seed": seed,
                "result_count": len(results),
            },
            "schema_version": RAW_SCHEMA_VERSION,
            "counts_bit_order": "lsb_rightmost",
        }

        env = self.store.create(
            stage=ArtifactStage.RESULTS_RAW,
            kind="RawResultArtifact",
            name=f"raw_results_sim_{job.manifest.artifact_id}",
            description="Synthetic raw results from simulator placeholder",
            payload=payload,
            metrics={"result_count": len(results)},
            inputs=[
                ArtifactRef(stage=ArtifactStage.JOBS, artifact_id=job.manifest.artifact_id),
                ArtifactRef(stage=ArtifactStage.COMPILED, artifact_id=compiled_aid),
            ],
            code_ref=CodeRef(),
            config_refs=[],
            seed=seed,
            backend_name=backend_name,
            backend_profile_sha256=compiled.manifest.backend_profile_sha256,
            extra_manifest={"backend_type": "simulator", "synthetic": True},
        )
        return env
