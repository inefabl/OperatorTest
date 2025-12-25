# src/qopexp/backends/wukong72_backend.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List

from qopexp.contracts import ArtifactStage, ArtifactRef, CodeRef, ConfigRef
from qopexp.contracts.result_schema import RAW_SCHEMA_VERSION
from qopexp.io.artifact_store import ArtifactStore
from qopexp.io.serializers import read_json
from .utils import expand_env_vars, require, get_nested


@dataclass
class Wukong72BackendAdapter:
    store: ArtifactStore

    def _maybe_fetch_qcloud_info(self, params: Dict[str, Any]) -> Dict[str, Any]:
        api_key = str(params.get("api_key", "")).strip()
        backend_name = str(params.get("qcloud_backend", "origin_wukong")).strip()
        if not api_key:
            return {}

        try:
            from pyqpanda3.qcloud import QCloudService  # type: ignore
        except Exception:
            return {"error": "pyqpanda3 not installed; skip qcloud fetch"}

        service = QCloudService(api_key)
        backend = service.backend(backend_name)
        chip_info = backend.chip_info()
        topo = chip_info.get_chip_topology()

        single_qubits = []
        for q in chip_info.single_qubit_info() or []:
            # Best-effort dict serialization
            single_qubits.append(getattr(q, "to_dict", lambda: {"value": str(q)})())

        double_qubits = []
        for dq in chip_info.double_qubits_info() or []:
            double_qubits.append(
                {
                    "qubits": dq.get_qubits(),
                    "fidelity": dq.get_fidelity(),
                }
            )

        return {
            "backend": backend_name,
            "topology": topo,
            "single_qubits": single_qubits,
            "double_qubits": double_qubits,
        }

    def submit(self, backend_cfg: Dict[str, Any], compiled):
        cfg = expand_env_vars(backend_cfg)
        require(cfg, ["name", "params"], where="backend_cfg")

        backend_name = str(cfg["name"])
        params = cfg["params"]
        require(params, ["device", "runtime", "compilation"], where="backend_cfg.params")

        max_walltime = int(get_nested(params, "runtime.max_walltime_sec", 60))
        shots_cap = int(get_nested(params, "runtime.shots_cap_per_job", 200000))
        concurrency = int(get_nested(params, "runtime.concurrency", 1))
        retry_enabled = bool(get_nested(params, "runtime.retry.enabled", True))
        max_retries = int(get_nested(params, "runtime.retry.max_retries", 2))

        compiled_circuits = list((compiled.payload.get("compiled_circuits") or []))

        # Policy: group circuits into a single "task" job for real backend (common in many SDKs),
        # but still allow split later.
        jobs = []
        circuit_ids = [str(c.get("circuit_id")) for c in compiled_circuits]

        # Determine shots per circuit from tags
        shots_per_circuit = []
        for c in compiled_circuits:
            tags = c.get("tags", {}) or {}
            shots = int(tags.get("shots", 4096) or 4096)
            shots_per_circuit.append(min(shots, shots_cap))

        jobs.append(
            {
                "job_id": f"wukong72::{compiled.manifest.artifact_id}",
                "circuit_ids": circuit_ids,
                "shots": shots_per_circuit,
                "status": "PENDING_SUBMISSION",
                "extra": {
                    "max_walltime_sec": max_walltime,
                    "concurrency": concurrency,
                    "retry": {"enabled": retry_enabled, "max_retries": max_retries},
                    "compilation_policy": params.get("compilation", {}),
                },
            }
        )

        qcloud_info = self._maybe_fetch_qcloud_info(params)

        payload = {
            "backend_name": backend_name,
            "submission": {
                "mode": "real_device_placeholder",
                "device": params.get("device", {}),
                "note": "This is a placeholder. Integrate the vendor SDK to actually submit jobs.",
            },
            "jobs": jobs,
            "qcloud_info": qcloud_info,
        }

        env = self.store.create(
            stage=ArtifactStage.JOBS,
            kind="JobArtifact",
            name=f"wukong72_jobs_{compiled.manifest.artifact_id}",
            description="Real-device job descriptor (placeholder; not submitted)",
            payload=payload,
            metrics={"job_count": len(jobs), "circuit_count": len(circuit_ids)},
            inputs=[ArtifactRef(stage=ArtifactStage.COMPILED, artifact_id=compiled.manifest.artifact_id)],
            code_ref=CodeRef(),
            config_refs=[],
            seed=compiled.manifest.seed,
            backend_name=backend_name,
            backend_profile_sha256=compiled.manifest.backend_profile_sha256,
            extra_manifest={"backend_type": "real_device", "device_qubits": int(get_nested(params, "device.qubits", 72))},
        )
        return env

    def ingest(self, backend_cfg: Dict[str, Any], job):
        cfg = expand_env_vars(backend_cfg)
        require(cfg, ["name", "params"], where="backend_cfg")

        backend_name = str(cfg["name"])
        params = cfg["params"]

        ingest_cfg = get_nested(params, "runtime.ingest", {}) or {}
        results_path = ingest_cfg.get("from_json", ingest_cfg.get("path", None))
        if not results_path:
            raise ValueError(
                "Wukong72BackendAdapter.ingest requires params.runtime.ingest.from_json "
                "to point to a results JSON file."
            )

        rp = Path(str(results_path)).expanduser()
        if not rp.exists():
            raise FileNotFoundError(f"Ingest results file not found: {rp}")

        data = read_json(rp)
        if isinstance(data, list):
            results = data
            backend_meta = {}
        elif isinstance(data, dict):
            results = data.get("results", []) or []
            backend_meta = data.get("backend_metadata", {}) or {}
            backend_name = str(data.get("backend_name", backend_name))
        else:
            raise ValueError("Ingest results JSON must be a list or dict with a 'results' field.")

        # Attach tags from compiled circuits when missing
        compiled_aid = str((job.manifest.inputs[0].artifact_id) if job.manifest.inputs else "")
        compiled = self.store.load(ArtifactStage.COMPILED, compiled_aid) if compiled_aid else None
        compiled_tags = {}
        if compiled is not None:
            for c in (compiled.payload.get("compiled_circuits") or []):
                compiled_tags[str(c.get("circuit_id"))] = c.get("tags", {}) or {}

        normalized: List[Dict[str, Any]] = []
        for r in results:
            if not isinstance(r, dict):
                continue
            cid = str(r.get("circuit_id", ""))
            counts = r.get("counts", {}) or {}
            shots = r.get("shots", None)
            if shots is None:
                try:
                    shots = int(sum(int(v) for v in counts.values()))
                except Exception:
                    shots = None

            tags = r.get("tags", {}) or {}
            if not tags and cid in compiled_tags:
                tags = compiled_tags[cid]
            if "flag_bit_index" not in tags:
                tags["flag_bit_index"] = 0  # default: c[0] is rightmost bit

            normalized.append(
                {
                    "job_id": r.get("job_id", str(job.manifest.artifact_id)),
                    "circuit_id": cid,
                    "shots": shots,
                    "counts": counts,
                    "metadata": r.get("metadata", {}) or {},
                    "tags": tags,
                }
            )

        payload = {
            "backend_name": backend_name,
            "results": normalized,
            "backend_metadata": backend_meta,
            "schema_version": RAW_SCHEMA_VERSION,
            "counts_bit_order": "lsb_rightmost",
        }

        env = self.store.create(
            stage=ArtifactStage.RESULTS_RAW,
            kind="RawResultArtifact",
            name=f"raw_results_{backend_name}_{job.manifest.artifact_id}",
            description="Raw results ingested from real device",
            payload=payload,
            metrics={"result_count": len(normalized)},
            inputs=[
                ArtifactRef(stage=ArtifactStage.JOBS, artifact_id=job.manifest.artifact_id),
                ArtifactRef(stage=ArtifactStage.COMPILED, artifact_id=compiled_aid) if compiled_aid else ArtifactRef(stage=ArtifactStage.JOBS, artifact_id=job.manifest.artifact_id),
            ],
            code_ref=CodeRef(),
            config_refs=[],
            seed=job.manifest.seed,
            backend_name=backend_name,
            backend_profile_sha256=job.manifest.backend_profile_sha256,
            extra_manifest={"backend_type": "real_device"},
        )
        return env
