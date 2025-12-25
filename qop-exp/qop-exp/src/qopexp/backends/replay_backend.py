# src/qopexp/backends/replay_backend.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from qopexp.contracts import ArtifactStage, ArtifactRef, CodeRef, ConfigRef
from qopexp.io.artifact_store import ArtifactStore
from .utils import expand_env_vars, require, get_nested


@dataclass
class ReplayBackendAdapter:
    store: ArtifactStore

    def submit(self, backend_cfg: Dict[str, Any], compiled):
        """
        Replay backend does not create real jobs; it returns a JobArtifact that references the replay source.
        """
        cfg = expand_env_vars(backend_cfg)
        require(cfg, ["name", "params"], where="backend_cfg")

        backend_name = str(cfg["name"])
        params = cfg["params"]
        replay = params.get("replay", {}) or {}
        require(replay, ["from_artifact_id"], where="backend_cfg.params.replay")

        src_aid = str(replay["from_artifact_id"])

        payload = {
            "backend_name": backend_name,
            "submission": {"mode": "replay", "source_results_raw_artifact_id": src_aid},
            "jobs": [{"job_id": f"replay::{src_aid}", "circuit_ids": [], "shots": None, "status": "READY"}],
        }

        env = self.store.create(
            stage=ArtifactStage.JOBS,
            kind="JobArtifact",
            name=f"replay_job_{src_aid}",
            description="Replay job (no real submission)",
            payload=payload,
            metrics={"mode": "replay"},
            inputs=[ArtifactRef(stage=ArtifactStage.COMPILED, artifact_id=compiled.manifest.artifact_id)],
            code_ref=CodeRef(),
            config_refs=[],
            seed=compiled.manifest.seed,
            backend_name=backend_name,
            backend_profile_sha256=None,
            extra_manifest={"replay_source": src_aid},
        )
        return env

    def ingest(self, backend_cfg: Dict[str, Any], job):
        cfg = expand_env_vars(backend_cfg)
        params = cfg.get("params", {}) or {}
        replay = params.get("replay", {}) or {}
        src_aid = str(replay.get("from_artifact_id", ""))

        if not src_aid:
            # Try to read from job payload
            src_aid = str((job.payload.get("submission", {}) or {}).get("source_results_raw_artifact_id", ""))

        if not src_aid:
            raise ValueError("ReplayBackendAdapter requires params.replay.from_artifact_id")

        # Load and return existing RawResultArtifact
        raw = self.store.load(ArtifactStage.RESULTS_RAW, src_aid)
        return raw
