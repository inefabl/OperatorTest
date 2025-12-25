# src/qopexp/io/artifact_store.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

from qopexp.contracts import (
    ArtifactEnvelope,
    ArtifactManifest,
    ArtifactRef,
    ArtifactStage,
    CodeRef,
    ConfigRef,
)
from qopexp.contracts.validation import validate_envelope, ContractError
from .serializers import write_json, read_json
from .hashing import compute_artifact_id


@dataclass(frozen=True)
class StorePaths:
    """
    Defines where artifacts live on disk.
    """
    repo_root: Path
    artifacts_root: Path

    @staticmethod
    def from_repo_root(repo_root: str | Path) -> "StorePaths":
        rr = Path(repo_root).resolve()
        return StorePaths(repo_root=rr, artifacts_root=rr / "artifacts")


class ArtifactStore:
    """
    Artifact storage is immutable-by-convention:
    - create_* writes new artifact_id directory.
    - load_* reads an existing artifact.

    On disk:
      artifacts/<stage>/<artifact_id>/
        manifest.json
        payload.json
        metrics.json (optional)
    """

    def __init__(self, paths: StorePaths):
        self.paths = paths
        self.paths.artifacts_root.mkdir(parents=True, exist_ok=True)

    def _artifact_dir(self, stage: ArtifactStage, artifact_id: str) -> Path:
        return self.paths.artifacts_root / stage.value / artifact_id

    def exists(self, stage: ArtifactStage, artifact_id: str) -> bool:
        d = self._artifact_dir(stage, artifact_id)
        return (d / "manifest.json").exists() and (d / "payload.json").exists()

    def load(self, stage: ArtifactStage, artifact_id: str) -> ArtifactEnvelope:
        d = self._artifact_dir(stage, artifact_id)
        manifest_path = d / "manifest.json"
        payload_path = d / "payload.json"
        metrics_path = d / "metrics.json"

        if not manifest_path.exists():
            raise FileNotFoundError(f"Missing manifest: {manifest_path}")
        if not payload_path.exists():
            raise FileNotFoundError(f"Missing payload: {payload_path}")

        m = ArtifactManifest.from_dict(read_json(manifest_path))
        payload = read_json(payload_path)
        metrics = read_json(metrics_path) if metrics_path.exists() else None

        env = ArtifactEnvelope(manifest=m, payload=payload, metrics=metrics)
        validate_envelope(env)
        return env

    def create(
        self,
        *,
        stage: ArtifactStage,
        kind: str,
        name: str,
        description: str,
        payload: Dict[str, Any],
        metrics: Optional[Dict[str, Any]] = None,
        inputs: Optional[list[ArtifactRef]] = None,
        code_ref: Optional[CodeRef] = None,
        config_refs: Optional[list[ConfigRef]] = None,
        seed: Optional[int] = None,
        backend_name: Optional[str] = None,
        backend_profile_sha256: Optional[str] = None,
        spec_version: int = 1,
        allow_overwrite: bool = False,
        extra_manifest: Optional[Dict[str, Any]] = None,
    ) -> ArtifactEnvelope:
        """
        Creates an artifact with a deterministic artifact_id based on content + provenance.

        Note:
        - created_at_utc is not part of hashing.
        - artifact_id directory is created and files are written.
        """
        inputs = inputs or []
        config_refs = config_refs or []
        code_ref = code_ref or CodeRef()
        extra_manifest = extra_manifest or {}

        # Prepare hash inputs (dict forms)
        inputs_dict = [x.to_dict() for x in inputs]
        config_dict = [c.to_dict() for c in config_refs]

        artifact_id = compute_artifact_id(
            stage=stage.value,
            kind=kind,
            name=name,
            inputs=inputs_dict,
            config_refs=config_dict,
            seed=seed,
            backend_name=backend_name,
            backend_profile_sha256=backend_profile_sha256,
            payload=payload,
            metrics=metrics,
        )

        d = self._artifact_dir(stage, artifact_id)
        if d.exists() and not allow_overwrite:
            # If it exists, ensure it is consistent and just return it.
            try:
                env = self.load(stage, artifact_id)
                return env
            except Exception as e:
                raise RuntimeError(
                    f"Artifact dir exists but failed to load: {d}. "
                    f"Use allow_overwrite=True if you intend to overwrite."
                ) from e

        d.mkdir(parents=True, exist_ok=True)

        manifest = ArtifactManifest(
            spec_version=spec_version,
            stage=stage,
            artifact_id=artifact_id,
            name=name,
            kind=kind,
            description=description,
            inputs=inputs,
            code_ref=code_ref,
            config_refs=config_refs,
            seed=seed,
            backend_name=backend_name,
            backend_profile_sha256=backend_profile_sha256,
            extra=extra_manifest,
        )

        env = ArtifactEnvelope(manifest=manifest, payload=payload, metrics=metrics)
        validate_envelope(env)

        # Write to disk
        write_json(d / "manifest.json", manifest.to_dict())
        write_json(d / "payload.json", payload)
        if metrics is not None:
            write_json(d / "metrics.json", metrics)

        return env

    def validate_on_disk(self, stage: ArtifactStage, artifact_id: str) -> None:
        """
        Loads and validates an artifact. Useful for CI.
        """
        env = self.load(stage, artifact_id)
        try:
            validate_envelope(env)
        except ContractError as e:
            raise ContractError(f"Validation failed for {stage.value}/{artifact_id}: {e}") from e
