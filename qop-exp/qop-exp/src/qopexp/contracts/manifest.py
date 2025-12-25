# src/qopexp/contracts/manifest.py
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from .enums import ArtifactStage


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


@dataclass(frozen=True)
class CodeRef:
    """
    Immutable reference to the producing code version.
    Use git_commit when available; fallback to a semantic version.
    """

    git_commit: Optional[str] = None
    version: Optional[str] = None
    dirty: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ConfigRef:
    """
    Reference to a config file and its content hash (for provenance and reproducibility).
    """

    path: str
    sha256: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ArtifactRef:
    """
    Lightweight pointer to an upstream artifact (by stage + artifact_id).
    """

    stage: ArtifactStage
    artifact_id: str

    def to_dict(self) -> Dict[str, Any]:
        return {"stage": self.stage.value, "artifact_id": self.artifact_id}


@dataclass
class ArtifactManifest:
    """
    Required metadata for every artifact directory:
      artifacts/<stage>/<artifact_id>/manifest.json

    This manifest must be sufficient to reproduce the artifact from its inputs.
    """

    spec_version: int
    stage: ArtifactStage
    artifact_id: str

    name: str
    kind: str  # e.g., "DatasetArtifact", "PlanArtifact", "RawResultArtifact"
    description: str = ""

    created_at_utc: str = field(default_factory=_utc_now_iso)

    inputs: List[ArtifactRef] = field(default_factory=list)

    # Provenance anchors
    code_ref: CodeRef = field(default_factory=CodeRef)
    config_refs: List[ConfigRef] = field(default_factory=list)

    # Determinism controls
    seed: Optional[int] = None

    # Backend context (optional, but recommended for compiled/jobs/results)
    backend_name: Optional[str] = None
    backend_profile_sha256: Optional[str] = None

    # Free-form extension point (strictly metadata; not payload)
    extra: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {
            "spec_version": self.spec_version,
            "stage": self.stage.value,
            "artifact_id": self.artifact_id,
            "name": self.name,
            "kind": self.kind,
            "description": self.description,
            "created_at_utc": self.created_at_utc,
            "inputs": [x.to_dict() for x in self.inputs],
            "code_ref": self.code_ref.to_dict(),
            "config_refs": [c.to_dict() for c in self.config_refs],
            "seed": self.seed,
            "backend_name": self.backend_name,
            "backend_profile_sha256": self.backend_profile_sha256,
            "extra": self.extra,
        }
        return d

    @staticmethod
    def from_dict(obj: Dict[str, Any]) -> "ArtifactManifest":
        return ArtifactManifest(
            spec_version=int(obj["spec_version"]),
            stage=ArtifactStage(obj["stage"]),
            artifact_id=str(obj["artifact_id"]),
            name=str(obj["name"]),
            kind=str(obj["kind"]),
            description=str(obj.get("description", "")),
            created_at_utc=str(obj.get("created_at_utc", _utc_now_iso())),
            inputs=[
                ArtifactRef(stage=ArtifactStage(x["stage"]), artifact_id=str(x["artifact_id"]))
                for x in obj.get("inputs", [])
            ],
            code_ref=CodeRef(**obj.get("code_ref", {})),
            config_refs=[ConfigRef(**c) for c in obj.get("config_refs", [])],
            seed=obj.get("seed", None),
            backend_name=obj.get("backend_name", None),
            backend_profile_sha256=obj.get("backend_profile_sha256", None),
            extra=dict(obj.get("extra", {})),
        )
