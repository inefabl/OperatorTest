# src/qopexp/contracts/validation.py
from __future__ import annotations

from typing import Any, Dict, Optional

from .artifacts import expected_stage_for_kind, ArtifactEnvelope
from .enums import ArtifactStage


class ContractError(ValueError):
    pass


def validate_envelope(envelope: ArtifactEnvelope) -> None:
    """
    Minimal structural validation. Keep strictness here modest; deeper validation
    belongs in stage implementations.
    """
    m = envelope.manifest
    if m.spec_version <= 0:
        raise ContractError("manifest.spec_version must be positive")

    expected = expected_stage_for_kind(m.kind)
    if expected is not None and expected != m.stage:
        raise ContractError(f"kind={m.kind} must use stage={expected.value}, got {m.stage.value}")

    if not m.artifact_id:
        raise ContractError("manifest.artifact_id is required")
    if not m.name:
        raise ContractError("manifest.name is required")
    if not m.kind:
        raise ContractError("manifest.kind is required")

    if not isinstance(envelope.payload, dict):
        raise ContractError("payload must be a dict")

    # Optional: basic stage sanity checks
    if m.stage == ArtifactStage.RESULTS_CURATED:
        if "table_path" not in envelope.payload:
            raise ContractError("CuratedResultArtifact payload must include table_path")


def require_keys(obj: Dict[str, Any], keys: list[str], *, where: str) -> None:
    missing = [k for k in keys if k not in obj]
    if missing:
        raise ContractError(f"Missing keys at {where}: {missing}")
