# src/qopexp/contracts/__init__.py
"""
Contracts for qop-exp.

This package defines:
- Artifact stages, manifests, and canonical payload schemas
- Adapter protocols (dataset/workload/planner/kernel/compiler/backend/evaluator/viz)
- Minimal validation utilities

All other modules MUST depend on these contracts, and MUST NOT introduce
cross-module coupling outside these contracts.
"""

from .enums import ArtifactStage, BackendType
from .manifest import ArtifactManifest, ArtifactRef, CodeRef, ConfigRef
from .artifacts import (
    DatasetArtifact,
    WorkloadInstanceArtifact,
    PlanArtifact,
    CircuitArtifact,
    CompiledCircuitArtifact,
    JobArtifact,
    RawResultArtifact,
    CuratedResultArtifact,
    ArtifactEnvelope,
)
from .protocols import (
    DatasetAdapter,
    WorkloadAdapter,
    Planner,
    KernelBuilder,
    Compiler,
    BackendAdapter,
    Evaluator,
    Reporter,
)

__all__ = [
    "ArtifactStage",
    "BackendType",
    "ArtifactManifest",
    "ArtifactRef",
    "CodeRef",
    "ConfigRef",
    "DatasetArtifact",
    "WorkloadInstanceArtifact",
    "PlanArtifact",
    "CircuitArtifact",
    "CompiledCircuitArtifact",
    "JobArtifact",
    "RawResultArtifact",
    "CuratedResultArtifact",
    "ArtifactEnvelope",
    "DatasetAdapter",
    "WorkloadAdapter",
    "Planner",
    "KernelBuilder",
    "Compiler",
    "BackendAdapter",
    "Evaluator",
    "Reporter",
]
