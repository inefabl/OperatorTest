# src/qopexp/contracts/protocols.py
from __future__ import annotations

from typing import Any, Dict, Protocol, runtime_checkable, Optional

from .artifacts import (
    DatasetArtifact,
    WorkloadInstanceArtifact,
    PlanArtifact,
    CircuitArtifact,
    CompiledCircuitArtifact,
    JobArtifact,
    RawResultArtifact,
    CuratedResultArtifact,
)


@runtime_checkable
class DatasetAdapter(Protocol):
    """
    Responsible for producing a DatasetArtifact from a dataset config.
    Must be pure w.r.t. other stages (no workload/planner/backend logic).
    """
    def build(self, dataset_cfg: Dict[str, Any]) -> DatasetArtifact: ...


@runtime_checkable
class WorkloadAdapter(Protocol):
    """
    Materializes query instances (with sweeps expanded) from a workload config and DatasetArtifact.
    """
    def instantiate(self, workload_cfg: Dict[str, Any], dataset: DatasetArtifact) -> WorkloadInstanceArtifact: ...


@runtime_checkable
class Planner(Protocol):
    """
    Builds classic/hybrid execution plans.
    Must not depend on backend SDKs; must not compile circuits.
    """
    def build(self, experiment_cfg: Dict[str, Any], workload: WorkloadInstanceArtifact) -> PlanArtifact: ...


@runtime_checkable
class KernelBuilder(Protocol):
    """
    Generates circuits from a plan (or plan fragments). Must not submit to any backend.
    """
    def build(self, kernel_cfg: Dict[str, Any], plan: PlanArtifact) -> CircuitArtifact: ...


@runtime_checkable
class Compiler(Protocol):
    """
    Compiles/maps circuits for a backend profile. Must be backend-agnostic at the interface level.
    (Backend-specific compilation details live behind the compiler implementation.)
    """
    def compile(self, backend_cfg: Dict[str, Any], circuit: CircuitArtifact) -> CompiledCircuitArtifact: ...


@runtime_checkable
class BackendAdapter(Protocol):
    """
    Executes compiled circuits and returns raw results.
    Real-device adapters must enforce runtime policies (timeouts, shot caps, retries).
    """
    def submit(self, backend_cfg: Dict[str, Any], compiled: CompiledCircuitArtifact) -> JobArtifact: ...
    def ingest(self, backend_cfg: Dict[str, Any], job: JobArtifact) -> RawResultArtifact: ...


@runtime_checkable
class Evaluator(Protocol):
    """
    Normalizes raw results into a curated result table and computes metrics.
    """
    def evaluate(
        self,
        experiment_cfg: Dict[str, Any],
        raw: RawResultArtifact,
        *,
        ground_truth: Optional[Dict[str, Any]] = None,
    ) -> CuratedResultArtifact: ...


@runtime_checkable
class Reporter(Protocol):
    """
    Generates plots/tables from curated results.
    """
    def report(self, experiment_cfg: Dict[str, Any], curated: CuratedResultArtifact) -> Dict[str, Any]: ...
