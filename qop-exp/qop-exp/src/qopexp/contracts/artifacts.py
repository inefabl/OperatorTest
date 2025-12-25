# src/qopexp/contracts/artifacts.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, TypedDict, Literal

from .enums import ArtifactStage
from .manifest import ArtifactManifest


# -------------------------
# Canonical payload typings
# -------------------------

class DatasetPayload(TypedDict, total=False):
    dataset_name: str
    storage_format: str                  # parquet / hdf5 / npz / mixed
    root_dir: str
    tables: List[str]                    # for relational datasets
    vector_dim: int                      # for vector datasets
    metric: str                          # l2 / cosine / angular
    derived_columns: List[str]           # e.g., ["proj_0", "qid"]
    views: List[Dict[str, Any]]          # subsets/materialized views
    stats: Dict[str, Any]                # histograms, percentiles, etc.


class QueryInstance(TypedDict, total=False):
    query_id: str
    sql: str
    params: Dict[str, Any]
    predicate: Dict[str, Any]            # oracle-friendly predicate form (optional)
    ground_truth: Dict[str, Any]         # optional exact answer or count
    tags: Dict[str, Any]                 # e.g., {"selectivity":0.01,"N":131072}


class WorkloadInstancePayload(TypedDict, total=False):
    workload_name: str
    dataset_artifact_id: str
    instances: List[QueryInstance]
    batching: Dict[str, Any]


class PlanOperator(TypedDict, total=False):
    op_id: str
    op_type: str                         # scan/filter/agg/join/qkernel_hook/verify
    inputs: List[str]                    # upstream op_ids
    params: Dict[str, Any]
    fallback: Optional[Dict[str, Any]]   # classical fallback operator (optional)


class PlanPayload(TypedDict, total=False):
    experiment_name: str
    workload_artifact_id: str
    operators: List[PlanOperator]
    plan_metadata: Dict[str, Any]


class CircuitPayload(TypedDict, total=False):
    kernel_name: str
    circuit_format: str                  # "qasm2" | "qasm3" | "ir"
    circuits: List[Dict[str, Any]]       # each: {"circuit_id","qasm","tags","logical_metrics"}
    logical_metrics: Dict[str, Any]      # aggregate logical metrics (optional)


class CompiledCircuitPayload(TypedDict, total=False):
    backend_name: str
    compiler: Dict[str, Any]
    compiled_circuits: List[Dict[str, Any]]  # {"circuit_id","qasm","mapping","metrics"}
    compiled_metrics: Dict[str, Any]


class JobPayload(TypedDict, total=False):
    backend_name: str
    submission: Dict[str, Any]
    jobs: List[Dict[str, Any]]           # {"job_id","circuit_ids","shots","status","extra"}


class RawResultPayload(TypedDict, total=False):
    backend_name: str
    results: List[Dict[str, Any]]        # {"job_id","circuit_id","shots","counts","metadata"}
    backend_metadata: Dict[str, Any]


class CuratedResultPayload(TypedDict, total=False):
    table_format: str                    # "parquet" | "csv"
    table_path: str                      # path to normalized results table (relative or absolute)
    schema_version: int
    summary: Dict[str, Any]


# -------------------------
# Artifact envelope
# -------------------------

@dataclass
class ArtifactEnvelope:
    """
    In-memory representation of an artifact.
    On disk: manifest.json + payload.* (+ metrics.json optional).
    """
    manifest: ArtifactManifest
    payload: Dict[str, Any]              # stage-specific payload (see TypedDicts)
    metrics: Optional[Dict[str, Any]] = None


# -------------------------
# Stage-specific aliases
# -------------------------

DatasetArtifact = ArtifactEnvelope
WorkloadInstanceArtifact = ArtifactEnvelope
PlanArtifact = ArtifactEnvelope
CircuitArtifact = ArtifactEnvelope
CompiledCircuitArtifact = ArtifactEnvelope
JobArtifact = ArtifactEnvelope
RawResultArtifact = ArtifactEnvelope
CuratedResultArtifact = ArtifactEnvelope


# -------------------------
# Helpers for stage naming
# -------------------------

def expected_stage_for_kind(kind: str) -> Optional[ArtifactStage]:
    """
    Optional helper for validation / routing.
    """
    mapping = {
        "DatasetArtifact": ArtifactStage.DATASETS,
        "WorkloadInstanceArtifact": ArtifactStage.WORKLOAD_INSTANCES,
        "PlanArtifact": ArtifactStage.PLANS,
        "CircuitArtifact": ArtifactStage.CIRCUITS,
        "CompiledCircuitArtifact": ArtifactStage.COMPILED,
        "JobArtifact": ArtifactStage.JOBS,
        "RawResultArtifact": ArtifactStage.RESULTS_RAW,
        "CuratedResultArtifact": ArtifactStage.RESULTS_CURATED,
    }
    return mapping.get(kind)
