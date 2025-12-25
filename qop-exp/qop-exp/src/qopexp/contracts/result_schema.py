# src/qopexp/contracts/result_schema.py
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Dict, Any


RAW_SCHEMA_VERSION = 1
CURATED_SCHEMA_VERSION = 1

# Raw results payload schema:
# - payload.results: list of per-circuit results with counts
# - counts may be a 1-bit dict {"0": x, "1": y} or full bitstring counts
# - for full bitstring counts, success is defined by the measured flag bit (default c[0])
RAW_RESULT_FIELDS_V1: List[str] = [
    "job_id",
    "circuit_id",
    "shots",
    "counts",
    "metadata",
    "tags",
]

# Recommended canonical columns for results_curated payload table.
# Keep this stable; only bump CURATED_SCHEMA_VERSION when breaking changes occur.
CURATED_COLUMNS_V1: List[str] = [
    # Identity / provenance
    "experiment_name",
    "dataset_name",
    "workload_name",
    "kernel_name",
    "backend_name",
    "variant",                    # "classical" | "quantum"

    # Scale / regime
    "N",                          # total candidate space size
    "selectivity",                # M/N (if applicable)
    "dim",                        # vector dimension (if applicable)
    "topk",                       # k (if applicable)

    # Quantum execution knobs
    "shots",
    "grover_iterations",
    "ae_k",                       # for AE schedules (optional)

    # Correctness / quality
    "success_rate",               # e.g., find-a-solution success probability
    "abs_error",                  # estimation absolute error
    "rel_error",                  # estimation relative error
    "confidence_level",           # e.g., 0.95 (optional)
    "ci_low",                     # optional
    "ci_high",                    # optional

    # Cost / resources
    "compile_depth",
    "compile_2q_gates",
    "compile_qubits",
    "walltime_sec_total",
    "walltime_sec_device",
    "walltime_sec_orchestration",

    # Operational signals
    "fallback_used",              # bool
    "failure_reason",             # str or empty
    "seed",
    "repeat_id",
]


@dataclass(frozen=True)
class CuratedTableSpec:
    schema_version: int
    columns: List[str]

    def to_dict(self) -> Dict[str, Any]:
        return {"schema_version": self.schema_version, "columns": list(self.columns)}


CURATED_TABLE_SPEC_V1 = CuratedTableSpec(
    schema_version=CURATED_SCHEMA_VERSION,
    columns=CURATED_COLUMNS_V1,
)
