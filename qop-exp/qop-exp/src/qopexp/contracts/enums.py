# src/qopexp/contracts/enums.py
from __future__ import annotations

from enum import Enum


class ArtifactStage(str, Enum):
    """
    Pipeline stages that correspond 1:1 with artifacts/* subdirectories.
    """

    DATASETS = "datasets"
    WORKLOAD_INSTANCES = "workload_instances"
    PLANS = "plans"
    CIRCUITS = "circuits"
    COMPILED = "compiled"
    JOBS = "jobs"
    RESULTS_RAW = "results_raw"
    RESULTS_CURATED = "results_curated"
    REPORTS = "reports"


class BackendType(str, Enum):
    REAL_DEVICE = "real_device"
    SIMULATOR = "simulator"
    REPLAY = "replay"
