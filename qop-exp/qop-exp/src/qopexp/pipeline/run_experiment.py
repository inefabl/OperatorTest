# src/qopexp/pipeline/run_experiment.py
from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

from qopexp.contracts import ArtifactStage
from qopexp.io.artifact_store import ArtifactStore

from qopexp.datasets.registry import get_dataset_registry
from qopexp.workloads.registry import get_workload_registry
from qopexp.planner.registry import get_planner_registry
from qopexp.kernels.registry import get_kernel_registry
from qopexp.compiler.registry import get_compiler_registry
from qopexp.backends.registry import get_backend_registry
from qopexp.evaluator.registry import get_evaluator_registry
from qopexp.viz.registry import get_reporter_registry
from qopexp.baselines.registry import get_baseline_registry

from .config_loader import LoadedConfig

logger = logging.getLogger(__name__)


@dataclass
class PipelineOutputs:
    dataset: Any
    workload: Any
    plan: Any
    circuits: Any
    compiled: Any
    job: Any
    raw: Any
    curated: Any
    report: Any


def _open_store(artifacts_root: str | Path) -> ArtifactStore:
    """
    Try a few constructor patterns to avoid coupling to a single ArtifactStore API.
    Adjust if your ArtifactStore uses a different factory.
    """
    root = Path(artifacts_root).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)

    # Pattern A: ArtifactStore.from_root(root)
    if hasattr(ArtifactStore, "from_root"):
        return ArtifactStore.from_root(root)  # type: ignore

    # Pattern B: ArtifactStore.open(root)
    if hasattr(ArtifactStore, "open"):
        return ArtifactStore.open(root)  # type: ignore

    # Pattern C: ArtifactStore(root) or ArtifactStore(artifacts_root=...)
    try:
        return ArtifactStore(root)  # type: ignore
    except Exception:
        return ArtifactStore(artifacts_root=root)  # type: ignore


def run_experiment(
    *,
    artifacts_root: str | Path,
    dataset_cfg: LoadedConfig,
    workload_cfg: LoadedConfig,
    experiment_cfg: LoadedConfig,
    kernel_cfg: LoadedConfig,
    backend_cfg: LoadedConfig,
    report_cfg: Optional[LoadedConfig] = None,
    dry_run: bool = False,
) -> PipelineOutputs:
    """
    End-to-end pipeline:
      Dataset -> Workload -> Plan -> Circuits -> Compile -> Submit/Ingest -> Curate -> Report

    Notes:
      - If dry_run=True: stops after compilation (no backend submit/ingest).
      - Wukong72 backend is a placeholder; for real runs use sim or replay until ingest is implemented.
    """
    store = _open_store(artifacts_root)

    # 1) Dataset
    logger.info("Building dataset artifact from %s", dataset_cfg.path)
    dataset_registry = get_dataset_registry(store)
    dataset_env = dataset_registry.build(dataset_cfg.data)

    # 2) Workload instances
    logger.info("Instantiating workload from %s", workload_cfg.path)
    workload_registry = get_workload_registry(store)
    workload_env = workload_registry.instantiate(workload_cfg.data, dataset_env)

    # 3) Plan
    logger.info("Planning experiment from %s", experiment_cfg.path)
    planner_registry = get_planner_registry(store)
    plan_env = planner_registry.build(experiment_cfg.data, workload_env)

    # 4) Circuits (kernel)
    logger.info("Building circuits from %s", kernel_cfg.path)
    kernel_registry = get_kernel_registry(store)
    circuit_env = kernel_registry.build(kernel_cfg.data, plan_env)

    # 5) Compile
    logger.info("Compiling circuits using backend config %s", backend_cfg.path)
    compiler_registry = get_compiler_registry(store)
    compiled_env = compiler_registry.compile(backend_cfg.data, circuit_env)

    if dry_run:
        logger.info("Dry-run enabled: stopping after compilation.")
        return PipelineOutputs(
            dataset=dataset_env,
            workload=workload_env,
            plan=plan_env,
            circuits=circuit_env,
            compiled=compiled_env,
            job=None,
            raw=None,
            curated=None,
            report=None,
        )

    # 6) Submit + ingest
    logger.info("Submitting jobs to backend")
    backend_registry = get_backend_registry(store)
    job_env = backend_registry.submit(backend_cfg.data, compiled_env)

    logger.info("Ingesting results from backend")
    raw_env = backend_registry.ingest(backend_cfg.data, job_env)

       # 7) Baselines (optional but recommended for apples-to-apples table)
    logger.info("Running classical baselines (if any)")
    baseline_registry = get_baseline_registry(store)
    baseline_envs = baseline_registry.run(experiment_cfg.data, workload_env, dataset_env=dataset_env)

    # 8) Curate (merge quantum raw + baseline results)
    logger.info("Evaluating / curating results")
    evaluator_registry = get_evaluator_registry(store)
    curated_env = evaluator_registry.evaluate(experiment_cfg.data, raw_env, ground_truth={"baselines": baseline_envs})

    # 9) Report (optional)
    report_env = None
    if report_cfg is not None:
        logger.info("Building report from %s", report_cfg.path)
        reporter_registry = get_reporter_registry(store)
        report_env = reporter_registry.build(report_cfg.data, curated_env)

    return PipelineOutputs(
        dataset=dataset_env,
        workload=workload_env,
        plan=plan_env,
        circuits=circuit_env,
        compiled=compiled_env,
        job=job_env,
        raw=raw_env,
        curated=curated_env,
        report=report_env,
    )
