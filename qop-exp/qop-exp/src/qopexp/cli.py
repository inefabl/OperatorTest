# src/qopexp/cli.py
from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Optional

from qopexp.pipeline.config_loader import load_yaml_config, maybe_load
from qopexp.pipeline.run_experiment import run_experiment


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="qopexp", description="Quantum Operator Experiment Pipeline (decoupled)")
    p.add_argument("--artifacts-root", type=str, default="artifacts", help="Artifacts root directory")

    p.add_argument("--dataset", type=str, required=True, help="Path to dataset config YAML")
    p.add_argument("--workload", type=str, required=True, help="Path to workload config YAML")
    p.add_argument("--experiment", type=str, required=True, help="Path to experiment config YAML")
    p.add_argument("--kernel", type=str, required=True, help="Path to kernel config YAML")
    p.add_argument("--backend", type=str, required=True, help="Path to backend config YAML")
    p.add_argument("--report", type=str, default=None, help="Path to report config YAML (optional)")

    p.add_argument("--dry-run", action="store_true", help="Stop after compilation (no backend submit/ingest)")
    p.add_argument("--log-level", type=str, default="INFO", help="Logging level (DEBUG/INFO/WARN/ERROR)")
    return p


def main(argv: Optional[list[str]] = None) -> int:
    args = _build_parser().parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    dataset_cfg = load_yaml_config(args.dataset)
    workload_cfg = load_yaml_config(args.workload)
    experiment_cfg = load_yaml_config(args.experiment)
    kernel_cfg = load_yaml_config(args.kernel)
    backend_cfg = load_yaml_config(args.backend)
    report_cfg = maybe_load(args.report)

    outs = run_experiment(
        artifacts_root=args.artifacts_root,
        dataset_cfg=dataset_cfg,
        workload_cfg=workload_cfg,
        experiment_cfg=experiment_cfg,
        kernel_cfg=kernel_cfg,
        backend_cfg=backend_cfg,
        report_cfg=report_cfg,
        dry_run=bool(args.dry_run),
    )

    # Minimal user-facing summary (IDs are sufficient for traceability)
    print("=== Pipeline completed ===")
    print(f"Dataset:   {outs.dataset.manifest.artifact_id}")
    print(f"Workload:  {outs.workload.manifest.artifact_id}")
    print(f"Plan:      {outs.plan.manifest.artifact_id}")
    print(f"Circuits:  {outs.circuits.manifest.artifact_id}")
    print(f"Compiled:  {outs.compiled.manifest.artifact_id}")
    if outs.job is not None:
        print(f"Job:       {outs.job.manifest.artifact_id}")
    if outs.raw is not None:
        print(f"Raw:       {outs.raw.manifest.artifact_id}")
    if outs.curated is not None:
        print(f"Curated:   {outs.curated.manifest.artifact_id}")
    if outs.report is not None:
        print(f"Report:    {outs.report.manifest.artifact_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
