# src/qopexp/viz/report_builder.py
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from qopexp.contracts import ArtifactStage, ArtifactRef, CodeRef, ConfigRef
from qopexp.io.artifact_store import ArtifactStore
from qopexp.viz.utils import expand_env_vars, require, ensure_dir
from qopexp.viz.table_reader import read_curated_table
from qopexp.viz.summary import build_summary
from qopexp.viz.plots import (
    plot_error_vs_shots,
    plot_success_vs_shots,
    plot_walltime_by_variant,
    plot_compile_cost_vs_selectivity,
)


@dataclass
class SimpleReporter:
    """
    Builds a report from CuratedResultArtifact.
    Writes:
      - summary.json
      - figures/*.png
    Produces:
      - ReportArtifact (stage=REPORTS) with payload listing outputs.
    """
    store: ArtifactStore

    def build(self, report_cfg: Dict[str, Any], curated):
        cfg = expand_env_vars(report_cfg)
        require(cfg, ["name", "params"], where="report_cfg")

        rname = str(cfg["name"])
        params = cfg["params"]

        curated_table_rel = str(curated.payload.get("table_path", "table.csv"))
        curated_dir = self.store.paths.artifacts_root / ArtifactStage.RESULTS_CURATED.value / curated.manifest.artifact_id

        df = read_curated_table(curated_dir, curated_table_rel)

        summary = build_summary(df)
        figures: List[Dict[str, str]] = []

        # Create report artifact early (to get artifact_id dir)
        payload0: Dict[str, Any] = {
            "report_name": rname,
            "source_curated_artifact_id": curated.manifest.artifact_id,
            "outputs": {
                "summary_json": "summary.json",
                "figures": [],
            },
        }

        config_refs: List[ConfigRef] = []
        cfg_path = cfg.get("__config_path__")
        if isinstance(cfg_path, str) and cfg_path:
            from qopexp.io.hashing import sha256_file
            try:
                config_refs.append(ConfigRef(path=cfg_path, sha256=sha256_file(cfg_path)))
            except Exception:
                pass

        env = self.store.create(
            stage=ArtifactStage.REPORTS,
            kind="ReportArtifact",
            name=rname,
            description=str(cfg.get("description", "")),
            payload=payload0,
            metrics={"row_count": summary.row_count},
            inputs=[ArtifactRef(stage=ArtifactStage.RESULTS_CURATED, artifact_id=curated.manifest.artifact_id)],
            code_ref=CodeRef(),
            config_refs=config_refs,
            seed=curated.manifest.seed,
            backend_name=curated.manifest.backend_name,
            backend_profile_sha256=curated.manifest.backend_profile_sha256,
            extra_manifest={"reporter": "SimpleReporter"},
        )

        report_dir = self.store.paths.artifacts_root / ArtifactStage.REPORTS.value / env.manifest.artifact_id
        fig_dir = ensure_dir(report_dir / "figures")

        # Write summary.json
        summary_path = report_dir / "summary.json"
        with summary_path.open("w", encoding="utf-8") as f:
            json.dump(summary.to_dict(), f, ensure_ascii=False, indent=2, sort_keys=True)

        # Generate figures (controlled by cfg.params.figures.enabled)
        fig_cfg = params.get("figures", {}) or {}
        enabled = bool(fig_cfg.get("enabled", True))

        if enabled:
            p1 = fig_dir / "abs_error_vs_shots.png"
            plot_error_vs_shots(df, p1)
            if p1.exists():
                figures.append({"name": "abs_error_vs_shots", "path": "figures/abs_error_vs_shots.png"})

            p2 = fig_dir / "success_rate_vs_shots.png"
            plot_success_vs_shots(df, p2)
            if p2.exists():
                figures.append({"name": "success_rate_vs_shots", "path": "figures/success_rate_vs_shots.png"})

            p3 = fig_dir / "walltime_by_variant.png"
            plot_walltime_by_variant(df, p3)
            if p3.exists():
                figures.append({"name": "walltime_by_variant", "path": "figures/walltime_by_variant.png"})

            p4 = fig_dir / "compile_depth_vs_selectivity.png"
            plot_compile_cost_vs_selectivity(df, p4)
            if p4.exists():
                figures.append({"name": "compile_depth_vs_selectivity", "path": "figures/compile_depth_vs_selectivity.png"})

        # Rewrite payload with figure list
        payload = {
            "report_name": rname,
            "source_curated_artifact_id": curated.manifest.artifact_id,
            "outputs": {
                "summary_json": "summary.json",
                "figures": figures,
            },
        }
        self._rewrite_payload(env.manifest.artifact_id, payload)

        return self.store.load(ArtifactStage.REPORTS, env.manifest.artifact_id)

    def _rewrite_payload(self, artifact_id: str, payload: Dict[str, Any]) -> None:
        from qopexp.io.serializers import write_json
        art_dir = self.store.paths.artifacts_root / ArtifactStage.REPORTS.value / artifact_id
        write_json(art_dir / "payload.json", payload)
