# src/qopexp/viz/registry.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict

from qopexp.contracts.protocols import Reporter
from qopexp.io.artifact_store import ArtifactStore

from .report_builder import SimpleReporter


@dataclass
class ReporterRegistry:
    store: ArtifactStore

    def resolve(self, report_cfg: Dict[str, Any]) -> Reporter:
        # Future: switch by report_cfg.params.type
        return SimpleReporter(store=self.store)

    def build(self, report_cfg: Dict[str, Any], curated):
        return self.resolve(report_cfg).build(report_cfg, curated)


def get_reporter_registry(store: ArtifactStore) -> ReporterRegistry:
    return ReporterRegistry(store=store)
