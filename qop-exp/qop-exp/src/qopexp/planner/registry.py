# src/qopexp/planner/registry.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict

from qopexp.contracts.protocols import Planner
from qopexp.io.artifact_store import ArtifactStore

from .simple_planner import SimpleHybridPlanner


@dataclass
class PlannerRegistry:
    """
    Chooses a Planner implementation for an experiment config.
    For now we provide a single conservative planner that emits:
      - classical baseline plan
      - hybrid plan with qkernel hook + verification + fallback
    """
    store: ArtifactStore

    def resolve(self, experiment_cfg: Dict[str, Any]) -> Planner:
        # Future extension point: switch planner by experiment_cfg.params.planner.type
        return SimpleHybridPlanner(store=self.store)

    def build(self, experiment_cfg: Dict[str, Any], workload):
        planner = self.resolve(experiment_cfg)
        return planner.build(experiment_cfg, workload)


def get_planner_registry(store: ArtifactStore) -> PlannerRegistry:
    return PlannerRegistry(store=store)
