# src/qopexp/evaluator/registry.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from qopexp.contracts.protocols import Evaluator
from qopexp.io.artifact_store import ArtifactStore

from .simple_evaluator import SimpleEvaluator


@dataclass
class EvaluatorRegistry:
    store: ArtifactStore

    def resolve(self, experiment_cfg: Dict[str, Any]) -> Evaluator:
        # Future: select by experiment_cfg.policies.evaluator.type
        return SimpleEvaluator(store=self.store)

    def evaluate(self, experiment_cfg: Dict[str, Any], raw, *, ground_truth: Optional[Dict[str, Any]] = None):
        return self.resolve(experiment_cfg).evaluate(experiment_cfg, raw, ground_truth=ground_truth)


def get_evaluator_registry(store: ArtifactStore) -> EvaluatorRegistry:
    return EvaluatorRegistry(store=store)
