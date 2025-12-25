# src/qopexp/workloads/registry.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict

from qopexp.io.artifact_store import ArtifactStore
from qopexp.contracts.protocols import WorkloadAdapter

from .tpch_filter_selectivity import TPCHFilterSelectivityWorkload
from .ann_candidate_range import ANNCandidateRangeWorkload
from .sel_estimation_ae import SelectivityEstimationWorkload


@dataclass
class WorkloadAdapterRegistry:
    store: ArtifactStore

    def resolve(self, workload_cfg: Dict[str, Any]) -> WorkloadAdapter:
        name = str(workload_cfg.get("name", "")).lower()

        if "tpch_filter_selectivity" in name or ("tpch" in name and "filter" in name):
            return TPCHFilterSelectivityWorkload(store=self.store)

        if "ann_candidate_range" in name or ("ann" in name and "candidate" in name):
            return ANNCandidateRangeWorkload(store=self.store)

        if "sel_estimation_ae" in name or ("selectivity" in name and "ae" in name):
            return SelectivityEstimationWorkload(store=self.store)

        raise ValueError(f"Unable to resolve WorkloadAdapter for workload config name={workload_cfg.get('name')}")

    def instantiate(self, workload_cfg: Dict[str, Any], dataset):
        adapter = self.resolve(workload_cfg)
        return adapter.instantiate(workload_cfg, dataset)


def get_workload_registry(store: ArtifactStore) -> WorkloadAdapterRegistry:
    return WorkloadAdapterRegistry(store=store)
