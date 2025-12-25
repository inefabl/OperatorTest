# src/qopexp/datasets/registry.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Callable

from qopexp.io.artifact_store import ArtifactStore
from qopexp.contracts.protocols import DatasetAdapter

from .tpch_dbgen_adapter import TPCHDbgenDatasetAdapter
from .ann_hdf5_adapter import ANNHdf5DatasetAdapter


@dataclass
class DatasetAdapterRegistry:
    """
    Minimal registry that chooses an adapter based on dataset config.
    The rule here is explicit and conservative: use dataset 'name' or hints in params.source/params.generator.
    """
    store: ArtifactStore

    def resolve(self, dataset_cfg: Dict[str, Any]) -> DatasetAdapter:
        name = str(dataset_cfg.get("name", "")).lower()
        params = dataset_cfg.get("params", {}) or {}

        # Heuristic routing
        if "tpch" in name or (isinstance(params.get("generator", {}), dict) and params["generator"].get("type") == "tpch-dbgen"):
            return TPCHDbgenDatasetAdapter(store=self.store)

        # HDF5 ANN datasets (SIFT/DEEP/ANN-benchmarks style)
        fmt = str(params.get("format", "")).lower()
        source = params.get("source", {}) or {}
        local_path = str(source.get("local_path", "")).lower()
        if fmt == "hdf5" or local_path.endswith(".hdf5") or "sift" in name or "deep" in name:
            return ANNHdf5DatasetAdapter(store=self.store)

        raise ValueError(f"Unable to resolve DatasetAdapter for dataset config name={dataset_cfg.get('name')}")

    def build(self, dataset_cfg: Dict[str, Any]):
        adapter = self.resolve(dataset_cfg)
        return adapter.build(dataset_cfg)


_registry_singleton: DatasetAdapterRegistry | None = None


def get_dataset_registry(store: ArtifactStore) -> DatasetAdapterRegistry:
    global _registry_singleton
    # Keep it simple; allow caller to manage lifecycle
    return DatasetAdapterRegistry(store=store)
