# src/qopexp/kernels/registry.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict

from qopexp.contracts.protocols import KernelBuilder
from qopexp.io.artifact_store import ArtifactStore

from .qfilter_grover import GroverQFilterKernel
from .qsel_mlae import MLAESelectivityKernel


@dataclass
class KernelRegistry:
    store: ArtifactStore

    def resolve(self, kernel_cfg: Dict[str, Any]) -> KernelBuilder:
        name = str(kernel_cfg.get("name", "")).lower()
        params = kernel_cfg.get("params", {}) or {}
        ktype = str(params.get("kernel_type", "")).lower()

        if "qfilter" in name or ktype == "grover_qfilter":
            return GroverQFilterKernel(store=self.store)

        if "mlae" in name or ktype == "selectivity_mlae":
            return MLAESelectivityKernel(store=self.store)

        # Swap/similarity can be added later
        raise ValueError(f"Unable to resolve KernelBuilder for kernel config name={kernel_cfg.get('name')}")

    def build(self, kernel_cfg: Dict[str, Any], plan):
        k = self.resolve(kernel_cfg)
        return k.build(kernel_cfg, plan)


def get_kernel_registry(store: ArtifactStore) -> KernelRegistry:
    return KernelRegistry(store=store)
