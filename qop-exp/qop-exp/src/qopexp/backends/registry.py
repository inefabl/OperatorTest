# src/qopexp/backends/registry.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict

from qopexp.contracts.protocols import BackendAdapter
from qopexp.io.artifact_store import ArtifactStore

from .replay_backend import ReplayBackendAdapter
from .sim_backend import SimBackendAdapter
from .wukong72_backend import Wukong72BackendAdapter


@dataclass
class BackendRegistry:
    store: ArtifactStore

    def resolve(self, backend_cfg: Dict[str, Any]) -> BackendAdapter:
        name = str(backend_cfg.get("name", "")).lower()
        params = backend_cfg.get("params", {}) or {}
        btype = str(params.get("backend_type", "")).lower()

        if btype == "replay" or "replay" in name:
            return ReplayBackendAdapter(store=self.store)

        if btype == "simulator" or "sim" in name:
            return SimBackendAdapter(store=self.store)

        # Default: real device
        return Wukong72BackendAdapter(store=self.store)

    def submit(self, backend_cfg: Dict[str, Any], compiled):
        return self.resolve(backend_cfg).submit(backend_cfg, compiled)

    def ingest(self, backend_cfg: Dict[str, Any], job):
        return self.resolve(backend_cfg).ingest(backend_cfg, job)


def get_backend_registry(store: ArtifactStore) -> BackendRegistry:
    return BackendRegistry(store=store)
