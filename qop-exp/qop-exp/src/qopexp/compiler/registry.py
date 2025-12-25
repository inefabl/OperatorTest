# src/qopexp/compiler/registry.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict

from qopexp.contracts.protocols import Compiler
from qopexp.io.artifact_store import ArtifactStore

from .simple_compiler import SimpleCompiler


@dataclass
class CompilerRegistry:
    store: ArtifactStore

    def resolve(self, backend_cfg: Dict[str, Any]) -> Compiler:
        # Future: choose based on backend_cfg.params.device.name or backend type
        return SimpleCompiler(store=self.store)

    def compile(self, backend_cfg: Dict[str, Any], circuit):
        comp = self.resolve(backend_cfg)
        return comp.compile(backend_cfg, circuit)


def get_compiler_registry(store: ArtifactStore) -> CompilerRegistry:
    return CompilerRegistry(store=store)
