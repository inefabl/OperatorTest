# src/qopexp/backends/__init__.py
from .registry import BackendRegistry, get_backend_registry

__all__ = ["BackendRegistry", "get_backend_registry"]
