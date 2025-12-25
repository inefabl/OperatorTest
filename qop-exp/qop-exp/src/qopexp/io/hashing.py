# src/qopexp/io/hashing.py
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Dict, Optional


def sha256_bytes(data: bytes) -> str:
    h = hashlib.sha256()
    h.update(data)
    return h.hexdigest()


def sha256_file(path: str | Path) -> str:
    p = Path(path)
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def canonical_json_bytes(obj: Any) -> bytes:
    """
    Canonical JSON serialization for stable hashing.

    - sort_keys=True ensures deterministic order
    - separators minimize whitespace
    - ensure_ascii=False for UTF-8 stable bytes
    """
    s = json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return s.encode("utf-8")


def compute_artifact_id(
    *,
    stage: str,
    kind: str,
    name: str,
    inputs: list[dict[str, str]],
    config_refs: list[dict[str, str]],
    seed: Optional[int],
    backend_name: Optional[str],
    backend_profile_sha256: Optional[str],
    payload: Dict[str, Any],
    metrics: Optional[Dict[str, Any]],
) -> str:
    """
    Content-addressable artifact id.

    Important design choice:
    - Exclude created_at_utc and exclude artifact_id (obviously).
    - Include provenance anchors (inputs + config hashes + seed + backend profile hash).
    - Include payload + metrics (canonicalized).
    """
    obj = {
        "stage": stage,
        "kind": kind,
        "name": name,
        "inputs": inputs,
        "config_refs": config_refs,
        "seed": seed,
        "backend_name": backend_name,
        "backend_profile_sha256": backend_profile_sha256,
        "payload": payload,
        "metrics": metrics or {},
    }
    return sha256_bytes(canonical_json_bytes(obj))
