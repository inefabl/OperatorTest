#!/usr/bin/env python
from __future__ import annotations

import argparse
import sys
from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _load_store(artifacts_root: Path):
    repo_root = _repo_root()
    sys.path.insert(0, str(repo_root / "src"))

    from qopexp.io.artifact_store import ArtifactStore, StorePaths

    paths = StorePaths(repo_root=repo_root, artifacts_root=artifacts_root)
    return ArtifactStore(paths)


def _iter_artifacts(artifacts_root: Path):
    from qopexp.contracts.enums import ArtifactStage

    if not artifacts_root.exists():
        return []

    items = []
    for stage_dir in artifacts_root.iterdir():
        if not stage_dir.is_dir():
            continue
        try:
            stage = ArtifactStage(stage_dir.name)
        except Exception:
            continue
        for artifact_dir in stage_dir.iterdir():
            if artifact_dir.is_dir():
                items.append((stage, artifact_dir.name))
    return items


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Validate on-disk artifacts against schemas")
    p.add_argument(
        "--artifacts-root",
        type=str,
        default=None,
        help="Artifacts root directory (default: <repo>/artifacts)",
    )
    args = p.parse_args(argv)

    repo_root = _repo_root()
    artifacts_root = Path(args.artifacts_root) if args.artifacts_root else repo_root / "artifacts"
    store = _load_store(artifacts_root)

    items = _iter_artifacts(artifacts_root)
    if not items:
        print(f"No artifacts found under {artifacts_root}")
        return 0

    failures = []
    for stage, artifact_id in items:
        try:
            store.validate_on_disk(stage, artifact_id)
        except Exception as exc:
            failures.append((stage.value, artifact_id, str(exc)))

    if failures:
        print("Artifact validation failed:")
        for stage, artifact_id, msg in failures:
            print(f"- {stage}/{artifact_id}: {msg}")
        return 1

    print(f"Validated {len(items)} artifacts under {artifacts_root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
