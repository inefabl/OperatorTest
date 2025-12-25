# src/qopexp/datasets/ann_hdf5_adapter.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import numpy as np

try:
    import h5py  # type: ignore
except Exception as e:  # pragma: no cover
    h5py = None

from qopexp.contracts import ArtifactStage, CodeRef, ConfigRef
from qopexp.io.artifact_store import ArtifactStore
from qopexp.io.hashing import sha256_file
from qopexp.datasets.utils import expand_env_vars, ensure_dir, require, get_nested


def _l2_normalize(x: np.ndarray, eps: float = 1e-12) -> np.ndarray:
    n = np.linalg.norm(x, axis=1, keepdims=True)
    return x / (n + eps)


def _random_projection_1d(x: np.ndarray, seed: int = 0) -> Tuple[np.ndarray, np.ndarray]:
    rng = np.random.default_rng(seed)
    w = rng.standard_normal((x.shape[1],), dtype=np.float64)
    w = w / (np.linalg.norm(w) + 1e-12)
    proj = x @ w
    return proj.astype(np.float64), w


def _pca_first_component_1d(x: np.ndarray, *, max_samples: int = 20000, iters: int = 30, seed: int = 0) -> Tuple[np.ndarray, np.ndarray]:
    """
    Lightweight PCA-1D via power iteration on covariance using a subsample.
    Returns (projection, component_vector).
    """
    rng = np.random.default_rng(seed)
    n = x.shape[0]
    if n > max_samples:
        idx = rng.choice(n, size=max_samples, replace=False)
        xs = x[idx].astype(np.float64)
    else:
        xs = x.astype(np.float64)

    mu = xs.mean(axis=0, keepdims=True)
    xc = xs - mu

    # Power iteration on covariance matrix C = X^T X
    v = rng.standard_normal((x.shape[1],), dtype=np.float64)
    v = v / (np.linalg.norm(v) + 1e-12)
    for _ in range(iters):
        # compute (X^T X) v without materializing C
        t = xc @ v
        v_new = xc.T @ t
        v = v_new / (np.linalg.norm(v_new) + 1e-12)

    # Project full data with mean from xs
    mu_full = x.astype(np.float64).mean(axis=0, keepdims=True)
    proj = (x.astype(np.float64) - mu_full) @ v
    return proj.astype(np.float64), v


def _read_hdf5_vectors(path: Path, base_key: str, query_key: str) -> Tuple[np.ndarray, np.ndarray]:
    if h5py is None:
        raise RuntimeError("h5py is not installed. Install with: pip install h5py")

    with h5py.File(path, "r") as f:
        if base_key not in f:
            raise KeyError(f"HDF5 base_key '{base_key}' not found in {path}. Available: {list(f.keys())}")
        if query_key not in f:
            raise KeyError(f"HDF5 query_key '{query_key}' not found in {path}. Available: {list(f.keys())}")
        base = np.asarray(f[base_key], dtype=np.float32)
        query = np.asarray(f[query_key], dtype=np.float32)
    return base, query


@dataclass
class ANNHdf5DatasetAdapter:
    store: ArtifactStore

    def build(self, dataset_cfg: Dict[str, Any]):
        cfg = expand_env_vars(dataset_cfg)
        require(cfg, ["name", "params"], where="dataset_cfg")

        name = str(cfg["name"])
        params = cfg["params"]

        require(params, ["source", "vectors"], where="dataset_cfg.params")
        source = params["source"]
        vectors = params["vectors"]

        require(source, ["local_path"], where="dataset_cfg.params.source")
        require(vectors, ["base_key", "query_key", "dim", "metric"], where="dataset_cfg.params.vectors")

        h5_path = Path(str(source["local_path"])).expanduser()
        if not h5_path.exists():
            raise FileNotFoundError(f"HDF5 dataset not found: {h5_path}")

        base_key = str(vectors["base_key"])
        query_key = str(vectors["query_key"])
        dim = int(vectors["dim"])
        metric = str(vectors["metric"])

        base, query = _read_hdf5_vectors(h5_path, base_key=base_key, query_key=query_key)
        if base.shape[1] != dim:
            raise ValueError(f"base vectors dim mismatch: expected {dim}, got {base.shape[1]}")
        if query.shape[1] != dim:
            raise ValueError(f"query vectors dim mismatch: expected {dim}, got {query.shape[1]}")

        # Preprocess normalize
        norm_cfg = get_nested(params, "preprocessing.normalize", {"enabled": False})
        if bool(norm_cfg.get("enabled", False)):
            base = _l2_normalize(base.astype(np.float64)).astype(np.float32)
            query = _l2_normalize(query.astype(np.float64)).astype(np.float32)

        # Projection to 1D
        proj_cfg = get_nested(params, "preprocessing.projection", {"enabled": True, "method": "random_projection", "target_dim": 1})
        if not bool(proj_cfg.get("enabled", True)):
            raise ValueError("ANNHdf5DatasetAdapter expects projection.enabled=true for oracle-friendly workloads")

        if int(proj_cfg.get("target_dim", 1)) != 1:
            raise ValueError("Only target_dim=1 is supported in this minimal adapter")

        method = str(proj_cfg.get("method", "random_projection")).lower()
        seed = int(proj_cfg.get("seed", 2025))

        if method == "random_projection":
            proj0, vec = _random_projection_1d(base.astype(np.float64), seed=seed)
            proj0_q, _ = _random_projection_1d(query.astype(np.float64), seed=seed)
            proj_meta = {"method": "random_projection", "seed": seed, "w_norm": float(np.linalg.norm(vec))}
        elif method == "pca":
            proj0, comp = _pca_first_component_1d(base.astype(np.float64), seed=seed)
            proj0_q = (query.astype(np.float64) - query.astype(np.float64).mean(axis=0, keepdims=True)) @ comp
            proj_meta = {"method": "pca_1d_power_iter", "seed": seed, "comp_norm": float(np.linalg.norm(comp))}
        else:
            raise ValueError(f"Unknown projection method: {method}")

        # Clustered layout by proj_0
        order = np.argsort(proj0, kind="mergesort")
        base_sorted = base[order]
        proj0_sorted = proj0[order]
        qid = np.arange(base_sorted.shape[0], dtype=np.int64)

        # Subsets
        subsets_cfg = get_nested(params, "preprocessing.subsets", {"enabled": False})
        subset_sizes = list(subsets_cfg.get("sizes", [])) if bool(subsets_cfg.get("enabled", False)) else []

        payload: Dict[str, Any] = {
            "dataset_name": name,
            "storage_format": "npz",
            "root_dir": str(h5_path.parent),
            "vector_dim": dim,
            "metric": metric,
            "derived_columns": ["proj_0", "qid"],
            "stats": {
                "base_count": int(base_sorted.shape[0]),
                "query_count": int(query.shape[0]),
                "projection": proj_meta,
            },
            "views": [],
        }

        # Config refs if caller provides a path (optional)
        config_refs = []
        cfg_path = cfg.get("__config_path__")
        if isinstance(cfg_path, str) and cfg_path:
            try:
                config_refs.append(ConfigRef(path=cfg_path, sha256=sha256_file(cfg_path)))
            except Exception:
                pass

        # Create artifact first (payload includes relative file names, stable)
        env = self.store.create(
            stage=ArtifactStage.DATASETS,
            kind="DatasetArtifact",
            name=name,
            description=str(cfg.get("description", "")),
            payload=payload,
            metrics=None,
            inputs=[],
            code_ref=CodeRef(),
            config_refs=config_refs,
            seed=seed,
            backend_name=None,
            backend_profile_sha256=None,
            extra_manifest={"source_hdf5": str(h5_path.name)},
        )

        art_dir = self.store.paths.artifacts_root / ArtifactStage.DATASETS.value / env.manifest.artifact_id
        data_dir = ensure_dir(art_dir / "data")

        # Write full materialization (sorted base + proj_0 + qid + queries)
        base_npz = data_dir / "base_sorted.npz"
        query_npz = data_dir / "queries.npz"
        if not base_npz.exists():
            np.savez_compressed(
                base_npz,
                base=base_sorted,
                proj_0=proj0_sorted.astype(np.float32),
                qid=qid,
            )
        if not query_npz.exists():
            np.savez_compressed(
                query_npz,
                query=query,
                proj_0=proj0_q.astype(np.float32),
            )

        # Write subsets (prefix of sorted arrays)
        views = []
        for n in subset_sizes:
            n = int(n)
            if n <= 0 or n > base_sorted.shape[0]:
                continue
            fn = f"subset_{n}.npz"
            p = data_dir / fn
            if not p.exists():
                np.savez_compressed(
                    p,
                    base=base_sorted[:n],
                    proj_0=proj0_sorted[:n].astype(np.float32),
                    qid=np.arange(n, dtype=np.int64),
                )
            views.append({"name": f"subset_{n}", "N": n, "path": f"data/{fn}"})

        # Update payload with file references and views
        payload["base_path"] = "data/base_sorted.npz"
        payload["query_path"] = "data/queries.npz"
        payload["views"] = views

        self._rewrite_payload(env.manifest.artifact_id, payload)

        return self.store.load(ArtifactStage.DATASETS, env.manifest.artifact_id)

    def _rewrite_payload(self, artifact_id: str, payload: Dict[str, Any]) -> None:
        art_dir = self.store.paths.artifacts_root / ArtifactStage.DATASETS.value / artifact_id
        from qopexp.io.serializers import write_json
        write_json(art_dir / "payload.json", payload)
