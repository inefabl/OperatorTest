# src/qopexp/datasets/tpch_dbgen_adapter.py
from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

from qopexp.contracts import ArtifactStage, ArtifactRef, CodeRef, ConfigRef
from qopexp.io.artifact_store import ArtifactStore
from qopexp.io.hashing import sha256_file
from qopexp.datasets.utils import expand_env_vars, ensure_dir, require, get_nested


def _run_cmd(cmd: list[str], cwd: Optional[Path] = None) -> None:
    proc = subprocess.run(cmd, cwd=str(cwd) if cwd else None, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(
            "Command failed:\n"
            f"  cmd: {' '.join(cmd)}\n"
            f"  cwd: {cwd}\n"
            f"  stdout:\n{proc.stdout}\n"
            f"  stderr:\n{proc.stderr}\n"
        )


def _read_lineitem_tbl(path: Path) -> pd.DataFrame:
    """
    TPC-H .tbl is pipe-delimited with a trailing '|'.
    lineitem schema has 16 columns:
      L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE,
      L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE, L_COMMITDATE,
      L_RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE, L_COMMENT
    """
    cols = [
        "l_orderkey", "l_partkey", "l_suppkey", "l_linenumber",
        "l_quantity", "l_extendedprice", "l_discount", "l_tax",
        "l_returnflag", "l_linestatus", "l_shipdate", "l_commitdate",
        "l_receiptdate", "l_shipinstruct", "l_shipmode", "l_comment",
    ]
    df = pd.read_csv(
        path,
        sep="|",
        header=None,
        names=cols + ["_trailing"],
        engine="python",
        dtype={
            "l_orderkey": "int64",
            "l_partkey": "int64",
            "l_suppkey": "int64",
            "l_linenumber": "int64",
            "l_quantity": "float64",
            "l_extendedprice": "float64",
            "l_discount": "float64",
            "l_tax": "float64",
            "l_returnflag": "string",
            "l_linestatus": "string",
            "l_shipinstruct": "string",
            "l_shipmode": "string",
            "l_comment": "string",
        },
    )
    df = df.drop(columns=["_trailing"])
    # Parse dates
    for c in ["l_shipdate", "l_commitdate", "l_receiptdate"]:
        df[c] = pd.to_datetime(df[c], format="%Y-%m-%d", errors="raise")
    return df


@dataclass
class TPCHDbgenDatasetAdapter:
    store: ArtifactStore

    def build(self, dataset_cfg: Dict[str, Any]):
        cfg = expand_env_vars(dataset_cfg)
        require(cfg, ["name", "params"], where="dataset_cfg")

        name = str(cfg["name"])
        params = cfg["params"]

        require(params, ["scale_factor", "generator", "storage"], where="dataset_cfg.params")
        gen = params["generator"]
        st = params["storage"]

        require(gen, ["type", "dbgen_path", "seed"], where="dataset_cfg.params.generator")
        require(st, ["root_dir", "output_dir"], where="dataset_cfg.params.storage")

        if gen["type"] != "tpch-dbgen":
            raise ValueError(f"TPCHDbgenDatasetAdapter only supports generator.type=tpch-dbgen, got {gen['type']}")

        sf = float(params["scale_factor"])
        dbgen_path = Path(str(gen["dbgen_path"])).expanduser()
        seed = int(gen["seed"])
        tables = list(gen.get("tables", ["lineitem"]))

        root_dir = ensure_dir(st["root_dir"])
        out_dir = ensure_dir(st["output_dir"])
        tbl_dir = ensure_dir(root_dir / "tbl")

        # We always materialize parquet under the artifact directory for immutability.
        # But dbgen intermediate files can live in tbl_dir.
        # If .tbl not present, run dbgen once.
        lineitem_tbl = tbl_dir / "lineitem.tbl"
        if not lineitem_tbl.exists():
            if not dbgen_path.exists():
                raise FileNotFoundError(f"dbgen not found: {dbgen_path}. Set TPCH_DBGEN_PATH or params.generator.dbgen_path")

            # dbgen writes into current working directory.
            # Use -s (scale), -f (force overwrite), -C/-S are for parallel chunks (not used here).
            # Some dbgen builds support -b for dists; keep minimal.
            cmd = [str(dbgen_path), "-s", str(sf), "-f"]
            _run_cmd(cmd, cwd=tbl_dir)

            if not lineitem_tbl.exists():
                raise RuntimeError(f"dbgen completed but lineitem.tbl not found at {lineitem_tbl}")

        # Build payload skeleton first (relative paths inside artifact dir)
        payload = {
            "dataset_name": name,
            "storage_format": "parquet",
            "tables": ["lineitem"],
            "root_dir": str(root_dir),
            "derived_columns": ["qid"],
            "stats": {},
        }

        # Record config hash if caller provides a path; optional
        config_refs = []
        cfg_path = cfg.get("__config_path__")
        if isinstance(cfg_path, str) and cfg_path:
            try:
                config_refs.append(ConfigRef(path=cfg_path, sha256=sha256_file(cfg_path)))
            except Exception:
                pass

        # Create artifact directory deterministically (artifact_id includes payload, but not the parquet bytes)
        env = self.store.create(
            stage=ArtifactStage.DATASETS,
            kind="DatasetArtifact",
            name=name,
            description=str(cfg.get("description", "")),
            payload=payload,
            metrics=None,
            inputs=[],
            code_ref=CodeRef(),  # CLI can fill in git info later
            config_refs=config_refs,
            seed=seed,
            backend_name=None,
            backend_profile_sha256=None,
            extra_manifest={"generator": "tpch-dbgen", "scale_factor": sf},
        )

        # Write parquet into artifact dir
        art_dir = self.store.paths.artifacts_root / ArtifactStage.DATASETS.value / env.manifest.artifact_id
        data_dir = ensure_dir(art_dir / "data")

        parquet_path = data_dir / "lineitem.parquet"
        if not parquet_path.exists():
            df = _read_lineitem_tbl(lineitem_tbl)

            # Clustered layout on l_shipdate if enabled
            clustered = bool(get_nested(params, "preprocessing.clustered_layout.enabled", True))
            order_col = get_nested(params, "preprocessing.clustered_layout.order_by.0.column", "l_shipdate")
            direction = get_nested(params, "preprocessing.clustered_layout.order_by.0.direction", "asc")
            ascending = (str(direction).lower() != "desc")

            if clustered:
                if order_col not in df.columns:
                    raise ValueError(f"order_by column not found in lineitem: {order_col}")
                df = df.sort_values(by=[order_col], ascending=ascending).reset_index(drop=True)

            # Add ordinal id
            qid_col = get_nested(params, "preprocessing.clustered_layout.add_ordinal_id.column", "qid")
            df[qid_col] = range(len(df))

            df.to_parquet(parquet_path, index=False)

        # Compute simple stats (percentiles/histogram) if requested
        stats_cfg = get_nested(params, "preprocessing.stats", {"enabled": True})
        if bool(stats_cfg.get("enabled", True)):
            df_small = pd.read_parquet(parquet_path, columns=["l_shipdate"])
            # percentiles used to derive selectivity cutoffs
            percentiles = [0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1]
            pvals = df_small["l_shipdate"].quantile(percentiles).astype("datetime64[ns]").to_dict()
            payload["stats"] = {
                "l_shipdate_percentiles": {str(k): str(v) for k, v in pvals.items()},
                "row_count": int(len(df_small)),
            }

        # Update payload.json in-place (artifact immutability by convention; payload update is acceptable during build)
        # We keep it conservative: only add file references and stats.
        payload["tables"] = ["lineitem"]
        payload["table_paths"] = {"lineitem": "data/lineitem.parquet"}
        self._rewrite_payload(env.manifest.artifact_id, payload)

        return self.store.load(ArtifactStage.DATASETS, env.manifest.artifact_id)

    def _rewrite_payload(self, artifact_id: str, payload: Dict[str, Any]) -> None:
        art_dir = self.store.paths.artifacts_root / ArtifactStage.DATASETS.value / artifact_id
        from qopexp.io.serializers import write_json
        write_json(art_dir / "payload.json", payload)
