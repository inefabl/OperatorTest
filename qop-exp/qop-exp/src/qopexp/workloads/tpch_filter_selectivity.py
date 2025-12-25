# src/qopexp/workloads/tpch_filter_selectivity.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List

from qopexp.contracts import ArtifactStage, ArtifactRef, CodeRef, ConfigRef
from qopexp.io.artifact_store import ArtifactStore
from qopexp.workloads.utils import expand_env_vars, require, stable_id, parse_datetime, percentile_to_M


@dataclass
class TPCHFilterSelectivityWorkload:
    store: ArtifactStore

    def instantiate(self, workload_cfg: Dict[str, Any], dataset):
        cfg = expand_env_vars(workload_cfg)
        require(cfg, ["name", "params"], where="workload_cfg")

        wname = str(cfg["name"])
        params = cfg["params"]

        require(params, ["target_table", "predicate", "query_template", "sweeps"], where="workload_cfg.params")
        sweeps = params["sweeps"]
        require(sweeps, ["selectivity_percentiles", "repeats", "seeds"], where="workload_cfg.params.sweeps")

        # Dataset stats required: row_count and shipdate percentiles (from dataset adapter)
        dp = dataset.payload
        stats = dp.get("stats", {}) or {}
        row_count = int(stats.get("row_count", 0))
        ship_p = stats.get("l_shipdate_percentiles", {}) or {}
        if row_count <= 0 or not ship_p:
            raise ValueError(
                "TPCHFilterSelectivityWorkload requires dataset.payload.stats.row_count and "
                "dataset.payload.stats.l_shipdate_percentiles. Rebuild dataset artifact with stats enabled."
            )

        sql_template = str(params["query_template"]["sql"])
        pred = params["predicate"]
        pred_col = str(pred.get("column", "l_shipdate"))
        ordinal_col = str(pred.get("mapping", {}).get("ordinal_id_column", "qid"))

        percentiles: List[float] = [float(x) for x in sweeps["selectivity_percentiles"]]
        repeats = int(sweeps["repeats"])
        seeds: List[int] = [int(x) for x in sweeps["seeds"]]

        instances: List[Dict[str, Any]] = []

        for p in percentiles:
            # Convert percentile to cutoff date (best-effort; percentiles stored as strings)
            p_key = str(p)
            if p_key not in ship_p:
                # tolerate minor float string mismatch: search closest key
                keys = sorted((float(k), k) for k in ship_p.keys())
                closest = min(keys, key=lambda t: abs(t[0] - p))[1]
                cutoff_str = ship_p[closest]
            else:
                cutoff_str = ship_p[p_key]

            cutoff_dt = parse_datetime(str(cutoff_str))
            M = percentile_to_M(p, row_count)

            # Oracle-friendly predicate: qid < M
            predicate = {
                "type": "qid_lt",
                "column": ordinal_col,
                "M": int(M),
                "N": int(row_count),
                "selectivity": float(M) / float(row_count) if row_count > 0 else 0.0,
            }

            # Expand repeats × seeds
            for seed in seeds:
                for r in range(repeats):
                    qid = stable_id(wname, "p", p, "seed", seed, "r", r)
                    instances.append(
                        {
                            "query_id": qid,
                            "sql": sql_template,
                            "params": {"shipdate_cutoff": cutoff_dt.date().isoformat()},
                            "predicate": predicate,
                            "tags": {
                                "workload": wname,
                                "selectivity_target": p,
                                "selectivity_effective": predicate["selectivity"],
                                "N": int(row_count),
                                "M": int(M),
                                "seed": seed,
                                "repeat_id": r,
                                "predicate_column": pred_col,
                            },
                        }
                    )

        batching = params.get("batching", {"batch_size": 50, "shuffle": True})
        payload = {
            "workload_name": wname,
            "dataset_artifact_id": dataset.manifest.artifact_id,
            "instances": instances,
            "batching": batching,
        }

        # Optional config_ref if caller injects __config_path__
        config_refs: List[ConfigRef] = []
        cfg_path = cfg.get("__config_path__")
        if isinstance(cfg_path, str) and cfg_path:
            from qopexp.io.hashing import sha256_file
            try:
                config_refs.append(ConfigRef(path=cfg_path, sha256=sha256_file(cfg_path)))
            except Exception:
                pass

        env = self.store.create(
            stage=ArtifactStage.WORKLOAD_INSTANCES,
            kind="WorkloadInstanceArtifact",
            name=wname,
            description=str(cfg.get("description", "")),
            payload=payload,
            metrics={"instance_count": len(instances)},
            inputs=[ArtifactRef(stage=ArtifactStage.DATASETS, artifact_id=dataset.manifest.artifact_id)],
            code_ref=CodeRef(),
            config_refs=config_refs,
            seed=None,
            backend_name=None,
            backend_profile_sha256=None,
            extra_manifest={"source": "tpch_filter_selectivity"},
        )
        return env
