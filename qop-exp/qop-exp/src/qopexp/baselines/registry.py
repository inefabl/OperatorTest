# src/qopexp/baselines/registry.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List

from qopexp.io.artifact_store import ArtifactStore

from .tpch_scan_filter import TPCHScanFilterBaseline
from .ann_range_verify import ANNRangeVerifyBaseline

from .tpch_parquet_scan_filter import TPCHParquetScanFilterBaseline
from .ann_range_verify_real import ANNRangeVerifyBaselineReal


@dataclass
class BaselineRegistry:
    store: ArtifactStore

    def run(self, experiment_cfg: Dict[str, Any], workload, dataset_env=None):
        """
        dataset_env is optional for backward compatibility; real baselines prefer it.
        """
        baselines = list(experiment_cfg.get("baselines", []) or [])
        outputs = []

        for b in baselines:
            btype = str(b.get("type", "")).lower()
            real = bool(b.get("real_timing", True))  # default: True for paper-quality runs

            # Choose implementation by type and real_timing
            if btype in ("scan_filter", "tpch_scan_filter"):
                impl = TPCHParquetScanFilterBaseline(store=self.store) if real else TPCHScanFilterBaseline(store=self.store)
                outputs.append(impl.run(b, experiment_cfg, workload, dataset_env) if real else impl.run(b, experiment_cfg, workload))

            elif btype in ("range_verify", "ann_range_verify"):
                impl = ANNRangeVerifyBaselineReal(store=self.store) if real else ANNRangeVerifyBaseline(store=self.store)
                outputs.append(impl.run(b, experiment_cfg, workload, dataset_env) if real else impl.run(b, experiment_cfg, workload))

            else:
                # Conservative default: keep old cheap baseline
                impl = TPCHScanFilterBaseline(store=self.store)
                outputs.append(impl.run(b, experiment_cfg, workload))

        return outputs


def get_baseline_registry(store: ArtifactStore) -> BaselineRegistry:
    return BaselineRegistry(store=store)
