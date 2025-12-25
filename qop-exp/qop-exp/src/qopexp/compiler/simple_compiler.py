# src/qopexp/compiler/simple_compiler.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from qopexp.contracts import ArtifactStage, ArtifactRef, CodeRef, ConfigRef
from qopexp.io.artifact_store import ArtifactStore
from qopexp.compiler.qasm_analyzer import analyze_qasm2
from qopexp.compiler.utils import expand_env_vars, require, get_nested


@dataclass
class SimpleCompiler:
    """
    A minimal compiler that:
    - reads backend compilation policy (record only)
    - analyzes QASM2 for resources
    - returns CompiledCircuitArtifact with pass-through QASM

    This keeps the compiler interface stable while you later integrate
    real mapping/optimization for Wukong72.
    """
    store: ArtifactStore

    def compile(self, backend_cfg: Dict[str, Any], circuit):
        bcfg = expand_env_vars(backend_cfg)
        require(bcfg, ["name", "params"], where="backend_cfg")

        backend_name = str(bcfg["name"])
        bparams = bcfg["params"]
        require(bparams, ["backend_type", "device", "compilation"], where="backend_cfg.params")

        # Record compilation policy (do not enforce yet)
        comp_policy = bparams.get("compilation", {}) or {}

        # Optional: backend profile hash if config path injected
        backend_profile_sha256 = None
        bcfg_path = bcfg.get("__config_path__")
        if isinstance(bcfg_path, str) and bcfg_path:
            from qopexp.io.hashing import sha256_file
            try:
                backend_profile_sha256 = sha256_file(bcfg_path)
            except Exception:
                backend_profile_sha256 = None

        # Circuit artifact payload
        cp = circuit.payload
        fmt = str(cp.get("circuit_format", "qasm2")).lower()
        if fmt != "qasm2":
            raise ValueError(f"SimpleCompiler only supports circuit_format=qasm2 for now, got {fmt}")

        circuits = list(cp.get("circuits") or [])
        compiled_circuits: List[Dict[str, Any]] = []

        # Aggregate metrics
        agg = {
            "backend_name": backend_name,
            "compiled_count": len(circuits),
            "compile_qubits_max": 0,
            "compile_2q_gates_sum": 0,
            "compile_depth_est_max": 0,
        }

        for c in circuits:
            cid = str(c.get("circuit_id"))
            qasm = str(c.get("qasm", ""))

            qm = analyze_qasm2(qasm)
            md = qm.to_dict()

            agg["compile_qubits_max"] = max(agg["compile_qubits_max"], md["compile_qubits"])
            agg["compile_2q_gates_sum"] += int(md["compile_2q_gates"])
            agg["compile_depth_est_max"] = max(agg["compile_depth_est_max"], md["compile_depth_est"])

            compiled_circuits.append(
                {
                    "circuit_id": cid,
                    "qasm": qasm,  # pass-through
                    "mapping": None,  # placeholder for future mapping info
                    "metrics": md,
                    "tags": c.get("tags", {}) or {},
                }
            )

        payload: Dict[str, Any] = {
            "backend_name": backend_name,
            "compiler": {
                "name": "SimpleCompiler",
                "policy": comp_policy,
            },
            "compiled_circuits": compiled_circuits,
            "compiled_metrics": agg,
        }

        # Record config refs (optional)
        config_refs: List[ConfigRef] = []

        # backend config ref
        if isinstance(bcfg_path, str) and bcfg_path:
            from qopexp.io.hashing import sha256_file
            try:
                config_refs.append(ConfigRef(path=bcfg_path, sha256=sha256_file(bcfg_path)))
            except Exception:
                pass

        # circuit config ref if injected upstream
        ccfg_path = getattr(circuit.manifest, "extra", {}).get("kernel_cfg_path")  # optional future
        if isinstance(ccfg_path, str) and ccfg_path:
            from qopexp.io.hashing import sha256_file
            try:
                config_refs.append(ConfigRef(path=ccfg_path, sha256=sha256_file(ccfg_path)))
            except Exception:
                pass

        inputs = [
            ArtifactRef(stage=ArtifactStage.CIRCUITS, artifact_id=circuit.manifest.artifact_id),
            ArtifactRef(stage=ArtifactStage.PLANS, artifact_id=circuit.manifest.inputs[0].artifact_id) if circuit.manifest.inputs else ArtifactRef(stage=ArtifactStage.CIRCUITS, artifact_id=circuit.manifest.artifact_id),
        ]

        env = self.store.create(
            stage=ArtifactStage.COMPILED,
            kind="CompiledCircuitArtifact",
            name=f"compiled_{circuit.manifest.name}_on_{backend_name}",
            description=f"Compiled circuits for backend={backend_name}",
            payload=payload,
            metrics=agg,
            inputs=[ArtifactRef(stage=ArtifactStage.CIRCUITS, artifact_id=circuit.manifest.artifact_id)],
            code_ref=CodeRef(),
            config_refs=config_refs,
            seed=circuit.manifest.seed,
            backend_name=backend_name,
            backend_profile_sha256=backend_profile_sha256,
            extra_manifest={"compiler": "SimpleCompiler"},
        )
        return env
