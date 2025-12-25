# src/qopexp/compiler/qasm_analyzer.py
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Dict, Tuple


_QREG_RE = re.compile(r"^\s*qreg\s+([A-Za-z_][A-Za-z0-9_]*)\s*\[\s*(\d+)\s*\]\s*;\s*$")
# Gate lines are assumed to be "op args;" style; we keep it permissive.
_CX_RE = re.compile(r"^\s*cx\s+")
_MEASURE_RE = re.compile(r"^\s*measure\s+")
_COMMENT_RE = re.compile(r"^\s*//")
_EMPTY_RE = re.compile(r"^\s*$")


@dataclass(frozen=True)
class QasmMetrics:
    qubits: int
    cx_gates: int
    gate_lines: int
    depth_est: int

    def to_dict(self) -> Dict[str, int]:
        return {
            "compile_qubits": self.qubits,
            "compile_2q_gates": self.cx_gates,
            "compile_gate_lines": self.gate_lines,
            "compile_depth_est": self.depth_est,
        }


def analyze_qasm2(qasm: str) -> QasmMetrics:
    """
    Minimal analyzer that estimates:
      - total qubits = sum of qreg sizes
      - 2Q gates: count lines starting with 'cx'
      - gate_lines: count non-empty, non-comment, non-header lines that look like ops
      - depth_est: use gate_lines as a conservative proxy (not parallelism-aware)

    This is intentionally simple and backend-agnostic.
    """
    qubits = 0
    cx = 0
    gate_lines = 0

    for line in qasm.splitlines():
        if _EMPTY_RE.match(line) or _COMMENT_RE.match(line):
            continue

        m = _QREG_RE.match(line)
        if m:
            qubits += int(m.group(2))
            continue

        # Skip headers/includes/creg
        if line.strip().startswith(("OPENQASM", "include", "creg")):
            continue

        # Count operations
        if _CX_RE.match(line):
            cx += 1
            gate_lines += 1
            continue

        if _MEASURE_RE.match(line):
            gate_lines += 1
            continue

        # Treat other op lines as gate lines if they end with ';'
        if ";" in line:
            gate_lines += 1

    depth_est = gate_lines
    return QasmMetrics(qubits=qubits, cx_gates=cx, gate_lines=gate_lines, depth_est=depth_est)
