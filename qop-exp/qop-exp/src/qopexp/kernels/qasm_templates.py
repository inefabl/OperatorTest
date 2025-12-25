# src/qopexp/kernels/qasm_templates.py
from __future__ import annotations

from typing import List

from .utils import qasm2_header


def placeholder_grover_qasm2(index_qubits: int, ancilla_qubits: int, iterations: int) -> str:
    """
    QASM2 placeholder for Grover-like structure.
    - Prepares uniform superposition on index register.
    - Applies a synthetic 'oracle+diffusion' block 'iterations' times using simple gate patterns.
    This is not a correct Grover implementation; it is a structurally representative, compilable circuit.
    """
    n = index_qubits + ancilla_qubits
    if n <= 0:
        raise ValueError("Total qubits must be positive")
    q = "q"

    lines: List[str] = [qasm2_header(), f"qreg {q}[{n}];", "creg c[1];"]

    # Prepare superposition on index register
    for i in range(index_qubits):
        lines.append(f"h {q}[{i}];")

    # Synthetic oracle+diffusion
    # Oracle placeholder: a chain of controlled-Z like patterns (implemented via cx+rz+cx)
    # Diffusion placeholder: h...x...h on index register with an mcx-like approximation via ancilla usage.
    for it in range(max(0, iterations)):
        # "Oracle" placeholder
        if index_qubits >= 2:
            for i in range(index_qubits - 1):
                a = i
                b = i + 1
                lines.append(f"cx {q}[{a}],{q}[{b}];")
                lines.append(f"rz(0.3141592653) {q}[{b}];")
                lines.append(f"cx {q}[{a}],{q}[{b}];")
        else:
            lines.append(f"rz(0.3141592653) {q}[0];")

        # "Diffusion" placeholder (approx)
        for i in range(index_qubits):
            lines.append(f"h {q}[{i}];")
            lines.append(f"x {q}[{i}];")

        # Use one ancilla if available to create a multi-qubit-dependent phase kick (placeholder)
        if ancilla_qubits > 0 and index_qubits > 0:
            anc = index_qubits  # first ancilla
            ctrl = 0
            lines.append(f"cx {q}[{ctrl}],{q}[{anc}];")
            lines.append(f"rz(0.6283185307) {q}[{anc}];")
            lines.append(f"cx {q}[{ctrl}],{q}[{anc}];")

        for i in range(index_qubits):
            lines.append(f"x {q}[{i}];")
            lines.append(f"h {q}[{i}];")

    # One measurement just to make it "complete"
    lines.append(f"measure {q}[0] -> c[0];")
    return "\n".join(lines) + "\n"


def placeholder_ae_qasm2(index_qubits: int, ancilla_qubits: int, k: int) -> str:
    """
    QASM2 placeholder for AE schedule element Q^k.
    Represent Q applications by repeating a synthetic block k times.
    """
    n = index_qubits + ancilla_qubits
    if n <= 0:
        raise ValueError("Total qubits must be positive")
    q = "q"

    lines: List[str] = [qasm2_header(), f"qreg {q}[{n}];", "creg c[1];"]

    # Prepare superposition
    for i in range(index_qubits):
        lines.append(f"h {q}[{i}];")

    # Synthetic "Q" repeated k times (k=0 still yields a valid circuit)
    reps = max(0, int(k))
    for _ in range(reps):
        # Use a small entangling pattern to represent Q
        if index_qubits >= 2:
            lines.append(f"cx {q}[0],{q}[1];")
            lines.append(f"rz(0.3141592653) {q}[1];")
            lines.append(f"cx {q}[0],{q}[1];")
        else:
            lines.append(f"rz(0.3141592653) {q}[0];")

        if ancilla_qubits > 0 and index_qubits > 0:
            anc = index_qubits
            lines.append(f"cx {q}[0],{q}[{anc}];")
            lines.append(f"rz(0.1570796327) {q}[{anc}];")
            lines.append(f"cx {q}[0],{q}[{anc}];")

    lines.append(f"measure {q}[0] -> c[0];")
    return "\n".join(lines) + "\n"
