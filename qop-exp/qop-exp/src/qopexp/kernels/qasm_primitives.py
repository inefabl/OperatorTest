# src/qopexp/kernels/qasm_primitives.py
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Tuple

from .utils import qasm2_header


@dataclass(frozen=True)
class QRegLayout:
    """
    Qubit layout in a single qreg q[...]:
      - index:   q[0 .. n-1]
      - flags:   q[n .. n+f-1]
      - work:    q[n+f .. n+f+w-1]
    """
    index: List[int]
    flags: List[int]
    work: List[int]

    @property
    def n_index(self) -> int:
        return len(self.index)


def _inv(lines: List[str], q: str, qubits: List[int]) -> None:
    for qb in qubits:
        lines.append(f"x {q}[{qb}];")


def mcx_ladder(lines: List[str], q: str, controls: List[int], target: int, work: List[int]) -> None:
    """
    Multi-controlled X using a ladder of CCX gates.
    Requires:
      - if len(controls) <= 2: no work required
      - if k controls >= 3: needs (k-2) work qubits

    This is a standard AND-ladder:
      a0,a1 -> w0
      w0,a2 -> w1
      ...
      w_{k-3}, a_{k-1} -> target (CCX)
      then uncompute in reverse.
    """
    k = len(controls)
    if k == 0:
        lines.append(f"x {q}[{target}];")
        return
    if k == 1:
        lines.append(f"cx {q}[{controls[0]}],{q}[{target}];")
        return
    if k == 2:
        lines.append(f"ccx {q}[{controls[0]}],{q}[{controls[1]}],{q}[{target}];")
        return

    need = k - 2
    if len(work) < need:
        raise ValueError(f"mcx_ladder needs {need} work qubits for {k} controls, got {len(work)}")

    # compute ladder
    lines.append(f"ccx {q}[{controls[0]}],{q}[{controls[1]}],{q}[{work[0]}];")
    for i in range(2, k - 1):
        lines.append(f"ccx {q}[{controls[i]}],{q}[{work[i-2]}],{q}[{work[i-1]}];")
    # last step into target
    lines.append(f"ccx {q}[{controls[k-1]}],{q}[{work[need-1]}],{q}[{target}];")
    # uncompute ladder
    for i in range(k - 2, 1, -1):
        lines.append(f"ccx {q}[{controls[i]}],{q}[{work[i-2]}],{q}[{work[i-1]}];")
    lines.append(f"ccx {q}[{controls[0]}],{q}[{controls[1]}],{q}[{work[0]}];")


def mark_less_than_constant_disjoint_terms(
    lines: List[str],
    q: str,
    index: List[int],
    flag: int,
    C: int,
    work: List[int],
) -> None:
    """
    Compute predicate (x < C) into 'flag' by XOR-accumulating disjoint prefix terms.

    For binary C = c_{n-1}...c_0, the set {x | x < C} can be partitioned into at most popcount(C) disjoint blocks:
      for each bit i where c_i=1:
        controls:
          higher bits j>i fixed to c_j
          bit i fixed to 0
        lower bits free
      then flip flag under those controls.

    Implementation:
      - apply X to any control qubit that should be 0 to convert to 1-controlled mcx
      - apply mcx_ladder(controls, flag)
      - undo X

    Requirements:
      - flag must be initialized to |0> prior to calling
      - C is clamped to [0, 2^n]
    """
    n = len(index)
    if C <= 0:
        return
    maxC = 1 << n
    if C >= maxC:
        # x < 2^n is always true: just flip flag once
        lines.append(f"x {q}[{flag}];")
        return

    # bits list MSB..LSB
    bits = [(C >> i) & 1 for i in range(n)]  # LSB..MSB
    # iterate MSB->LSB
    for i in range(n - 1, -1, -1):
        if bits[i] != 1:
            continue

        controls: List[int] = []
        zero_controls: List[int] = []

        # higher bits j>i fixed to bits[j]
        for j in range(n - 1, i, -1):
            qb = index[j]
            if bits[j] == 1:
                controls.append(qb)
            else:
                controls.append(qb)
                zero_controls.append(qb)

        # bit i fixed to 0
        qb_i = index[i]
        controls.append(qb_i)
        zero_controls.append(qb_i)

        # Convert 0-controls to 1-controls
        _inv(lines, q, zero_controls)
        mcx_ladder(lines, q, controls, flag, work)
        _inv(lines, q, zero_controls)


def mark_range_lo_hi(
    lines: List[str],
    q: str,
    index: List[int],
    flag_out: int,
    tmp_hi: int,
    tmp_lo: int,
    lo: int,
    hi: int,
    work: List[int],
) -> None:
    """
    Compute predicate (lo <= x < hi) into flag_out.
    We compute:
      tmp_hi = [x < hi]
      tmp_lo = [x < lo]
      flag_out = tmp_hi AND (NOT tmp_lo)

    All flags are assumed initialized to |0>.
    """
    # tmp_hi = (x < hi)
    mark_less_than_constant_disjoint_terms(lines, q, index, tmp_hi, hi, work)
    # tmp_lo = (x < lo)
    mark_less_than_constant_disjoint_terms(lines, q, index, tmp_lo, lo, work)

    # flag_out = tmp_hi AND (~tmp_lo)
    lines.append(f"x {q}[{tmp_lo}];")
    lines.append(f"ccx {q}[{tmp_hi}],{q}[{tmp_lo}],{q}[{flag_out}];")
    lines.append(f"x {q}[{tmp_lo}];")


def uncompute_range_lo_hi(
    lines: List[str],
    q: str,
    index: List[int],
    flag_out: int,
    tmp_hi: int,
    tmp_lo: int,
    lo: int,
    hi: int,
    work: List[int],
) -> None:
    """
    Reverse of mark_range_lo_hi, assuming we want to reset flags to |0>.
    """
    lines.append(f"x {q}[{tmp_lo}];")
    lines.append(f"ccx {q}[{tmp_hi}],{q}[{tmp_lo}],{q}[{flag_out}];")
    lines.append(f"x {q}[{tmp_lo}];")

    # uncompute tmp_lo and tmp_hi by re-applying the same XOR-term constructions
    mark_less_than_constant_disjoint_terms(lines, q, index, tmp_lo, lo, work)
    mark_less_than_constant_disjoint_terms(lines, q, index, tmp_hi, hi, work)


def phase_oracle_qfilter(
    lines: List[str],
    q: str,
    index: List[int],
    flag_range: int,
    tmp_hi: int,
    tmp_lo: int,
    pred_type: str,
    lo: int,
    hi: int,
    work: List[int],
) -> None:
    """
    Apply phase oracle Sf: |x> -> (-1)^{f(x)}|x> where f encodes the predicate.
    We compute f into flag_range, apply Z to flag_range, then uncompute.
    """
    if pred_type == "qid_lt":
        # Interpret as x < hi; use tmp_hi as the only flag and alias flag_range
        mark_less_than_constant_disjoint_terms(lines, q, index, flag_range, hi, work)
        lines.append(f"z {q}[{flag_range}];")
        mark_less_than_constant_disjoint_terms(lines, q, index, flag_range, hi, work)
        return

    # default: range predicate
    mark_range_lo_hi(lines, q, index, flag_range, tmp_hi, tmp_lo, lo, hi, work)
    lines.append(f"z {q}[{flag_range}];")
    uncompute_range_lo_hi(lines, q, index, flag_range, tmp_hi, tmp_lo, lo, hi, work)


def diffusion_about_uniform(lines: List[str], q: str, index: List[int], work: List[int]) -> None:
    """
    Standard diffusion over uniform superposition on index register:
      H^n X^n (I - 2|0><0|) X^n H^n
    The phase flip on |0...0> can be implemented by flipping phase on |1...1> after X^n,
    i.e. apply multi-controlled Z on the last index qubit with remaining as controls.

    MCZ implementation:
      H on target
      MCX on target
      H on target
    """
    n = len(index)
    for qb in index:
        lines.append(f"h {q}[{qb}];")
    for qb in index:
        lines.append(f"x {q}[{qb}];")

    if n == 1:
        # For 1 qubit diffusion is just Z in the middle
        lines.append(f"z {q}[{index[0]}];")
    else:
        target = index[0]
        controls = index[1:]  # control on other bits being 1
        lines.append(f"h {q}[{target}];")
        mcx_ladder(lines, q, controls, target, work)
        lines.append(f"h {q}[{target}];")

    for qb in index:
        lines.append(f"x {q}[{qb}];")
    for qb in index:
        lines.append(f"h {q}[{qb}];")


def prepare_uniform(lines: List[str], q: str, index: List[int]) -> None:
    for qb in index:
        lines.append(f"h {q}[{qb}];")


def final_compute_and_measure_flag(
    lines: List[str],
    q: str,
    index: List[int],
    flag_out: int,
    tmp_hi: int,
    tmp_lo: int,
    pred_type: str,
    lo: int,
    hi: int,
    work: List[int],
) -> None:
    """
    After Grover iterations, compute predicate into flag_out and measure it.
    This preserves the 1-bit measurement model used by your current evaluator/sim.
    """
    if pred_type == "qid_lt":
        mark_less_than_constant_disjoint_terms(lines, q, index, flag_out, hi, work)
        lines.append(f"measure {q}[{flag_out}] -> c[0];")
        return

    mark_range_lo_hi(lines, q, index, flag_out, tmp_hi, tmp_lo, lo, hi, work)
    lines.append(f"measure {q}[{flag_out}] -> c[0];")


def build_qasm2_grover_qfilter(
    *,
    n_index: int,
    iterations: int,
    pred_type: str,
    lo: int,
    hi: int,
    flags_count: int = 3,
) -> str:
    """
    Build an executable Grover-QFilter circuit that outputs a single classical bit (flag).
    Layout:
      - index qubits: n_index
      - flags: 3 (flag_range, tmp_hi, tmp_lo) to support range; for qid_lt only flag_range is used
      - work ancillas: max(0, n_index-2) for mcx ladder

    Total qubits = n_index + flags_count + max(0, n_index-2) = 2*n_index + flags_count - 2.
    For n_index=18, flags_count=3 => 37 qubits (safe under 72).
    """
    q = "q"
    work_n = max(0, n_index - 2)
    total = n_index + flags_count + work_n

    index = list(range(0, n_index))
    flags = list(range(n_index, n_index + flags_count))
    work = list(range(n_index + flags_count, total))

    flag_range = flags[0]
    tmp_hi = flags[1] if flags_count >= 2 else flags[0]
    tmp_lo = flags[2] if flags_count >= 3 else flags[0]

    lines: List[str] = [
        qasm2_header(),
        f"qreg {q}[{total}];",
        "creg c[1];",
        f"// kernel=grover_qfilter n_index={n_index} iters={iterations} pred={pred_type} lo={lo} hi={hi}",
    ]

    # A: uniform superposition
    prepare_uniform(lines, q, index)

    # Grover iterations: Sf + diffusion
    for _ in range(max(0, int(iterations))):
        phase_oracle_qfilter(lines, q, index, flag_range, tmp_hi, tmp_lo, pred_type, lo, hi, work)
        diffusion_about_uniform(lines, q, index, work)

    # Final compute predicate into flag_range and measure it (1-bit output)
    final_compute_and_measure_flag(lines, q, index, flag_range, tmp_hi, tmp_lo, pred_type, lo, hi, work)

    return "\n".join(lines) + "\n"


def build_qasm2_mlae_schedule_element(
    *,
    n_index: int,
    k: int,
    pred_type: str,
    lo: int,
    hi: int,
    flags_count: int = 3,
) -> str:
    """
    MLAE-style schedule element circuit:
      - Prepare A (uniform)
      - Apply Q^k where Q = diffusion * Sf
      - Compute predicate into flag and measure it (1-bit)

    This produces the measurement statistics needed by classical MLE aggregation.
    """
    q = "q"
    work_n = max(0, n_index - 2)
    total = n_index + flags_count + work_n

    index = list(range(0, n_index))
    flags = list(range(n_index, n_index + flags_count))
    work = list(range(n_index + flags_count, total))

    flag_range = flags[0]
    tmp_hi = flags[1] if flags_count >= 2 else flags[0]
    tmp_lo = flags[2] if flags_count >= 3 else flags[0]

    lines: List[str] = [
        qasm2_header(),
        f"qreg {q}[{total}];",
        "creg c[1];",
        f"// kernel=mlae_selectivity n_index={n_index} k={k} pred={pred_type} lo={lo} hi={hi}",
    ]

    prepare_uniform(lines, q, index)

    reps = max(0, int(k))
    for _ in range(reps):
        phase_oracle_qfilter(lines, q, index, flag_range, tmp_hi, tmp_lo, pred_type, lo, hi, work)
        diffusion_about_uniform(lines, q, index, work)

    final_compute_and_measure_flag(lines, q, index, flag_range, tmp_hi, tmp_lo, pred_type, lo, hi, work)
    return "\n".join(lines) + "\n"
