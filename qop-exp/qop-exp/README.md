# qop-exp: Fully-Decoupled Quantum Operator Experiment Suite

## 1. Overview

This repository provides a fully decoupled experiment framework for evaluating **quantum-kernel operator replacement** in database-style workloads under NISQ constraints (e.g., 72-qubit devices). The system is designed around **artifact-driven stage boundaries** to ensure reproducibility, extensibility, and backend-agnostic execution.

**Key design goals**

* Fully decoupled components: dataset, workload, planner, kernel, compiler, backend, evaluation, visualization
* Artifact lineage and reproducibility (manifest-based provenance)
* Support for real-device submission and offline replay

---

## 2. Repository Layout

```text
qop-exp/                       # Quantum Operator Experiments
├─ README.md
├─ pyproject.toml
├─ configs/
│  ├─ datasets/                # 数据集声明（只描述，不存数据）
│  │  ├─ tpch_sf01.yaml
│  │  ├─ sift1m.yaml
│  │  └─ deep_subset.yaml
│  ├─ workloads/               # 工作负载声明（查询模板/参数 sweep）
│  │  ├─ tpch_filter_selectivity.yaml
│  │  ├─ ann_candidate_range.yaml
│  │  └─ sel_estimation_ae.yaml
│  ├─ kernels/                 # 量子核声明（Grover/AE/Swap 等）
│  │  ├─ qfilter_grover.yaml
│  │  ├─ qsel_mlae.yaml
│  │  └─ qsim_swap.yaml
│  ├─ backends/
│  │  ├─ wukong72.yaml
│  │  ├─ noisysim.yaml
│  │  └─ statesim.yaml
│  └─ experiments/             # 实验“装配”层：引用 dataset/workload/kernel/backend
│     ├─ exp01_tpch_qfilter_vs_scan.yaml
│     ├─ exp02_ann_qcand_vs_bruteforce.yaml
│     └─ exp03_selectivity_ae_vs_mc.yaml
│
├─ artifacts/                  # 所有阶段产物（强烈建议不手改）
│  ├─ datasets/                # 数据准备产物：索引/投影/分桶、子集
│  ├─ workload_instances/      # workload 实例化后的“查询批次”
│  ├─ plans/                   # 逻辑/物理执行计划（含 fallback）
│  ├─ circuits/                # 编译前 QASM/IR + 编译参数
│  ├─ compiled/                # 编译后（映射/优化后）电路与资源统计
│  ├─ jobs/                    # 提交后端的 job 描述（批任务）
│  ├─ results_raw/             # 后端原始返回（shots、counts、metadata）
│  ├─ results_curated/         # 清洗与标准化后的结果表
│  └─ reports/                 # 图表与论文表格/JSON summary
│
├─ src/
│  ├─ qopexp/
│  │  ├─ contracts/            # 核心接口契约（Protocol/ABC）与工件 schema
│  │  ├─ io/                   # artifact 读写（json/yaml/parquet/npz）
│  │  ├─ datasets/             # dataset adapter：下载/转换/子集/索引构建
│  │  ├─ workloads/            # workload adapter：模板→实例化→batch
│  │  ├─ planner/              # plan builder：classic plan / hybrid plan
│  │  ├─ kernels/              # quantum kernels：oracle/grover/ae 等（只产电路）
│  │  ├─ compiler/             # 编译/映射/深度统计（后端无关）
│  │  ├─ backends/             # 真机/模拟器 adapter（提交/查询/回放）
│  │  ├─ evaluator/            # 指标定义、统计检验、误差模型
│  │  ├─ viz/                  # 画图与表格生成（只吃 curated results）
│  │  └─ cli.py                # 统一命令行入口（只编排阶段）
│  └─ tests/
│
└─ tools/
   ├─ pin_env.sh               # 固化依赖与环境信息
   └─ ci_validate_artifacts.py  # 校验工件 schema 与可复现性
```

---

## 3. Quickstart

### 3.1 Environment

* OS: Ubuntu/Windows supported
* Python: 3.10+ recommended
* Optional: a device SDK for real-device submission (backend-specific)

### 3.2 Install

```bash
# from repo root
pip install -e .
```

### 3.3 Minimal Run (Offline)

This example runs a full pipeline using a simulator backend.

```bash
qopexp dataset build -c configs/datasets/<DATASET>.yaml
qopexp workload instantiate -c configs/workloads/<WORKLOAD>.yaml --dataset <DATASET_ARTIFACT_ID>
qopexp plan build -c configs/experiments/<EXPERIMENT>.yaml --workload <WORKLOAD_ARTIFACT_ID>
qopexp circuit build --plan <PLAN_ARTIFACT_ID>
qopexp compile --circuit <CIRCUIT_ARTIFACT_ID> --backend configs/backends/<BACKEND>.yaml
qopexp submit --compiled <COMPILED_ARTIFACT_ID>
qopexp ingest --job <JOB_ARTIFACT_ID>
qopexp evaluate --raw <RAW_RESULT_ARTIFACT_ID>
qopexp report --curated <CURATED_RESULT_ARTIFACT_ID>
```

---

## 4. Decoupled Pipeline Stages (Artifact Contracts)

Each stage consumes and produces **versioned artifacts** stored under `artifacts/`. Artifacts are immutable; every artifact directory contains a `manifest.json` describing lineage and configuration hashes.

### 4.1 Stage: Dataset Preparation

**Input:** dataset config
**Output:** `artifacts/datasets/<artifact_id>/`

Typical responsibilities:

* Download / validate / convert formats
* Generate subsets for NISQ-friendly scaling
* Build clustered layouts, projections, or buckets for oracle-friendly predicates

### 4.2 Stage: Workload Materialization

**Input:** workload config + dataset artifact
**Output:** `artifacts/workload_instances/<artifact_id>/`

Typical responsibilities:

* Expand sweeps into query batches
* Produce canonical query instance format

### 4.3 Stage: Plan Construction

**Input:** workload instances (+ kernel spec)
**Output:** `artifacts/plans/<artifact_id>/`

Typical responsibilities:

* Produce classical plan and hybrid plan with fallback
* Bind kernel hooks at operator boundaries (operator replacement points)

### 4.4 Stage: Circuit Generation (Kernel)

**Input:** plan artifact
**Output:** `artifacts/circuits/<artifact_id>/`

Typical responsibilities:

* Generate QASM/IR for Grover/AE/Swap-style kernels
* Emit kernel-side resource estimates (logical depth, qubits)

### 4.5 Stage: Compilation & Mapping

**Input:** circuit artifact + backend profile
**Output:** `artifacts/compiled/<artifact_id>/`

Typical responsibilities:

* Hardware-aware mapping and optimizations
* Extract post-compile metrics (depth, 2Q gate count)

### 4.6 Stage: Backend Submission

**Input:** compiled artifact + job policy
**Output:** `artifacts/jobs/<artifact_id>/`

Typical responsibilities:

* Submit batch jobs to real device or simulator
* Enforce timeout, retry, and abort policies

### 4.7 Stage: Ingest (Raw Results)

**Input:** backend job
**Output:** `artifacts/results_raw/<artifact_id>/`

Typical responsibilities:

* Store raw counts, metadata, calibration snapshot identifiers (if any)

### 4.8 Stage: Evaluation (Curated Results)

**Input:** raw results (+ optional ground truth)
**Output:** `artifacts/results_curated/<artifact_id>/`

Typical responsibilities:

* Normalize results into a standard table schema
* Compute metrics: success rate, error vs shots, time breakdown, etc.

### 4.9 Stage: Visualization & Reports

**Input:** curated results
**Output:** `artifacts/reports/<artifact_id>/`

Typical responsibilities:

* Paper-ready plots and tables
* Summary JSON for automated regression checks

---

## 5. Experiments Provided

### 5.1 Exp-01: TPC-H Low-Selectivity Filter (Grover vs Scan)

**Goal:** Demonstrate operator replacement for `Scan+Filter` under low selectivity.
**Kernel:** Grover-based QFilter
**Dataset:** TPC-H (generated)
**Key outputs:** success vs shots; cost breakdown; resource table

### 5.2 Exp-02: ANN Candidate Generation (Projection Range + Grover)

**Goal:** High-dimensional vector workloads with candidate generation under sampling.
**Kernel:** Grover-based candidate search
**Dataset:** SIFT/DEEP subset
**Key outputs:** candidate reduction; end-to-end verification cost; accuracy impact

### 5.3 Exp-03: Selectivity Estimation (AE vs Monte Carlo)

**Goal:** Sampling-driven estimation efficiency.
**Kernel:** MLAE / iterative AE
**Dataset:** TPC-H or ANN predicates
**Key outputs:** error vs shots; shots-to-threshold; robustness under noise

---

## 6. Backend Profiles and Real-Device Execution

### 6.1 Backend Abstraction

Backends are accessed only through adapters. No experiment code should reference device-specific SDKs directly.

### 6.2 Wukong72 (72 Qubits) Notes

* Keep Grover iterations low (1–3) and rely on repeated shots
* Prefer NISQ-friendly AE variants (iterative/MLAE), avoid QFT-heavy circuits
* Always record post-compile depth and 2Q gate count as primary constraints

---

## 7. Configuration Reference

### 7.1 Dataset Config (`configs/datasets/*.yaml`)

Expected sections:

* source (download/url/path)
* preprocessing (subset policy, normalization)
* layout (clustering/projection/bucketing for oracle-friendly predicates)

### 7.2 Workload Config (`configs/workloads/*.yaml`)

Expected sections:

* query templates
* parameter sweeps (selectivity, thresholds, k)
* batching policy (batch size, repeats, seeds)

### 7.3 Kernel Config (`configs/kernels/*.yaml`)

Expected sections:

* kernel type (grover / ae / swap / etc.)
* circuit policy (iterations, shots schedule)
* verification policy (classical fallback, post-check)

### 7.4 Backend Config (`configs/backends/*.yaml`)

Expected sections:

* backend type (device/sim/replay)
* compilation options (mapping/optimization)
* runtime policy (timeout, retries, concurrency)

### 7.5 Experiment Assembly (`configs/experiments/*.yaml`)

Expected sections:

* references to dataset/workload/kernel/backend configs
* overrides for sweeps and policies

---

## 8. Artifacts and Provenance

### 8.1 Artifact Structure

Each artifact is stored as:

```text
artifacts/<stage>/<artifact_id>/
├─ manifest.json
├─ payload.*
└─ metrics.json
```

### 8.2 Manifest Fields (Minimum)

* artifact_id, stage, inputs
* config refs + hashes
* code ref (git commit/version)
* seeds
* backend profile summary
* timestamps

---

## 9. Evaluation Metrics

Recommended standardized metrics:

* **Success rate** (Grover candidate find rate)
* **Error vs shots** and **shots-to-threshold** (AE vs MC)
* **Cost breakdown**: compile depth, 2Q gate count, wall-clock (device + orchestration)
* **Robustness**: variance across runs, failure modes, fallback rate

---

## 10. Adding New Components

### 10.1 Add a Dataset

* Add `configs/datasets/<new>.yaml`
* Implement `src/qopexp/datasets/<new>.py` (DatasetAdapter)

### 10.2 Add a Workload

* Add `configs/workloads/<new>.yaml`
* Implement `src/qopexp/workloads/<new>.py` (WorkloadAdapter)

### 10.3 Add a Kernel

* Add `configs/kernels/<new>.yaml`
* Implement `src/qopexp/kernels/<new>.py` (KernelBuilder)

### 10.4 Add a Backend

* Add `configs/backends/<new>.yaml`
* Implement `src/qopexp/backends/<new>.py` (BackendAdapter)

---

## 11. Reproducibility Checklist

* Pin Python dependencies and record `pip freeze`
* Store config hashes in every manifest
* Store compilation options and hardware profile snapshot
* Store random seeds at workload and kernel stages
* Always support replay from `results_raw`

---

## 12. License and Citation

* License: <TBD>
* Citation: <TBD>
* Contact: <TBD>
