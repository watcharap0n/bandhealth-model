# 07 — Diagrams (Mermaid)

## Purpose
รวม diagram ด้วย Mermaid เพื่ออธิบายระบบแบบ end-to-end: system context, data flow, sequence, component, และ failure/fallback flows

**หลักฐานใน repo:** pipeline orchestration: `run_pipeline.py::main`, inference: `src/infer.py::predict_with_drivers`, export: `src/infer.py::save_predictions`

## What you will learn
- ใครเรียกใคร และ artifact/output ไหลไปไหน
- ขั้นตอน transformations ที่สำคัญสำหรับ inference/explain/action
- จุด failure และ fallback behavior ที่ต้องเตรียมใน infra/monitoring

**หลักฐานใน repo:** warnings/guardrails: `src/infer.py::_build_target_segments_for_row`, `src/playbook.py::map_drivers_to_actions`

## Definitions/Glossary
- **Context diagram**: มองระบบจากมุมผู้เรียก/ผู้ใช้ output
- **Data flow**: โฟลว์ของตาราง/ไฟล์ระหว่าง stage
- **Sequence**: ลำดับ call ใน inference
- **Component**: แผนที่ module ใน `src/`
- **Failure/Fallback**: เงื่อนไขที่ทำให้ segment/action ถูก drop หรือระบบ fallback ไปข้อความทั่วไป

## 1) System Context Diagram
```mermaid
flowchart LR
  subgraph Sources["Data Sources"]
    DS1["Parquet datasets (datasets/*/*.parquet)"]
    DS2["Unity Catalog (Databricks)"]
  end

  subgraph Pipeline["Brand Health Pipeline"]
    CLI["CLI: run_pipeline.py::main"]
    NB["Notebook: notebooks/databricks/01_brand_health_pipeline_mlflow.py"]
  end

  subgraph Storage["Artifacts/Outputs"]
    ART["artifacts/ (model + metadata + importance)"]
    OUT["outputs/ (predictions + diagnostics + examples)"]
    REP["reports/ (markdown + summary json)"]
  end

  subgraph Consumers["Consumers"]
    MA["Marketing Automation / Backend"]
    DASH["Dashboard / Analyst"]
  end

  DS1 --> CLI --> ART
  DS1 --> CLI --> OUT --> MA
  DS1 --> CLI --> REP --> DASH
  DS2 --> NB --> ART
  DS2 --> NB --> OUT --> MA
```

**หลักฐานใน repo:** input layout: `src/data_load.py::TABLE_FILES`, CLI orchestration: `run_pipeline.py::main`, notebook pack: `notebooks/databricks/README.md`, export outputs: `src/infer.py::save_predictions`

## 2) Data Flow Diagram (input → transforms → output)
```mermaid
flowchart TD
  A["datasets/*/*.parquet"] --> B["load_tables\nsrc/data_load.py::load_tables"]
  B --> C["join diagnostics\nsrc/data_load.py::build_purchase_item_join_diagnostics"]
  B --> D["feature table\nsrc/features.py::build_feature_table"]
  B --> E["segment KPIs\nsrc/segments.py::compute_segment_kpis"]
  D --> F["weak labels\nsrc/labeling.py::generate_weak_labels"]
  F --> G["train (optional)\nsrc/train.py::train_models"]
  F --> H["inference\nsrc/infer.py::predict_with_drivers"]
  E --> H
  H --> I["export predictions\nsrc/infer.py::save_predictions"]
  H --> J["examples_last4*\nrun_pipeline.py::main"]
```

**หลักฐานใน repo:** `run_pipeline.py::main`

## 3) Sequence Diagram (API call → inference)
> Repo ยังไม่พบ API server; diagram นี้อธิบายลำดับ “ภายใน” `predict_with_drivers` เพื่อใช้เป็น reference ตอนออกแบบ service

```mermaid
sequenceDiagram
  participant S as "Serving layer (proposed)"
  participant INF as "src/infer.py::predict_with_drivers"
  participant DR as "src/drivers.py::attach_drivers"
  participant PB as "src/playbook.py::attach_actions"

  S->>INF: feature_df + model + feature_columns + class_labels (+ segment_kpis_df)
  INF->>INF: _prepare_inference_frame (fill missing cols)
  INF->>INF: model.predict + predict_proba
  INF->>INF: confidence_band + i18n fields
  INF->>DR: attach_drivers(...)
  DR-->>INF: drivers[]
  INF->>INF: _build_target_segments_for_row + _validate_target_segments_row
  INF->>PB: attach_actions(...)
  PB-->>INF: suggested_actions[]
  INF-->>S: payload (per row)
```

**หลักฐานใน repo:** `src/infer.py::predict_with_drivers`, `src/infer.py::_prepare_inference_frame`, `src/drivers.py::attach_drivers`, `src/playbook.py::attach_actions`

## 4) Component Diagram (modules)
```mermaid
flowchart LR
  subgraph SRC["src/ modules"]
    DL["data_load.py\n(load/profile/diagnostics)"]
    IDU["id_utils.py\n(normalize_id)"]
    FE["features.py\n(feature engineering)"]
    SG["segments.py\n(segment KPIs)"]
    LB["labeling.py\n(weak labels)"]
    TR["train.py\n(train + artifacts)"]
    INF["infer.py\n(inference + QA)"]
    DM["driver_mapping.py\n(key->metric_family)"]
    DR["drivers.py\n(driver extraction)"]
    PB["playbook.py\n(actions + i18n)"]
    MO["memory_opt.py\n(memory + parquet writer)"]
    SP["sampling.py\n(train/eval sampling)"]
  end

  DL --> IDU
  FE --> IDU
  SG --> IDU
  INF --> DM
  INF --> DR
  INF --> PB
  TR --> MO
  INF --> MO
```

**หลักฐานใน repo:** import graph ใน `run_pipeline.py` และ `src/infer.py`

## 5) Failure / Fallback Flow
```mermaid
flowchart TD
  A["Row enters target segment builder"] --> B{"drivers present?"}
  B -- "no" --> Z1["target_segments=[]\n(no warnings)"]
  B -- "yes" --> C{"driver_sign_mismatch?"}
  C -- "yes" --> W1["warning: driver_sign_mismatch:*"]
  C -- "no" --> D{"metric_family commerce & commerce_joinable=false?"}
  D -- "yes" --> W2["warning: commerce_mode_block:* \nskip family segments"]
  D -- "no" --> E{"delta noisy/flat?"}
  E -- "yes" --> W3["warning: noisy_metric:* \nskip family"]
  E -- "no" --> F["evaluate segments candidates"]
  F --> G{"eligible segments exist?"}
  G -- "no" --> W4["warning: no_eligible_segments:*"]
  G -- "yes" --> H{"denom>0?"}
  H -- "no" --> W5["warning: zero_denom:*"]
  H -- "yes" --> Z2["emit target_segments[*]"]
```

**หลักฐานใน repo:** `src/infer.py::_build_target_segments_for_row`, `src/infer.py::_validate_target_segments_row`

## Decisions & Implications
- warnings เหล่านี้ควรถูกส่งไป metrics/logging ใน production เพื่อดู drift และคุณภาพ attribution/action (เช่น rate ของ `commerce_mode_block` หรือ `no_eligible_segments`)  
  **หลักฐานใน repo:** `src/infer.py::predict_with_drivers` (field `attribution_warnings`, attrs `attribution_qa`)

## Open Questions
- จะยก warnings/qa counter ให้เป็น structured telemetry (Prometheus, logs) อย่างไรเมื่อทำ serving — repo ยังไม่พบหลักฐานของ telemetry stack; มีเพียง JSON file output `outputs/attribution_qa.json` (`run_pipeline.py::main`)

