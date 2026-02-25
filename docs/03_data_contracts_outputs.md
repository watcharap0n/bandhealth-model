# 03 — Data Contracts (Outputs)

## Purpose
กำหนดสัญญาข้อมูล (data contracts) สำหรับ output ของ pipeline ทั้งในรูป “ไฟล์ intermediate” (feature/segment/label) และ “ผล inference” (predictions payload) รวมถึง artifacts ของโมเดล

**หลักฐานใน repo:** `run_pipeline.py::main`, `src/infer.py::save_predictions`, `src/train.py::train_models`

## What you will learn
- ไฟล์ output ที่ pipeline เขียนออก (`outputs/`, `reports/`, `artifacts/`) และใครคือ consumer
- schema ของ `predictions_with_drivers.*` และ payload JSON ที่ส่งต่อ backend/automation
- contract ของ `drivers`, `target_segments`, `suggested_actions`, `*_i18n`, และ `attribution_warnings`

**หลักฐานใน repo:** `src/infer.py::predict_with_drivers` (สร้าง payload), `src/drivers.py::attach_drivers`, `src/playbook.py::attach_actions`

## Definitions/Glossary
- **artifacts**: ไฟล์ที่ต้องมีเพื่อ reuse model (`brand_health_model.joblib`, `model_metadata.json`, `feature_importance.json`) — `src/train.py::train_models`, `src/infer.py::load_model_artifacts`
- **predictions_df**: DataFrame ที่รวม feature + prediction + explain/action ก่อน export — `src/infer.py::predict_with_drivers`
- **payload**: dict ที่ export เป็น JSONL (`outputs/predictions_with_drivers.jsonl`) — `src/infer.py::predict_with_drivers` (ฟังก์ชันย่อย `_to_payload`)

## 1) Artifacts contract (สำหรับ inference-only / production)
Artifacts ถูกเขียนตอน train และถูกอ่านตอน `--skip-train`

### 1.1 `artifacts/brand_health_model.joblib`
- เป็น scikit-learn `Pipeline` หรือ `CalibratedClassifierCV` ที่ถูก `joblib.dump`

**หลักฐานใน repo:** `src/train.py::train_models` (เขียน `brand_health_model.joblib`), `src/infer.py::load_model_artifacts` (อ่านด้วย `joblib.load`)

### 1.2 `artifacts/model_metadata.json`
มีข้อมูลสำคัญ:
- `feature_columns`: รายชื่อ feature ที่ต้องส่งเข้า model (contract สำหรับ X)
- `class_labels`: รายชื่อคลาสที่ model ส่ง probability ออก
- `metrics`: สรุป eval (time split, cross-brand, calibration ฯลฯ)

**หลักฐานใน repo:** `src/train.py::train_models` (เขียน metadata), `src/infer.py::load_model_artifacts` (อ่าน metadata), ตัวอย่างไฟล์: `artifacts/model_metadata.json`

### 1.3 `artifacts/feature_importance.json`
ใช้สร้าง “model-based drivers” โดยดู feature importance ที่ได้จาก permutation importance

**หลักฐานใน repo:** `src/train.py::train_models` (เขียนไฟล์), `src/drivers.py::build_model_importance_drivers` (อ่านผ่าน dict), ตัวอย่างไฟล์: `artifacts/feature_importance.json`

## 2) Intermediate outputs (feature/segment/label)
### 2.1 Feature table
- `outputs/feature_table.parquet` และ `outputs/feature_table_sample.csv`
- `outputs/feature_definitions.csv` (ความหมายเชิง suffix-based)

**หลักฐานใน repo:** `run_pipeline.py::main` (เขียนไฟล์), `src/features.py::build_feature_table`, `src/features.py::feature_definitions`, `src/memory_opt.py::write_parquet_chunked`

### 2.2 Segment KPIs
- `outputs/segment_kpis.parquet` และ `outputs/segment_kpis.csv`
- มีคอลัมน์ `seg_<segment_key>_<metric_suffix>` สำหรับหลาย segment

**หลักฐานใน repo:** `run_pipeline.py::main` (เขียนไฟล์), `src/segments.py::compute_segment_kpis`, คีย์/metric suffix: `src/segments.py::SEGMENT_KEYS`, `src/segments.py::SEGMENT_METRIC_SUFFIXES`

### 2.3 Labeled feature table
- `outputs/labeled_feature_table.parquet`
- เพิ่มคอลัมน์ `label_health_score`, `label_health_class`, `label_health_class_int`

**หลักฐานใน repo:** `run_pipeline.py::main` (เขียนไฟล์), `src/labeling.py::generate_weak_labels`

## 3) Prediction outputs (สิ่งที่ downstream consume)
### 3.1 ไฟล์ที่เขียนออก
`src/infer.py::save_predictions` เขียน:
- `outputs/predictions_with_drivers.csv`
- `outputs/predictions_with_drivers.parquet`
- `outputs/predictions_with_drivers.jsonl` (จาก `payload`)

**หลักฐานใน repo:** `src/infer.py::save_predictions`

นอกจากนี้ `run_pipeline.py::main` เขียน snapshot สำหรับ dashboard:
- `outputs/examples_last4_windows.json`
- `outputs/examples_last4_with_segments.json`
- `outputs/examples_before_after_2windows.json`

**หลักฐานใน repo:** `run_pipeline.py::main` (ส่วน "Last 4 windows per brand" และ `_build_before_after_examples`)

### 3.2 Schema: `payload` ใน JSONL
คีย์หลักของ payload ถูกกำหนดใน `src/infer.py::predict_with_drivers` (ฟังก์ชันย่อย `_to_payload`):
- identifiers: `brand_id`, `window_end_date`, `window_size`
- prediction: `predicted_health_class`, `predicted_health_statement`, `predicted_health_score`
- confidence: `confidence_band`, `confidence_band_i18n`
- probabilities: `probabilities` (map class → prob)
- explain/action: `drivers`, `target_segments`, `suggested_actions`, `suggested_actions_i18n`
- QA/warnings: `attribution_warnings`

**หลักฐานใน repo:** `src/infer.py::predict_with_drivers` (ฟังก์ชันย่อย `_to_payload`), output จริง: `outputs/predictions_with_drivers.jsonl`

## 4) Nested contracts
### 4.1 Driver object
Driver ถูกสร้างโดย `src/drivers.py::build_drivers` และถูก attach ด้วย `src/drivers.py::attach_drivers` โดยแต่ละ driver มีโครงสร้างหลัก:
- `key` + `key_i18n`
- `statement` + `statement_i18n`
- `severity`, `direction` + `direction_i18n`
- `metric_family`
- `metrics` (เช่น `*_wow_pct`, `*_zscore`, และ `importance` สำหรับ model driver)

**หลักฐานใน repo:** `src/drivers.py::_add_driver`, `src/drivers.py::build_metric_drivers`, `src/drivers.py::build_model_importance_drivers`

### 4.2 Target segment object
Target segment ถูกสร้างโดย `src/infer.py::_build_target_segments_for_row` และผ่าน validation ใน `src/infer.py::_validate_target_segments_row` โดยแต่ละ segment มี:
- `metric_family` + `metric_family_i18n`
- `segment_key` + `segment_label_i18n`
- `direction` + `direction_i18n`
- `contribution_share` (สัดส่วน contribution ต่อ denom ของ delta)
- `reason_statement` + `reason_statement_i18n`
- `evidence_metrics` (delta_seg, delta_total, share/count, wow_pct_seg, note, cold_start_increase)
- `segment_confidence`

**หลักฐานใน repo:** `src/infer.py::_build_target_segments_for_row`, `src/infer.py::_validate_target_segments_row`

### 4.3 Suggested actions + i18n
Suggested actions มาจาก `src/playbook.py::map_drivers_to_actions` และถูกแปลง i18n โดย `src/playbook.py::build_actions_i18n`

**หลักฐานใน repo:** `src/playbook.py::attach_actions`, `src/playbook.py::build_actions_i18n`, `src/infer.py::predict_with_drivers` (เติม `suggested_actions_i18n`)

### 4.4 Attribution warnings (QA signals)
warnings ถูกเติมในขั้นสร้าง/validate target_segments เช่น:
- `driver_sign_mismatch:<metric_family>:<driver_key>`
- `commerce_mode_block:<metric_family>`
- `noisy_metric:<metric_family>`
- `no_eligible_segments:<metric_family>`
- `zero_denom:<metric_family>`
- `drop_presence:*`, `drop_direction:*`, `drop_commerce_nonjoinable:*`

**หลักฐานใน repo:** `src/infer.py::_build_target_segments_for_row`, `src/infer.py::_validate_target_segments_row`

## 5) Examples
### 5.1 ตัวอย่าง “แถวสำคัญ” ของ predictions (สรุปจาก JSONL จริง)
ตารางนี้ยกตัวอย่างจาก `outputs/predictions_with_drivers.jsonl` เพื่อชี้ว่าผลลัพธ์ระดับ row ประกอบด้วยอะไรบ้าง (ค่าตัวอย่างมาจากไฟล์ output จริง)

| field | example (มี segments) | example (commerce ถูก block) |
|---|---:|---:|
| brand_id | c-vit | c-vit |
| window_end_date | 2021-01-17 00:00:00+00:00 | 2021-09-05 00:00:00+00:00 |
| window_size | 30d | 30d |
| predicted_health_class | Healthy | Healthy |
| predicted_health_score | 86.92139509555095 | 87.19499941555134 |
| confidence_band | high | high |
| n_drivers | 2 | 2 |
| n_target_segments | 5 | 0 |
| suggested_actions[0] | Trigger winback rewards for `dormant_15_30d`... | Monitor the next 1-2 windows... |
| attribution_warnings | [] | ["commerce_mode_block:gmv_net","commerce_mode_block:transaction_count"] |

**หลักฐานใน repo:** output จริง `outputs/predictions_with_drivers.jsonl`, schema payload: `src/infer.py::predict_with_drivers` (`_to_payload`), การ block commerce: `src/infer.py::_build_target_segments_for_row`, fallback actions: `src/playbook.py::map_drivers_to_actions`

### 5.2 ตัวอย่าง schema สำหรับ report/dashboard snapshots
- `outputs/examples_last4_with_segments.json` เป็น export ของ DataFrame `examples` ที่ถูกเลือกจาก `pred_df` (last 4 windows ต่อแบรนด์)  
  **หลักฐานใน repo:** `run_pipeline.py::main` (ส่วน `examples = ... tail(4)`), ไฟล์ตัวอย่าง: `outputs/examples_last4_with_segments.json`

## Decisions & Implications
- ถ้าจะใช้ `predictions_with_drivers.jsonl` เป็น contract ระหว่างทีม แนะนำทำ schema versioning และ contract test โดยอิง `_to_payload` เป็น source-of-truth — `src/infer.py::predict_with_drivers`
- ถ้าจะใช้ใน downstream ที่ต้องการ i18n ให้ถือว่า `*_i18n` เป็น optional-but-recommended และควรมี fallback เป็น EN/TH เท่ากันตาม `_i18n` — `src/infer.py::_i18n`

## Open Questions
- จะ persist `commerce_joinable` ลงใน payload หรือ metadata สำหรับ consumer หรือไม่ (ตอนนี้ payload ไม่ได้ include field นี้) — `src/infer.py::predict_with_drivers` (`_to_payload` ไม่มี `commerce_joinable`)

