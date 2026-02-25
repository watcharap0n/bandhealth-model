# 04 — Inference & Pipeline Design (End-to-End)

## Purpose
อธิบาย inference flow ของระบบนี้แบบ end-to-end ตั้งแต่ input เป็น feature_df → model predict → confidence → drivers → target_segments → actions → export รวมถึงจุดที่เป็น rule-based vs model-based, guardrails, และ fallback behavior

**หลักฐานใน repo:** `src/infer.py::predict_with_drivers`, `src/infer.py::save_predictions`

## What you will learn
- ขั้นตอน inference ที่เกิดขึ้นภายใน `predict_with_drivers`
- ข้อมูลขั้นต่ำที่ต้องมีเพื่อให้ drivers/segments/actions ทำงานได้
- guardrails/warnings ที่ออกแบบไว้เพื่อลด noise
- ความต่างระหว่าง “batch inference” (ที่มีใน repo) vs “online inference” (แนวทางออกแบบ; repo ยังไม่พบ service)

**หลักฐานใน repo:** `run_pipeline.py::main` (เรียก inference stage), `requirements.txt` (ไม่มี web framework)

## Definitions/Glossary
- **feature_df**: ตารางฟีเจอร์ระดับ `(brand_id, window_end_date, window_size)` ที่เข้าโมเดล (`src/features.py::build_feature_table`)
- **segment_kpis_df**: ตาราง KPI รายเซกเมนต์ที่ merge เข้าเพื่อทำ attribution (`src/segments.py::compute_segment_kpis`, `src/infer.py::predict_with_drivers`)
- **driver_mapping**: mapping ระหว่าง driver key กับ metric family เพื่อควบคุม attribution (`src/driver_mapping.py::infer_metric_family_from_key`)
- **guardrails**: เงื่อนไขคัดกรอง/ลด noise ของ target segments (`src/infer.py::_build_target_segments_for_row`, `src/infer.py::_validate_target_segments_row`)

## 1) Batch pipeline orchestration (ภาพรวม)
`run_pipeline.py::main` ทำหน้าที่:
- สร้าง `feature_df` (`src/features.py::build_feature_table`)
- สร้าง `segment_kpi_df` (`src/segments.py::compute_segment_kpis`)
- สร้าง `labeled_df` (`src/labeling.py::generate_weak_labels`)
- train model หรือ load artifacts (`src/train.py::train_models`, `src/infer.py::load_model_artifacts`)
- เรียก `src/infer.py::predict_with_drivers` เพื่อสร้าง `pred_df`
- เรียก `src/infer.py::save_predictions` เพื่อ export

**หลักฐานใน repo:** `run_pipeline.py::main`

## 2) Inference core: `predict_with_drivers`
### 2.1 Prepare inference frame (X)
ระบบสร้างคอลัมน์เวลาเพิ่ม (seasonality-like) และเติมคอลัมน์ที่ขาดให้เป็น 0.0 ตาม `feature_columns` จาก metadata

**หลักฐานใน repo:** `src/infer.py::_prepare_inference_frame`, `src/train.py::_prepare_training_frame` (สร้างคอลัมน์คล้ายกันตอน train), `artifacts/model_metadata.json` (`feature_columns`)

### 2.2 Predict class + probabilities + score
- `pred = model.predict(X)`
- `proba = model.predict_proba(X)`
- สร้าง `predicted_health_score` โดยคูณ prob กับ `CLASS_SCORE_MAP`

**หลักฐานใน repo:** `src/infer.py::predict_with_drivers` (ช่วง `pred = ...`, `CLASS_SCORE_MAP`)

### 2.3 Confidence band
ระบบคำนวณ confidence จาก:
- top probability
- margin ระหว่าง top กับ second
แล้ว map เป็น `high/medium/low`

**หลักฐานใน repo:** `src/infer.py::_confidence_row`, `src/infer.py::predict_with_drivers` (เติม `confidence_band`)

### 2.4 i18n for prediction fields
เติม:
- `predicted_health_class_i18n`
- `confidence_band_i18n`
- `predicted_health_statement_i18n` (รวม logic borderline)

**หลักฐานใน repo:** `src/infer.py::_i18n`, `src/infer.py::_health_statement_i18n`, `src/infer.py::HEALTH_CLASS_TH`, `src/infer.py::CONFIDENCE_BAND_TH`

## 3) Explainability: Drivers
Drivers มาจาก 2 แหล่ง:
1) **Rule-based metric drivers** จาก trend threshold (`*_wow_pct`, `dormant_share_wow_pct`, ฯลฯ)  
2) **Model-importance drivers** จาก `feature_importance` (permutation importance) และค่า feature ใน row

**หลักฐานใน repo:** `src/drivers.py::build_metric_drivers`, `src/drivers.py::build_model_importance_drivers`, `src/drivers.py::attach_drivers`

### Mapping rule สำคัญ
การจัดกลุ่ม driver เป็น metric family ใช้:
- strict map ใน `src/driver_mapping.py::DRIVER_METRIC_MAP`
- fallback สำหรับ key ที่ขึ้นต้นด้วย `model_` ใน `src/driver_mapping.py::infer_metric_family_from_key`

**หลักฐานใน repo:** `src/driver_mapping.py::infer_metric_family_from_key`

## 4) Segment attribution: Target segments
### 4.1 Input ที่ target_segments ต้องพึ่งพา
การสร้าง target segments ต้องใช้:
- `drivers` ต่อ row
- delta ของ metric family (`<total_col>_delta_window`) ซึ่งถูกสร้างใน `src/infer.py::_add_total_metric_deltas`
- segment KPI columns + deltas (`seg_<seg>_<metric>`, `<...>_delta`, `<...>_prev`, `<...>_wow_pct`) ซึ่งมาจากการ merge `segment_kpis_df` แล้วผ่าน `src/infer.py::_add_segment_deltas`

**หลักฐานใน repo:** `src/infer.py::_add_total_metric_deltas`, `src/infer.py::_add_segment_deltas`, `src/infer.py::_build_target_segments_for_row`, `src/segments.py::compute_segment_kpis`

### 4.2 Guardrails / drop reasons (สำคัญต่อ infra)
Guardrails หลัก:
- ตัด metric_family ที่ driver sign mismatch (`driver_sign_mismatch:*`)  
- ตัด commerce metric families ถ้า `commerce_joinable` เป็น false (`commerce_mode_block:*`)  
- ตัด metric family ที่ delta “เล็ก/flat” (`noisy_metric:*`)  
- ตัด segment ที่ presence ต่ำ (share/count) (`segments_dropped_*`)  
- กัน cold-start และกรณี `prev=0` ด้วย note ใน evidence_metrics

**หลักฐานใน repo:** `src/infer.py::_build_target_segments_for_row`, `src/infer.py::_validate_target_segments_row`

## 5) Action engine: Suggested actions
ระบบสร้าง `suggested_actions` โดย:
1) พยายามสร้าง “segment-specific actions” ก่อน (เช่น winback สำหรับ dormant)  
2) ถ้าไม่พอ/ไม่มี จะ fallback ไป `ACTION_MAP` ตาม metric family/driver key  
3) ถ้ายังว่าง จะใช้ generic fallback 3 ข้อ

**หลักฐานใน repo:** `src/playbook.py::map_drivers_to_actions`, `src/playbook.py::ACTION_MAP`

## 6) Export contract
สุดท้าย `predict_with_drivers` สร้าง `payload` ต่อ row และ `save_predictions` เขียนออกเป็น:
- CSV (flattened)
- Parquet (serialize list/dict เป็น JSON string ในคอลัมน์ object)
- JSONL (payload)

**หลักฐานใน repo:** `src/infer.py::predict_with_drivers` (ฟังก์ชันย่อย `_to_payload`), `src/infer.py::save_predictions`

## 7) Online inference vs Batch inference
### Batch (มีใน repo)
- อ่านข้อมูลดิบจาก parquet/catalog → aggregate → train/infer → export เป็นไฟล์

**หลักฐานใน repo:** `run_pipeline.py::main`, `notebooks/databricks/01_brand_health_pipeline_mlflow.py`

### Online (แนวทางออกแบบ; ยังไม่พบใน repo)
หากต้องทำ online serving มี 2 แนวทางหลัก:
1) **Feature-serving API**: client ส่ง “feature vector ตาม model_metadata.feature_columns” แล้ว service ทำ `predict_with_drivers` เฉพาะ inference/explain/action  
2) **Raw-event-serving API**: client ส่ง raw events แล้ว service คำนวณ features/segments on-the-fly (เสี่ยง latency/compute สูง)

> Repo ยังไม่พบหลักฐานของ web service/endpoint และ dependency สำหรับ API serving

**หลักฐานใน repo:** feature contract ใน `artifacts/model_metadata.json`, inference entrypoints ใน `src/infer.py::predict_with_drivers`, “ไม่มี web framework” ใน `requirements.txt`

## 8) Decisions & Implications
- `target_segments` และ action engine เป็น “hybrid” (rule-based + model-based) ซึ่งต้องการ QA hooks (`attribution_warnings`, `attribution_qa`) สำหรับสังเกตระบบใน production  
  **หลักฐานใน repo:** `src/infer.py::predict_with_drivers` (attrs `attribution_qa`, field `attribution_warnings`)

## 9) Examples
### Example 1 — แถวที่มี drivers + target_segments + actions
ตัวอย่างจริงพบใน `outputs/predictions_with_drivers.jsonl` (เช่น record ที่ `brand_id="c-vit"` และ `window_end_date="2021-01-17 00:00:00+00:00"`) ซึ่งแสดงว่า:
- `drivers` ถูก attach แล้ว
- `target_segments` ถูกสร้างจาก driver + segment deltas
- `suggested_actions` ถูก map จาก segment/driver

**หลักฐานใน repo:** output: `outputs/predictions_with_drivers.jsonl`, driver attach: `src/drivers.py::attach_drivers`, segment build: `src/infer.py::_build_target_segments_for_row`, action map: `src/playbook.py::map_drivers_to_actions`

### Example 2 — กรณี commerce ถูก block → warnings + fallback actions
ตัวอย่างจริงพบใน `outputs/predictions_with_drivers.jsonl` ที่มี `attribution_warnings` เช่น `commerce_mode_block:gmv_net` และ `commerce_mode_block:transaction_count` ซึ่งสอดคล้องกับโค้ดที่:
- block commerce metric families เมื่อ `commerce_joinable=false`
- ทำให้ action จาก commerce metric families ถูก skip และอาจ fallback เป็น generic actions

**หลักฐานใน repo:** warning + block: `src/infer.py::_build_target_segments_for_row`, skip+fallback actions: `src/playbook.py::map_drivers_to_actions`, output: `outputs/predictions_with_drivers.jsonl`

## Open Questions
- ต้องการ persist ค่า `commerce_joinable` และ `activity_enrichment_joinable` ใน payload หรือ metadata เพื่อให้ downstream debug ได้ง่ายขึ้นหรือไม่ (payload ปัจจุบันไม่ include) — `src/infer.py::predict_with_drivers` (`_to_payload`)
