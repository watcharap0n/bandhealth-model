# 01 — Repo Map + Evidence Map

## Purpose
ทำแผนที่โครงสร้าง repo และ “แผนที่ component” สำหรับระบบโมเดล/อินเฟอเรนซ์ เพื่อให้คนใหม่หา entrypoint และโค้ดที่เกี่ยวข้องได้เร็ว พร้อม Evidence Map ที่อ้างอิงตำแหน่งใน repo

**หลักฐานใน repo:** `README.md` (หัวข้อ "Project structure"), `run_pipeline.py::main`

## What you will learn
- โฟลเดอร์หลัก/entrypoint/config/output อยู่ตรงไหน
- component map: ingestion/preprocess/feature/driver/explain/action/i18n/export/api
- Evidence Map ที่เอกสารชุดนี้ใช้เป็นหลักฐาน

**หลักฐานใน repo:** `README.md`, `run_pipeline.py::main`, `src/*`

## Definitions/Glossary (สรุปสั้น)
- **Entrypoint**: จุดเริ่มการรัน pipeline เช่น CLI หรือ notebook (`run_pipeline.py::main`, `notebooks/databricks/01_brand_health_pipeline_mlflow.py`)
- **Artifacts**: ไฟล์โมเดลและ metadata ที่ใช้ inference (`src/train.py::train_models`, `src/infer.py::load_model_artifacts`)
- **Outputs**: ไฟล์ผลลัพธ์ที่ downstream consume (`src/infer.py::save_predictions`, `run_pipeline.py::main`)

## 1) โครงสร้าง repo (high-level)
- `run_pipeline.py` — CLI entrypoint สำหรับรัน end-to-end pipeline (`run_pipeline.py::main`)
- `src/` — โค้ดหลักสำหรับ ingest/feature/label/train/infer/actions (`README.md` หัวข้อ "Project structure")
- `datasets/` — input parquet ตาม subset folder (`README.md` หัวข้อ "Dataset layout", `src/data_load.py::load_tables`)
- `artifacts/` — model artifacts + metadata (`src/train.py::train_models`, `src/infer.py::load_model_artifacts`)
- `outputs/` — predictions/diagnostics/ตัวอย่าง JSON (`src/infer.py::save_predictions`, `run_pipeline.py::main`)
- `reports/` — report markdown + summary json (`run_pipeline.py::_write_markdown_report`)
- `notebooks/databricks/` — notebook pack สำหรับ Databricks + MLflow (`notebooks/databricks/README.md`)

## 2) Component Map (ตามระบบ end-to-end)
> หมายเหตุ: ในที่นี้ “component” คือชุดความรับผิดชอบใน pipeline (ไม่จำเป็นต้องเป็น service แยก)

### 2.1 ingestion / load
- Scan subset folders และโหลด parquet หลายตาราง: `src/data_load.py::load_tables`
- Dataset profiling (schema/missing/join coverage): `src/data_load.py::profile_dataset`
- Join diagnostics + commerce_joinable: `src/data_load.py::build_purchase_item_join_diagnostics`

### 2.2 preprocess / normalization
- Normalize join keys ให้ robust: `src/id_utils.py::normalize_id`
- Memory optimization (dtype downcast + RSS logging): `src/memory_opt.py::optimize_table_dict`, `src/memory_opt.py::log_memory_rss`

### 2.3 feature engineering
- สร้าง feature table ระดับแบรนด์-วินโดว์: `src/features.py::build_feature_table`
- สร้าง feature definitions สำหรับ export: `src/features.py::feature_definitions`

### 2.4 segment KPIs (เพื่อ targeting/attribution)
- สร้าง KPI รายเซกเมนต์ (segment_kpis): `src/segments.py::compute_segment_kpis`
- เซกเมนต์หลักถูกกำหนดใน `src/segments.py::ACTIVITY_SEGMENT_KEYS`, `src/segments.py::COMMERCE_SEGMENT_KEYS`

### 2.5 labeling (weak supervision)
- สร้าง `label_health_score` และ `label_health_class`: `src/labeling.py::generate_weak_labels`

### 2.6 training / evaluation
- Train scikit-learn pipelines + calibration + export artifacts: `src/train.py::train_models`
- Sampling train/eval สำหรับลด resource: `src/sampling.py::build_train_eval_samples`

### 2.7 inference / scoring
- เตรียม X, predict, confidence, i18n: `src/infer.py::predict_with_drivers`
- โหลด artifacts สำหรับ skip-train mode: `src/infer.py::load_model_artifacts`
- export predictions เป็น CSV/Parquet/JSONL: `src/infer.py::save_predictions`

### 2.8 driver extraction (explain)
- Rule-based drivers + model-importance drivers: `src/drivers.py::build_drivers`, `src/drivers.py::attach_drivers`
- Mapping driver key → metric family: `src/driver_mapping.py::infer_metric_family_from_key`

### 2.9 target segments (explain + marketing)
- สร้าง target_segments จาก driver + segment deltas + guardrails: `src/infer.py::_build_target_segments_for_row`
- validate/filter เพิ่มเติม: `src/infer.py::_validate_target_segments_row`

### 2.10 action engine
- Map drivers/segments → suggested_actions (รวม fallback): `src/playbook.py::map_drivers_to_actions`
- Attach เข้า predictions_df: `src/playbook.py::attach_actions`

### 2.11 i18n
- i18n สำหรับ health class/confidence/segments: `src/infer.py::_i18n` และ map TH ใน `src/infer.py::HEALTH_CLASS_TH` เป็นต้น
- i18n สำหรับ drivers: `src/drivers.py::_add_driver` (สร้าง `key_i18n`, `statement_i18n`, `direction_i18n`)
- i18n สำหรับ actions: `src/playbook.py::build_actions_i18n`

### 2.12 export / consumers
- `outputs/predictions_with_drivers.jsonl` สำหรับ backend/automation: `src/infer.py::save_predictions`
- `outputs/examples_last4_with_segments.json` สำหรับ dashboard-like snapshot: `run_pipeline.py::main` (ส่วน "Last 4 windows per brand")
- `reports/*.md` สำหรับอ่านแบบ human-friendly: `run_pipeline.py::_write_markdown_report`

### 2.13 API (ยังไม่พบหลักฐานใน repo)
- ยังไม่พบไฟล์ที่ implement web server/endpoint และไม่มี dependency ประเภท FastAPI/Flask ใน `requirements.txt`  
  **หลักฐานใน repo:** `run_pipeline.py::main` (CLI), `requirements.txt`, `Dockerfile` (CMD `python run_pipeline.py --help`)

## 3) Evidence Map (ไฟล์/ตำแหน่งที่ใช้เป็นหลักฐาน)
รายการนี้คือ “ตำแหน่งใน repo” ที่เอกสารชุดนี้ยึดเพื่ออธิบายระบบ (อาจมากกว่าที่ลิสต์ด้านล่างในบางหัวข้อ แต่รายการนี้คือแกนหลัก)

### Entrypoints
- `run_pipeline.py::main`
- `notebooks/databricks/01_brand_health_pipeline_mlflow.py` (config + pipeline ใน Databricks)

### Data ingestion + diagnostics
- `src/data_load.py::TABLE_FILES`
- `src/data_load.py::load_tables`
- `src/data_load.py::profile_dataset`
- `src/data_load.py::validate_join_coverage`
- `src/data_load.py::build_purchase_item_join_diagnostics`
- `src/data_load.py::write_join_diagnostics_markdown`

### Normalization + memory
- `src/id_utils.py::normalize_id`
- `src/memory_opt.py::optimize_table_dict`
- `src/memory_opt.py::write_parquet_chunked`
- `src/memory_opt.py::log_memory_rss`

### Feature/Segment/Label
- `src/features.py::build_feature_table`
- `src/features.py::_compute_commerce_features`
- `src/features.py::_add_relative_and_trend_features`
- `src/segments.py::compute_segment_kpis`
- `src/labeling.py::generate_weak_labels`

### Training artifacts
- `src/train.py::train_models`
- `artifacts/model_metadata.json` (feature_columns, class_labels, metrics)
- `artifacts/feature_importance.json`

### Inference + contracts + exports
- `src/infer.py::load_model_artifacts`
- `src/infer.py::predict_with_drivers` (รวม `_to_payload`)
- `src/infer.py::_build_target_segments_for_row`
- `src/infer.py::_validate_target_segments_row`
- `src/infer.py::save_predictions`

### Drivers/Actions/I18n
- `src/driver_mapping.py::canonical_driver_key`
- `src/driver_mapping.py::infer_metric_family_from_key`
- `src/drivers.py::attach_drivers`
- `src/playbook.py::ACTION_MAP`
- `src/playbook.py::map_drivers_to_actions`
- `src/playbook.py::build_actions_i18n`

### ตัวอย่าง outputs ที่ใช้ประกอบ (data evidence)
- `outputs/predictions_with_drivers.jsonl`
- `outputs/examples_last4_with_segments.json`
- `outputs/join_diagnostics.md`
- `outputs/examples_before_after_2windows.json`
- `reports/band_health_report_V2_TH.md` (เอกสารสรุป logic ที่อ้างอิงไฟล์โค้ด)

## Decisions & Implications
- โครงสร้าง repo ถูกออกแบบให้ “รันแบบ batch pipeline” เป็นหลัก (CLI + notebooks) ซึ่งทำให้ infra ที่เหมาะคือ orchestration (cron/Airflow/Databricks job) + artifact storage มากกว่า request-serving โดยตรง  
  **หลักฐานใน repo:** `run_pipeline.py::main`, `notebooks/databricks/README.md`, `Dockerfile`

## Examples
### Example 1 — อยากรู้ว่า “ไฟล์ output นี้มาจากโค้ดส่วนไหน”
- ถ้า consumer ใช้ `outputs/predictions_with_drivers.jsonl` เป็นหลัก ให้ถือว่า schema มาจาก `_to_payload` ใน `src/infer.py::predict_with_drivers` และการเขียนไฟล์อยู่ใน `src/infer.py::save_predictions`  
  **หลักฐานใน repo:** `src/infer.py::predict_with_drivers` (ฟังก์ชันย่อย `_to_payload`), `src/infer.py::save_predictions`

### Example 2 — อยากเพิ่มแบรนด์/app_id ใหม่
- แก้ mapping ใน `run_pipeline.py::BRAND_APP_ID_FILTERS` (CLI) หรือใน `notebooks/databricks/01_brand_health_pipeline_mlflow.py` (Databricks)  
  **หลักฐานใน repo:** `run_pipeline.py::BRAND_APP_ID_FILTERS`, `notebooks/databricks/01_brand_health_pipeline_mlflow.py` (ตัวแปร `BRAND_APP_ID_FILTERS`)

## Open Questions
- ถ้าต้องการ API จริง: จะเลือกโหมด online แบบ “รับ feature vector” หรือ “รับ raw events” (ผลต่อ latency/compute อย่างมาก) — ดู `src/features.py::build_feature_table`, `src/segments.py::compute_segment_kpis`
