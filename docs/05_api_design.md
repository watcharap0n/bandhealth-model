# 05 — API Design Guidance (สำหรับ Production Serving)

## Purpose
ให้แนวทางออกแบบ API สำหรับนำโมเดล/อินเฟอเรนซ์ของ repo นี้ไป serve ใน production โดยเน้น:
- endpoint ที่ควรมี
- request/response JSON schema + examples
- validation rules + error codes
- schema versioning + backward compatibility

> หมายเหตุสำคัญ: ใน repo “ยังไม่พบ” การ implement API server (เช่น FastAPI/Flask) เอกสารนี้จึงเป็น “ข้อเสนอแนะ” โดยอิง contract จากโค้ด inference (`src/infer.py`)

**หลักฐานใน repo:** ข้อเท็จจริงว่าเป็น CLI/batch: `run_pipeline.py::main`, dependency: `requirements.txt`, contract payload: `src/infer.py::predict_with_drivers`

## What you will learn
- จะออกแบบ API ให้สอดคล้องกับ pipeline นี้อย่างไร (โดยไม่ทำให้ schema drift)
- field ไหน “จำเป็น” เพื่อให้ explain/action ทำงานได้
- error cases ที่ควรทำให้ชัด (validation, missing artifacts, schema mismatch)

**หลักฐานใน repo:** `src/infer.py::load_model_artifacts`, `src/infer.py::predict_with_drivers`, `artifacts/model_metadata.json`

## Definitions/Glossary
- **Schema versioning**: การผูกสัญญา JSON ด้วย version เช่น `v1`, `v2` เพื่อไม่ให้ consumer พังเมื่อมีการเพิ่ม/เปลี่ยน field (แนวทาง; ยังไม่พบใน repo)
- **Feature vector**: list/obj ของ feature ที่ต้องเข้าโมเดลตาม `model_metadata.feature_columns` (`src/train.py::train_models`)
- **Explainability bundle**: `drivers`, `target_segments`, `suggested_actions` ที่ได้จาก `predict_with_drivers` (`src/infer.py::predict_with_drivers`)

## 1) Endpoint ที่แนะนำ
### 1.1 `GET /health`
คืนสถานะของ service (liveness/readiness)

> ใน repo ยังไม่พบหลักฐานของ endpoint นี้ (ข้อเสนอแนะ)

### 1.2 `GET /metadata`
คืนข้อมูล model/artifact เช่น:
- `model_version` (ถ้ามี)
- `class_labels`
- `feature_columns`
- `selected_model`/metrics (optional; ระวังขนาด payload)

**หลักฐานใน repo (สำหรับสิ่งที่ดึงได้จริง):** `artifacts/model_metadata.json`, `src/infer.py::load_model_artifacts`

### 1.3 `POST /predict`
รับ feature(s) แล้วคืน payload ตาม contract ของ `src/infer.py::predict_with_drivers` (`_to_payload`)

**หลักฐานใน repo:** `src/infer.py::predict_with_drivers`

### 1.4 `POST /explain` (optional)
ถ้าต้องการแยก “predict-only” กับ “predict+explain”
- `predict`: คืนเฉพาะ class/score/probabilities
- `explain`: เพิ่ม drivers/segments/actions

**หลักฐานใน repo:** โค้ดรวมทุกอย่างอยู่ใน `src/infer.py::predict_with_drivers` (ข้อเสนอแนะคือแยก flag/endpoint)

## 2) Request/Response schemas (อิงจาก contract ใน repo)
### 2.1 Schema: `PredictRequestV1` (แบบ feature-serving)
แนะนำให้ request ระบุ:
- `schema_version`: "v1"
- `rows`: array ของ row ที่มี `brand_id`, `window_end_date`, `window_size` และ feature columns
- `options`: knobs เช่น `top_n_drivers`, `top_n_target_segments`, `top_n_actions`, `include_i18n`

เหตุผล: `predict_with_drivers` ต้องใช้ identifiers และ feature columns; และ knobs ถูก expose ใน signature

**หลักฐานใน repo:** `src/infer.py::predict_with_drivers` (signature), `artifacts/model_metadata.json` (`feature_columns`)

ตัวอย่าง (ย่อ):
```json
{
  "schema_version": "v1",
  "rows": [
    {
      "brand_id": "c-vit",
      "window_end_date": "2026-02-15T00:00:00Z",
      "window_size": "30d",
      "active_users": 1234,
      "gmv_net": 567890,
      "transaction_count": 321,
      "dormant_share": 0.42
    }
  ],
  "options": {
    "top_n_drivers": 5,
    "top_n_target_segments": 3,
    "top_n_actions": 3
  }
}
```

> หมายเหตุ: ตัวอย่างนี้โชว์เพียงบาง feature; ใน production ต้องส่งครบ `feature_columns` ตาม `model_metadata.json` หรือให้ server เติม default 0.0 (เหมือน `_prepare_inference_frame`)

**หลักฐานใน repo:** `src/infer.py::_prepare_inference_frame`, `artifacts/model_metadata.json`

### 2.2 Schema: `PredictResponseV1`
แนะนำให้ response คืน:
- `schema_version`
- `results`: array ของ payload (source-of-truth คือ `_to_payload`)
- `warnings` (ระดับ request เช่น artifacts missing)

**หลักฐานใน repo:** payload keys: `src/infer.py::predict_with_drivers` (ฟังก์ชันย่อย `_to_payload`)

## 3) Error codes + validation rules
> Repo ยังไม่พบ error model สำหรับ API; ด้านล่างคือข้อเสนอแนะ

### 3.1 Validation rules (ควรทำ)
- `schema_version` ต้องรู้จัก
- ทุก row ต้องมี `brand_id`, `window_end_date`, `window_size`
- ตรวจ `window_size` ต้องอยู่ใน vocabulary ของ pipeline (เช่น `"7d"|"30d"|"60d"|"90d"`) ถ้าใช้แบบเดียวกับ training/infer

**หลักฐานใน repo:** `src/features.py::WINDOW_SIZES` (7/30/60/90), `src/segments.py::compute_segment_kpis` (สร้าง `window_size` เป็น f"{w}d")

### 3.2 Error codes (ตัวอย่าง)
- `400 INVALID_ARGUMENT`: schema_version ไม่รองรับ / field จำเป็นหาย
- `404 ARTIFACT_NOT_FOUND`: ไม่มีไฟล์ `brand_health_model.joblib` หรือ `model_metadata.json`
- `422 SCHEMA_MISMATCH`: feature_columns ไม่สอดคล้องกับ metadata (หรือ parse datetime ไม่ได้)
- `500 INTERNAL`: model predict ล้มเหลว

**หลักฐานใน repo (สิ่งที่ทำให้เกิด error ได้จริง):** การอ่าน artifacts: `src/infer.py::load_model_artifacts`, การเติม missing feature: `src/infer.py::_prepare_inference_frame`

## 4) Versioning + backward compatibility
ข้อเสนอแนะ:
- ใส่ `schema_version` ใน request/response
- อนุญาต “เพิ่ม field ใหม่” แบบ additive ใน payload แต่ห้ามเปลี่ยนความหมายของ field เดิมโดยไม่ bump version
- ทำ contract test โดย compare output keys กับ `_to_payload`

**หลักฐานใน repo:** source-of-truth payload คือ `src/infer.py::predict_with_drivers` (ฟังก์ชันย่อย `_to_payload`)

## 5) Examples (ต้องมี 2 แบบ: commerce_joinable=true/false)
> ใน output payload ปัจจุบัน “ไม่ได้ include” `commerce_joinable` โดยตรง ดังนั้นตัวอย่างด้านล่างจะทำที่ระดับ “input row/processing context” เพื่ออธิบาย behavior ตามโค้ดของ target_segments/actions

### 5.1 Example A — `commerce_joinable=true` (ยอมให้ commerce metric/segment/action ทำงาน)
เงื่อนไขในโค้ด:
- target_segments: ถ้า `metric_family in COMMERCE_METRIC_FAMILIES` และ `commerce_joinable=false` จะถูก block; ดังนั้นเมื่อ `true` จะไม่ถูก block ที่จุดนี้  
- actions: commerce metric families (`gmv_net`, `transaction_count`, `sku_concentration`) จะไม่ถูก skip ใน `map_drivers_to_actions`

**หลักฐานใน repo:** `src/infer.py::_build_target_segments_for_row` (เช็ค `COMMERCE_METRIC_FAMILIES`), `src/playbook.py::map_drivers_to_actions` (skip เมื่อ non-joinable), `src/driver_mapping.py::COMMERCE_METRIC_FAMILIES`

ตัวอย่าง request row (ย่อ; ต้องมี `commerce_joinable` ใน context ของ server):
```json
{
  "brand_id": "see-chan",
  "window_end_date": "2026-02-02T00:00:00Z",
  "window_size": "30d",
  "commerce_joinable": 1.0,
  "gmv_net": 58712551.0,
  "transaction_count": 122633.0
}
```

**หลักฐานใน repo:** การใช้ `commerce_joinable` ใน row: `src/infer.py::_build_target_segments_for_row`, `src/playbook.py::map_drivers_to_actions`, ตัวอย่าง field ใน output intermediate: `outputs/examples_last4_with_segments.json`

### 5.2 Example B — `commerce_joinable=false` (ต้องเห็น behavior ที่ skip action บางประเภท)
Behavior ตามโค้ด:
- target_segments: จะ append warning `commerce_mode_block:<metric_family>` และ skip สร้าง segment สำหรับ commerce metric families
- actions: จะ skip action ที่มาจาก metric family commerce และถ้าไม่เหลือ action เลย จะ fallback เป็น generic actions

**หลักฐานใน repo:** `src/infer.py::_build_target_segments_for_row` (เติม warning และ continue), `src/playbook.py::map_drivers_to_actions` (skip commerce และมี fallback generic)

ตัวอย่าง “output จริง” ที่เห็น warning `commerce_mode_block` และ actions เป็น generic อยู่ใน `outputs/predictions_with_drivers.jsonl`:
- record ที่มี `window_end_date="2021-09-05 00:00:00+00:00"` และมี `attribution_warnings=["commerce_mode_block:gmv_net","commerce_mode_block:transaction_count"]`

**หลักฐานใน repo:** output: `outputs/predictions_with_drivers.jsonl`, warning semantics: `src/infer.py::_build_target_segments_for_row`, fallback actions: `src/playbook.py::map_drivers_to_actions`

## Decisions & Implications
- ถ้า API ต้องการ behavior นี้แบบ deterministic แนะนำให้ include `commerce_joinable` ใน “processing context” (หรือ embed ใน feature row) เพื่อเลี่ยงกรณี field หายแล้วถูกตีความเป็น false โดย default `row.get("commerce_joinable", 0.0)`  
  **หลักฐานใน repo:** `src/infer.py::_build_target_segments_for_row` (default 0.0), `src/playbook.py::map_drivers_to_actions` (default 0.0)

## Open Questions
- จะให้ `commerce_joinable` เป็น responsibility ของ upstream (ส่งมา) หรือคำนวณใน service (join diagnostics) — ดู `src/data_load.py::build_purchase_item_join_diagnostics`

