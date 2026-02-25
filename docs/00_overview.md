# 00 — ภาพรวมระบบ Brand Health (End-to-End)

## Purpose
อธิบายระบบโมเดล/อินเฟอเรนซ์แบบ end-to-end ของ repo นี้ ตั้งแต่รับข้อมูลดิบ (parquet) → สร้าง feature → weak label → train model → inference → drivers/target_segments/actions → export outputs

**หลักฐานใน repo:** `run_pipeline.py::main`, `README.md` (หัวข้อ "The pipeline")

## What you will learn
- pipeline ทำงานในระดับอะไร (granularity) และมี stage อะไรบ้าง
- output หลักที่ผู้ใช้งาน downstream จะ consume คืออะไร
- จุดที่เป็น model-based vs rule-based และจุดที่มี fallback/guardrail
- สถานะการรองรับ online API (ยังไม่พบใน repo)

**หลักฐานใน repo:** `src/labeling.py::generate_weak_labels`, `src/train.py::train_models`, `src/infer.py::predict_with_drivers`, `requirements.txt`

## Definitions/Glossary (สรุปสั้น)
- **Window / windows**: ช่วงเวลาที่ใช้ aggregate KPI เช่น `30d` (window_size) และวันสิ้นสุด window (window_end_date) (`src/features.py::build_feature_table`, `src/segments.py::compute_segment_kpis`)
- **Weak labels**: label ที่สร้างจาก heuristic เพื่อฝึกโมเดลโดยไม่ต้องมี ground truth (`src/labeling.py::generate_weak_labels`)
- **Driver**: เหตุผล/สัญญาณที่สรุปออกจาก KPI trend และ/หรือ feature importance (`src/drivers.py::build_drivers`)
- **Target segment**: เซกเมนต์ผู้ใช้ที่เป็นตัวขับหลักของการเปลี่ยนแปลง KPI ใน window นั้น (`src/infer.py::_build_target_segments_for_row`)
- **Commerce joinability (commerce_joinable)**: ธงต่อแบรนด์ที่บอกว่า join `purchase` ↔ `purchase_items` เชื่อถือได้หรือไม่ (`src/data_load.py::build_purchase_item_join_diagnostics`)

ดูรายละเอียดเพิ่มเติม: `docs/99_glossary_loyalty_101.md`

## 1) ระบบนี้ “ทำอะไร”
ระบบสร้าง “คะแนนสุขภาพแบรนด์” และคลาส `Healthy/Warning/AtRisk` ในระดับ `(brand_id, window_end_date, window_size)` แล้ว export:
1) ผลทำนาย + ความมั่นใจ (confidence band)  
2) เหตุผล (drivers)  
3) เซกเมนต์เป้าหมาย (target_segments)  
4) ข้อแนะนำเชิงปฏิบัติ (suggested_actions) พร้อม i18n EN/TH

**หลักฐานใน repo:** `src/infer.py::predict_with_drivers` (สร้าง predicted fields/target_segments/actions/i18n), `src/infer.py::save_predictions` (export), `src/drivers.py::attach_drivers`, `src/playbook.py::attach_actions`

## 2) ระบบนี้ “ไม่ทำอะไร” (ขอบเขตที่ยังไม่พบใน repo)
- ยังไม่พบ API server/endpoint สำหรับ online inference (เช่น FastAPI/Flask) และ dependency ที่เกี่ยวข้องก็ไม่มีใน `requirements.txt`  
- รูปแบบ “serving” ที่มีใน repo ตอนนี้เป็น batch file export (CSV/Parquet/JSONL) ผ่าน CLI `run_pipeline.py`

**หลักฐานใน repo:** `requirements.txt`, `run_pipeline.py::main`, `README.md` (หัวข้อ "Main outputs")

## 3) End-to-End flow (มองแบบ pipeline)
Pipeline หลักทำงาน 0/7 → 7/7 ดังนี้:
0) Join diagnostics: ตรวจคุณภาพ join `purchase` ↔ `purchase_items` และตัดสิน `commerce_joinable` ต่อแบรนด์  
1) Profiling: สรุป schema/missingness/join coverage  
2) Load tables: โหลด parquet หลายตาราง โดยเลือกเฉพาะคอลัมน์ที่ต้องใช้  
3) Build features: aggregate KPI + trend features ตาม window  
4) Segment KPIs: สร้าง KPI รายเซกเมนต์เพื่อใช้ทำ attribution/targeting  
5) Weak labels: สร้าง `label_health_*` สำหรับการ train  
6) Train/eval: ฝึก scikit-learn model + เลือก model + export artifacts  
7) Inference: predict + confidence + drivers + target_segments + actions + export

**หลักฐานใน repo:** `run_pipeline.py::main` (log string `[0/7] ... [7/7] ...` และเรียกแต่ละ stage), `src/data_load.py::build_purchase_item_join_diagnostics`, `src/features.py::build_feature_table`, `src/segments.py::compute_segment_kpis`, `src/labeling.py::generate_weak_labels`, `src/train.py::train_models`, `src/infer.py::predict_with_drivers`

## 4) ตัวอย่าง output แบบย่อ (payload ที่ export เป็น JSONL)
โครงสร้าง payload ถูกกำหนดโดย `src/infer.py::predict_with_drivers` (ฟังก์ชันย่อย `_to_payload`) และถูกเขียนเป็น JSONL โดย `src/infer.py::save_predictions`

ตัวอย่าง 1 (มี drivers + target_segments + actions) สามารถพบได้ใน `outputs/predictions_with_drivers.jsonl`:
- record ที่มี `window_end_date="2021-01-17 00:00:00+00:00"` และ `brand_id="c-vit"` (ตัวอย่างนี้แสดงการมี `drivers_keys` และ `target_segments`)

**หลักฐานใน repo:** `src/infer.py::predict_with_drivers` (คีย์ใน payload), `src/infer.py::save_predictions` (ไฟล์ JSONL), `outputs/predictions_with_drivers.jsonl` (ตัวอย่างจริง)

## 5) Decisions & Implications (ผลต่อ API/Infra)
- การสร้าง feature/segment KPI ใน repo ตอนนี้อิง “ข้อมูลดิบจำนวนมาก” (parquet) และคำนวณเป็น window-based aggregation จึงเหมาะกับ batch pipeline มากกว่า online แบบ request-per-user  
  **หลักฐานใน repo:** `src/features.py::build_feature_table`, `src/segments.py::compute_segment_kpis`, `run_pipeline.py::main`
- หากต้องทำ online serving แนะนำแยก “feature store/aggregation service” ออกจาก “model inference service” และทำสัญญา feature schema ตาม `artifacts/model_metadata.json`  
  **หลักฐานใน repo:** `src/infer.py::load_model_artifacts` (อ่าน metadata), `artifacts/model_metadata.json`

## 6) Open Questions (ต้องถามทีม)
- นิยาม business ของ “brand health” ที่คาดหวังให้โมเดลสะท้อนคืออะไร (repo ใช้ weak-label heuristic เป็นหลัก) — `src/labeling.py::generate_weak_labels`
- จะนิยาม versioning ของ output schema และการ backward compatibility อย่างไร (ปัจจุบัน schema ผูกกับ `_to_payload`) — `src/infer.py::predict_with_drivers`

