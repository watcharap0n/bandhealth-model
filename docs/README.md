# เอกสารระบบ Brand Health (Index)

## Purpose
เป็นสารบัญ (index) และแนวทางการอ่านเอกสารชุดนี้ เพื่อทำความเข้าใจระบบโมเดล/อินเฟอเรนซ์แบบ end-to-end ใน repo นี้ โดยยึดหลักฐานจากโค้ดจริงเป็นหลัก

**หลักฐานใน repo:** `run_pipeline.py::main`, `README.md` (หัวข้อ "Project structure")

## What you will learn
- ระบบนี้ประกอบด้วย pipeline อะไรบ้างตั้งแต่ ingest → feature → label → train → infer → drivers/segments/actions → export
- สัญญาข้อมูล (data contracts) ของ input/output ในแต่ละ stage
- โครงสร้าง payload ที่ export ออกไป (JSONL/CSV/Parquet) และความหมายของ field สำคัญ
- แนวทางออกแบบ API สำหรับ production serving (ใน repo ยังไม่พบ API server)

**หลักฐานใน repo:** `README.md` (หัวข้อ "The pipeline"), `src/infer.py::predict_with_drivers`, `src/infer.py::save_predictions`

## Definitions/Glossary (สรุปสั้น)
- **brand_id**: ตัวระบุแบรนด์ที่ pipeline ทำงานในระดับแบรนด์ต่อ window (`run_pipeline.py::BRAND_APP_ID_FILTERS`, `src/features.py::build_feature_table`)
- **window_end_date / window_size**: หน้าต่างเวลา (window) ที่ใช้คำนวณ KPI/feature และทำนายผล (`src/features.py::build_feature_table`, `src/segments.py::compute_segment_kpis`)
- **drivers**: เหตุผล/สัญญาณที่ดึงจาก rule + feature importance เพื่ออธิบายการเปลี่ยนแปลง (`src/drivers.py::attach_drivers`)
- **target_segments**: เซกเมนต์เป้าหมายที่เชื่อมโยงกับ driver แล้วผ่าน guardrails (`src/infer.py::_build_target_segments_for_row`, `src/infer.py::_validate_target_segments_row`)
- **suggested_actions**: ข้อแนะนำเชิงปฏิบัติที่ map จาก drivers/segments (`src/playbook.py::map_drivers_to_actions`)
- **i18n**: โครงสร้าง bilingual `{"en": "...", "th": "..."}` สำหรับ output (`src/infer.py::_i18n`, `src/drivers.py::_i18n`, `src/playbook.py::build_actions_i18n`)

ดู glossary แบบละเอียด: `docs/99_glossary_loyalty_101.md`

## เอกสารที่มีในชุดนี้
1) [00_overview.md](00_overview.md) — ภาพรวม end-to-end และสิ่งที่ระบบทำ/ไม่ทำ  
2) [01_repo_map.md](01_repo_map.md) — แผนที่ repo + Evidence Map  
3) [02_data_contracts_inputs.md](02_data_contracts_inputs.md) — สัญญาข้อมูล input (parquet/canonical columns)  
4) [03_data_contracts_outputs.md](03_data_contracts_outputs.md) — สัญญาข้อมูล output (artifacts/predictions/payload)  
5) [04_inference_pipeline.md](04_inference_pipeline.md) — อธิบาย inference flow + guardrails + fallback  
6) [05_api_design.md](05_api_design.md) — แนวทางออกแบบ API + JSON schema + error rules  
7) [06_action_engine_and_i18n.md](06_action_engine_and_i18n.md) — playbook/action engine + i18n end-to-end  
8) [07_diagrams.md](07_diagrams.md) — Mermaid diagrams (context/dataflow/sequence/component/failure)  
9) [08_examples_last4_windows_keys.md](08_examples_last4_windows_keys.md) — dictionary ของ keys ใน `outputs/examples_last4_windows.json` (+ คำอธิบายสำหรับ end user)  
10) [09_platform_display_schema.md](09_platform_display_schema.md) — สเปก “คัดฟีเจอร์สำหรับ platform display” (KEEP/DROP + joinability behavior)  
11) [99_glossary_loyalty_101.md](99_glossary_loyalty_101.md) — glossary สำหรับมือใหม่ + ชี้ตำแหน่งใน pipeline

## วิธีอ่าน (แนะนำ)
1) เริ่มจาก `docs/00_overview.md` เพื่อเข้าใจภาพรวมและภาษากลาง  
2) อ่าน `docs/01_repo_map.md` เพื่อรู้ว่า code อยู่ตรงไหน  
3) ทำ data contracts ด้วย `docs/02_*` และ `docs/03_*`  
4) ลงรายละเอียด infer/guardrails ใน `docs/04_inference_pipeline.md`  
5) ถ้าจะทำ UI/contract สำหรับ platform ให้ดู `docs/09_platform_display_schema.md` และใช้ `docs/08_examples_last4_windows_keys.md` เป็น dictionary อ้างอิง keys  
6) ถ้าต้องเอาไปทำ serving ให้ไป `docs/05_api_design.md` และ `docs/07_diagrams.md`

## Open Questions (ต้องถามทีมเพิ่ม)
- จะนำ pipeline นี้ไป serve แบบ online inference จริงหรือไม่ (ใน repo ยังไม่พบ web service/endpoint) — หลักฐานว่ามีแต่ CLI: `run_pipeline.py::main`, dependency list: `requirements.txt`
- ต้องการ versioning และ contract test ของ schema output อย่างไร (ปัจจุบัน export เป็นไฟล์ใน `outputs/` และ `reports/`) — `src/infer.py::save_predictions`, `run_pipeline.py::main`
