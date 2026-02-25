# 06 — Action Engine & i18n (Drivers → Segments → Actions)

## Purpose
อธิบาย “action engine” และการทำ i18n (EN/TH) แบบ end-to-end ตั้งแต่:
drivers → metric_family mapping → target_segments guardrails → action selection → action i18n

**หลักฐานใน repo:** `src/drivers.py::attach_drivers`, `src/driver_mapping.py::infer_metric_family_from_key`, `src/infer.py::_build_target_segments_for_row`, `src/playbook.py::map_drivers_to_actions`

## What you will learn
- โครงสร้าง driver/segment/action ที่ส่งออก
- mapping rules ที่ใช้ควบคุม attribution/action
- กติกา commerce_joinable ที่ทำให้ action/segment บางกลุ่มถูก skip
- i18n contract และจุดที่เติม i18n ให้แต่ละ field

**หลักฐานใน repo:** `src/infer.py::predict_with_drivers`, `src/playbook.py::build_actions_i18n`, `src/drivers.py::_add_driver`

## Definitions/Glossary
- **metric_family**: กลุ่ม KPI ที่ driver/segment/action อ้างถึง เช่น `active_users`, `gmv_net` (`src/driver_mapping.py::DRIVER_METRIC_MAP`)
- **ACTION_MAP**: mapping metric_family → list ของข้อความแนะนำ (EN) (`src/playbook.py::ACTION_MAP`)
- **COMMERCE_SEGMENT_KEYS**: เซกเมนต์เชิง commerce เช่น buyers/repeat_buyers (`src/playbook.py::COMMERCE_SEGMENT_KEYS`, `src/segments.py::COMMERCE_SEGMENT_KEYS`)
- **i18n field**: dict `{"en": "...", "th": "..."}` (`src/infer.py::_i18n`, `src/drivers.py::_i18n`, `src/playbook.py::_action_to_i18n`)

## 1) Drivers → metric_family mapping
### 1.1 canonical_driver_key
ระบบ normalize alias ของ driver key (เช่น `completion_down` → `completion_drop`)

**หลักฐานใน repo:** `src/driver_mapping.py::canonical_driver_key`, `src/driver_mapping.py::DRIVER_ALIAS_MAP`

### 1.2 infer_metric_family_from_key
ระบบ map driver key ไป metric_family ด้วย:
- strict map (`DRIVER_METRIC_MAP`)
- fallback rules สำหรับ key ที่ขึ้นต้นด้วย `model_`

**หลักฐานใน repo:** `src/driver_mapping.py::infer_metric_family_from_key`

## 2) Target segments: การเลือก candidate segments
การเลือก candidate segment ขึ้นกับ metric_family:
- ถ้า metric_family เป็น commerce (`gmv_net`, `transaction_count`, `sku_concentration`) จะใช้ `COMMERCE_SEGMENT_KEYS` เมื่อ `commerce_joinable=true` ไม่เช่นนั้นไม่มี candidate  
- ถ้าไม่ใช่ commerce จะใช้ `ACTIVITY_SEGMENT_KEYS`

**หลักฐานใน repo:** `src/infer.py::_segment_candidates`, `src/driver_mapping.py::COMMERCE_METRIC_FAMILIES`, `src/segments.py::ACTIVITY_SEGMENT_KEYS`, `src/segments.py::COMMERCE_SEGMENT_KEYS`

## 3) Action engine: map_drivers_to_actions
### 3.1 ลำดับความสำคัญ (priority)
`map_drivers_to_actions` ทำงานโดย:
1) ใช้ target_segments ก่อน เพื่อสร้าง segment-specific actions  
2) ถ้ายังไม่ครบ `top_n` จะเติมจาก `ACTION_MAP` ตาม metric_family/driver key  
3) ถ้ายังว่าง จะใช้ fallback generic actions

**หลักฐานใน repo:** `src/playbook.py::map_drivers_to_actions`, `src/playbook.py::_segment_specific_actions`, `src/playbook.py::ACTION_MAP`

### 3.2 Commerce gating (skip บางประเภทเมื่อ non-joinable)
ถ้า `commerce_joinable=false`:
- จะ skip segment ที่อยู่ใน `COMMERCE_SEGMENT_KEYS`
- จะ skip action ที่มาจาก metric families `gmv_net/transaction_count/sku_concentration`

**หลักฐานใน repo:** `src/playbook.py::map_drivers_to_actions` (เงื่อนไข `if not commerce_joinable ... continue`), `src/playbook.py::COMMERCE_SEGMENT_KEYS`

## 4) i18n contract (EN/TH)
### 4.1 i18n สำหรับ prediction fields
เติมโดย `src/infer.py` เช่น:
- `predicted_health_class_i18n` (map ด้วย `HEALTH_CLASS_TH`)
- `confidence_band_i18n` (map ด้วย `CONFIDENCE_BAND_TH`)
- `predicted_health_statement_i18n` (รองรับ borderline)

**หลักฐานใน repo:** `src/infer.py::_health_class_i18n`, `src/infer.py::_confidence_band_i18n`, `src/infer.py::_health_statement_i18n`

### 4.2 i18n สำหรับ drivers
Driver มี:
- `key_i18n` โดย map จาก `DRIVER_KEY_TH`
- `statement_i18n` (EN/TH)
- `direction_i18n`

**หลักฐานใน repo:** `src/drivers.py::DRIVER_KEY_TH`, `src/drivers.py::_add_driver`

### 4.3 i18n สำหรับ target_segments
Segment ใช้:
- `SEGMENT_LABEL_TH` สำหรับ label ของ `segment_key`
- `METRIC_FAMILY_LABELS` สำหรับ label ของ metric_family
- `reason_statement_i18n` ถูกสร้างจาก template ที่มีทั้ง EN/TH

**หลักฐานใน repo:** `src/infer.py::SEGMENT_LABEL_TH`, `src/infer.py::METRIC_FAMILY_LABELS`, `src/infer.py::_build_target_segments_for_row`

### 4.4 i18n สำหรับ actions
`build_actions_i18n` คืน list ของ `{"en": "...", "th": "..."}` โดย:
- map แบบ fixed dictionary (`ACTION_TH_MAP`) และ/หรือ regex patterns สำหรับ action ที่มี segment key embed อยู่ใน string

**หลักฐานใน repo:** `src/playbook.py::ACTION_TH_MAP`, `src/playbook.py::_action_to_i18n`, `src/playbook.py::build_actions_i18n`

## 5) Examples
### 5.1 ตัวอย่าง behavior: commerce_mode_block → actions fallback
มี output จริงที่แสดง `commerce_mode_block:*` ใน `attribution_warnings` และ actions เป็น generic fallback ใน `outputs/predictions_with_drivers.jsonl`

**หลักฐานใน repo:** `outputs/predictions_with_drivers.jsonl`, `src/infer.py::_build_target_segments_for_row`, `src/playbook.py::map_drivers_to_actions`

## Decisions & Implications
- action strings ใน repo เป็น “ข้อความ human-readable” ไม่ใช่ structured action object; ถ้าจะ integrate กับ marketing automation system จริง อาจต้องทำ mapping เป็น action_id + parameters (ข้อเสนอแนะ)  
  **หลักฐานใน repo:** `src/playbook.py::ACTION_MAP` (เป็น list ของ string), `src/playbook.py::_segment_specific_actions` (สร้าง string ด้วย template)

## Open Questions
- ต้องการ policy เรื่องการแปล (TH) ว่าควรเป็น fixed dictionary หรือให้รองรับ locale อื่น ๆ เพิ่ม (repo มี EN/TH เท่านั้น) — `src/infer.py::_i18n`, `src/playbook.py::ACTION_TH_MAP`

