# 09 — Platform Display Schema (Brand Health)

## Purpose
กำหนด “ชุดข้อมูลสำหรับแสดงผลบน Platform Brand Health” แบบ **Standard profile** เพื่อให้:
- UI/consumer ไม่ต้องรับ feature ทั้งหมดจาก pipeline (ซึ่งมีจำนวนมาก โดยเฉพาะ `seg_*`)
- schema มีขนาดเหมาะสม แต่ยังคงข้อมูลที่จำเป็นต่อการ “อธิบายผล” (drivers/segments/actions) และการทำ UX เช่น warning เมื่อ `commerce_joinable=false`

เอกสารนี้เป็น “ข้อเสนอแนะการคัด (projection)” จาก output ของ pipeline โดยยึด **source-of-truth** จาก contract payload ใน `src/infer.py::predict_with_drivers` (ฟังก์ชันย่อย `_to_payload`) และ key KPI ที่ถูกใช้ใน rules/labeling

**หลักฐานใน repo:** payload contract `src/infer.py::predict_with_drivers` (ฟังก์ชันย่อย `_to_payload`), rule-based drivers `src/drivers.py::build_metric_drivers`, weak labeling `src/labeling.py::generate_weak_labels`, ตัวอย่าง output ที่ keys บวม `outputs/examples_last4_windows.json`

## What you will learn
- “แสดงผล” ควรยึด field ชุดไหนเป็นหลัก (payload vs KPI)
- KPI ขั้นต่ำที่ควรแสดงเพื่อสอดคล้องกับ driver rules และ weak labels
- กติกา DROP แบบ pattern (เช่น `seg_*`, `label_health_*`, `activity_type_*`) เพื่อทำ schema ให้เสถียร
- behavior ที่ต้องสะท้อนใน UI เมื่อ `commerce_joinable=false` (skip commerce segments/actions)

**หลักฐานใน repo:** guardrail/skip commerce `src/infer.py::_build_target_segments_for_row`, action skip + fallback `src/playbook.py::map_drivers_to_actions`, กลุ่ม commerce metric families `src/driver_mapping.py::COMMERCE_METRIC_FAMILIES`

## Definitions/Glossary
> glossary พื้นฐานเพิ่มเติมดู `docs/99_glossary_loyalty_101.md`

- **Platform Display Output**: output สำหรับ UI/consumer (ไม่จำเป็นต้องเท่ากับ feature vector ของโมเดล) — อิงจาก output ของ `src/infer.py::predict_with_drivers`
- **payload**: ก้อนผลลัพธ์ที่ export ออกมาเพื่อ consumption (class/score/confidence/probabilities + explainability) — `src/infer.py::predict_with_drivers` (ฟังก์ชันย่อย `_to_payload`)
- **drivers**: เหตุผล/สัญญาณที่สรุปสาเหตุของการเปลี่ยนแปลง KPI (rule-based + model importance) — `src/drivers.py::build_drivers`, `src/drivers.py::attach_drivers`
- **target_segments**: กลุ่มผู้ใช้เป้าหมายที่เชื่อมกับ drivers แล้วผ่าน guardrails — `src/infer.py::_build_target_segments_for_row`, `src/infer.py::_validate_target_segments_row`
- **suggested_actions**: ข้อเสนอการกระทำที่ map จาก drivers/segments พร้อม fallback — `src/playbook.py::map_drivers_to_actions`
- **joinability**: ความพร้อมของข้อมูลบางชุด (เช่น commerce join) ที่กระทบการทำ segment/action — `src/segments.py::compute_segment_kpis`, `src/infer.py::_build_target_segments_for_row`, `src/playbook.py::map_drivers_to_actions`
- **WoW** (Week-over-Week): เปรียบเทียบกับ window ก่อนหน้า (ใน repo ใช้กับ window ที่เลื่อนทีละ `snapshot_freq`) — `src/features.py::_add_relative_and_trend_features`
- **z-score**: ค่ามาตรฐานเทียบ baseline rolling เพื่อจับ “ความผิดปกติ” — `src/features.py::_add_relative_and_trend_features`

---

## 1) เป้าหมายของ “Platform Display Output”
### 1.1 แยก “สิ่งที่โมเดลทำนาย” vs “สิ่งที่ UI ต้องโชว์”
ใน `src/infer.py::predict_with_drivers` pipeline จะผลิตทั้ง:
- **ผลทำนาย/คำอธิบาย (payload)**: ใช้เป็น contract หลักสำหรับ platform เพราะรวม class/score/confidence/probabilities + drivers/segments/actions ไว้แล้ว — `src/infer.py::predict_with_drivers` (ฟังก์ชันย่อย `_to_payload`)
- **ฟีเจอร์/KPI จำนวนมาก**: รวม `seg_*` (segment KPI + deltas) ที่มีจำนวนมากและไม่เหมาะกับ default UI contract — ที่มาของ `seg_*`: `src/segments.py::compute_segment_kpis`, deltas: `src/infer.py::_add_segment_deltas`

### 1.2 หลักคิดการ “คัด (projection)”
Standard profile แนะนำให้ platform แสดง:
1) payload เป็น “แกนกลาง” (เพื่ออธิบายผล)
2) KPI หลักที่สอดคล้องกับ driver rules + weak labels (เพื่อให้ผู้ใช้เห็นตัวเลขประกอบคำอธิบาย)
3) joinability flags (เพื่อทำ UI messaging/disable commerce drilldown เมื่อ join ไม่ได้)

**หลักฐานใน repo:** drivers ใช้ KPI บางชุดแบบ explicit `src/drivers.py::build_metric_drivers`, weak labels ใช้ KPI ชุดเดียวกัน `src/labeling.py::generate_weak_labels`

---

## 2) Standard Display Schema (แนะนำ)
> รูปแบบด้านล่างเป็น “ข้อเสนอแนะ” เพื่อให้ schema เล็กและเสถียร โดยยังโยงกับ contract ที่มีอยู่จริงใน repo

### 2.1 Schema: `PlatformBrandHealthDisplayRowV1` (flat + `kpis` bundle)
แนะนำให้ 1 แถวประกอบด้วย:

**Identifiers**
- `brand_id`
- `window_end_date`
- `window_size`

**Joinability (ต้อง normalize ให้เหลือ field เดียว)**
- `commerce_joinable` (boolean/0-1)
- `activity_enrichment_joinable` (boolean/0-1)

> หมายเหตุ: ใน `outputs/examples_last4_windows.json` พบ `commerce_joinable_x` และ `commerce_joinable_y` (schema drift จากการ merge ที่มีคอลัมน์ชื่อซ้ำ) — `outputs/examples_last4_windows.json`, จุด merge: `src/infer.py::predict_with_drivers`

**Health prediction / confidence**
- `predicted_health_class`
- `predicted_health_score`
- `predicted_health_statement`
- `confidence_band`
- `probabilities` (dict: `{class_label: probability}`) — `src/infer.py::predict_with_drivers` (ฟังก์ชันย่อย `_to_payload`)

> หมายเหตุ: ใน DataFrame output จะมีคอลัมน์ `prob_*` (เช่น `prob_Healthy`) แต่ payload contract จะบรรจุเป็น dict ที่ชื่อ `probabilities` — `src/infer.py::predict_with_drivers`

**Explainability bundle (ควรยึดจาก payload contract)**
- `drivers`
- `target_segments`
- `suggested_actions`
- `attribution_warnings`

**KPI bundle (แนะนำให้จัดกลุ่มเป็น object เพื่อไม่ทำให้ schema root บวม)**
- `kpis`: object ที่มี key ตาม “KEEP LIST” ในหัวข้อ 2.2

> หมายเหตุ: ในไฟล์ตัวอย่าง `outputs/examples_last4_windows.json` KPI จะอยู่เป็นคอลัมน์ top-level (เพราะเป็น DataFrame export) แต่สำหรับ platform contract แนะนำให้จัดกลุ่มเป็น `kpis` เพื่อให้อ่านง่าย/ทำ versioning ง่ายขึ้น (เป็นข้อเสนอแนะ; ยังไม่พบ schema มาตรฐานใน repo)

> i18n: โค้ดปัจจุบันมีการสร้าง `*_i18n` หลาย field ใน payload แต่ artifact ใน `outputs/examples_last4_windows.json` ยังไม่แสดง field เหล่านี้ (แนะนำ regenerate output หากต้องใช้ i18n เป็น contract) — `src/infer.py::predict_with_drivers` (ส่วน `predicted_health_class_i18n`, `confidence_band_i18n`, `predicted_health_statement_i18n`, `suggested_actions_i18n`), `outputs/examples_last4_windows.json`

### 2.2 KPI “ขั้นต่ำ” สำหรับ dashboard (KEEP LIST)
KPI ต่อไปนี้ถูกใช้ใน driver rules และ/หรือ weak labeling จึงแนะนำให้แสดงใน UI เพื่อให้สอดคล้องกับเหตุผล/คะแนน:

**Engagement / Active**
- `active_users`, `active_users_wow_pct`, `active_users_zscore` — drivers: `src/drivers.py::build_metric_drivers`, labels: `src/labeling.py::generate_weak_labels`

**Activity conversion**
- `activity_completion_rate`, `activity_completion_rate_wow_pct`, `activity_completion_rate_zscore` — drivers: `src/drivers.py::build_metric_drivers`, labels: `src/labeling.py::generate_weak_labels`

**Dormancy**
- `dormant_share`, `dormant_share_wow_pct`, `dormant_share_zscore` — drivers: `src/drivers.py::build_metric_drivers`, labels: `src/labeling.py::generate_weak_labels`

**Reward pressure / efficiency**
- `reward_efficiency`, `reward_efficiency_wow_pct`, `reward_efficiency_zscore` — drivers: `src/drivers.py::build_metric_drivers`, labels: `src/labeling.py::generate_weak_labels`
- `activity_points_per_active`, `activity_points_per_active_wow_pct` — drivers: `src/drivers.py::build_metric_drivers`, labels: `src/labeling.py::generate_weak_labels`

**Redemption (proxy)**
- `activity_redeem_rate`, `activity_redeem_rate_wow_pct`, `activity_redeem_rate_zscore` — drivers: `src/drivers.py::build_metric_drivers`, trends: `src/features.py::_add_relative_and_trend_features`

**Commerce (แสดงเมื่อ `commerce_joinable=true`)**
- `gmv_net`, `gmv_net_wow_pct`, `gmv_net_zscore` — drivers: `src/drivers.py::build_metric_drivers`, labels: `src/labeling.py::generate_weak_labels`
- `transaction_count`, `transaction_count_wow_pct`, `transaction_count_zscore` — drivers: `src/drivers.py::build_metric_drivers`, labels: `src/labeling.py::generate_weak_labels`
- `sku_top_share` — driver rule: `src/drivers.py::build_metric_drivers`

> หมายเหตุ: “แสดงเมื่อ joinable” เพราะระบบมีการ block การสร้าง target segments/actions ฝั่ง commerce เมื่อ `commerce_joinable=false` — `src/infer.py::_build_target_segments_for_row`, `src/playbook.py::map_drivers_to_actions`, และนิยามกลุ่ม commerce families `src/driver_mapping.py::COMMERCE_METRIC_FAMILIES`

---

## 3) DROP RULES/PATTERNS (ตัดจาก default display)
> ด้านล่างเป็น “กติกาเชิง pattern” เพื่อทำ schema ให้เล็กและเสถียร โดยยังสามารถเก็บไว้สำหรับ debug/advanced drilldown ได้

1) **ตัดทุก key ที่ขึ้นต้น `seg_`**  
เหตุผล: เป็น internal/verbose (segment KPI + deltas) และ platform สามารถใช้ `payload.target_segments` สำหรับเล่าเหตุผลเชิง segment ได้ในรูปแบบที่เล็กกว่า  
**หลักฐานใน repo:** ที่มาของ `seg_*` `src/segments.py::compute_segment_kpis`, การเพิ่ม `_prev/_delta/_wow_pct/_cold_start_increase` `src/infer.py::_add_segment_deltas`, และ `payload.target_segments` `src/infer.py::predict_with_drivers` (ฟังก์ชันย่อย `_to_payload`)

2) **ตัด `label_health_*`**  
เหตุผล: เป็น weak labels สำหรับ train/eval ไม่ใช่ค่าที่ควรโชว์ end user  
**หลักฐานใน repo:** `src/labeling.py::generate_weak_labels` (สร้าง `label_health_score`, `label_health_class`, `label_health_class_int`)

3) **ตัด scaffolding/debug deltas ใน default UI**  
เช่น `*_prev_window`, `*_delta_window`, `*_wow_pct_window` และ `seg_*_(prev|delta|wow_pct|cold_start_increase)`  
**หลักฐานใน repo:** total deltas `src/infer.py::_add_total_metric_deltas`, segment deltas `src/infer.py::_add_segment_deltas`

4) **ตัด keys ที่เป็น data-dependent/high-cardinality**  
เช่น `activity_type_*` และ `status_*` เพราะถูกสร้างจาก top-N ที่ “ขึ้นกับ dataset” ทำให้ contract ไม่เสถียรข้ามแบรนด์/ช่วงเวลา  
**หลักฐานใน repo:** activity top-6 types `src/features.py::_compute_activity_features`, commerce top-4 status buckets `src/features.py::_compute_commerce_features`

5) **ตัด RFM distribution keys (`rfm_*`) จาก default display**  
เหตุผล: เป็น advanced analytics และมี keys จำนวนมาก (tier distributions) เหมาะกับ drilldown มากกว่า summary  
**หลักฐานใน repo:** `src/features.py::_compute_rfm_features`

---

## 4) Behavior เมื่อ `commerce_joinable=false` (ต้องสะท้อนใน UI)
### 4.1 ผลต่อ target segments
เมื่อ `commerce_joinable=false` ระบบจะ **ไม่สร้าง** target segments สำหรับ metric families ฝั่ง commerce และใส่ warning `commerce_mode_block:<metric_family>`  
**หลักฐานใน repo:** `src/infer.py::_build_target_segments_for_row`, families: `src/driver_mapping.py::COMMERCE_METRIC_FAMILIES`

### 4.2 ผลต่อ suggested actions
เมื่อ `commerce_joinable=false` ระบบจะ **skip** actions ที่ map จาก commerce metric families/segments และถ้าไม่เหลือ action เลยจะ fallback เป็น generic actions  
**หลักฐานใน repo:** `src/playbook.py::map_drivers_to_actions`

---

## 5) Examples (อย่างน้อย 2 แบบ)
> ตัวอย่างด้านล่างเป็น “ตัวอย่าง schema สำหรับ platform display” (ข้อเสนอแนะ) โดยอ้างอิง field ที่มีอยู่จริงใน output/payload ของ repo

### 5.1 Example A — `commerce_joinable=true`
```json
{
  "schema_version": "platform_display_v1",
  "brand_id": "see-chan",
  "window_end_date": "2026-01-07T00:00:00.000Z",
  "window_size": "30d",
  "commerce_joinable": true,
  "activity_enrichment_joinable": true,
  "predicted_health_class": "Healthy",
  "predicted_health_statement": "Healthy (borderline)",
  "predicted_health_score": 68.3058572321,
  "confidence_band": "low",
  "probabilities": {
    "Healthy": 0.5483234712,
    "Warning": 0.2189951887,
    "AtRisk": 0.2326813401
  },
  "drivers": [
    {
      "key": "active_down",
      "statement": "Active users 30d down -12.7% WoW",
      "severity": 0.1273109924,
      "direction": "down",
      "metric_family": "active_users",
      "metrics": {
        "active_users_wow_pct": -0.1273109924
      }
    }
  ],
  "target_segments": [
    {
      "metric_family": "active_users",
      "segment_key": "dormant_31_60d",
      "direction": "down",
      "contribution_share": 0.37184218,
      "reason_statement": "Active users down driven by `dormant_31_60d` (driver: Active users 30d down -12.7% WoW)",
      "evidence_metrics": {
        "delta_seg": -9170.0,
        "delta_total": -3932.0,
        "segment_share_now": 0.1444053309,
        "segment_count_now": 24531,
        "wow_pct_seg": -0.2720987508,
        "cold_start_increase": false,
        "note": "stable"
      },
      "segment_confidence": "high"
    }
  ],
  "suggested_actions": [
    "Trigger winback rewards for `dormant_31_60d` with short expiry and capped frequency.",
    "Set reactivation journeys for `dormant_31_60d` using low-friction missions first.",
    "Boost first-7-day activation missions for `new_users_0_7d` with immediate low-friction rewards."
  ],
  "attribution_warnings": [],
  "kpis": {
    "active_users": 26953.0,
    "active_users_wow_pct": -0.1273109924,
    "active_users_zscore": -1.4197443642,
    "activity_completion_rate": 0.0,
    "activity_completion_rate_wow_pct": 0.0,
    "activity_completion_rate_zscore": -0.5353041555,
    "dormant_share": 0.8413325406,
    "dormant_share_wow_pct": 0.0286450664,
    "dormant_share_zscore": 1.3129149663,
    "reward_efficiency": 0.0,
    "reward_efficiency_wow_pct": 0.0,
    "reward_efficiency_zscore": -0.5400617249,
    "activity_points_per_active": 0.0,
    "activity_points_per_active_wow_pct": 0.0,
    "activity_redeem_rate": 0.0,
    "activity_redeem_rate_wow_pct": 0.0,
    "activity_redeem_rate_zscore": 0.0,
    "gmv_net": 58712551.0,
    "gmv_net_wow_pct": 0.0287425648,
    "gmv_net_zscore": -1.0689718583,
    "transaction_count": 122633.0,
    "transaction_count_wow_pct": 0.0035433715,
    "transaction_count_zscore": 0.7637052525,
    "sku_top_share": 0.1491117179
  }
}
```

**หลักฐานใน repo:** ตัวอย่างนี้ดึงจาก `outputs/examples_last4_windows.json` (แสดงเฉพาะ driver/segment รายการแรกเพื่อความกระชับ), และ contract ของ fields มาจาก `src/infer.py::predict_with_drivers` (payload `_to_payload`), KPI trends มาจาก `src/features.py::_add_relative_and_trend_features`

### 5.2 Example B — `commerce_joinable=false` และต้องเห็นการ skip
แนวทางแสดงผล:
- ซ่อน/แสดงเป็น N/A สำหรับ KPI commerce (`gmv_net`, `transaction_count`, `sku_top_share`)
- คาดหวังว่าฝั่ง explainability จะไม่มี commerce segments/actions และมี warning `commerce_mode_block:*`

```json
{
  "schema_version": "platform_display_v1",
  "brand_id": "c-vit",
  "window_end_date": "2021-09-05 00:00:00+00:00",
  "window_size": "30d",
  "commerce_joinable": false,
  "predicted_health_class": "Healthy",
  "predicted_health_statement": "Healthy",
  "predicted_health_score": 87.19499941555134,
  "confidence_band": "high",
  "probabilities": {
    "AtRisk": 0.0035610564168510953,
    "Healthy": 0.9106545463380376,
    "Warning": 0.08578439724511129
  },
  "drivers": [
    {
      "key": "model_gmv_net_zscore",
      "statement": "Model-highlighted anomaly: gmv_net_zscore=2.47 (up vs baseline)",
      "severity": 0.022064255923666246,
      "direction": "up",
      "metric_family": "gmv_net",
      "metrics": {
        "gmv_net_zscore": 2.4748737341529163,
        "importance": 0.008915305705977058
      }
    }
  ],
  "target_segments": [],
  "suggested_actions": [
    "Monitor the next 1-2 windows and trigger targeted interventions for the weakest KPI.",
    "Run controlled experiments on campaign cadence and reward mix.",
    "Review segment-level funnel drops and prioritize high-impact fixes."
  ],
  "attribution_warnings": [
    "commerce_mode_block:gmv_net",
    "commerce_mode_block:transaction_count"
  ]
}
```

**หลักฐานใน repo:** ตัวอย่างนี้ดึงจาก `outputs/predictions_with_drivers.jsonl` (payload ที่มี `attribution_warnings` เป็น `commerce_mode_block:*`), warning มาจาก `src/infer.py::_build_target_segments_for_row`, การ skip action + fallback generic จาก `src/playbook.py::map_drivers_to_actions`, กลุ่ม commerce families จาก `src/driver_mapping.py::COMMERCE_METRIC_FAMILIES`

> หมายเหตุ: ใน payload ตัวอย่างไม่ได้ส่ง `commerce_joinable` ตรงๆ แต่การมี `commerce_mode_block:*` เป็นผลจากเงื่อนไข `not commerce_joinable` ใน `_build_target_segments_for_row` (จึงตีความเพื่อ UX ได้ว่า join ไม่ได้) — `src/infer.py::_build_target_segments_for_row`

---

## Decisions & Implications
- **ใช้ payload เป็นแกนกลางของ contract**: ลดการพึ่งพา `seg_*` และทำให้ explainability ส่งต่อได้ชัดเจน — `src/infer.py::predict_with_drivers` (ฟังก์ชันย่อย `_to_payload`)
- **คัด KPI ตามสิ่งที่ระบบใช้จริง**: ทำให้ UI สอดคล้องกับ driver rules/labels ไม่เล่า “คนละเรื่อง” กับที่โมเดล/heuristic ใช้ — `src/drivers.py::build_metric_drivers`, `src/labeling.py::generate_weak_labels`
- **ต้อง normalize joinability fields**: เพื่อเลี่ยง schema drift (`commerce_joinable_x/y`) และเพื่อให้ guardrails/actions ทำงานแบบ deterministic ใน serving — merge ใน `src/infer.py::predict_with_drivers`, การอ่าน `commerce_joinable` ใน `src/infer.py::_build_target_segments_for_row` และ `src/playbook.py::map_drivers_to_actions`

## Open Questions
- จะยึด contract ของ `payload` จาก “โค้ดปัจจุบัน” หรือ “artifact ที่ถูกเขียนไว้ใน outputs” (ปัจจุบันพบ drift เรื่อง i18n fields) — `src/infer.py::predict_with_drivers`, `outputs/examples_last4_windows.json`
- KPI commerce ควรถูก “ซ่อน” หรือ “แสดงเป็น N/A พร้อมคำอธิบาย” เมื่อ `commerce_joinable=false` (เป็น decision UX; ยังไม่พบหลักฐานใน repo)
