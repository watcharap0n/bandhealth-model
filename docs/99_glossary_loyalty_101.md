# 99 — Glossary: Loyalty/Brand Health 101 (สำหรับมือใหม่)

## Purpose
รวมคำศัพท์พื้นฐานด้าน loyalty/CRM/commerce ที่เกี่ยวข้องกับ pipeline นี้ พร้อมชี้ “ไปอยู่ตรงไหน” ใน code/output ของ repo เพื่อให้คนเริ่มเข้าใจได้เร็วและสื่อสารกับทีมได้ตรงกัน

**หลักฐานใน repo:** feature columns ใน `artifacts/model_metadata.json`, metric family config ใน `src/driver_mapping.py::METRIC_FAMILY_CONFIG`, segment keys ใน `src/segments.py::SEGMENT_KEYS`

## What you will learn
- ความหมายไทยของคำศัพท์สำคัญ (อังกฤษได้ แต่ต้องเข้าใจไทย)
- ใน pipeline นี้คำเหล่านี้ปรากฏใน feature/output อย่างไร (ชื่อคอลัมน์/metric family/segment key)
- ข้อจำกัด: คำบางคำเป็นแนวคิดทั่วไป แต่ repo อาจไม่ได้คำนวณตรง ๆ (จะระบุว่า “ยังไม่พบหลักฐานใน repo”)

## Definitions/Glossary
> รูปแบบการอ้างอิง: ใช้ `path::symbol` เป็นหลักฐาน

## 1) Active users, DAU/WAU/MAU
### Active users
ความหมาย: จำนวน “ผู้ใช้งานที่มี activity” ใน window นั้น (ระดับแบรนด์)
- ใน repo ปรากฏเป็น feature: `active_users` และอนุพันธ์ เช่น `active_users_wow_pct`, `active_users_zscore`  
- ถูกใช้ทั้งใน weak labeling และ driver extraction

**หลักฐานใน repo:** feature trend: `src/features.py::_add_relative_and_trend_features` (สร้าง `*_wow_pct`/`*_zscore`), label: `src/labeling.py::generate_weak_labels` (อ่าน `active_users_wow_pct`, `active_users_zscore`), drivers: `src/drivers.py::build_metric_drivers`

### DAU/WAU/MAU
ความหมาย: active users ในช่วง 1/7/30 วัน
- ใน repo **ยังไม่พบหลักฐาน** ของการคำนวณ “DAU” โดยตรง (รายวัน)  
- แต่มี concept ใกล้เคียงผ่าน `window_size` เช่น `7d` ~ WAU และ `30d` ~ MAU

**หลักฐานใน repo:** window sizes: `src/features.py::WINDOW_SIZES`, output `window_size` เป็น `"7d"|"30d"|...` ใน `src/segments.py::compute_segment_kpis`

## 2) Dormant / Churn / Lapsed
### Dormant (ผู้ใช้ไม่เคลื่อนไหว)
ความหมาย: ผู้ใช้ที่ไม่ได้มี presence/activity ในช่วงเวลาหนึ่ง
- ใน repo สื่อผ่าน segment keys: `dormant_15_30d`, `dormant_31_60d`, `dormant_60d_plus`
- และ feature `dormant_share` + trend เช่น `dormant_share_wow_pct`

**หลักฐานใน repo:** segment keys: `src/segments.py::ACTIVITY_SEGMENT_KEYS`, การคำนวณ masks: `src/segments.py::compute_segment_kpis` (ตัวแปร `rec_days` และ masks), feature family: `src/driver_mapping.py::METRIC_FAMILY_CONFIG["dormant_share"]`, driver: `src/drivers.py::build_metric_drivers`

### Lapsed / Churn
ความหมายทั่วไป: ผู้ใช้ที่เคย active แต่หยุดมาแล้วช่วงหนึ่ง (เสี่ยง churn)
- ใน repo มี segment “เพิ่งหลุด”: `recently_lapsed_8_14d` ซึ่งจับคนที่ห่างหาย 8–14 วัน (window-based)
- คำว่า “churn” ไม่ถูกคำนวณเป็น metric แยกในโค้ดที่พบ

**หลักฐานใน repo:** segment key: `src/segments.py::ACTIVITY_SEGMENT_KEYS`, การคำนวณ mask: `src/segments.py::compute_segment_kpis`

## 3) Redeem rate / Burn rate
### Redeem rate (อัตราการแลกรับ)
ความหมาย: สัดส่วนของการ “แลก/ใช้สิทธิ” ต่อกิจกรรมทั้งหมด (ใน repo เป็น proxy)
- ใน repo มี feature: `activity_redeem_rate` และ trend: `activity_redeem_rate_wow_pct`
- ใน driver mapping เรียก metric family ว่า `redeem_rate` และ map ไปที่ total_col `activity_redeem_rate`

**หลักฐานใน repo:** feature columns: `artifacts/model_metadata.json` (มี `activity_redeem_rate*`), mapping: `src/driver_mapping.py::METRIC_FAMILY_CONFIG["redeem_rate"]`, drivers: `src/drivers.py::build_metric_drivers` (ตัวแปร `redeem_wow`)

### Burn rate
ความหมายทั่วไป: อัตราการ “เผา/ใช้แต้ม” (points burn) ต่อช่วงเวลา/ฐานผู้ใช้
- ใน repo **ยังไม่พบหลักฐาน** ของ metric ชื่อ burn rate โดยตรง
- ใกล้เคียงที่สุดคือการนับ redeem event proxy (`redeem_count`, `activity_redeem_rate`) ใน segments (`redeem_count`) และ features (`activity_redeem_count`, `activity_redeem_rate`)

**หลักฐานใน repo:** segments redeem proxy: `src/segments.py::compute_segment_kpis` (ตัวแปร `redeem_i` และ `redeem_count`), features redeem fields: `artifacts/model_metadata.json` (มี `activity_redeem_count`, `activity_redeem_rate`)

## 4) GMV / AOV / Frequency
### GMV (Gross Merchandise Value)
ความหมาย: มูลค่าการซื้อรวม (ใน repo ใช้ `gmv_net` เป็นหลัก)
- `gmv_net` ถูกคำนวณจาก `purchase_items` (qty * price_net) เมื่อ joinable หรือ fallback เป็น `purchase.net_amount` เมื่อไม่ joinable

**หลักฐานใน repo:** การคำนวณ: `src/features.py::_compute_commerce_features` (ตัวแปร `use_items_for_value` และ `gmv_net_daily` fallback), mapping: `src/driver_mapping.py::METRIC_FAMILY_CONFIG["gmv_net"]`

### AOV (Average Order Value)
ความหมาย: มูลค่าเฉลี่ยต่อออเดอร์
- ใน repo เป็น feature: `aov_net = gmv_net / transaction_count` (ระดับ window)

**หลักฐานใน repo:** `src/features.py::_compute_commerce_features` (ตั้ง `aov_net_{tag}`), feature columns: `artifacts/model_metadata.json` (มี `aov_net`)

### Frequency
ความหมาย: ความถี่การซื้อ/กิจกรรม
- ใน repo ใกล้เคียงคือ `transaction_count` (จำนวนธุรกรรมใน window) และ `rfm_frequency_mean`

**หลักฐานใน repo:** feature columns: `artifacts/model_metadata.json` (มี `transaction_count`, `rfm_frequency_mean`), mapping: `src/driver_mapping.py::METRIC_FAMILY_CONFIG["transaction_count"]`

## 5) SKU concentration / Pareto
### SKU concentration (ความกระจุกตัวของ SKU)
ความหมาย: ความเสี่ยงที่ยอดขาย/การซื้อพึ่งพา SKU น้อยตัว
- ใน repo มี feature: `sku_top_share` และ driver rule `sku_concentration_high` เมื่อ `sku_top_share >= 0.55`

**หลักฐานใน repo:** feature columns: `artifacts/model_metadata.json` (มี `sku_top_share`), driver rule: `src/drivers.py::build_metric_drivers` (เช็ค `sku_top_share`), mapping: `src/driver_mapping.py::METRIC_FAMILY_CONFIG["sku_concentration"]`

### Pareto
ความหมายทั่วไป: 80/20 (SKU น้อยตัวสร้างยอดมาก)
- ใน repo **ยังไม่พบหลักฐาน** ของการคำนวณ “pareto” โดยตรงเป็น metric/field
- แต่สามารถใช้ `sku_top_share`/`sku_entropy` เป็น proxy เชิง distribution

**หลักฐานใน repo:** `artifacts/model_metadata.json` (มี `sku_top_share`, `sku_entropy`)

## 6) Reward efficiency / Point inflation
### Reward efficiency (ประสิทธิภาพรางวัล)
ความหมาย: “ได้ completion ต่อ point” (ความคุ้มค่าการแจกแต้ม/รางวัล)
- ใน repo นิยาม `reward_efficiency = activity_completed_events / (abs(activity_points_sum) + 1.0)`
- ใช้เป็น feature และมี driver/labeling heuristic ที่เกี่ยวข้อง

**หลักฐานใน repo:** นิยาม feature: `src/features.py::_add_relative_and_trend_features` (ตั้ง `reward_efficiency`), labeling heuristic: `src/labeling.py::generate_weak_labels` (ดู `reward_efficiency_wow_pct` และ `activity_points_per_active_wow_pct`), action text: `src/playbook.py::ACTION_MAP["reward_efficiency"]`

### Point inflation (แต้มเฟ้อ)
ความหมาย: แจกแต้มมากขึ้นแต่ไม่ก่อ engagement/conversion ตามสัดส่วน
- ใน repo **ยังไม่พบหลักฐาน** ของ metric ชื่อ point_inflation โดยตรง
- แต่มีสัญญาณ proxy คือ `activity_points_per_active_wow_pct` เพิ่มขึ้นพร้อม completion ลดลง (ถูกใช้เป็น heuristic ใน weak labels และ driver `efficiency_drop`)

**หลักฐานใน repo:** heuristic: `src/labeling.py::generate_weak_labels` (ตัวแปร `eff_drop`), drivers: `src/drivers.py::build_metric_drivers` (driver `efficiency_drop`), feature columns: `artifacts/model_metadata.json` (มี `activity_points_per_active_wow_pct`)

## 7) เชื่อมกลับไปยัง output จริง
ใน output JSONL (`outputs/predictions_with_drivers.jsonl`) จะเห็นคำศัพท์เหล่านี้ผ่าน:
- `drivers[*].metric_family` (เช่น `active_users`, `gmv_net`, `dormant_share`)
- `target_segments[*].segment_key` (เช่น `recently_lapsed_8_14d`, `dormant_15_30d`)

**หลักฐานใน repo:** payload keys: `src/infer.py::predict_with_drivers` (`_to_payload`), output file: `outputs/predictions_with_drivers.jsonl`

## Decisions & Implications
- คำศัพท์ที่เป็น “conceptual” แต่ไม่ได้คำนวณตรง ๆ (เช่น DAU, Pareto) ควรถูกนิยามให้ชัดในเอกสาร/สัญญาข้อมูลของทีม เพื่อไม่ให้สื่อสารคลาดเคลื่อน  
  **หลักฐานใน repo:** การใช้ window-based aggregation เป็นหลัก: `src/features.py::build_feature_table`

## Open Questions
- ต้องการ standard glossary สำหรับทั้งทีม (รวมชื่อ metric/segment ที่อนุญาต) หรือไม่ — repo ยังไม่พบไฟล์ schema/enum กลางนอกเหนือจาก constants ใน `src/segments.py` และ mapping ใน `src/driver_mapping.py`

