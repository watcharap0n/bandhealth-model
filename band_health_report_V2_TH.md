# Brand Health Report V2 (TH)

เอกสารนี้สรุป logic ล่าสุดของระบบ Brand Health ตามโค้ดปัจจุบัน โดยไม่ต้อง rerun pipeline ใหม่

## 1) เป้าหมายระบบ

ระบบจะพยากรณ์สุขภาพแบรนด์ในระดับ `(brand_id, window_end_date, window_size)`
- window ที่รองรับ: `7d`, `30d`, `60d`, `90d`
- ผลลัพธ์หลัก: `Healthy / Warning / AtRisk`
- พร้อมเหตุผล (`drivers`), กลุ่มเป้าหมาย (`target_segments`), และคำแนะนำเชิงปฏิบัติ (`suggested_actions`)

## 2) Logic การโหลดข้อมูล (สำคัญ)

ใช้ logic ใหม่ใน `src/data_load.py`:
- สแกนทุก subset folder ใต้ `datasets/*`
- แยกแบรนด์ด้วย `app_id` (ไม่อิงชื่อโฟลเดอร์อย่างเดียว)
- รองรับเคสที่ใน folder เดียวมีหลาย app_id ปนกัน

### App Mapping ปัจจุบัน
- `c-vit` -> `1993744540760190`
- `see-chan` -> `838315041537793`

mapping นี้กำหนดใน `run_pipeline.py` (`BRAND_APP_ID_FILTERS`)

## 3) Join Diagnostics และโหมด Commerce

ระบบคำนวณ diagnostics และกำหนด `commerce_joinable` ต่อแบรนด์
- key หลัก: `purchase.transaction_id` ↔ `purchase_items.transaction_id`
- ใช้ค่า normalized coverage + time range overlap
- เกณฑ์ joinable: `row_coverage_norm >= 0.80` และช่วงเวลา overlap

เมื่อ `commerce_joinable=false`:
- ไม่ใช้ attribution/segment เชิง commerce
- ใช้เฉพาะ activity-based attribution/action

เมื่อ `commerce_joinable=true`:
- ใช้ segment attribution/action ได้ทั้ง activity + commerce

## 4) Feature + Label + Model

### Feature Engineering
- Engagement: active/new/returning/recency/session proxy
- Activity funnel: completion/redeem/points/reward efficiency
- Commerce: transaction/GMV/discount/AOV/SKU mix (ตาม joinability)
- RFM distribution + transition
- Trend/Stability: WoW/MoM/z-score/volatility

### Weak Labels
- สร้าง score จากสัญญาณเสื่อมหลาย metric แล้ว map เป็น 3 class
- ใช้ baseline แบบ rolling ต่อแบรนด์

### Model (scikit-learn)
- baseline: `LogisticRegression`
- strong: `HistGradientBoostingClassifier`
- calibration: `CalibratedClassifierCV`
- มี time split + cross-brand holdout + group-by-brand evaluation

## 5) Target Segment Attribution Guardrails

ระบบมี guardrails เพื่อลด segment noise:
- ต้องสอดคล้องทิศทาง driver (up/down)
- ใช้ `delta` เป็นหลักในการคำนวณ contribution
- กันหารศูนย์ (`prev=0`) ด้วย cold-start note
- ตัด segment ที่ share/count ต่ำเกิน threshold
- ตัด commerce segment อัตโนมัติเมื่อแบรนด์ non-joinable

## 6) Output Schema (รองรับ EN/TH)

ระบบยังคง field เดิม และเพิ่ม bilingual field แบบ:

```json
{"en": "...", "th": "..."}
```

### ฟิลด์ i18n สำคัญ
- `predicted_health_class_i18n`
- `predicted_health_statement_i18n`
- `confidence_band_i18n`
- `drivers[*].key_i18n`
- `drivers[*].statement_i18n`
- `drivers[*].direction_i18n`
- `target_segments[*].metric_family_i18n`
- `target_segments[*].segment_label_i18n`
- `target_segments[*].direction_i18n`
- `target_segments[*].reason_statement_i18n`
- `suggested_actions_i18n` (list)

## 7) ตัวอย่าง Payload (ย่อ)

```json
{
  "predicted_health_class": "Warning",
  "predicted_health_class_i18n": {"en": "Warning", "th": "เริ่มน่ากังวล"},
  "predicted_health_statement": "Warning (borderline)",
  "predicted_health_statement_i18n": {"en": "Warning (borderline)", "th": "เริ่มน่ากังวล (ใกล้เส้นแบ่ง)"},
  "confidence_band": "low",
  "confidence_band_i18n": {"en": "low", "th": "ต่ำ"},
  "drivers": [
    {
      "key": "active_down",
      "key_i18n": {"en": "active_down", "th": "ผู้ใช้งานลดลง"},
      "statement": "Active users 30d down -18.0% WoW",
      "statement_i18n": {"en": "Active users 30d down -18.0% WoW", "th": "ผู้ใช้งานที่แอคทีฟ 30d ลดลง -18.0% WoW"}
    }
  ],
  "target_segments": [
    {
      "metric_family": "active_users",
      "metric_family_i18n": {"en": "Active users", "th": "ผู้ใช้งานที่แอคทีฟ"},
      "segment_key": "recently_lapsed_8_14d",
      "segment_label_i18n": {"en": "recently_lapsed_8_14d", "th": "ผู้ใช้ที่เพิ่งหลุด 8-14 วัน"},
      "direction": "down",
      "direction_i18n": {"en": "down", "th": "ลดลง"},
      "reason_statement": "Active users down driven by `recently_lapsed_8_14d` (...)",
      "reason_statement_i18n": {"en": "...", "th": "..."}
    }
  ],
  "suggested_actions": [
    "Trigger winback rewards for `recently_lapsed_8_14d` with short expiry and capped frequency."
  ],
  "suggested_actions_i18n": [
    {
      "en": "Trigger winback rewards for `recently_lapsed_8_14d` with short expiry and capped frequency.",
      "th": "ส่งรางวัล winback ให้เซกเมนต์ `recently_lapsed_8_14d` โดยกำหนดอายุสั้นและคุมความถี่การส่ง"
    }
  ]
}
```

## 8) วิธีรัน

### Full run (train ใหม่)
```bash
python3 run_pipeline.py \
  --dataset-root datasets \
  --reports-dir reports \
  --outputs-dir outputs \
  --artifacts-dir artifacts \
  --snapshot-freq 7D \
  --report-name band_health_report_V2.md
```

### Skip train (ใช้ artifacts เดิม)
```bash
python3 run_pipeline.py \
  --dataset-root datasets \
  --reports-dir reports \
  --outputs-dir outputs \
  --artifacts-dir artifacts \
  --snapshot-freq 7D \
  --report-name band_health_report_V2.md \
  --skip-train
```

## 9) หมายเหตุ

- เอกสารนี้เป็น logic reference ตามโค้ดปัจจุบัน
- หากต้องการตัวเลขผลลัพธ์ล่าสุดในรายงาน ต้องรัน pipeline อีกครั้งบนเครื่อง/คลาวด์
