# 02 — Data Contracts (Inputs)

## Purpose
กำหนดสัญญาข้อมูล (data contracts) สำหรับ input ที่ pipeline ใช้จริง: โครงสร้างโฟลเดอร์ `datasets/`, รายชื่อตาราง parquet, คอลัมน์ขั้นต่ำที่ pipeline ต้องมี, constraint ที่สำคัญ, และกติกา joinability ที่มีผลกับ inference/action

**หลักฐานใน repo:** `src/data_load.py::TABLE_FILES`, `run_pipeline.py::main` (ตัวแปร `columns_map`)

## What you will learn
- input tables ที่ pipeline โหลด (ชื่อไฟล์/ชื่อ table)
- column list ขั้นต่ำที่ต้องมีในแต่ละ table สำหรับ pipeline นี้
- กติกา assign `brand_id` ด้วย `app_id` และผลกระทบต่อ ingestion
- สัญญา join key ที่ใช้ตัดสิน `commerce_joinable`

**หลักฐานใน repo:** `src/data_load.py::load_tables`, `run_pipeline.py::BRAND_APP_ID_FILTERS`, `src/data_load.py::build_purchase_item_join_diagnostics`

## Definitions/Glossary
- **dataset_root**: โฟลเดอร์รากที่มี subset หลายโฟลเดอร์อยู่ภายใน (`run_pipeline.py::main` รับ `--dataset-root`)
- **subset folder**: โฟลเดอร์ใต ้`datasets/*` ที่เก็บ parquet แต่ pipeline “ไม่” ผูก brand กับชื่อโฟลเดอร์อย่างเดียว (`src/data_load.py::load_tables`)
- **app_id → brand_id mapping**: กติกาที่ใช้กำหนด `brand_id` ต่อแถวจาก `app_id` (`run_pipeline.py::BRAND_APP_ID_FILTERS`, `src/data_load.py::_app_to_brand_map`)
- **commerce_joinable**: ธงต่อแบรนด์ที่ใช้กำหนดว่าการ join เชิง commerce เชื่อถือได้หรือไม่ (`src/data_load.py::JoinDiagnostics`, `src/data_load.py::build_purchase_item_join_diagnostics`)

## 1) Dataset layout (โครงสร้างโฟลเดอร์)
Pipeline คาดหวังว่า input อยู่ในรูป:

```text
datasets/
  <subset_1>/
    activity_transaction.parquet
    purchase.parquet
    purchase_items.parquet
    user_device.parquet
    user_identity.parquet
    user_info.parquet
    user_view.parquet
    user_visitor.parquet
  <subset_2>/
    ...
```

**หลักฐานใน repo:** `README.md` (หัวข้อ "Dataset layout"), `src/data_load.py::TABLE_FILES`

### ข้อกำหนดสำคัญ: การหาแบรนด์จาก app_id (ไม่ผูกกับชื่อโฟลเดอร์)
เมื่อส่ง `brand_app_ids` เข้า loader, ระบบ assign `brand_id` โดย mapping จาก `app_id` และรองรับเคสที่ใน subset folder เดียวมีหลาย app_id ปนกัน

**หลักฐานใน repo:** `src/data_load.py::load_tables` (docstring), `src/data_load.py::_load_table_multi_subset` (ใส่ `brand_id`), `run_pipeline.py::BRAND_APP_ID_FILTERS`

## 2) รายชื่อ input tables (ไฟล์ parquet ที่ loader สแกน)
รายชื่อไฟล์มาตรฐานถูกกำหนดใน `src/data_load.py::TABLE_FILES`:
- `activity_transaction.parquet`
- `purchase.parquet`
- `purchase_items.parquet`
- `user_device.parquet`
- `user_identity.parquet`
- `user_info.parquet`
- `user_view.parquet`
- `user_visitor.parquet`

**หลักฐานใน repo:** `src/data_load.py::TABLE_FILES`

## 3) Column contracts ขั้นต่ำที่ pipeline ใช้ (per table)
> หมายเหตุ: รายการนี้คือ “ขั้นต่ำ” ที่ `run_pipeline.py` ระบุว่าจะโหลดมาใช้จริงใน pipeline (ผ่าน `columns_map`)

### 3.1 activity_transaction
คอลัมน์ขั้นต่ำ:
- `app_id`, `user_id`, `transaction_id`
- `activity_datetime`, `activity_type`, `activity_name`
- `reward_type`, `is_completed`, `reward`, `points`

**หลักฐานใน repo:** `run_pipeline.py::main` (ตัวแปร `columns_map["activity_transaction"]`), การใช้งานใน segment KPI: `src/segments.py::compute_segment_kpis`, การใช้งานใน feature: `src/features.py::_compute_engagement_features` และ `src/features.py::_compute_activity_features`

### 3.2 purchase
คอลัมน์ขั้นต่ำ:
- `app_id`, `transaction_id`, `user_id`
- `create_datetime`, `paid_datetime`, `transaction_status`
- `itemsold`, `subtotal_amount`, `discount_amount`, `shipping_fee`, `net_amount`

**หลักฐานใน repo:** `run_pipeline.py::main` (`columns_map["purchase"]`), การใช้งานใน feature: `src/features.py::_compute_commerce_features`, การใช้งานใน segments: `src/segments.py::compute_segment_kpis`

### 3.3 purchase_items
คอลัมน์ขั้นต่ำ:
- `app_id`, `transaction_id`, `user_id`
- `create_datetime`, `paid_datetime`, `transaction_status`
- `sku_id`, `quantity`, `price_sell`, `price_discount`, `price_net`
- `delivered`, `is_shiped`

**หลักฐานใน repo:** `run_pipeline.py::main` (`columns_map["purchase_items"]`), การใช้งานใน feature: `src/features.py::_compute_commerce_features` และ SKU mix: `src/features.py::_simulate_sku_mix_features`, การใช้งานใน segments: `src/segments.py::compute_segment_kpis`

### 3.4 user_view / user_visitor (activity enrichment)
คอลัมน์ขั้นต่ำ:
- `user_view`: `app_id`, `user_id`, `join_datetime`, `inactive_datetime`, `user_type`
- `user_visitor`: `app_id`, `tbl_type`, `idsite`, `user_id`, `user_type`, `visit_datetime`, `visit_end_datetime`, `actions`, `interactions`, `searches`, `events`

**หลักฐานใน repo:** `run_pipeline.py::main` (`columns_map["user_view"]`, `columns_map["user_visitor"]`), การใช้งานใน segments (presence enrichment): `src/segments.py::compute_segment_kpis` (พารามิเตอร์ `activity_enrichment_joinable_by_brand`)

### 3.5 user_device / user_identity / user_info
คอลัมน์ขั้นต่ำ:
- `user_device`: `app_id`, `user_id`, `lastaccess`, `device_type`, `os_name`
- `user_identity`: `app_id`, `user_id`, `line_id`, `external_id`
- `user_info`: `app_id`, `user_id`, `dateofbirth`, `gender`

**หลักฐานใน repo:** `run_pipeline.py::main` (`columns_map[...]`), การโหลดผ่าน `src/data_load.py::load_tables` (ไม่ได้บังคับว่าต้องถูกใช้ downstream ทุกคอลัมน์)

## 4) Key constraints ที่ควร enforce (validation ที่ควรมีสำหรับ production)
> Repo มีการ “coerce” หลายจุด แต่ไม่ได้มี schema validator แบบ explicit; ข้อกำหนดนี้เป็น “สิ่งที่ควรตรวจ” เพื่อให้ pipeline เสถียรขึ้น

### 4.1 Datetime parsing
หลายโมดูลแปลงคอลัมน์เวลาเป็น UTC datetime ด้วย `pd.to_datetime(..., utc=True)` และ `errors="coerce"` ดังนั้นค่าที่ parse ไม่ได้จะกลายเป็น NaT และอาจทำให้ window สร้างไม่ได้

**หลักฐานใน repo:** `src/features.py::_to_datetime`, `src/segments.py::_to_datetime`, `src/infer.py::_prepare_inference_frame`, `src/train.py::_prepare_training_frame`

### 4.2 ID normalization สำหรับ join
ระบบ normalize ID ด้วยการ strip/lower และลบ non-alnum (ยกเว้น `-` `_`) เพื่อให้ join robust

**หลักฐานใน repo:** `src/id_utils.py::normalize_id`, การใช้ใน join coverage/diagnostics: `src/data_load.py::validate_join_coverage`, `src/data_load.py::build_purchase_item_join_diagnostics`

## 5) Commerce joinability contract (purchase ↔ purchase_items)
ระบบตัดสิน `commerce_joinable` ต่อแบรนด์โดยใช้:
- key หลัก: `transaction_id` หลัง normalize (`transaction_id_norm`)
- เงื่อนไข: `row_coverage_norm >= 0.80` และช่วงเวลา overlap ระหว่างตาราง

**หลักฐานใน repo:** `src/data_load.py::build_purchase_item_join_diagnostics` (ตัวแปร `joinable = ...`), `src/data_load.py::write_join_diagnostics_markdown` (เขียนสรุป)

## 6) Examples (template; ค่าตัวอย่างเป็น placeholder)
> ตัวอย่างนี้เป็น “template” เพื่อสื่อ schema เท่านั้น (ชื่อฟิลด์มาจาก `run_pipeline.py::main`); ค่าไม่ได้อ้างว่าเป็นค่าจริงจาก dataset

### 6.1 activity_transaction row (template)
```json
{
  "app_id": "838315041537793",
  "user_id": "U123",
  "transaction_id": "T456",
  "activity_datetime": "2026-01-01T00:00:00Z",
  "activity_type": "login",
  "activity_name": "daily_login",
  "reward_type": "points",
  "is_completed": 1,
  "reward": "10_points",
  "points": 10
}
```

**หลักฐานใน repo:** `run_pipeline.py::main` (`columns_map["activity_transaction"]`)

### 6.2 purchase / purchase_items row (template)
```json
{
  "purchase": {
    "app_id": "838315041537793",
    "transaction_id": "T456",
    "user_id": "U123",
    "create_datetime": "2026-01-01T00:00:00Z",
    "paid_datetime": "2026-01-01T01:00:00Z",
    "transaction_status": "success",
    "itemsold": 2,
    "subtotal_amount": 200.0,
    "discount_amount": 20.0,
    "shipping_fee": 0.0,
    "net_amount": 180.0
  },
  "purchase_items": {
    "app_id": "838315041537793",
    "transaction_id": "T456",
    "user_id": "U123",
    "create_datetime": "2026-01-01T00:00:00Z",
    "paid_datetime": "2026-01-01T01:00:00Z",
    "transaction_status": "success",
    "sku_id": "SKU-001",
    "quantity": 1,
    "price_sell": 200.0,
    "price_discount": 20.0,
    "price_net": 180.0,
    "delivered": true,
    "is_shiped": true
  }
}
```

**หลักฐานใน repo:** `run_pipeline.py::main` (`columns_map["purchase"]`, `columns_map["purchase_items"]`)

## Decisions & Implications
- การใช้ `app_id` เป็นตัวตั้งทำให้ ingestion รองรับ mixed subsets แต่ต้องมี governance เรื่อง mapping (`BRAND_APP_ID_FILTERS`) — `run_pipeline.py::BRAND_APP_ID_FILTERS`, `src/data_load.py::load_tables`
- `commerce_joinable` เป็น contract ระดับแบรนด์ที่ส่งผล downstream ทั้ง feature/segments/actions จึงควรถูก persist และอธิบายใน output/metadata — `src/features.py::build_feature_table` (เติม `commerce_joinable`), `src/segments.py::compute_segment_kpis` (มี `commerce_joinable` ใน output)

## Open Questions
- ค่าของ `transaction_status`, `reward_type`, `activity_type` มี vocabulary มาตรฐานจาก upstream อะไร (repo ไม่ได้มี enum/schema กลาง) — ยังไม่พบหลักฐานใน repo นอกเหนือจากการอ่านเป็น string ใน `src/features.py::_compute_commerce_features` และ `src/segments.py::compute_segment_kpis`

