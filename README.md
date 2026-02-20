# Brand Health Pipeline (scikit-learn)

Production-oriented Brand Health modeling + segment targeting pipeline for Marketing Automation.

The pipeline:
- Loads parquet tables from `datasets/*` (all subset folders)
- Splits rows to brand by `app_id`
- Builds brand-window features (7d/30d/60d/90d)
- Generates weak health labels (Healthy/Warning/AtRisk)
- Trains/evaluates scikit-learn models
- Produces predictions, drivers, target segments, and suggested actions

## 1) Project structure

- `run_pipeline.py`: end-to-end pipeline entrypoint
- `src/data_load.py`: loading, validation, join diagnostics
- `src/features.py`: feature engineering
- `src/segments.py`: canonical segment KPIs
- `src/labeling.py`: weak-label generation
- `src/train.py`: sklearn training/evaluation
- `src/infer.py`: inference + attribution + QA guardrails
- `src/drivers.py`: driver extraction
- `src/playbook.py`: action mapping

## 2) Requirements

- Python 3.10+ (recommended 3.11)
- Linux/macOS cloud VM (recommended for large parquet scans)

Install dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install pandas numpy pyarrow scikit-learn joblib
```

## 3) Dataset layout

Expected layout:

```text
datasets/
  c-vit/
    activity_transaction.parquet
    purchase.parquet
    purchase_items.parquet
    user_device.parquet
    user_identity.parquet
    user_info.parquet
    user_view.parquet
    user_visitor.parquet
  see-chan/
    (same files)
  <future-subset>/
    (same files)
```

Important behavior:
- Loader scans **all folders** under `datasets/*`.
- Brand assignment uses `app_id` mapping (not folder name only).
- This allows mixed app_ids in `datasets/see-chan` and future subset folders.

Current app mapping in `run_pipeline.py`:

- `c-vit`: `1993744540760190`
- `see-chan`: `838315041537793`

If you add new brand/app_ids, update `BRAND_APP_ID_FILTERS` in:
- `run_pipeline.py`

## 4) Run pipeline

### 4.1 Full run (with training)

```bash
python3 run_pipeline.py \
  --dataset-root datasets \
  --reports-dir reports \
  --outputs-dir outputs \
  --artifacts-dir artifacts \
  --snapshot-freq 7D \
  --report-name band_health_report_V2.md
```

### 4.2 Reuse existing model artifacts (skip training)

Use when you already have:
- `artifacts/brand_health_model.joblib`
- `artifacts/model_metadata.json`
- `artifacts/feature_importance.json`

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

## 5) What happens during run (0/7 ... 7/7)

1. Build join diagnostics (`purchase` ↔ `purchase_items`, key normalization, time overlap)
2. Profile dataset (schema/missingness/join coverage)
3. Load tables for feature engineering
4. Build brand-window features
5. Build segment KPI table
6. Generate weak labels
7. Train/evaluate (unless `--skip-train`) and run inference/drivers/actions

## 6) Main outputs

### Reports
- `reports/band_health_report_V2.md`: final markdown report
- `reports/pipeline_summary.json`: model + metrics + joinability summary
- `reports/data_profile/table_profile.csv`
- `reports/data_profile/schema_profile.csv`
- `reports/data_profile/join_coverage.csv`

### Diagnostics
- `outputs/join_diagnostics.md`
- `outputs/coverage_notes.md`
- `outputs/profiling_report.md`

### Features/labels
- `outputs/feature_table.parquet`
- `outputs/feature_definitions.csv`
- `outputs/labeled_feature_table.parquet`
- `outputs/segment_kpis.parquet`

### Predictions/attribution
- `outputs/predictions_with_drivers.jsonl`
- `outputs/predictions_with_drivers.csv`
- `outputs/examples_last4_with_segments.json`
- `outputs/examples_before_after_2windows.json`
- `outputs/attribution_qa.json`

## 7) Cloud run checklist

1. Upload code + dataset folders under `datasets/*`.
2. Create virtualenv and install packages.
3. Ensure enough memory/CPU for large parquet scans.
4. Choose mode:
- full train: run command in 4.1
- skip-train: run command in 4.2
5. Download outputs from `reports/` and `outputs/`.

## 8) Troubleshooting

- `--skip-train` fails with missing artifact files:
  - Run full training once, or upload the 3 required files in `artifacts/`.

- Run is heavy / machine freezes:
  - Use a larger cloud instance.
  - First run with `--skip-train` if artifacts already exist.

- Brand split looks wrong:
  - Check `app_id` values in source parquet.
  - Verify `BRAND_APP_ID_FILTERS` in `run_pipeline.py`.
  - Review `outputs/join_diagnostics.md` and `reports/data_profile/table_profile.csv`.
