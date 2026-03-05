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
- `run_pipeline_hops.py`: hop-based pipeline entrypoint for stage-by-stage execution
- `notebooks/databricks/`: Azure Databricks + MLflow notebook pack
- `src/data_load.py`: loading, validation, join diagnostics
- `src/features.py`: feature engineering
- `src/segments.py`: canonical segment KPIs
- `src/labeling.py`: weak-label generation
- `src/train.py`: sklearn training/evaluation
- `src/infer.py`: inference + attribution + QA guardrails
- `src/memory_opt.py`: memory optimization helpers (dtype downcast, chunked parquet, RSS logging)
- `src/pipeline_checkpoints.py`: run-id checkpoint manager for hop pipeline state
- `src/drivers.py`: driver extraction
- `src/playbook.py`: action mapping

## 2) Requirements

### Option A: Docker (Recommended for Production)

- Docker Engine 20.10+ or Docker Desktop
- Docker Compose 2.0+ (optional)
- 8GB+ RAM available

See [DOCKER_GUIDE.md](DOCKER_GUIDE.md) for complete Docker usage instructions.

Quick start with Docker:
```bash
# Build and run with Docker Compose
docker-compose up

# Or use Makefile shortcuts
make build
make run
```

### Option B: Local Python Environment

- Python 3.10+ (recommended 3.11)
- Linux/macOS cloud VM (recommended for large parquet scans)

Install dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
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

Full CLI argument table and usage examples are documented in:
- `RUN_PIPELINE_INSTRUCTIONS.md`

Databricks notebook instructions are documented in:
- `notebooks/databricks/README.md`

### 4.1 Quick sampling mode (fastest, Mac-safe)

```bash
python3 run_pipeline.py \
  --dataset-root datasets \
  --reports-dir reports \
  --outputs-dir outputs \
  --artifacts-dir artifacts \
  --snapshot-freq 7D \
  --report-name band_health_report_V2.md \
  --train_sample_mode quick \
  --train_sample_frac 0.02 \
  --train_max_train_rows 200000 \
  --train_max_eval_rows 60000 \
  --train_stratify_cols "brand_id,predicted_health_class" \
  --train_group_col "brand_id" \
  --train_weight_classes true \
  --n_jobs 4
```

Notes:
- If `--report-name` is omitted, default report file is `reports/brand_health_report.md`.
- Use `--report-name band_health_report_V2.md` when you want the V2 report filename.

### 4.2 Smart sampling mode (time-aware + cluster-aware)

```bash
python3 run_pipeline.py \
  --dataset-root datasets \
  --reports-dir reports \
  --outputs-dir outputs \
  --artifacts-dir artifacts \
  --snapshot-freq 7D \
  --report-name band_health_report_V2.md \
  --train_sample_mode smart \
  --train_recent_days 180 \
  --train_max_train_rows 200000 \
  --train_max_eval_rows 60000 \
  --train_stratify_cols "brand_id,predicted_health_class" \
  --train_group_col "brand_id" \
  --train_weight_classes true \
  --n_jobs 4
```

### 4.3 Real production run (full data, no sampling)

```bash
python3 run_pipeline.py \
  --dataset-root datasets \
  --reports-dir reports \
  --outputs-dir outputs \
  --artifacts-dir artifacts \
  --snapshot-freq 7D \
  --report-name band_health_report_V2.md \
  --train_sample_mode off \
  --n_jobs 4
```

### 4.4 Reuse existing model artifacts (skip training)

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

### 4.5 Hop-based execution (manual / Databricks Jobs)

Use this mode when you want to run or rerun only specific stages without rerunning the whole flow.

Command format:

```bash
python3 run_pipeline_hops.py <hop> \
  --run-id <RUN_ID> \
  [--auto-upstream] \
  [--force] \
  --dataset-root datasets \
  --reports-dir reports \
  --outputs-dir outputs \
  --artifacts-dir artifacts \
  --snapshot-freq 7D
```

Available hops:
- `load_tables`
- `join_diagnostics`
- `profile`
- `features`
- `segments`
- `labels`
- `train`
- `infer`
- `publish`

Examples:

```bash
# Run one hop (strict mode: dependencies must already be completed)
python3 run_pipeline_hops.py features --run-id bh-20260305 \
  --dataset-root datasets --reports-dir reports --outputs-dir outputs --artifacts-dir artifacts

# Auto-run missing upstream hops before target hop
python3 run_pipeline_hops.py infer --run-id bh-20260305 --auto-upstream \
  --dataset-root datasets --reports-dir reports --outputs-dir outputs --artifacts-dir artifacts

# Force rerun target hop only (upstream hops stay skipped if already completed)
python3 run_pipeline_hops.py train --run-id bh-20260305 --force \
  --dataset-root datasets --reports-dir reports --outputs-dir outputs --artifacts-dir artifacts
```

Checkpoint layout:
- `outputs/checkpoints/<RUN_ID>/status/<hop>.json`
- `outputs/checkpoints/<RUN_ID>/load_tables/tables/*.parquet`
- `outputs/checkpoints/<RUN_ID>/...` (join/profile/train/infer/publish stage data)

Databricks Jobs pattern:
- Use the same `--run-id` across all tasks in one job run.
- Chain tasks in this order:
  1. `load_tables`
  2. `join_diagnostics`
  3. `profile`
  4. `features`
  5. `segments`
  6. `labels`
  7. `train`
  8. `infer`
  9. `publish`

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
- `reports/brand_health_report.md`: default final markdown report (when `--report-name` is not set)
- `reports/band_health_report_V2.md`: optional final markdown report filename (when `--report-name band_health_report_V2.md`)
- `reports/pipeline_summary.json`: model + metrics + joinability summary
- `reports/data_profile/table_profile.csv`
- `reports/data_profile/schema_profile.csv`
- `reports/data_profile/join_coverage.csv`

### Diagnostics
- `outputs/join_diagnostics.md`
- `outputs/coverage_notes.md`
- `outputs/profiling_report.md`
- `outputs/memory_optimization_report.json`
- `outputs/memory_dtype_optimization.csv` (when memory optimization is enabled)

### Features/labels
- `outputs/feature_table.parquet`
- `outputs/feature_definitions.csv`
- `outputs/labeled_feature_table.parquet`
- `outputs/segment_kpis.parquet`

### Predictions/attribution
- `outputs/predictions_with_drivers.jsonl`
- `outputs/predictions_with_drivers.csv`
- `outputs/predictions_with_drivers.parquet`
- `outputs/examples_last4_with_segments.json`
- `outputs/examples_before_after_2windows.json`
- `outputs/attribution_qa.json`
- `outputs/model_metrics_sample.json` (written when `--train_sample_mode != off`)
- `outputs/predictions_last_windows_sample.json` (written when `--train_sample_mode != off`)

Optional Unity Catalog publish (Databricks Spark mode):
- enable `--publish-kpis-predicted true` with `--source-mode databricks_pyspark`
- default target table: `projects_prd.marketingautomation.kpis_predicted`
- current write mode: `overwrite`

### Sampling artifacts (quick/smart)
- `outputs/sample_train_indices.csv`
- `outputs/sample_eval_indices.csv`
- `outputs/sample_qa_report.json`

Important i18n fields in prediction payload:
- `predicted_health_class_i18n`: `{ \"en\": \"...\", \"th\": \"...\" }`
- `predicted_health_statement_i18n`: `{ \"en\": \"...\", \"th\": \"...\" }`
- `confidence_band_i18n`: `{ \"en\": \"...\", \"th\": \"...\" }`
- driver-level `key_i18n`, `statement_i18n`, `direction_i18n`
- target-segment `reason_statement_i18n`, `metric_family_i18n`, `segment_label_i18n`, `direction_i18n`
- `suggested_actions_i18n`: list of `{ \"en\": \"...\", \"th\": \"...\" }`

## 7) Cloud run checklist

1. Upload code + dataset folders under `datasets/*`.
2. Create virtualenv and install packages.
3. Ensure enough memory/CPU for large parquet scans.
4. Choose mode:
- quick sampling: run command in 4.1
- smart sampling: run command in 4.2
- full production (no sampling): run command in 4.3
- skip-train: run command in 4.4
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
