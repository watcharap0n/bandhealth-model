# Run Pipeline Instructions

This document explains how to run `run_pipeline.py` in practice, including a full argument table (meaning, default, and examples) and ready-to-use command examples.

## 1) Quick start

### Train + infer in fast mode (Quick sample)

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
  --train_weight_classes true \
  --n_jobs 4
```

### Infer only (reuse existing model, no retraining)

```bash
python3 run_pipeline.py \
  --dataset-root datasets \
  --reports-dir reports \
  --outputs-dir outputs_infer_only \
  --artifacts-dir artifacts \
  --snapshot-freq 7D \
  --report-name infer_only_report.md \
  --skip-train
```

## 2) Argument reference (complete)

| Argument | Type | Default | Meaning | When to use | Example |
|---|---|---|---|---|---|
| `--dataset-root` | `str` | `datasets` | Root folder for input parquet datasets (`datasets/*`). | Always | `--dataset-root datasets` |
| `--reports-dir` | `str` | `reports` | Output folder for markdown/json reports. | Always | `--reports-dir reports` |
| `--outputs-dir` | `str` | `outputs` | Output folder for features/predictions/diagnostics artifacts. | Always | `--outputs-dir outputs_prod` |
| `--artifacts-dir` | `str` | `artifacts` | Model artifact folder (`brand_health_model.joblib`, `model_metadata.json`). | Train or skip-train | `--artifacts-dir artifacts_v1` |
| `--snapshot-freq` | `str` | `7D` | Snapshot/window frequency used to build feature windows (pandas offset alias). | Always | `--snapshot-freq 7D` |
| `--report-name` | `str` | `brand_health_report.md` | Main report filename written under `reports-dir`. | Always | `--report-name band_health_report_V2.md` |
| `--skip-train` | flag | `False` | Skip train/eval and load model from `artifacts-dir` for inference only. | When artifacts already exist | `--skip-train` |
| `--train_sample_mode` | `off\|quick\|smart` | `off` | Sampling mode for training/evaluation. | To reduce resource usage | `--train_sample_mode quick` |
| `--train_sample_seed` | `int` | `42` | Random seed for sampling logic. | quick/smart | `--train_sample_seed 123` |
| `--train_sample_frac` | `float` | `0.02` | Sampling fraction used by `quick` mode. | quick | `--train_sample_frac 0.03` |
| `--train_max_train_rows` | `int` | `200000` | Maximum sampled train rows. | quick/smart | `--train_max_train_rows 150000` |
| `--train_max_eval_rows` | `int` | `60000` | Maximum sampled eval rows. | quick/smart | `--train_max_eval_rows 40000` |
| `--train_recent_days` | `int` | `180` | In `smart` mode, first keep most recent N days before further sampling. | smart | `--train_recent_days 120` |
| `--train_stratify_cols` | `csv str` | `brand_id,predicted_health_class` | Stratification columns for sampling (comma-separated). | quick/smart | `--train_stratify_cols "brand_id,label_health_class"` |
| `--train_group_col` | `str` | `brand_id` | Group column for cross-brand evaluation and QA reporting. | quick/smart/off | `--train_group_col brand_id` |
| `--train_weight_classes` | `bool-like str` | `true` | Enable class weighting (`balanced`) in Logistic/HGB models. | If class imbalance exists | `--train_weight_classes false` |
| `--n_jobs` | `int` | `4` | Limits BLAS/OpenMP threads to reduce CPU pressure and prevent local hangs. | Always | `--n_jobs 2` |
| `--memory_optimize` | `bool-like str` | `true` | Enables memory optimization pass (dtype downcast + memory logging). | Recommended for large runs | `--memory_optimize true` |
| `--memory_float_downcast` | `bool-like str` | `false` | Allows float64→float32 downcast when `allclose(rtol=1e-6)` validation passes. Keep `false` for strict parity. | Large-memory datasets after parity check | `--memory_float_downcast true` |
| `--memory_cat_ratio_threshold` | `float` | `0.5` | Converts object columns to category when `nunique/nrows < threshold`. | Memory-heavy string columns | `--memory_cat_ratio_threshold 0.4` |
| `--memory_validate_downcast` | `bool-like str` | `true` | Validates value parity after downcast/categorical conversion. | Keep enabled for safety | `--memory_validate_downcast true` |
| `--publish-kpis-predicted` | `bool-like str` | `false` | Publish full `pred_df` to Unity Catalog table (only with `--source-mode databricks_pyspark`). | Databricks production scoring | `--publish-kpis-predicted true` |
| `--publish-kpis-table` | `str` | `projects_prd.marketingautomation.kpis_predicted` | Target Unity Catalog table for prediction publish. | When enabling publish | `--publish-kpis-table projects_prd.marketingautomation.kpis_predicted` |
| `--publish-kpis-write-mode` | `overwrite` or `merge` | `overwrite` | Write behavior for publish target table. Use `overwrite` for full table replacement or `merge` to upsert by `(brand_id, window_end_date, window_size)` and keep older windows. | When enabling publish | `--publish-kpis-write-mode merge` |
| `--publish-kpis-fail-on-cast-error` | `bool-like str` | `true` | Fail pipeline when schema-alignment cast fails before writing to catalog. | Keep enabled for data safety | `--publish-kpis-fail-on-cast-error true` |

Important notes:
- `--train_sample_frac` is used only in `quick` mode.
- `--train_recent_days` is used only in `smart` mode.
- With `--train_sample_mode off`, sampling parameters are ignored for row reduction.
- `predicted_health_class` in `--train_stratify_cols` is internally mapped to `label_health_class`.
- Memory diagnostics are written to `outputs/memory_optimization_report.json`.
- Catalog publish requires Spark runtime and `--source-mode databricks_pyspark`.

## 3) Recommended presets

| Use case | Recommended mode | Key args |
|---|---|---|
| Fast local run (Mac/laptop-safe) | `quick` | `--train_sample_mode quick --train_sample_frac 0.02 --n_jobs 2` |
| Better representation across time/distribution | `smart` | `--train_sample_mode smart --train_recent_days 180 --n_jobs 4` |
| Full production training on all data | `off` | `--train_sample_mode off --n_jobs 4` |
| Inference only with existing artifacts | `skip-train` | `--skip-train` |

## 4) Additional command examples

### 4.1 Smart mode (good pre-production validation)

```bash
python3 run_pipeline.py \
  --dataset-root datasets \
  --reports-dir reports \
  --outputs-dir outputs_smart \
  --artifacts-dir artifacts \
  --report-name band_health_report_V2.md \
  --train_sample_mode smart \
  --train_recent_days 180 \
  --train_max_train_rows 200000 \
  --train_max_eval_rows 60000 \
  --train_weight_classes true \
  --n_jobs 4
```

### 4.2 Full production run (no sampling)

```bash
python3 run_pipeline.py \
  --dataset-root datasets \
  --reports-dir reports \
  --outputs-dir outputs_prod \
  --artifacts-dir artifacts_prod \
  --report-name band_health_report_prod.md \
  --train_sample_mode off \
  --train_weight_classes true \
  --n_jobs 4
```

### 4.3 Ultra-light quick mode (small machine)

```bash
python3 run_pipeline.py \
  --dataset-root datasets \
  --reports-dir reports \
  --outputs-dir outputs_quick_light \
  --artifacts-dir artifacts_quick \
  --report-name band_health_report_quick.md \
  --train_sample_mode quick \
  --train_sample_frac 0.01 \
  --train_max_train_rows 100000 \
  --train_max_eval_rows 30000 \
  --train_weight_classes true \
  --n_jobs 2
```

### 4.4 Infer a new app_id (example: buzz) without retraining

Preconditions:
- Add mapping in `run_pipeline.py` under `BRAND_APP_ID_FILTERS`, e.g. `"buzz": [3]`.
- Required model artifacts already exist in `artifacts-dir`.

```bash
python3 run_pipeline.py \
  --dataset-root datasets \
  --reports-dir reports \
  --outputs-dir outputs_buzz \
  --artifacts-dir artifacts \
  --report-name buzz_infer_report.md \
  --snapshot-freq 7D \
  --skip-train
```

### 4.5 Publish predicted KPIs to Unity Catalog table

```bash
python3 run_pipeline.py \
  --source-mode databricks_pyspark \
  --query-app-ids 1993744540760190,838315041537793 \
  --brand-aliases 1993744540760190=c-vit,838315041537793=see-chan \
  --databricks-catalog projects_prd \
  --databricks-database datacleansing \
  --publish-kpis-predicted true \
  --publish-kpis-table projects_prd.marketingautomation.kpis_predicted \
  --publish-kpis-write-mode merge \
  --publish-kpis-fail-on-cast-error true
```

### 4.6 Hop-based execution (stage-by-stage with run-id checkpoints)

Use this when you want to rerun only failed stages and continue from previous completed stages.

Command format:

```bash
python3 run_pipeline_hops.py <hop> \
  --run-id <RUN_ID> \
  [--auto-upstream] \
  [--force] \
  --dataset-root datasets \
  --reports-dir reports \
  --outputs-dir outputs \
  --artifacts-dir artifacts
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
# Run one hop only (strict mode: upstream hops must already be completed)
python3 run_pipeline_hops.py features \
  --run-id bh-20260305 \
  --dataset-root datasets \
  --reports-dir reports \
  --outputs-dir outputs \
  --artifacts-dir artifacts

# Auto-run missing upstream hops before infer
python3 run_pipeline_hops.py infer \
  --run-id bh-20260305 \
  --auto-upstream \
  --dataset-root datasets \
  --reports-dir reports \
  --outputs-dir outputs \
  --artifacts-dir artifacts

# Force rerun target hop only
python3 run_pipeline_hops.py train \
  --run-id bh-20260305 \
  --force \
  --dataset-root datasets \
  --reports-dir reports \
  --outputs-dir outputs \
  --artifacts-dir artifacts
```

Checkpoint paths:
- `outputs/checkpoints/<RUN_ID>/status/<hop>.json`
- `outputs/checkpoints/<RUN_ID>/load_tables/tables/*.parquet`
- `outputs/checkpoints/<RUN_ID>/join_diagnostics/*`
- `outputs/checkpoints/<RUN_ID>/profile/*`
- `outputs/checkpoints/<RUN_ID>/train/*`
- `outputs/checkpoints/<RUN_ID>/infer/*`
- `outputs/checkpoints/<RUN_ID>/publish/*`

Databricks task chain (same `--run-id` for all tasks):
1. `load_tables`
2. `join_diagnostics`
3. `profile`
4. `features`
5. `segments`
6. `labels`
7. `train`
8. `infer`
9. `publish`

### 4.7 Export hybrid MLOps snapshots + candidate model bundle

```bash
python3 run_pipeline.py \
  --run-id bh-20260327-train \
  --source-mode databricks_pyspark \
  --query-app-ids 1993744540760190,838315041537793 \
  --brand-aliases 1993744540760190=c-vit,838315041537793=see-chan \
  --snapshot-root /dbfs/tmp/brand-health/mlops_snapshots \
  --model-bundle-root /dbfs/tmp/brand-health/model_registry \
  --export-training-snapshot true \
  --export-scoring-snapshot true \
  --publish-kpis-predicted true \
  --publish-kpis-write-mode merge
```

### 4.8 Score from an approved artifact bundle manifest

```bash
python3 run_pipeline.py \
  --run-id bh-20260327-score \
  --source-mode databricks_pyspark \
  --query-app-ids 1993744540760190,838315041537793 \
  --brand-aliases 1993744540760190=c-vit,838315041537793=see-chan \
  --skip-train \
  --model-source artifact_bundle \
  --model-release-manifest /dbfs/tmp/brand-health/model_registry/production_manifest.json \
  --publish-kpis-predicted true \
  --publish-kpis-write-mode merge
```

## 5) Key output files to verify

| Category | Files |
|---|---|
| Main predictions | `outputs/predictions_with_drivers.jsonl`, `outputs/predictions_with_drivers.csv`, `outputs/predictions_with_drivers.parquet` |
| Dashboard examples | `outputs/examples_last4_with_segments.json` |
| Attribution QA | `outputs/attribution_qa.json` |
| Sampling QA | `outputs/sample_qa_report.json` |
| Data validation | `outputs/data_validation_report.json` |
| Memory QA | `outputs/memory_optimization_report.json`, `outputs/memory_dtype_optimization.csv` |
| Snapshot manifests | `outputs/mlops_snapshots/training_snapshot/<RUN_ID>/snapshot_manifest.json`, `outputs/mlops_snapshots/scoring_snapshot/<RUN_ID>/snapshot_manifest.json` |
| Model release | `artifacts/model_registry/<RUN_ID>/model_release_manifest.json`, `artifacts/model_registry/latest_candidate.json`, `artifacts/model_registry/production_manifest.json` |
| Metrics summary | `reports/pipeline_summary.json`, `outputs/model_metrics_sample.json` |
| Hop checkpoints | `outputs/checkpoints/<RUN_ID>/status/*.json` and stage checkpoint files |
