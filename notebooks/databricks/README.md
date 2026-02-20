# Databricks Notebook Pack (Azure + MLflow)

This folder contains notebooks to run the full Brand Health pipeline on Azure Databricks.

## Files

- `00_catalog_read_example.py`
  - Minimal app_id-filtered Spark catalog read example.
  - Matches the direct read style (`spark.table(...).filter(app_id=...)`).

- `01_brand_health_pipeline_mlflow.py`
  - End-to-end pipeline:
    1. load data from catalog (or parquet)
    2. compute joinability guardrails
    3. build features + weak labels + segment KPIs
    4. train/evaluate scikit-learn model (or skip train)
    5. run inference + drivers + target segments + actions
    6. save outputs and log to MLflow

## How to use in Databricks

1. Open this repo in Databricks Repos.
2. Attach a cluster with required libraries (`pandas`, `numpy`, `scikit-learn`, `pyarrow`, `mlflow`).
3. Open `01_brand_health_pipeline_mlflow.py`.
4. Edit config block in section `## 1) Config`:
   - `SOURCE_MODE` (`catalog` or `parquet`)
   - `CATALOG_SCHEMA` and `CATALOG_TABLE_MAP`
   - `BRAND_APP_ID_FILTERS`
   - memory controls (`MEMORY_OPTIMIZE`, `MEMORY_FLOAT_DOWNCAST`, `MEMORY_CAT_RATIO_THRESHOLD`)
   - Spark conversion controls (`ENABLE_ARROW`, `SPARK_TO_PANDAS_CHUNK_THRESHOLD`, `SPARK_TO_PANDAS_CHUNK_ROWS`)
   - sampling and training flags
   - output/artifact DBFS paths
   - MLflow experiment path
5. Run all cells.

## App-ID mapping (important)

The notebook is dynamic by app_id mapping:

```python
BRAND_APP_ID_FILTERS = {
    "c-vit": ["1993744540760190"],
    "see-chan": ["838315041537793"],
    # "buzz": ["3"],
}
```

If you add a new brand/app_id, update this mapping only.

## Expected source tables (catalog mode)

Default table mapping:

```python
CATALOG_TABLE_MAP = {
    "activity_transaction": "activity_transaction",
    "purchase": "purchase_transaction",
    "purchase_items": "purchase_transactionitems",
    "user_device": "user_device",
    "user_identity": "user_identity",
    "user_info": "userinfo",
    "user_view": "user_view",
    "user_visitor": "user_visitor",
}
```

Adjust this map if your Unity Catalog table names differ.

## Outputs

Saved to `DBFS_OUTPUT_DIR`:

- `feature_table.parquet`
- `feature_definitions.csv`
- `labeled_feature_table.parquet`
- `segment_kpis.parquet`
- `predictions_with_drivers.jsonl`
- `predictions_with_drivers.csv`
- `predictions_with_drivers.parquet`
- `examples_last4_with_segments.json`
- `attribution_qa.json`
- `pipeline_run_summary.json`
- `join_diagnostics_catalog.csv`
- `join_time_ranges_catalog.csv`
- `memory_optimization_report.json`
- `memory_dtype_optimization.csv` (when optimization runs)
- sample files (`sample_train_indices.csv`, `sample_eval_indices.csv`, `sample_qa_report.json`) when sampling is enabled

## MLflow

The notebook logs:

- run parameters (mode, sampling, config)
- training metrics (macro F1 / balanced accuracy)
- joinability maps
- pipeline summary JSON
- model artifact (when training is enabled)
- output artifacts (if `LOG_OUTPUTS_TO_MLFLOW=True`)
