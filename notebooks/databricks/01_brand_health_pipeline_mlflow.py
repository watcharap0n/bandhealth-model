# Databricks notebook source
# MAGIC %md
# MAGIC # Brand Health Pipeline (Databricks + MLflow)
# MAGIC
# MAGIC End-to-end notebook for Azure Databricks:
# MAGIC - Read source data from Unity Catalog (`spark.table`) or local parquet folders
# MAGIC - Build features, weak labels, segment KPIs
# MAGIC - Train/evaluate scikit-learn model (or `--skip-train` style reuse)
# MAGIC - Run inference + drivers + segment-aware actions
# MAGIC - Log outputs/model/metrics to MLflow

# COMMAND ----------

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Mapping, Optional, Sequence, Tuple

import numpy as np
import pandas as pd
import mlflow
import mlflow.sklearn
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F

# Make sure repo imports work when running inside Databricks Repos.
REPO_ROOT = os.getcwd()
if REPO_ROOT not in sys.path:
    sys.path.append(REPO_ROOT)

from src.data_load import TABLE_FILES, load_tables
from src.features import build_feature_table, feature_definitions
from src.id_utils import normalize_id
from src.infer import load_model_artifacts, predict_with_drivers, save_predictions
from src.labeling import generate_weak_labels, labeling_thresholds
from src.memory_opt import (
    collect_garbage,
    log_memory_rss,
    optimize_dataframe_dtypes,
    optimize_table_dict,
    write_parquet_chunked,
)
from src.sampling import TrainSampleConfig, build_train_eval_samples, save_sample_outputs
from src.segments import compute_segment_kpis
from src.train import train_models

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1) Config

# COMMAND ----------

# Data source mode: "catalog" (Spark catalog) or "parquet" (local datasets folder)
SOURCE_MODE = "catalog"

# Catalog source config
CATALOG_SCHEMA = "projects_prd.datacleansing"
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

# Brand mapping by app_id (dynamic; add new brands here)
BRAND_APP_ID_FILTERS: Dict[str, List[str]] = {
    "c-vit": ["1993744540760190"],
    "see-chan": ["838315041537793"],
    # "buzz": ["3"],
}

# Optional catalog guards (set 0/None to disable)
CATALOG_RECENT_DAYS: int = 0
MAX_ROWS_PER_TABLE: int = 0

# Parquet source config
DATASET_ROOT = "datasets"

# Pipeline config
SNAPSHOT_FREQ = "7D"
SKIP_TRAIN = False
TRAIN_SAMPLE_MODE = "quick"  # off|quick|smart
TRAIN_SAMPLE_SEED = 42
TRAIN_SAMPLE_FRAC = 0.02
TRAIN_MAX_TRAIN_ROWS = 200_000
TRAIN_MAX_EVAL_ROWS = 60_000
TRAIN_RECENT_DAYS = 180
TRAIN_STRATIFY_COLS = ("brand_id", "predicted_health_class")
TRAIN_GROUP_COL = "brand_id"
TRAIN_WEIGHT_CLASSES = True
N_JOBS = 4

# Memory optimization controls
MEMORY_OPTIMIZE = True
MEMORY_FLOAT_DOWNCAST = False
MEMORY_CAT_RATIO_THRESHOLD = 0.5
MEMORY_VALIDATE_DOWNCAST = True

# Spark -> Pandas conversion controls
ENABLE_ARROW = True
EXPLAIN_PUSHDOWN = True
SPARK_TO_PANDAS_CHUNK_THRESHOLD = 2_000_000
SPARK_TO_PANDAS_CHUNK_ROWS = 200_000

# Output and artifact paths (DBFS URI supported)
DBFS_OUTPUT_DIR = "dbfs:/tmp/band-health/outputs"
DBFS_ARTIFACT_DIR = "dbfs:/tmp/band-health/artifacts"

# MLflow
MLFLOW_EXPERIMENT = "/Shared/brand-health"
MLFLOW_RUN_NAME = f"brand-health-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"
LOG_OUTPUTS_TO_MLFLOW = True

# Joinability thresholds
COMMERCE_JOIN_THRESHOLD = 0.80
ACTIVITY_ENRICH_THRESHOLD = 0.80

# Required columns used by existing pipeline modules
COLUMNS_MAP: Dict[str, List[str]] = {
    "activity_transaction": [
        "app_id",
        "user_id",
        "transaction_id",
        "activity_datetime",
        "activity_type",
        "activity_name",
        "reward_type",
        "is_completed",
        "reward",
        "points",
    ],
    "purchase": [
        "app_id",
        "transaction_id",
        "user_id",
        "create_datetime",
        "paid_datetime",
        "transaction_status",
        "itemsold",
        "subtotal_amount",
        "discount_amount",
        "shipping_fee",
        "net_amount",
    ],
    "purchase_items": [
        "app_id",
        "transaction_id",
        "user_id",
        "create_datetime",
        "paid_datetime",
        "transaction_status",
        "sku_id",
        "quantity",
        "price_sell",
        "price_discount",
        "price_net",
        "delivered",
        "is_shiped",
    ],
    "user_view": ["app_id", "user_id", "join_datetime", "inactive_datetime", "user_type"],
    "user_visitor": [
        "app_id",
        "tbl_type",
        "idsite",
        "user_id",
        "user_type",
        "visit_datetime",
        "visit_end_datetime",
        "actions",
        "interactions",
        "searches",
        "events",
    ],
    "user_device": ["app_id", "user_id", "lastaccess", "device_type", "os_name"],
    "user_identity": ["app_id", "user_id", "line_id", "external_id"],
    "user_info": ["app_id", "user_id", "dateofbirth", "gender"],
}

# Optional column aliases (source_col -> canonical_col)
COLUMN_ALIASES: Dict[str, Dict[str, str]] = {
    "purchase": {
        "created_datetime": "create_datetime",
        "payment_datetime": "paid_datetime",
        "status": "transaction_status",
    },
    "purchase_items": {
        "created_datetime": "create_datetime",
        "payment_datetime": "paid_datetime",
        "shipped": "is_shiped",
    },
    "user_info": {
        "dob": "dateofbirth",
    },
}

print("Config loaded")
print("SOURCE_MODE:", SOURCE_MODE)
print("BRAND_APP_ID_FILTERS:", BRAND_APP_ID_FILTERS)
if ENABLE_ARROW:
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    print("Arrow enabled: spark.sql.execution.arrow.pyspark.enabled=true")
memory_events: List[Dict[str, object]] = []
log_memory_rss("notebook_start", sink=memory_events)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2) Helpers

# COMMAND ----------


def _dbfs_uri_to_local_path(path_str: str) -> Path:
    if path_str.startswith("dbfs:/"):
        return Path("/dbfs") / path_str.replace("dbfs:/", "", 1)
    return Path(path_str)


def _build_app_to_brand_map(brand_app_ids: Mapping[str, Sequence[str]]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for brand_id, app_ids in brand_app_ids.items():
        for app_id in app_ids:
            key = str(app_id).strip()
            if not key:
                continue
            if key in out and out[key] != str(brand_id):
                raise ValueError(f"app_id {key} is mapped to multiple brands: {out[key]} and {brand_id}")
            out[key] = str(brand_id)
    return out


def _apply_aliases(df: pd.DataFrame, aliases: Mapping[str, str]) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    for src_col, tgt_col in aliases.items():
        if src_col in out.columns and tgt_col not in out.columns:
            out = out.rename(columns={src_col: tgt_col})
    return out


def _ensure_columns(df: pd.DataFrame, cols: Sequence[str]) -> pd.DataFrame:
    out = df.copy()
    for c in cols:
        if c not in out.columns:
            out[c] = pd.NA
    keep_cols = list(dict.fromkeys(cols))
    return out[keep_cols]


def _safe_existing_columns(sdf: SparkDataFrame, requested_cols: Sequence[str]) -> List[str]:
    existing = set(sdf.columns)
    return [c for c in requested_cols if c in existing]


def _spark_to_pandas_memory_safe(
    sdf: SparkDataFrame,
    selected_cols: Sequence[str],
    table_label: str,
    chunk_threshold_rows: int = 2_000_000,
    chunk_rows: int = 200_000,
    explain_pushdown: bool = False,
) -> Tuple[pd.DataFrame, int]:
    q = sdf.select(*selected_cols)
    if explain_pushdown:
        print(f"Spark plan for {table_label}:")
        q.explain(True)

    row_count = int(q.count())
    if row_count == 0:
        return pd.DataFrame(columns=list(selected_cols)), 0

    if row_count <= int(chunk_threshold_rows):
        pdf = q.toPandas()
        if len(pdf) != row_count:
            raise AssertionError(f"Row count mismatch for {table_label}: pandas={len(pdf)} spark={row_count}")
        return pdf, row_count

    print(
        f"{table_label}: using chunked toLocalIterator conversion "
        f"(rows={row_count:,}, chunk_rows={int(chunk_rows):,})"
    )
    parts: List[pd.DataFrame] = []
    buf: List[dict] = []
    for row in q.toLocalIterator():
        buf.append(row.asDict(recursive=False))
        if len(buf) >= int(chunk_rows):
            part = pd.DataFrame.from_records(buf, columns=list(selected_cols))
            parts.append(part)
            buf = []
    if buf:
        parts.append(pd.DataFrame.from_records(buf, columns=list(selected_cols)))

    if parts:
        pdf = pd.concat(parts, axis=0, ignore_index=True)
    else:
        pdf = pd.DataFrame(columns=list(selected_cols))

    if len(pdf) != row_count:
        raise AssertionError(f"Chunked row count mismatch for {table_label}: pandas={len(pdf)} spark={row_count}")
    return pdf, row_count


def _to_datetime_series(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce", utc=True)


def _coverage_norm(left: pd.Series, right: pd.Series) -> Tuple[int, float]:
    l = normalize_id(left)
    r = normalize_id(right)
    l_u = set(l.dropna().tolist())
    r_u = set(r.dropna().tolist())
    overlap = len(l_u.intersection(r_u))
    row_cov = float(l.isin(r_u).mean()) if len(l) else 0.0
    return overlap, row_cov


def _time_bounds(df: pd.DataFrame, dt_cols: Sequence[str]) -> Tuple[pd.Timestamp, pd.Timestamp]:
    mins: List[pd.Timestamp] = []
    maxs: List[pd.Timestamp] = []
    for c in dt_cols:
        if c not in df.columns:
            continue
        ts = _to_datetime_series(df[c]).dropna()
        if ts.empty:
            continue
        mins.append(ts.min())
        maxs.append(ts.max())
    if not mins:
        return (pd.NaT, pd.NaT)
    return (min(mins), max(maxs))


def _time_overlap(a_min: pd.Timestamp, a_max: pd.Timestamp, b_min: pd.Timestamp, b_max: pd.Timestamp) -> bool:
    if pd.isna(a_min) or pd.isna(a_max) or pd.isna(b_min) or pd.isna(b_max):
        return False
    return bool(max(a_min, b_min) <= min(a_max, b_max))


def _compute_joinability_maps(
    tables: Mapping[str, pd.DataFrame],
    threshold_commerce: float,
    threshold_activity: float,
) -> Tuple[Dict[str, bool], Dict[str, bool], pd.DataFrame, pd.DataFrame]:
    brands = sorted(
        set(
            pd.concat(
                [
                    df["brand_id"]
                    for df in tables.values()
                    if isinstance(df, pd.DataFrame) and not df.empty and "brand_id" in df.columns
                ],
                ignore_index=True,
            )
            .dropna()
            .astype(str)
            .tolist()
        )
    )

    commerce_joinable: Dict[str, bool] = {}
    activity_enrichment_joinable: Dict[str, bool] = {}

    join_rows: List[dict] = []
    time_rows: List[dict] = []

    for brand_id in brands:
        purchase = tables.get("purchase", pd.DataFrame())
        purchase = purchase[purchase.get("brand_id", "") == brand_id].copy() if not purchase.empty else purchase

        purchase_items = tables.get("purchase_items", pd.DataFrame())
        purchase_items = purchase_items[purchase_items.get("brand_id", "") == brand_id].copy() if not purchase_items.empty else purchase_items

        activity = tables.get("activity_transaction", pd.DataFrame())
        activity = activity[activity.get("brand_id", "") == brand_id].copy() if not activity.empty else activity

        user_view = tables.get("user_view", pd.DataFrame())
        user_view = user_view[user_view.get("brand_id", "") == brand_id].copy() if not user_view.empty else user_view

        user_visitor = tables.get("user_visitor", pd.DataFrame())
        user_visitor = user_visitor[user_visitor.get("brand_id", "") == brand_id].copy() if not user_visitor.empty else user_visitor

        tx_overlap, tx_cov = _coverage_norm(
            purchase.get("transaction_id", pd.Series(dtype="object")),
            purchase_items.get("transaction_id", pd.Series(dtype="object")),
        )
        uid_overlap, uid_cov = _coverage_norm(
            purchase.get("user_id", pd.Series(dtype="object")),
            purchase_items.get("user_id", pd.Series(dtype="object")),
        )

        p_min, p_max = _time_bounds(purchase, ["create_datetime", "paid_datetime"])
        pi_min, pi_max = _time_bounds(purchase_items, ["create_datetime", "paid_datetime", "delivered_datetime"])
        overlap_time = _time_overlap(p_min, p_max, pi_min, pi_max)

        is_joinable = bool((tx_cov >= threshold_commerce) and overlap_time)
        commerce_joinable[brand_id] = is_joinable

        view_overlap, view_cov = _coverage_norm(
            activity.get("user_id", pd.Series(dtype="object")),
            user_view.get("user_id", pd.Series(dtype="object")),
        )
        visitor_overlap, visitor_cov = _coverage_norm(
            activity.get("user_id", pd.Series(dtype="object")),
            user_visitor.get("user_id", pd.Series(dtype="object")),
        )
        act_enrich_ok = bool((view_cov >= threshold_activity) and (visitor_cov >= threshold_activity))
        activity_enrichment_joinable[brand_id] = act_enrich_ok

        join_rows.extend(
            [
                {
                    "brand_id": brand_id,
                    "left_table": "purchase",
                    "right_table": "purchase_items",
                    "key": "transaction_id",
                    "overlap_unique_norm": tx_overlap,
                    "row_coverage_norm": tx_cov,
                },
                {
                    "brand_id": brand_id,
                    "left_table": "purchase",
                    "right_table": "purchase_items",
                    "key": "user_id",
                    "overlap_unique_norm": uid_overlap,
                    "row_coverage_norm": uid_cov,
                },
                {
                    "brand_id": brand_id,
                    "left_table": "activity_transaction",
                    "right_table": "user_view",
                    "key": "user_id",
                    "overlap_unique_norm": view_overlap,
                    "row_coverage_norm": view_cov,
                },
                {
                    "brand_id": brand_id,
                    "left_table": "activity_transaction",
                    "right_table": "user_visitor",
                    "key": "user_id",
                    "overlap_unique_norm": visitor_overlap,
                    "row_coverage_norm": visitor_cov,
                },
            ]
        )

        time_rows.append(
            {
                "brand_id": brand_id,
                "purchase_min": str(p_min) if pd.notna(p_min) else None,
                "purchase_max": str(p_max) if pd.notna(p_max) else None,
                "purchase_items_min": str(pi_min) if pd.notna(pi_min) else None,
                "purchase_items_max": str(pi_max) if pd.notna(pi_max) else None,
                "time_range_overlap": overlap_time,
                "commerce_joinable": is_joinable,
            }
        )

    return commerce_joinable, activity_enrichment_joinable, pd.DataFrame(join_rows), pd.DataFrame(time_rows)


CATALOG_TIME_COLUMNS: Dict[str, List[str]] = {
    "activity_transaction": ["activity_datetime"],
    "purchase": ["create_datetime", "paid_datetime"],
    "purchase_items": ["create_datetime", "paid_datetime", "delivered_datetime"],
    "user_view": ["join_datetime", "inactive_datetime"],
    "user_visitor": ["visit_datetime", "visit_end_datetime"],
    "user_device": ["lastaccess"],
    "user_identity": [],
    "user_info": [],
}


def _load_tables_from_catalog(
    spark_session,
    catalog_schema: str,
    table_map: Mapping[str, str],
    brand_app_ids: Mapping[str, Sequence[str]],
    columns_map: Mapping[str, Sequence[str]],
    column_aliases: Optional[Mapping[str, Mapping[str, str]]] = None,
    recent_days: int = 0,
    max_rows_per_table: int = 0,
) -> Dict[str, pd.DataFrame]:
    app_to_brand = _build_app_to_brand_map(brand_app_ids)
    app_keys = sorted(app_to_brand.keys())
    out: Dict[str, pd.DataFrame] = {}

    for canonical_table, requested_cols in columns_map.items():
        source_table = table_map.get(canonical_table, canonical_table)
        full_name = f"{catalog_schema}.{source_table}"

        sdf = spark_session.table(full_name)
        if "app_id" not in sdf.columns:
            raise ValueError(f"{full_name} is missing required column: app_id")
        base_count = int(sdf.count())

        sdf = sdf.filter(F.col("app_id").cast("string").isin(app_keys))

        if recent_days and recent_days > 0:
            for dt_col in CATALOG_TIME_COLUMNS.get(canonical_table, []):
                if dt_col in sdf.columns:
                    sdf = sdf.filter(F.to_timestamp(F.col(dt_col)) >= F.date_sub(F.current_timestamp(), int(recent_days)))
                    break
        filtered_count = int(sdf.count())

        cols_to_pull = list(dict.fromkeys(list(requested_cols) + ["app_id"]))
        cols_existing = _safe_existing_columns(sdf, cols_to_pull)
        if not cols_existing:
            cols_existing = ["app_id"]

        if max_rows_per_table and max_rows_per_table > 0:
            sdf = sdf.limit(int(max_rows_per_table))

        pdf, spark_rows = _spark_to_pandas_memory_safe(
            sdf=sdf,
            selected_cols=cols_existing,
            table_label=full_name,
            chunk_threshold_rows=int(SPARK_TO_PANDAS_CHUNK_THRESHOLD),
            chunk_rows=int(SPARK_TO_PANDAS_CHUNK_ROWS),
            explain_pushdown=bool(EXPLAIN_PUSHDOWN),
        )

        aliases = (column_aliases or {}).get(canonical_table, {})
        pdf = _apply_aliases(pdf, aliases)
        pdf = _ensure_columns(pdf, cols_to_pull)

        app_series = pd.to_numeric(pdf["app_id"], errors="coerce").astype("Int64").astype(str)
        pdf["brand_id"] = app_series.map(app_to_brand)
        pdf = pdf[pdf["brand_id"].notna()].reset_index(drop=True)

        if MEMORY_OPTIMIZE:
            pdf, _ = optimize_dataframe_dtypes(
                pdf,
                table_name=canonical_table,
                allow_float_downcast=bool(MEMORY_FLOAT_DOWNCAST),
                float_rtol=1e-6,
                cat_ratio_threshold=float(MEMORY_CAT_RATIO_THRESHOLD),
                protect_columns=("brand_id",),
                validate=bool(MEMORY_VALIDATE_DOWNCAST),
            )

        out[canonical_table] = pdf
        print(
            f"Loaded {canonical_table} from {full_name}: "
            f"base_rows={base_count:,} filtered_rows={filtered_count:,} "
            f"spark_rows={spark_rows:,} pandas_rows={len(pdf):,}"
        )

    return out


# COMMAND ----------
# MAGIC %md
# MAGIC ## 3) Load data

# COMMAND ----------

if SOURCE_MODE.lower() == "catalog":
    tables = _load_tables_from_catalog(
        spark_session=spark,
        catalog_schema=CATALOG_SCHEMA,
        table_map=CATALOG_TABLE_MAP,
        brand_app_ids=BRAND_APP_ID_FILTERS,
        columns_map=COLUMNS_MAP,
        column_aliases=COLUMN_ALIASES,
        recent_days=CATALOG_RECENT_DAYS,
        max_rows_per_table=MAX_ROWS_PER_TABLE,
    )
else:
    tables = load_tables(
        dataset_root=DATASET_ROOT,
        table_files=TABLE_FILES,
        columns_map=COLUMNS_MAP,
        brand_app_ids=BRAND_APP_ID_FILTERS,
    )

print("Data load complete")
log_memory_rss("after_data_load", sink=memory_events)
table_rows_before_opt = {k: int(len(v)) for k, v in tables.items() if isinstance(v, pd.DataFrame)}

dtype_opt_summary = pd.DataFrame()
if MEMORY_OPTIMIZE:
    tables, dtype_opt_summary = optimize_table_dict(
        tables,
        allow_float_downcast=bool(MEMORY_FLOAT_DOWNCAST),
        float_rtol=1e-6,
        cat_ratio_threshold=float(MEMORY_CAT_RATIO_THRESHOLD),
        protect_columns=("brand_id",),
        validate=bool(MEMORY_VALIDATE_DOWNCAST),
    )
    for k, v in tables.items():
        if isinstance(v, pd.DataFrame):
            before_n = table_rows_before_opt.get(k, int(len(v)))
            after_n = int(len(v))
            if before_n != after_n:
                raise AssertionError(
                    f"Row count changed after dtype optimization for table={k}: before={before_n}, after={after_n}"
                )
    if not dtype_opt_summary.empty:
        display(dtype_opt_summary[["table_name", "rows", "cols", "bytes_before", "bytes_after", "reduced_pct"]])
log_memory_rss("after_dtype_optimization", sink=memory_events)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4) Compute joinability guardrails

# COMMAND ----------

(
    commerce_joinable,
    activity_enrichment_joinable,
    join_diag_df,
    time_diag_df,
) = _compute_joinability_maps(
    tables=tables,
    threshold_commerce=COMMERCE_JOIN_THRESHOLD,
    threshold_activity=ACTIVITY_ENRICH_THRESHOLD,
)

print("commerce_joinable:", commerce_joinable)
print("activity_enrichment_joinable:", activity_enrichment_joinable)

display(join_diag_df)
display(time_diag_df)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5) Build features + labels + segment KPIs

# COMMAND ----------

feature_df = build_feature_table(
    tables=tables,
    snapshot_freq=SNAPSHOT_FREQ,
    commerce_joinable_by_brand=commerce_joinable,
)
if feature_df.empty:
    raise RuntimeError("Feature table is empty. Check source timestamps and schema mapping.")

segment_kpi_df = compute_segment_kpis(
    tables=tables,
    snapshot_freq=SNAPSHOT_FREQ,
    commerce_joinable_by_brand=commerce_joinable,
    activity_enrichment_joinable_by_brand=activity_enrichment_joinable,
)

feature_defs_df = feature_definitions(feature_df)
labeled_df = generate_weak_labels(feature_df).reset_index(drop=True)
labeled_df["__row_id"] = np.arange(len(labeled_df), dtype=int)

print("feature rows:", len(feature_df))
print("segment kpi rows:", len(segment_kpi_df))
print("labeled rows:", len(labeled_df))
log_memory_rss("after_feature_label_build", sink=memory_events)

# Raw source tables are no longer needed after all derived frames are built.
del tables
collect_garbage("release_raw_tables", sink=memory_events)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6) Optional sampling (quick/smart)

# COMMAND ----------

sample_mode = str(TRAIN_SAMPLE_MODE).strip().lower()
sample_result = None
train_input_df = labeled_df
infer_input_df = labeled_df
train_row_ids: Optional[Sequence[int]] = None
eval_row_ids: Optional[Sequence[int]] = None

if sample_mode != "off":
    sample_cfg = TrainSampleConfig(
        mode=sample_mode,
        seed=int(TRAIN_SAMPLE_SEED),
        frac=float(TRAIN_SAMPLE_FRAC),
        max_train_rows=int(TRAIN_MAX_TRAIN_ROWS),
        max_eval_rows=int(TRAIN_MAX_EVAL_ROWS),
        recent_days=int(TRAIN_RECENT_DAYS),
        stratify_cols=tuple(TRAIN_STRATIFY_COLS),
        group_col=str(TRAIN_GROUP_COL),
    )
    sample_result = build_train_eval_samples(labeled_df, config=sample_cfg)
    train_input_df = sample_result.sampled_df
    infer_input_df = sample_result.sampled_df
    train_row_ids = sample_result.train_row_ids
    eval_row_ids = sample_result.eval_row_ids

    print(
        "sample mode={} train_rows={} eval_rows={} sampled_total={} representative_pass={}".format(
            sample_mode,
            len(sample_result.sampled_train_df),
            len(sample_result.sampled_eval_df),
            len(sample_result.sampled_df),
            sample_result.qa_report.get("representative_pass"),
        )
    )

    # Full labeled frame can be released in sampled mode.
    del labeled_df
    collect_garbage("release_full_labeled_after_sampling", sink=memory_events)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7) Train / load model and infer

# COMMAND ----------

artifact_dir_local = _dbfs_uri_to_local_path(DBFS_ARTIFACT_DIR)
artifact_dir_local.mkdir(parents=True, exist_ok=True)
log_memory_rss("before_train_stage", sink=memory_events)

if SKIP_TRAIN:
    loaded = load_model_artifacts(artifact_dir=artifact_dir_local)
    model = loaded["model"]
    metadata = loaded.get("metadata", {})
    feature_importance = loaded.get("feature_importance", {})
    feature_columns = metadata.get("feature_columns", [])
    class_labels = metadata.get("class_labels", ["AtRisk", "Healthy", "Warning"])
    metrics = metadata.get("metrics", {})
    selected_model = metadata.get("metrics", {}).get("selected_model", "loaded_artifact")
else:
    artifacts = train_models(
        train_input_df,
        artifact_dir=artifact_dir_local,
        train_row_ids=train_row_ids,
        eval_row_ids=eval_row_ids,
        sample_mode=sample_mode,
        n_jobs=int(N_JOBS),
        weight_classes=bool(TRAIN_WEIGHT_CLASSES),
        group_col=str(TRAIN_GROUP_COL),
        quick_top_k_features=80 if sample_mode == "quick" else None,
    )
    model = artifacts.final_model
    feature_importance = artifacts.feature_importance
    feature_columns = artifacts.feature_columns
    class_labels = artifacts.class_labels
    metrics = artifacts.metrics
    selected_model = metrics.get("selected_model")
log_memory_rss("after_train_stage", sink=memory_events)

pred_df = predict_with_drivers(
    feature_df=infer_input_df,
    model=model,
    feature_columns=feature_columns,
    class_labels=class_labels,
    feature_importance=feature_importance,
    segment_kpis_df=segment_kpi_df if not segment_kpi_df.empty else None,
    top_n_drivers=5,
    top_n_actions=3,
    top_n_target_segments=3,
)

print("predicted rows:", len(pred_df))
log_memory_rss("after_inference_stage", sink=memory_events)

if train_input_df is not infer_input_df:
    del train_input_df
collect_garbage("release_train_frame", sink=memory_events)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 8) Save outputs

# COMMAND ----------

output_dir_local = _dbfs_uri_to_local_path(DBFS_OUTPUT_DIR)
output_dir_local.mkdir(parents=True, exist_ok=True)

write_parquet_chunked(feature_df, output_dir_local / "feature_table.parquet", chunk_rows=100_000)
feature_defs_df.to_csv(output_dir_local / "feature_definitions.csv", index=False)
if "labeled_df" in locals():
    write_parquet_chunked(labeled_df, output_dir_local / "labeled_feature_table.parquet", chunk_rows=100_000)
else:
    # Keep output contract even when labeled_df has been released in sampled mode.
    write_parquet_chunked(infer_input_df, output_dir_local / "labeled_feature_table.parquet", chunk_rows=100_000)

if not segment_kpi_df.empty:
    write_parquet_chunked(segment_kpi_df, output_dir_local / "segment_kpis.parquet", chunk_rows=100_000)

save_predictions(pred_df, output_dir=output_dir_local)

attribution_qa = dict(pred_df.attrs.get("attribution_qa", {}))
(output_dir_local / "attribution_qa.json").write_text(json.dumps(attribution_qa, indent=2), encoding="utf-8")

join_diag_df.to_csv(output_dir_local / "join_diagnostics_catalog.csv", index=False)
time_diag_df.to_csv(output_dir_local / "join_time_ranges_catalog.csv", index=False)

sample_metrics_payload = {
    "source_mode": SOURCE_MODE,
    "snapshot_freq": SNAPSHOT_FREQ,
    "selected_model": selected_model,
    "metrics": metrics,
    "labeling_thresholds": labeling_thresholds(),
    "commerce_joinable": commerce_joinable,
    "activity_enrichment_joinable": activity_enrichment_joinable,
    "rows_scored": int(len(pred_df)),
}

if sample_result is not None:
    save_sample_outputs(sample_result, output_dir=output_dir_local)
    sample_metrics_payload["sampling"] = {
        "mode": sample_mode,
        "config_used": sample_result.config_used,
        "fallback_applied": sample_result.fallback_applied,
        "qa_report": sample_result.qa_report,
    }

dtype_opt_records = (
    dtype_opt_summary.to_dict(orient="records")
    if isinstance(dtype_opt_summary, pd.DataFrame) and not dtype_opt_summary.empty
    else []
)
memory_payload = {
    "memory_optimize": bool(MEMORY_OPTIMIZE),
    "memory_float_downcast": bool(MEMORY_FLOAT_DOWNCAST),
    "memory_cat_ratio_threshold": float(MEMORY_CAT_RATIO_THRESHOLD),
    "memory_validate_downcast": bool(MEMORY_VALIDATE_DOWNCAST),
    "events": memory_events,
    "dtype_optimization": dtype_opt_records,
}
(output_dir_local / "memory_optimization_report.json").write_text(
    json.dumps(memory_payload, indent=2, ensure_ascii=False),
    encoding="utf-8",
)
if not dtype_opt_summary.empty:
    dtype_opt_summary.to_csv(output_dir_local / "memory_dtype_optimization.csv", index=False)

sample_metrics_payload["memory_optimization"] = {
    "enabled": bool(MEMORY_OPTIMIZE),
    "dtype_tables_optimized": int(len(dtype_opt_records)),
    "events": memory_events,
}

(output_dir_local / "pipeline_run_summary.json").write_text(
    json.dumps(sample_metrics_payload, indent=2, ensure_ascii=False),
    encoding="utf-8",
)

# Last-4-window examples for dashboard
examples = (
    pred_df[pred_df["window_size"].astype(str) == "30d"].copy()
    if (pred_df["window_size"].astype(str) == "30d").any()
    else pred_df.copy()
)
examples = examples.sort_values("window_end_date").groupby("brand_id", as_index=False).tail(4)
examples.to_json(output_dir_local / "examples_last4_with_segments.json", orient="records", indent=2, date_format="iso")

print(f"Saved outputs to: {output_dir_local}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 9) Log to MLflow

# COMMAND ----------
from mlflow.models.signature import infer_signature

# End any lingering run
mlflow.end_run()

# Set experiment by id or name/path.
if str(MLFLOW_EXPERIMENT).strip().isdigit():
    mlflow.set_experiment(experiment_id=str(MLFLOW_EXPERIMENT).strip())
else:
    mlflow.set_experiment(experiment_name=str(MLFLOW_EXPERIMENT))

with mlflow.start_run(run_name=MLFLOW_RUN_NAME) as run:

    # --- Params ---
    mlflow.log_params({
        "source_mode": SOURCE_MODE,
        "snapshot_freq": SNAPSHOT_FREQ,
        "skip_train": bool(SKIP_TRAIN),
        "train_sample_mode": sample_mode,
        "n_jobs": int(N_JOBS),
        "train_weight_classes": bool(TRAIN_WEIGHT_CLASSES),
        "selected_model": str(selected_model),
        "rows_scored": int(len(pred_df)),
    })

    # --- Tags ---
    mlflow.set_tags({
        "pipeline": "brand-health",
        "brands": ",".join(sorted(BRAND_APP_ID_FILTERS.keys())),
    })

    # --- Metrics (safe) ---
    if isinstance(metrics, dict):
        time_split = metrics.get("time_split", {})
        if isinstance(time_split, dict):
            for model_name, m in time_split.items():
                if not isinstance(m, dict):
                    continue
                for metric_key in ["macro_f1", "balanced_accuracy",
                                   "weighted_f1", "accuracy"]:
                    val = m.get(metric_key)
                    if val is not None:
                        try:
                            mlflow.log_metric(
                                f"{model_name}_{metric_key}", float(val)
                            )
                        except (ValueError, TypeError):
                            pass

    # --- JSON artifacts ---
    mlflow.log_dict(sample_metrics_payload, "pipeline_run_summary.json")
    mlflow.log_dict(attribution_qa, "attribution_qa.json")
    mlflow.log_dict(memory_payload, "memory_optimization_report.json")

    # --- Model with signature ---
    if not SKIP_TRAIN and model is not None and feature_columns:
        try:
            X_sample = infer_input_df[feature_columns].head(5)
            y_sample = model.predict(X_sample)
            sig = infer_signature(X_sample, y_sample)
        except Exception:
            sig = None

        mlflow.sklearn.log_model(
            model,
            artifact_path="model",
            signature=sig,
        )

    # --- Output files (selective) ---
    if LOG_OUTPUTS_TO_MLFLOW:
        for fname in [
            "feature_definitions.csv",
            "join_diagnostics_catalog.csv",
            "examples_last4_with_segments.json",
        ]:
            fpath = output_dir_local / fname
            if fpath.exists():
                mlflow.log_artifact(str(fpath), artifact_path="outputs")

    print(f"MLflow run logged: {run.info.run_id}")

# Release heavy frames after outputs and logging are complete.
for _name in ["infer_input_df", "feature_df", "segment_kpi_df", "sample_result"]:
    if _name in locals():
        del globals()[_name]
collect_garbage("release_post_mlflow", sink=memory_events)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 10) Preview predictions

# COMMAND ----------

display(
    pred_df[
        [
            "brand_id",
            "window_end_date",
            "window_size",
            "predicted_health_class",
            "predicted_health_score",
            "confidence_band",
            "drivers",
            "target_segments",
            "suggested_actions",
        ]
    ].sort_values(["brand_id", "window_end_date", "window_size"]).tail(30)
)
