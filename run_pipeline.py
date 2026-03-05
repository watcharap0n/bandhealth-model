from __future__ import annotations

import argparse
import json
import os
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

from src.data_load import (
    TABLE_FILES,
    build_purchase_item_join_diagnostics,
    build_purchase_item_join_diagnostics_from_tables,
    profile_loaded_tables,
    profile_dataset,
    save_profile,
    load_tables,
    write_coverage_notes_markdown,
    write_join_diagnostics_markdown,
)
from src.databricks_sql import (
    CATALOG_TABLE_MAP,
    COLUMN_ALIASES,
    DatabricksSQLConfig,
    QuerySelection,
    group_app_ids_by_brand,
    load_tables_from_databricks_sql,
)
from src.databricks_pyspark import load_tables_from_databricks_pyspark
from src.databricks_catalog_publish import publish_kpis_predicted_to_catalog
from src.features import build_feature_table, feature_definitions
from src.infer import load_model_artifacts, load_model_from_mlflow, predict_with_drivers, save_predictions
from src.labeling import generate_weak_labels, labeling_thresholds
from src.sampling import TrainSampleConfig, build_train_eval_samples, save_sample_outputs
from src.segments import compute_segment_kpis
from src.train import train_models
from src.memory_opt import (
    collect_garbage,
    log_memory_rss,
    optimize_table_dict,
    write_parquet_chunked,
)

BRAND_APP_ID_FILTERS = {
    "c-vit": [1993744540760190],
    "see-chan": [838315041537793],
}

COLUMNS_MAP = {
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


def _brand_primary_app_id_map(brand_app_id_filters: Mapping[str, Sequence[int | str]]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for brand_id, app_ids in brand_app_id_filters.items():
        if not app_ids:
            continue
        out[str(brand_id)] = str(app_ids[0])
    return out


def _format_pct(v: float) -> str:
    return f"{v * 100:.2f}%"


def _log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}")


def _stage_done(step_label: str, started_at: float, extra: Optional[str] = None) -> None:
    elapsed = time.perf_counter() - started_at
    if extra:
        _log(f"{step_label} done in {elapsed:.2f}s | {extra}")
    else:
        _log(f"{step_label} done in {elapsed:.2f}s")


def _table_rows_summary(tables: Mapping[str, pd.DataFrame]) -> str:
    parts: List[str] = []
    for table_name in sorted(tables.keys()):
        value = tables.get(table_name)
        if isinstance(value, pd.DataFrame):
            parts.append(f"{table_name}={len(value)}")
    return ", ".join(parts)


def _zero_row_tables_summary(table_profile: pd.DataFrame) -> str:
    if table_profile.empty:
        return ""
    if "rows" not in table_profile.columns or "brand_id" not in table_profile.columns or "table" not in table_profile.columns:
        return ""
    zero_df = table_profile[pd.to_numeric(table_profile["rows"], errors="coerce").fillna(0).eq(0)]
    if zero_df.empty:
        return ""
    parts = [f"{row.brand_id}:{row.table}" for row in zero_df.itertuples()]
    return ", ".join(parts)


def _parse_bool_flag(v) -> bool:
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    return s in {"1", "true", "yes", "y", "on"}


def _parse_csv_cols(v: str) -> List[str]:
    return [x.strip() for x in str(v).split(",") if x.strip()]


def _parse_query_app_ids(v: Optional[str]) -> List[str]:
    if v is None:
        return []
    return [x.strip() for x in str(v).split(",") if x.strip()]


def _parse_brand_aliases(v: Optional[str]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if v is None:
        return out
    for item in str(v).split(","):
        item = item.strip()
        if not item:
            continue
        if "=" not in item:
            raise ValueError(f"Invalid brand alias '{item}'. Expected format app_id=brand_id")
        app_id, brand_id = item.split("=", 1)
        app_key = app_id.strip()
        brand_key = brand_id.strip()
        if not app_key or not brand_key:
            raise ValueError(f"Invalid brand alias '{item}'. Expected format app_id=brand_id")
        out[app_key] = brand_key
    return out


def _parse_optional_date(v: Optional[str], field_name: str) -> Optional[date]:
    if v is None:
        return None
    raw = str(v).strip()
    if not raw:
        return None
    try:
        return datetime.strptime(raw, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(f"{field_name} must be in YYYY-MM-DD format") from exc


def _runtime_brand_app_id_filters(
    query_app_ids: Sequence[str],
    brand_aliases: Mapping[str, str],
) -> Dict[str, List[str]]:
    return group_app_ids_by_brand(app_ids=query_app_ids, brand_aliases=brand_aliases)


def _default_brand_app_id_filters() -> Dict[str, List[str]]:
    return {str(brand_id): [str(app_id) for app_id in app_ids] for brand_id, app_ids in BRAND_APP_ID_FILTERS.items()}


def _flatten_runtime_app_ids(runtime_filters: Mapping[str, Sequence[str]]) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for brand_id in sorted(runtime_filters.keys()):
        for app_id in runtime_filters.get(brand_id, []):
            key = str(app_id).strip()
            if not key or key in seen:
                continue
            seen.add(key)
            out.append(key)
    return out


def _build_brand_aliases_from_runtime_filters(runtime_filters: Mapping[str, Sequence[str]]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for brand_id, app_ids in runtime_filters.items():
        for app_id in app_ids:
            key = str(app_id).strip()
            if not key:
                continue
            out[key] = str(brand_id)
    return out


def _env_or_value(explicit_value: Optional[str], env_name: str, default: Optional[str] = None) -> Optional[str]:
    if explicit_value is not None and str(explicit_value).strip():
        return str(explicit_value).strip()
    env_value = os.getenv(env_name)
    if env_value is not None and str(env_value).strip():
        return str(env_value).strip()
    return default


def _resolve_source_runtime(
    args: argparse.Namespace,
) -> Tuple[Dict[str, List[str]], Optional[DatabricksSQLConfig], Optional[QuerySelection]]:
    query_app_ids = _parse_query_app_ids(getattr(args, "query_app_ids", None))
    brand_aliases = _parse_brand_aliases(getattr(args, "brand_aliases", None))
    source_mode = str(getattr(args, "source_mode", "parquet")).strip().lower()

    runtime_filters = _runtime_brand_app_id_filters(query_app_ids, brand_aliases) if query_app_ids else _default_brand_app_id_filters()
    if source_mode == "parquet":
        return runtime_filters, None, None

    start_date = _parse_optional_date(getattr(args, "query_start_date", None), "query_start_date")
    end_date = _parse_optional_date(getattr(args, "query_end_date", None), "query_end_date")
    if start_date is not None and end_date is not None and start_date > end_date:
        raise ValueError("query_start_date cannot be later than query_end_date")

    if source_mode == "databricks_pyspark":
        selection_app_ids = list(query_app_ids) if query_app_ids else _flatten_runtime_app_ids(runtime_filters)
        if not selection_app_ids:
            raise ValueError("Missing required app_id filters for databricks_pyspark mode")
        selection = QuerySelection(
            app_ids=tuple(selection_app_ids),
            brand_aliases=_build_brand_aliases_from_runtime_filters(runtime_filters),
            start_date=start_date,
            end_date=end_date,
        )
        return runtime_filters, None, selection

    missing: List[str] = []
    host = _env_or_value(getattr(args, "databricks_host", None), "DATABRICKS_HOST")
    token = _env_or_value(getattr(args, "databricks_token", None), "DATABRICKS_TOKEN")
    warehouse_id = _env_or_value(getattr(args, "databricks_warehouse_id", None), "DATABRICKS_WAREHOUSE_ID")
    catalog = _env_or_value(getattr(args, "databricks_catalog", None), "DATABRICKS_CATALOG", default="projects_prd")
    database = _env_or_value(getattr(args, "databricks_database", None), "DATABRICKS_DATABASE", default="datacleansing")

    if not query_app_ids:
        missing.append("query_app_ids")
    if not host:
        missing.append("databricks_host")
    if not token:
        missing.append("databricks_token")
    if not warehouse_id:
        missing.append("databricks_warehouse_id")
    if missing:
        raise ValueError(f"Missing required arguments for databricks_sql mode: {', '.join(missing)}")

    config = DatabricksSQLConfig(
        host=str(host),
        token=str(token),
        warehouse_id=str(warehouse_id),
        catalog=str(catalog),
        database=str(database),
        wait_timeout=str(getattr(args, "databricks_wait_timeout", "30s")),
        poll_interval_seconds=int(getattr(args, "databricks_poll_interval_seconds", 2)),
    )
    selection = QuerySelection(
        app_ids=tuple(query_app_ids),
        brand_aliases=dict(brand_aliases),
        start_date=start_date,
        end_date=end_date,
    )
    return runtime_filters, config, selection


def _resolve_mlflow_runtime(args: argparse.Namespace) -> Dict[str, object]:
    enabled_raw = _env_or_value(getattr(args, "mlflow_enable", None), "MLFLOW_ENABLE", default="false")
    enabled = _parse_bool_flag(enabled_raw)
    experiment = _env_or_value(getattr(args, "mlflow_experiment", None), "MLFLOW_EXPERIMENT")
    run_name = _env_or_value(getattr(args, "mlflow_run_name", None), "MLFLOW_RUN_NAME")
    log_outputs_raw = _env_or_value(getattr(args, "mlflow_log_outputs", None), "MLFLOW_LOG_OUTPUTS", default="true")
    log_outputs = _parse_bool_flag(log_outputs_raw)

    if enabled and not experiment:
        raise ValueError("mlflow_experiment is required when MLflow logging is enabled")
    if enabled and not run_name:
        run_name = f"brand-health-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}"

    return {
        "enabled": bool(enabled),
        "experiment": str(experiment) if experiment else None,
        "run_name": str(run_name) if run_name else None,
        "log_outputs": bool(log_outputs),
    }


def _resolve_model_runtime(args: argparse.Namespace) -> Dict[str, str]:
    model_source = str(getattr(args, "model_source", "artifacts")).strip().lower()
    if model_source not in {"artifacts", "mlflow"}:
        raise ValueError("model_source must be one of: artifacts, mlflow")

    mlflow_model_uri = _env_or_value(getattr(args, "mlflow_model_uri", None), "MLFLOW_MODEL_URI")
    mlflow_registry_uri = _env_or_value(
        getattr(args, "mlflow_registry_uri", None),
        "MLFLOW_REGISTRY_URI",
        default="databricks",
    )

    if bool(getattr(args, "skip_train", False)) and model_source == "mlflow" and not mlflow_model_uri:
        raise ValueError("mlflow_model_uri is required when --skip-train is used with --model-source mlflow")

    return {
        "source": model_source,
        "mlflow_model_uri": str(mlflow_model_uri) if mlflow_model_uri else "",
        "mlflow_registry_uri": str(mlflow_registry_uri or "databricks"),
    }


def _resolve_publish_runtime(args: argparse.Namespace, source_mode: str) -> Dict[str, object]:
    enabled = _parse_bool_flag(getattr(args, "publish_kpis_predicted", "false"))
    table_name = str(getattr(args, "publish_kpis_table", "projects_prd.marketingautomation.kpis_predicted")).strip()
    write_mode = str(getattr(args, "publish_kpis_write_mode", "overwrite")).strip().lower() or "overwrite"
    fail_on_cast_error = _parse_bool_flag(getattr(args, "publish_kpis_fail_on_cast_error", "true"))

    if enabled and source_mode != "databricks_pyspark":
        raise ValueError("--publish-kpis-predicted requires --source-mode databricks_pyspark")
    if write_mode not in {"overwrite"}:
        raise ValueError("publish_kpis_write_mode must be: overwrite")
    if enabled and not table_name:
        raise ValueError("publish_kpis_table is required when publish_kpis_predicted is enabled")

    return {
        "enabled": bool(enabled),
        "table_name": table_name,
        "write_mode": write_mode,
        "fail_on_cast_error": bool(fail_on_cast_error),
    }


def _log_pipeline_to_mlflow(
    mlflow_cfg: Mapping[str, object],
    args: argparse.Namespace,
    source_mode: str,
    runtime_brand_app_filters: Mapping[str, Sequence[str]],
    selected_model: Optional[str],
    pred_df: pd.DataFrame,
    metrics: Mapping[str, object],
    summary: Mapping[str, object],
    memory_payload: Mapping[str, object],
    attribution_qa: Mapping[str, float],
    outputs_dir: Path,
    reports_dir: Path,
    report_path: Path,
    model,
    feature_columns: Sequence[str],
    model_input_sample: Optional[pd.DataFrame],
) -> None:
    if not bool(mlflow_cfg.get("enabled", False)):
        return

    try:
        import mlflow
        import mlflow.sklearn
        from mlflow.models.signature import infer_signature
    except ImportError as exc:
        raise RuntimeError("MLflow logging is enabled but mlflow is not installed in this environment") from exc

    experiment = str(mlflow_cfg.get("experiment") or "").strip()
    run_name = str(mlflow_cfg.get("run_name") or "").strip() or None
    log_outputs = bool(mlflow_cfg.get("log_outputs", True))

    mlflow.end_run()
    if experiment.isdigit():
        mlflow.set_experiment(experiment_id=experiment)
    else:
        mlflow.set_experiment(experiment_name=experiment)

    with mlflow.start_run(run_name=run_name) as run:
        mlflow.log_params(
            {
                "source_mode": str(source_mode),
                "model_source": str(getattr(args, "model_source", "artifacts")),
                "snapshot_freq": str(args.snapshot_freq),
                "skip_train": bool(args.skip_train),
                "train_sample_mode": str(args.train_sample_mode),
                "n_jobs": int(args.n_jobs),
                "train_weight_classes": bool(_parse_bool_flag(args.train_weight_classes)),
                "selected_model": str(selected_model or ""),
                "rows_scored": int(len(pred_df)),
                "feature_count": int(len(feature_columns)),
                "memory_optimize": bool(_parse_bool_flag(args.memory_optimize)),
                "brand_count": int(len(runtime_brand_app_filters)),
                "query_start_date": str(getattr(args, "query_start_date", "") or ""),
                "query_end_date": str(getattr(args, "query_end_date", "") or ""),
            }
        )

        brand_keys = ",".join(sorted(str(k) for k in runtime_brand_app_filters.keys()))
        app_values = sorted(
            {
                str(app_id)
                for app_ids in runtime_brand_app_filters.values()
                for app_id in app_ids
            }
        )
        mlflow.set_tags(
            {
                "pipeline": "brand-health",
                "brands": brand_keys,
                "app_ids": ",".join(app_values),
            }
        )

        time_split = metrics.get("time_split", {}) if isinstance(metrics, Mapping) else {}
        if isinstance(time_split, Mapping):
            for model_name, model_metrics in time_split.items():
                if not isinstance(model_metrics, Mapping):
                    continue
                for metric_key in ("macro_f1", "balanced_accuracy", "weighted_f1"):
                    val = model_metrics.get(metric_key)
                    if val is None:
                        continue
                    try:
                        mlflow.log_metric(f"{model_name}_{metric_key}", float(val))
                    except (TypeError, ValueError):
                        continue

        mlflow.log_dict(dict(summary), "pipeline_summary.json")
        mlflow.log_dict(dict(attribution_qa), "attribution_qa.json")
        mlflow.log_dict(dict(memory_payload), "memory_optimization_report.json")

        if not args.skip_train and model is not None and feature_columns:
            signature = None
            if model_input_sample is not None and not model_input_sample.empty:
                try:
                    y_sample = model.predict(model_input_sample)
                    signature = infer_signature(model_input_sample, y_sample)
                except Exception:
                    signature = None
            mlflow.sklearn.log_model(
                model,
                artifact_path="model",
                signature=signature,
            )
            model_metadata_path = Path(args.artifacts_dir) / "model_metadata.json"
            feature_importance_path = Path(args.artifacts_dir) / "feature_importance.json"
            if model_metadata_path.exists():
                mlflow.log_artifact(str(model_metadata_path), artifact_path="model")
            if feature_importance_path.exists():
                mlflow.log_artifact(str(feature_importance_path), artifact_path="model")

        if log_outputs:
            artifact_targets = [
                (outputs_dir / "feature_definitions.csv", "outputs"),
                (outputs_dir / "join_diagnostics.md", "outputs"),
                (outputs_dir / "coverage_notes.md", "outputs"),
                (outputs_dir / "attribution_qa.json", "outputs"),
                (outputs_dir / "memory_optimization_report.json", "outputs"),
                (outputs_dir / "examples_last4_with_segments.json", "outputs"),
                (outputs_dir / "predictions_with_drivers.csv", "outputs"),
                (outputs_dir / "predictions_with_drivers.jsonl", "outputs"),
                (reports_dir / "pipeline_summary.json", "reports"),
                (report_path, "reports"),
            ]
            for artifact_path, artifact_subdir in artifact_targets:
                if artifact_path.exists():
                    mlflow.log_artifact(str(artifact_path), artifact_path=artifact_subdir)

        print(f"MLflow run logged: {run.info.run_id}")


def _set_thread_limits(n_jobs: int) -> None:
    n = max(1, int(n_jobs))
    for key in [
        "OMP_NUM_THREADS",
        "OPENBLAS_NUM_THREADS",
        "MKL_NUM_THREADS",
        "NUMEXPR_NUM_THREADS",
        "VECLIB_MAXIMUM_THREADS",
    ]:
        os.environ[key] = str(n)


def _markdown_table(df: pd.DataFrame) -> str:
    if df.empty:
        return "_No rows_"

    cols = [str(c) for c in df.columns]
    rendered = df.copy()
    for c in rendered.columns:
        rendered[c] = rendered[c].apply(lambda x: "" if pd.isna(x) else str(x))

    widths = {c: max(len(c), rendered[c].map(len).max()) for c in cols}
    header = "| " + " | ".join(c.ljust(widths[c]) for c in cols) + " |"
    sep = "| " + " | ".join("-" * widths[c] for c in cols) + " |"
    body = [
        "| " + " | ".join(str(rendered.iloc[i, j]).ljust(widths[cols[j]]) for j in range(len(cols))) + " |"
        for i in range(len(rendered))
    ]
    return "\n".join([header, sep] + body)



def _write_markdown_report(
    report_path: Path,
    table_profile: pd.DataFrame,
    join_coverage: pd.DataFrame,
    feature_defs: pd.DataFrame,
    thresholds: Dict,
    metrics: Dict,
    examples: pd.DataFrame,
    before_after_examples: Optional[List[dict]] = None,
    attribution_qa: Optional[Mapping[str, float]] = None,
) -> None:
    lines: List[str] = []
    lines.append("# Brand Health Modeling Report")
    lines.append("")

    lines.append("## 1) Data Joins + Coverage Summary")
    lines.append("")
    lines.append("### Table row counts")
    lines.append("")
    lines.append(_markdown_table(table_profile))
    lines.append("")

    lines.append("### Join key coverage")
    lines.append("")
    lines.append(_markdown_table(join_coverage))
    lines.append("")

    lines.append("### Coverage notes")
    lines.append("")
    for brand, sub in join_coverage.groupby("brand_id"):
        p2i = sub[(sub["left_table"] == "purchase") & (sub["right_table"] == "purchase_items")]
        tx = p2i[p2i["key"] == "transaction_id"]
        uid = p2i[p2i["key"] == "user_id"]

        if not tx.empty:
            tx_cov_raw = float(tx["row_coverage"].iloc[0])
            tx_cov_norm = float(tx["row_coverage_norm"].iloc[0]) if "row_coverage_norm" in tx.columns else tx_cov_raw
            status = "valid" if tx_cov_norm >= 0.80 else "not valid"
            lines.append(
                f"- **{brand}**: canonical `purchase↔purchase_items` join key is `transaction_id` "
                f"(raw={tx_cov_raw:.4f}, normalized={tx_cov_norm:.4f}, {status})."
            )
        if not uid.empty:
            uid_cov_raw = float(uid["row_coverage"].iloc[0])
            uid_cov_norm = float(uid["row_coverage_norm"].iloc[0]) if "row_coverage_norm" in uid.columns else uid_cov_raw
            lines.append(
                f"- **{brand}**: `purchase_items.user_id` coverage is low "
                f"(raw={uid_cov_raw:.4f}, normalized={uid_cov_norm:.4f}); ignored for purchase↔items joins."
            )

        other = sub[
            ~((sub["left_table"] == "purchase") & (sub["right_table"] == "purchase_items"))
            & (sub["row_coverage_norm"].fillna(sub["row_coverage"].fillna(0.0)) < 0.5)
        ]
        if not other.empty:
            rels = ", ".join(
                [
                    f"{r.left_table}->{r.right_table}.{r.key} "
                    f"({float(r.row_coverage_norm if pd.notna(r.row_coverage_norm) else r.row_coverage):.4f})"
                    for r in other.itertuples()
                ]
            )
            lines.append(f"- **{brand}**: additional low-coverage relations: {rels}.")
        lines.append("")

    lines.append("## 2) Feature Table Schema")
    lines.append("")
    lines.append("Feature names and meanings are exported to `outputs/feature_definitions.csv`.")
    lines.append("")
    lines.append(_markdown_table(feature_defs))
    lines.append("")

    lines.append("## 3) Weak Labeling Logic")
    lines.append("")
    lines.append("Brand health score starts at 100 and subtracts penalties from multi-metric degradation signals:")
    lines.append("- Active users WoW drop")
    lines.append("- Completion rate WoW drop")
    lines.append("- GMV and transaction WoW drop")
    lines.append("- Dormant share WoW increase")
    lines.append("- Efficiency drop (points/reward pressure up while conversion efficiency down)")
    lines.append("- Additional baseline-relative penalties from rolling z-scores")
    lines.append("")
    lines.append(f"Threshold config: `{json.dumps(thresholds, indent=2)}`")
    lines.append("")
    lines.append("Class mapping:")
    lines.append("- Healthy: score >= 70")
    lines.append("- Warning: 45 <= score < 70")
    lines.append("- AtRisk: score < 45")
    lines.append("")

    lines.append("## 4) Model Selection + Evaluation")
    lines.append("")
    lines.append("Selected model based on time-split macro F1: **{}**".format(metrics.get("selected_model", "NA")))
    lines.append("")

    lines.append("### Time-based split results")
    lines.append("")
    time_rows = []
    for model_name, res in metrics.get("time_split", {}).items():
        time_rows.append(
            {
                "model": model_name,
                "macro_f1": res.get("macro_f1"),
                "balanced_accuracy": res.get("balanced_accuracy"),
                "calibration_ece": res.get("calibration", {}).get("calibration_ece"),
                "brier": res.get("calibration", {}).get("brier"),
            }
        )
    if time_rows:
        lines.append(_markdown_table(pd.DataFrame(time_rows)))
    lines.append("")

    lines.append("### Cross-brand holdout (train other brand, test holdout)")
    lines.append("")
    cross_rows = []
    for holdout, res in metrics.get("cross_brand", {}).items():
        if res.get("status") == "skipped":
            cross_rows.append(
                {
                    "holdout_brand": holdout,
                    "macro_f1": np.nan,
                    "balanced_accuracy": np.nan,
                    "note": res.get("reason"),
                }
            )
        else:
            cross_rows.append(
                {
                    "holdout_brand": holdout,
                    "macro_f1": res.get("macro_f1"),
                    "balanced_accuracy": res.get("balanced_accuracy"),
                    "note": "ok",
                }
            )
    if cross_rows:
        lines.append(_markdown_table(pd.DataFrame(cross_rows)))
    lines.append("")

    lines.append("### GroupKFold by brand")
    lines.append("")
    gk = metrics.get("group_kfold_brand", [])
    if gk:
        gk_rows = [{"fold": r.get("fold"), "macro_f1": r.get("macro_f1"), "balanced_accuracy": r.get("balanced_accuracy")} for r in gk]
        lines.append(_markdown_table(pd.DataFrame(gk_rows)))
    else:
        lines.append("No GroupKFold results available.")
    lines.append("")

    lines.append("## 5) Example Output (Last 4 Windows Per Brand)")
    lines.append("")

    for brand, sub in examples.groupby("brand_id"):
        lines.append(f"### Brand: {brand}")
        lines.append("")
        for _, row in sub.sort_values("window_end_date").iterrows():
            lines.append("```json")
            payload = {
                "brand_id": row["brand_id"],
                "window_end_date": str(row["window_end_date"]),
                "window_size": row["window_size"],
                "predicted_health_class": row["predicted_health_class"],
                "predicted_health_class_i18n": row.get("predicted_health_class_i18n", {"en": str(row["predicted_health_class"]), "th": str(row["predicted_health_class"])}),
                "predicted_health_statement": row.get("predicted_health_statement", row["predicted_health_class"]),
                "predicted_health_statement_i18n": row.get("predicted_health_statement_i18n", {"en": str(row.get("predicted_health_statement", row["predicted_health_class"])), "th": str(row.get("predicted_health_statement", row["predicted_health_class"]))}),
                "predicted_health_score": float(row["predicted_health_score"]),
                "confidence_band": row.get("confidence_band", "low"),
                "confidence_band_i18n": row.get("confidence_band_i18n", {"en": str(row.get("confidence_band", "low")), "th": str(row.get("confidence_band", "low"))}),
                "probabilities": {k.replace("prob_", ""): float(row[k]) for k in row.index if k.startswith("prob_")},
                "drivers": row["drivers"],
                "target_segments": row.get("target_segments", []),
                "suggested_actions": row["suggested_actions"],
                "suggested_actions_i18n": row.get("suggested_actions_i18n", []),
            }
            lines.append(json.dumps(payload, ensure_ascii=False, indent=2))
            lines.append("```")
            lines.append("")

    lines.append("## 6) Attribution QA")
    lines.append("")
    if attribution_qa:
        qa_items = [
            {"metric": k, "value": v}
            for k, v in sorted(attribution_qa.items())
            if k
        ]
        lines.append(_markdown_table(pd.DataFrame(qa_items)))
    else:
        lines.append("No attribution QA summary available.")
    lines.append("")

    lines.append("## 7) Before/After Target Segments (2 windows per brand)")
    lines.append("")
    if before_after_examples:
        for ex in before_after_examples:
            lines.append("```json")
            lines.append(json.dumps(ex, ensure_ascii=False, indent=2))
            lines.append("```")
            lines.append("")
    else:
        lines.append("No before/after examples available.")
        lines.append("")

    report_path.write_text("\n".join(lines), encoding="utf-8")


def _compute_activity_enrichment_joinable(
    join_coverage: pd.DataFrame,
    threshold: float = 0.80,
) -> Dict[str, bool]:
    out: Dict[str, bool] = {}
    if join_coverage.empty:
        return out

    for brand_id, sub in join_coverage.groupby("brand_id"):
        view_row = sub[
            (sub["left_table"] == "activity_transaction")
            & (sub["right_table"] == "user_view")
            & (sub["key"] == "user_id")
        ]
        visitor_row = sub[
            (sub["left_table"] == "activity_transaction")
            & (sub["right_table"] == "user_visitor")
            & (sub["key"] == "user_id")
        ]

        view_cov = float(view_row["row_coverage_norm"].iloc[0]) if not view_row.empty and "row_coverage_norm" in view_row.columns else (
            float(view_row["row_coverage"].iloc[0]) if not view_row.empty else np.nan
        )
        visitor_cov = float(visitor_row["row_coverage_norm"].iloc[0]) if not visitor_row.empty and "row_coverage_norm" in visitor_row.columns else (
            float(visitor_row["row_coverage"].iloc[0]) if not visitor_row.empty else np.nan
        )
        ok_view = (not pd.isna(view_cov)) and view_cov >= threshold
        ok_visitor = (not pd.isna(visitor_cov)) and visitor_cov >= threshold
        out[str(brand_id)] = bool(ok_view and ok_visitor)

    return out


def _build_before_after_examples(
    before_examples: Optional[pd.DataFrame],
    after_examples: pd.DataFrame,
    n_per_brand: int = 2,
) -> List[dict]:
    if after_examples.empty:
        return []

    def _norm_key(brand_id, window_end_date, window_size) -> tuple:
        ts = pd.to_datetime(window_end_date, errors="coerce", utc=True)
        ts_key = ts.isoformat() if pd.notna(ts) else str(window_end_date)
        return str(brand_id), ts_key, str(window_size)

    before_idx: Dict[tuple, dict] = {}
    if isinstance(before_examples, pd.DataFrame) and not before_examples.empty:
        for _, r in before_examples.iterrows():
            key = _norm_key(r.get("brand_id"), r.get("window_end_date"), r.get("window_size"))
            before_idx[key] = r.to_dict()

    out: List[dict] = []
    for brand_id, sub in after_examples.groupby("brand_id"):
        rows = sub.sort_values("window_end_date").tail(n_per_brand)
        for _, r in rows.iterrows():
            key = _norm_key(r.get("brand_id"), r.get("window_end_date"), r.get("window_size"))
            b = before_idx.get(key, {})
            out.append(
                {
                    "brand_id": str(r.get("brand_id")),
                    "window_end_date": str(r.get("window_end_date")),
                    "window_size": str(r.get("window_size")),
                    "before": {
                        "predicted_health_class": b.get("predicted_health_class"),
                        "target_segments": b.get("target_segments", []),
                    },
                    "after": {
                        "predicted_health_class": r.get("predicted_health_class"),
                        "target_segments": r.get("target_segments", []),
                    },
                }
            )
    return out


def _write_profiling_report(
    output_path: Path,
    join_diag_df: pd.DataFrame,
    time_diag_df: pd.DataFrame,
    commerce_joinable: Dict[str, bool],
) -> None:
    lines: List[str] = []
    lines.append("# Profiling Report")
    lines.append("")
    lines.append("## Commerce Join Guardrails")
    lines.append("")

    for brand_id in sorted(commerce_joinable.keys()):
        joinable = bool(commerce_joinable.get(brand_id, False))
        tx = join_diag_df[(join_diag_df["brand_id"] == brand_id) & (join_diag_df["key"] == "transaction_id")]
        cov_norm = float(tx["row_coverage_norm"].iloc[0]) if not tx.empty else np.nan
        t = time_diag_df[time_diag_df["brand_id"] == brand_id]
        overlap = bool(t["time_range_overlap"].iloc[0]) if not t.empty else False

        lines.append(f"- brand={brand_id} coverage_norm={cov_norm:.4f} time_overlap={overlap} commerce_joinable={joinable}")
        if (pd.isna(cov_norm) or cov_norm < 0.80) or (not overlap):
            lines.append(
                f"  WARNING: join coverage/time overlap failed for {brand_id}; "
                "using purchase-only commerce metrics + purchase_items-only SKU metrics (no cross-table join)."
            )
    lines.append("")
    output_path.write_text("\n".join(lines), encoding="utf-8")

def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Brand Health Pipeline")
    parser.add_argument("--dataset-root", type=str, default="datasets")
    parser.add_argument("--reports-dir", type=str, default="reports")
    parser.add_argument("--outputs-dir", type=str, default="outputs")
    parser.add_argument("--artifacts-dir", type=str, default="artifacts")
    parser.add_argument(
        "--source-mode",
        type=str,
        default="parquet",
        choices=["parquet", "databricks_sql", "databricks_pyspark"],
    )
    parser.add_argument("--query-app-ids", type=str, default=None)
    parser.add_argument("--brand-aliases", type=str, default=None)
    parser.add_argument("--query-start-date", type=str, default=None)
    parser.add_argument("--query-end-date", type=str, default=None)
    parser.add_argument("--databricks-host", type=str, default=None)
    parser.add_argument("--databricks-token", type=str, default=None)
    parser.add_argument("--databricks-warehouse-id", type=str, default=None)
    parser.add_argument("--databricks-catalog", type=str, default="projects_prd")
    parser.add_argument("--databricks-database", type=str, default="datacleansing")
    parser.add_argument("--databricks-wait-timeout", type=str, default="30s")
    parser.add_argument("--databricks-poll-interval-seconds", type=int, default=2)
    parser.add_argument("--databricks-spark-recent-days", type=int, default=0)
    parser.add_argument("--databricks-spark-max-rows-per-table", type=int, default=0)
    parser.add_argument("--databricks-spark-explain-pushdown", type=str, default="false")
    parser.add_argument("--publish-kpis-predicted", type=str, default="false")
    parser.add_argument("--publish-kpis-table", type=str, default="projects_prd.marketingautomation.kpis_predicted")
    parser.add_argument("--publish-kpis-write-mode", type=str, choices=["overwrite"], default="overwrite")
    parser.add_argument("--publish-kpis-fail-on-cast-error", type=str, default="true")
    parser.add_argument("--mlflow-enable", type=str, default="false")
    parser.add_argument("--mlflow-experiment", type=str, default=None)
    parser.add_argument("--mlflow-run-name", type=str, default=None)
    parser.add_argument("--mlflow-log-outputs", type=str, default="true")
    parser.add_argument("--model-source", type=str, default="artifacts", choices=["artifacts", "mlflow"])
    parser.add_argument("--mlflow-model-uri", type=str, default=None)
    parser.add_argument("--mlflow-registry-uri", type=str, default="databricks")
    parser.add_argument("--snapshot-freq", type=str, default="7D")
    parser.add_argument("--report-name", type=str, default="brand_health_report.md")
    parser.add_argument("--skip-train", action="store_true", help="Skip training and reuse model artifacts.")
    parser.add_argument("--train_sample_mode", type=str, default="off", choices=["off", "quick", "smart"])
    parser.add_argument("--train_sample_seed", type=int, default=42)
    parser.add_argument("--train_sample_frac", type=float, default=0.02)
    parser.add_argument("--train_max_train_rows", type=int, default=200000)
    parser.add_argument("--train_max_eval_rows", type=int, default=60000)
    parser.add_argument("--train_recent_days", type=int, default=180)
    parser.add_argument("--train_stratify_cols", type=str, default="brand_id,predicted_health_class")
    parser.add_argument("--train_group_col", type=str, default="brand_id")
    parser.add_argument("--train_weight_classes", type=str, default="true")
    parser.add_argument("--n_jobs", type=int, default=4)
    parser.add_argument("--memory_optimize", type=str, default="true")
    parser.add_argument("--memory_float_downcast", type=str, default="false")
    parser.add_argument("--memory_cat_ratio_threshold", type=float, default=0.5)
    parser.add_argument("--memory_validate_downcast", type=str, default="true")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    pipeline_started_at = time.perf_counter()
    pipeline_started_dt = datetime.now()

    dataset_root = Path(args.dataset_root)
    reports_dir = Path(args.reports_dir)
    outputs_dir = Path(args.outputs_dir)
    artifacts_dir = Path(args.artifacts_dir)
    source_mode = str(args.source_mode).strip().lower()
    publish_cfg = _resolve_publish_runtime(args, source_mode=source_mode)
    runtime_brand_app_filters, databricks_cfg, query_selection = _resolve_source_runtime(args)
    mlflow_cfg = _resolve_mlflow_runtime(args)
    model_cfg = _resolve_model_runtime(args)

    reports_dir.mkdir(parents=True, exist_ok=True)
    outputs_dir.mkdir(parents=True, exist_ok=True)
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    _set_thread_limits(args.n_jobs)
    memory_events: List[Dict[str, object]] = []
    log_memory_rss("pipeline_start", sink=memory_events)

    memory_optimize = _parse_bool_flag(args.memory_optimize)
    memory_float_downcast = _parse_bool_flag(args.memory_float_downcast)
    memory_validate_downcast = _parse_bool_flag(args.memory_validate_downcast)

    before_examples_path = outputs_dir / "examples_last4_with_segments.json"
    before_examples = pd.DataFrame()
    if before_examples_path.exists():
        try:
            before_examples = pd.read_json(before_examples_path)
        except ValueError:
            before_examples = pd.DataFrame()

    _log(
        "Pipeline start | "
        f"started_at={pipeline_started_dt.strftime('%Y-%m-%d %H:%M:%S')} "
        f"source_mode={source_mode} skip_train={bool(args.skip_train)} "
        f"sample_mode={str(args.train_sample_mode).strip().lower()} n_jobs={int(args.n_jobs)}"
    )
    _log(
        f"Paths | dataset_root={dataset_root} reports_dir={reports_dir} outputs_dir={outputs_dir} artifacts_dir={artifacts_dir}"
    )
    _log(f"Using app_id filters: {runtime_brand_app_filters}")
    _log(
        "Catalog publish config | "
        f"enabled={publish_cfg['enabled']} table={publish_cfg['table_name']} mode={publish_cfg['write_mode']}"
    )
    if source_mode in {"databricks_sql", "databricks_pyspark"}:
        if query_selection is None:
            raise ValueError(f"{source_mode} configuration is not available")
        db_query_range = f"{query_selection.start_date or '-inf'} -> {query_selection.end_date or '+inf'}"
        if source_mode == "databricks_sql":
            if databricks_cfg is None:
                raise ValueError("Databricks SQL configuration is not available")
            _log(
                f"Databricks source | host={databricks_cfg.base_url} catalog={databricks_cfg.catalog} "
                f"database={databricks_cfg.database} app_ids={list(query_selection.app_ids)} date_range={db_query_range}"
            )
            step_started_at = time.perf_counter()
            print("[0/7] Loading tables from Databricks SQL...")
            tables = load_tables_from_databricks_sql(
                config=databricks_cfg,
                selection=query_selection,
                columns_map=COLUMNS_MAP,
                table_map=CATALOG_TABLE_MAP,
                column_aliases=COLUMN_ALIASES,
            )
            _stage_done("[0/7] Loading tables from Databricks SQL", step_started_at, _table_rows_summary(tables))
        else:
            spark_recent_days = max(0, int(getattr(args, "databricks_spark_recent_days", 0)))
            spark_max_rows = max(0, int(getattr(args, "databricks_spark_max_rows_per_table", 0)))
            spark_explain = _parse_bool_flag(getattr(args, "databricks_spark_explain_pushdown", "false"))
            _log(
                f"Databricks PySpark source | catalog={args.databricks_catalog} database={args.databricks_database} "
                f"app_ids={list(query_selection.app_ids)} date_range={db_query_range} "
                f"recent_days={spark_recent_days} max_rows_per_table={spark_max_rows}"
            )
            step_started_at = time.perf_counter()
            print("[0/7] Loading tables from Databricks PySpark...")
            tables = load_tables_from_databricks_pyspark(
                selection=query_selection,
                columns_map=COLUMNS_MAP,
                catalog=str(args.databricks_catalog),
                database=str(args.databricks_database),
                table_map=CATALOG_TABLE_MAP,
                column_aliases=COLUMN_ALIASES,
                recent_days=spark_recent_days,
                max_rows_per_table=spark_max_rows,
                explain_pushdown=spark_explain,
            )
            _stage_done("[0/7] Loading tables from Databricks PySpark", step_started_at, _table_rows_summary(tables))
        log_memory_rss("after_load_tables", sink=memory_events)

        step_started_at = time.perf_counter()
        print("[1/7] Building join diagnostics...")
        join_diag = build_purchase_item_join_diagnostics_from_tables(tables)
        write_join_diagnostics_markdown(join_diag, outputs_dir / "join_diagnostics.md")
        _stage_done(
            "[1/7] Building join diagnostics",
            step_started_at,
            f"relations={len(join_diag.coverage_summary)}",
        )

        step_started_at = time.perf_counter()
        print("[2/7] Profiling loaded datasets...")
        profile = profile_loaded_tables(tables=tables, table_files=TABLE_FILES)
        _stage_done(
            "[2/7] Profiling loaded datasets",
            step_started_at,
            f"table_profile_rows={len(profile.table_profile)} join_coverage_rows={len(profile.join_coverage)}",
        )
    else:
        step_started_at = time.perf_counter()
        print("[0/7] Building join diagnostics...")
        join_diag = build_purchase_item_join_diagnostics(dataset_root, brand_app_ids=runtime_brand_app_filters)
        write_join_diagnostics_markdown(join_diag, outputs_dir / "join_diagnostics.md")
        _stage_done(
            "[0/7] Building join diagnostics",
            step_started_at,
            f"relations={len(join_diag.coverage_summary)}",
        )

        step_started_at = time.perf_counter()
        print("[1/7] Profiling parquet datasets...")
        profile = profile_dataset(dataset_root, brand_app_ids=runtime_brand_app_filters)
        _stage_done(
            "[1/7] Profiling parquet datasets",
            step_started_at,
            f"table_profile_rows={len(profile.table_profile)} join_coverage_rows={len(profile.join_coverage)}",
        )

        step_started_at = time.perf_counter()
        print("[2/7] Loading tables for feature engineering...")
        tables = load_tables(
            dataset_root,
            table_files=TABLE_FILES,
            columns_map=COLUMNS_MAP,
            brand_app_ids=runtime_brand_app_filters,
        )
        _stage_done("[2/7] Loading tables for feature engineering", step_started_at, _table_rows_summary(tables))
        log_memory_rss("after_load_tables", sink=memory_events)

    save_profile(profile, reports_dir / "data_profile")
    write_coverage_notes_markdown(profile.join_coverage, join_diag, outputs_dir / "coverage_notes.md")
    activity_enrichment_joinable = _compute_activity_enrichment_joinable(profile.join_coverage, threshold=0.80)
    _write_profiling_report(
        outputs_dir / "profiling_report.md",
        join_diag_df=join_diag.coverage_summary,
        time_diag_df=join_diag.time_range_summary,
        commerce_joinable=join_diag.commerce_joinable,
    )
    zero_row_summary = _zero_row_tables_summary(profile.table_profile)
    if zero_row_summary:
        _log(f"Data warning | zero-row tables detected: {zero_row_summary}")

    table_rows_before_opt = {k: int(len(v)) for k, v in tables.items() if isinstance(v, pd.DataFrame)}

    dtype_opt_summary = pd.DataFrame()
    if memory_optimize:
        step_started_at = time.perf_counter()
        tables, dtype_opt_summary = optimize_table_dict(
            tables,
            allow_float_downcast=memory_float_downcast,
            float_rtol=1e-6,
            cat_ratio_threshold=float(args.memory_cat_ratio_threshold),
            protect_columns=("brand_id",),
            validate=memory_validate_downcast,
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
            dtype_opt_summary.to_csv(outputs_dir / "memory_dtype_optimization.csv", index=False)
        _stage_done(
            "Memory optimization",
            step_started_at,
            f"tables_optimized={dtype_opt_summary['table'].nunique() if 'table' in dtype_opt_summary.columns else 0}",
        )
    log_memory_rss("after_optimize_tables", sink=memory_events)

    step_started_at = time.perf_counter()
    print("[3/7] Building features...")
    feature_df = build_feature_table(
        tables=tables,
        snapshot_freq=args.snapshot_freq,
        commerce_joinable_by_brand=join_diag.commerce_joinable,
    )
    if feature_df.empty:
        raise RuntimeError("Feature table is empty; check source timestamps and schema.")
    write_parquet_chunked(feature_df, outputs_dir / "feature_table.parquet", chunk_rows=100_000)
    feature_df.to_csv(outputs_dir / "feature_table_sample.csv", index=False)
    _stage_done(
        "[3/7] Building features",
        step_started_at,
        f"rows={len(feature_df)} cols={len(feature_df.columns)}",
    )
    log_memory_rss("after_build_feature_table", sink=memory_events)

    step_started_at = time.perf_counter()
    print("[4/7] Building segment KPIs for Marketing Automation...")
    segment_kpi_df = compute_segment_kpis(
        tables=tables,
        snapshot_freq=args.snapshot_freq,
        commerce_joinable_by_brand=join_diag.commerce_joinable,
        activity_enrichment_joinable_by_brand=activity_enrichment_joinable,
    )
    if not segment_kpi_df.empty:
        write_parquet_chunked(segment_kpi_df, outputs_dir / "segment_kpis.parquet", chunk_rows=100_000)
        segment_kpi_df.to_csv(outputs_dir / "segment_kpis.csv", index=False)
    _stage_done(
        "[4/7] Building segment KPIs for Marketing Automation",
        step_started_at,
        f"rows={len(segment_kpi_df)} cols={len(segment_kpi_df.columns) if not segment_kpi_df.empty else 0}",
    )
    log_memory_rss("after_build_segment_kpis", sink=memory_events)

    # Release raw event tables as soon as all derived frames are built.
    del tables
    collect_garbage("release_raw_tables", sink=memory_events)

    feature_defs = feature_definitions(feature_df)
    feature_defs.to_csv(outputs_dir / "feature_definitions.csv", index=False)

    step_started_at = time.perf_counter()
    print("[5/7] Generating weak labels...")
    labeled_df = generate_weak_labels(feature_df)
    labeled_df = labeled_df.reset_index(drop=True)
    labeled_df["__row_id"] = np.arange(len(labeled_df), dtype=int)
    write_parquet_chunked(labeled_df, outputs_dir / "labeled_feature_table.parquet", chunk_rows=100_000)
    _stage_done(
        "[5/7] Generating weak labels",
        step_started_at,
        f"rows={len(labeled_df)} cols={len(labeled_df.columns)}",
    )
    log_memory_rss("after_generate_labels", sink=memory_events)

    # No longer needed after labels/definitions are generated.
    del feature_df
    collect_garbage("release_feature_df", sink=memory_events)

    sample_mode = str(args.train_sample_mode).strip().lower()
    sample_result = None
    train_input_df = labeled_df
    infer_input_df = labeled_df
    train_row_ids: Optional[Sequence[int]] = None
    eval_row_ids: Optional[Sequence[int]] = None

    if sample_mode != "off":
        step_started_at = time.perf_counter()
        print("[5.5/7] Building sampled train/eval subsets...")
        sample_cfg = TrainSampleConfig(
            mode=sample_mode,
            seed=int(args.train_sample_seed),
            frac=float(args.train_sample_frac),
            max_train_rows=int(args.train_max_train_rows),
            max_eval_rows=int(args.train_max_eval_rows),
            recent_days=int(args.train_recent_days),
            stratify_cols=tuple(_parse_csv_cols(args.train_stratify_cols)),
            group_col=str(args.train_group_col),
        )
        sample_result = build_train_eval_samples(labeled_df, config=sample_cfg)
        save_sample_outputs(sample_result, output_dir=outputs_dir)

        train_input_df = sample_result.sampled_df
        infer_input_df = sample_result.sampled_df
        train_row_ids = sample_result.train_row_ids
        eval_row_ids = sample_result.eval_row_ids
        print(
            "Sample mode={} train_rows={} eval_rows={} total_sampled={} fallback_applied={}".format(
                sample_mode,
                len(sample_result.sampled_train_df),
                len(sample_result.sampled_eval_df),
                len(sample_result.sampled_df),
                sample_result.fallback_applied,
            )
        )
        print("Sample QA representative_pass={}".format(sample_result.qa_report.get("representative_pass")))
        _stage_done(
            "[5.5/7] Building sampled train/eval subsets",
            step_started_at,
            f"train_rows={len(sample_result.sampled_train_df)} eval_rows={len(sample_result.sampled_eval_df)} sampled_total={len(sample_result.sampled_df)}",
        )

        # In sampled mode, full labeled_df can be released.
        del labeled_df
        collect_garbage("release_labeled_full_after_sampling", sink=memory_events)

    step_started_at = time.perf_counter()
    print("[6/7] Training and evaluating models...")
    log_memory_rss("before_train_stage", sink=memory_events)
    train_weight_classes = _parse_bool_flag(args.train_weight_classes)
    if args.skip_train:
        if model_cfg["source"] == "mlflow":
            loaded = load_model_from_mlflow(
                model_uri=model_cfg["mlflow_model_uri"],
                registry_uri=model_cfg["mlflow_registry_uri"],
            )
        else:
            loaded = load_model_artifacts(artifact_dir=artifacts_dir)
        model = loaded["model"]
        metadata = loaded.get("metadata", {})
        feature_importance = loaded.get("feature_importance", {})
        feature_columns = metadata.get("feature_columns") or loaded.get("feature_columns", [])
        class_labels = metadata.get("class_labels") or loaded.get("class_labels", ["AtRisk", "Healthy", "Warning"])
        if not feature_columns:
            raise ValueError(
                "Loaded model is missing feature_columns metadata. "
                "Use local artifacts or a newer MLflow model that includes model metadata."
            )

        existing_summary = {}
        summary_path = reports_dir / "pipeline_summary.json"
        if summary_path.exists():
            try:
                existing_summary = json.loads(summary_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                existing_summary = {}
        metrics = metadata.get("metrics") or existing_summary.get("metrics", {})
        default_selected = "mlflow_sklearn_model" if model_cfg["source"] == "mlflow" else "hist_gradient_boosting_calibrated"
        selected_model = (
            (metrics.get("selected_model") if isinstance(metrics, Mapping) else None)
            or existing_summary.get("selected_model")
            or default_selected
        )
    else:
        artifacts = train_models(
            train_input_df,
            artifact_dir=artifacts_dir,
            train_row_ids=train_row_ids,
            eval_row_ids=eval_row_ids,
            sample_mode=sample_mode,
            n_jobs=int(args.n_jobs),
            weight_classes=train_weight_classes,
            group_col=str(args.train_group_col),
            quick_top_k_features=80 if sample_mode == "quick" else None,
        )
        model = artifacts.final_model
        feature_importance = artifacts.feature_importance
        feature_columns = artifacts.feature_columns
        class_labels = artifacts.class_labels
        metrics = artifacts.metrics
        selected_model = artifacts.metrics.get("selected_model")
    _stage_done(
        "[6/7] Training and evaluating models",
        step_started_at,
        f"selected_model={selected_model} feature_count={len(feature_columns)}",
    )
    log_memory_rss("after_train_stage", sink=memory_events)

    step_started_at = time.perf_counter()
    print("[7/7] Running inference + drivers + actions...")
    primary_app_id_map = _brand_primary_app_id_map(BRAND_APP_ID_FILTERS)
    pred_df = predict_with_drivers(
        feature_df=infer_input_df,
        model=model,
        feature_columns=feature_columns,
        class_labels=class_labels,
        feature_importance=feature_importance,
        segment_kpis_df=segment_kpi_df if not segment_kpi_df.empty else None,
        brand_primary_app_id_map=primary_app_id_map,
        top_n_drivers=5,
        top_n_actions=3,
        top_n_target_segments=3,
    )
    _stage_done(
        "[7/7] Running inference + drivers + actions",
        step_started_at,
        f"rows_scored={len(pred_df)} columns={len(pred_df.columns)}",
    )
    log_memory_rss("after_inference_stage", sink=memory_events)
    save_predictions(pred_df, output_dir=outputs_dir)
    catalog_publish_summary: Dict[str, object] = {
        "enabled": bool(publish_cfg.get("enabled", False)),
        "table": str(publish_cfg.get("table_name", "")),
        "write_mode": str(publish_cfg.get("write_mode", "overwrite")),
        "rows_written": 0,
        "status": "disabled",
        "error": "",
    }
    if bool(publish_cfg.get("enabled", False)):
        step_started_at = time.perf_counter()
        print("[7.1/7] Publishing predictions to Unity Catalog...")
        try:
            publish_result = publish_kpis_predicted_to_catalog(
                pred_df=pred_df,
                table_name=str(publish_cfg.get("table_name", "")),
                write_mode=str(publish_cfg.get("write_mode", "overwrite")),
                fail_on_cast_error=bool(publish_cfg.get("fail_on_cast_error", True)),
            )
        except Exception as exc:
            catalog_publish_summary["status"] = "failed"
            catalog_publish_summary["error"] = str(exc)
            _stage_done(
                "[7.1/7] Publishing predictions to Unity Catalog",
                step_started_at,
                "failed",
            )
            raise
        catalog_publish_summary["rows_written"] = int(publish_result.get("rows_written", 0))
        catalog_publish_summary["status"] = "success"
        _stage_done(
            "[7.1/7] Publishing predictions to Unity Catalog",
            step_started_at,
            f"rows_written={catalog_publish_summary['rows_written']} table={catalog_publish_summary['table']}",
        )

    attribution_qa = dict(pred_df.attrs.get("attribution_qa", {}))
    (outputs_dir / "attribution_qa.json").write_text(json.dumps(attribution_qa, indent=2), encoding="utf-8")

    mlflow_model_input_sample: Optional[pd.DataFrame] = None
    if not args.skip_train and feature_columns:
        available_cols = [c for c in feature_columns if c in infer_input_df.columns]
        if available_cols:
            mlflow_model_input_sample = infer_input_df[available_cols].head(5).copy()

    # Train/infer inputs can be released after predictions are materialized.
    if train_input_df is infer_input_df:
        del train_input_df, infer_input_df
    else:
        del train_input_df
        del infer_input_df
    if "labeled_df" in locals():
        del labeled_df
    collect_garbage("release_train_infer_frames", sink=memory_events)

    if sample_mode != "off":
        sample_metrics_payload = {
            "train_sample_mode": sample_mode,
            "config": {
                "train_sample_seed": int(args.train_sample_seed),
                "train_sample_frac": float(args.train_sample_frac),
                "train_max_train_rows": int(args.train_max_train_rows),
                "train_max_eval_rows": int(args.train_max_eval_rows),
                "train_recent_days": int(args.train_recent_days),
                "train_stratify_cols": _parse_csv_cols(args.train_stratify_cols),
                "train_group_col": str(args.train_group_col),
                "train_weight_classes": bool(train_weight_classes),
                "n_jobs": int(args.n_jobs),
            },
            "sample_qa": sample_result.qa_report if sample_result is not None else {},
            "metrics": metrics,
            "selected_model": selected_model,
            "feature_count": len(feature_columns),
            "rows_scored": int(len(pred_df)),
        }
        (outputs_dir / "model_metrics_sample.json").write_text(
            json.dumps(sample_metrics_payload, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

    # Last 4 windows per brand (prefer 30d windows for concise dashboard snapshots).
    ex = pred_df[pred_df["window_size"].astype(str) == "30d"].copy()
    if ex.empty:
        ex = pred_df.copy()
    examples = ex.sort_values("window_end_date").groupby("brand_id", as_index=False).tail(4)
    examples.to_json(outputs_dir / "examples_last4_windows.json", orient="records", indent=2, date_format="iso")
    examples.to_json(outputs_dir / "examples_last4_with_segments.json", orient="records", indent=2, date_format="iso")
    if sample_mode != "off":
        examples.to_json(outputs_dir / "predictions_last_windows_sample.json", orient="records", indent=2, date_format="iso")
    before_after_examples = _build_before_after_examples(before_examples=before_examples, after_examples=examples, n_per_brand=2)
    (outputs_dir / "examples_before_after_2windows.json").write_text(
        json.dumps(before_after_examples, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    # Final report.
    report_path = reports_dir / args.report_name
    _write_markdown_report(
        report_path=report_path,
        table_profile=profile.table_profile,
        join_coverage=profile.join_coverage,
        feature_defs=feature_defs,
        thresholds=labeling_thresholds(),
        metrics=metrics,
        examples=examples,
        before_after_examples=before_after_examples,
        attribution_qa=attribution_qa,
    )

    # Also write compact summary JSON for backend usage.
    dtype_opt_records = (
        dtype_opt_summary.to_dict(orient="records")
        if isinstance(dtype_opt_summary, pd.DataFrame) and not dtype_opt_summary.empty
        else []
    )
    log_memory_rss("before_pipeline_summary_write", sink=memory_events)
    memory_payload = {
        "memory_optimize": bool(memory_optimize),
        "memory_float_downcast": bool(memory_float_downcast),
        "memory_cat_ratio_threshold": float(args.memory_cat_ratio_threshold),
        "memory_validate_downcast": bool(memory_validate_downcast),
        "events": memory_events,
        "dtype_optimization": dtype_opt_records,
    }
    (outputs_dir / "memory_optimization_report.json").write_text(
        json.dumps(memory_payload, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    summary = {
        "join_coverage": profile.join_coverage.to_dict(orient="records"),
        "commerce_joinable": join_diag.commerce_joinable,
        "activity_enrichment_joinable": activity_enrichment_joinable,
        "selected_model": selected_model,
        "train_sample_mode": sample_mode,
        "metrics": metrics,
        "feature_count": len(feature_columns),
        "example_count": len(examples),
        "attribution_qa": attribution_qa,
        "catalog_publish": catalog_publish_summary,
        "memory_optimization": {
            "enabled": bool(memory_optimize),
            "dtype_tables_optimized": int(len(dtype_opt_records)),
            "memory_events": memory_events,
        },
    }
    (reports_dir / "pipeline_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

    _log_pipeline_to_mlflow(
        mlflow_cfg=mlflow_cfg,
        args=args,
        source_mode=source_mode,
        runtime_brand_app_filters=runtime_brand_app_filters,
        selected_model=selected_model,
        pred_df=pred_df,
        metrics=metrics if isinstance(metrics, Mapping) else {},
        summary=summary,
        memory_payload=memory_payload,
        attribution_qa=attribution_qa,
        outputs_dir=outputs_dir,
        reports_dir=reports_dir,
        report_path=report_path,
        model=model,
        feature_columns=feature_columns,
        model_input_sample=mlflow_model_input_sample,
    )

    total_elapsed = time.perf_counter() - pipeline_started_at
    _log(
        "Pipeline finished | "
        f"elapsed={total_elapsed:.2f}s selected_model={selected_model} "
        f"rows_scored={len(pred_df)} report={report_path} predictions={outputs_dir / 'predictions_with_drivers.jsonl'}"
    )
    print("Done.")
    print(f"Report: {report_path}")
    print(f"Predictions: {outputs_dir / 'predictions_with_drivers.jsonl'}")


if __name__ == "__main__":
    main()
