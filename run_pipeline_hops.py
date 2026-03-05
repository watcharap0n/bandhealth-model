from __future__ import annotations

import argparse
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, List, Mapping, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

import run_pipeline
from src.data_load import (
    DataProfile,
    JoinDiagnostics,
    TABLE_FILES,
    build_purchase_item_join_diagnostics_from_tables,
    load_tables,
    profile_loaded_tables,
    save_profile,
    write_coverage_notes_markdown,
    write_join_diagnostics_markdown,
)
from src.databricks_catalog_publish import publish_kpis_predicted_to_catalog
from src.databricks_pyspark import load_tables_from_databricks_pyspark
from src.databricks_sql import CATALOG_TABLE_MAP, COLUMN_ALIASES, DatabricksSQLConfig, QuerySelection, load_tables_from_databricks_sql
from src.features import build_feature_table, feature_definitions
from src.infer import load_model_artifacts, load_model_from_mlflow, predict_with_drivers, save_predictions
from src.labeling import generate_weak_labels, labeling_thresholds
from src.memory_opt import optimize_table_dict, write_parquet_chunked
from src.pipeline_checkpoints import CheckpointManager, build_arg_fingerprint, utc_now_iso
from src.sampling import TrainSampleConfig, build_train_eval_samples, save_sample_outputs
from src.segments import compute_segment_kpis
from src.train import train_models


HOP_ORDER: Tuple[str, ...] = (
    "load_tables",
    "join_diagnostics",
    "profile",
    "features",
    "segments",
    "labels",
    "train",
    "infer",
    "publish",
)

HOP_DEPENDENCIES: Dict[str, Tuple[str, ...]] = {
    "load_tables": (),
    "join_diagnostics": ("load_tables",),
    "profile": ("load_tables", "join_diagnostics"),
    "features": ("load_tables", "join_diagnostics"),
    "segments": ("load_tables", "join_diagnostics", "profile"),
    "labels": ("features",),
    "train": ("labels",),
    "infer": ("labels", "segments"),
    "publish": ("infer",),
}

HOP_HELP: Dict[str, str] = {
    "load_tables": "Load source tables and write run-scoped checkpoints.",
    "join_diagnostics": "Build join diagnostics from checkpointed tables.",
    "profile": "Build data profile and coverage notes from checkpointed data.",
    "features": "Build feature table and feature definitions.",
    "segments": "Build segment KPI table.",
    "labels": "Generate weak labels from feature table.",
    "train": "Train models (or validate reusable model with --skip-train).",
    "infer": "Run inference, drivers, examples, report, and summary.",
    "publish": "Publish predictions to Unity Catalog (optional).",
}


@dataclass
class HopRuntime:
    hop_args: argparse.Namespace
    pipeline_args: argparse.Namespace
    source_mode: str
    runtime_brand_app_filters: Dict[str, List[str]]
    databricks_cfg: Optional[DatabricksSQLConfig]
    query_selection: Optional[QuerySelection]
    mlflow_cfg: Dict[str, object]
    model_cfg: Dict[str, str]
    publish_cfg: Dict[str, object]
    dataset_root: Path
    reports_dir: Path
    outputs_dir: Path
    artifacts_dir: Path
    checkpoint: CheckpointManager
    arg_fingerprint: str


StageHandler = Callable[[HopRuntime], List[str]]


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Hop-based Brand Health pipeline runner for Databricks job orchestration.",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="hop", required=True)

    for hop in HOP_ORDER:
        sub = subparsers.add_parser(
            hop,
            help=HOP_HELP[hop],
            description=(
                f"{HOP_HELP[hop]}\n"
                "\n"
                "Pipeline options (dataset/report/model args) are inherited from run_pipeline.py\n"
                "and can be passed directly to this command."
            ),
        )
        sub.add_argument("--run-id", type=str, required=True, help="Run identifier used under checkpoint root.")
        sub.add_argument("--checkpoint-root", type=str, default="outputs/checkpoints", help="Checkpoint root directory.")
        sub.add_argument("--auto-upstream", action="store_true", help="Auto-run missing upstream hops.")
        sub.add_argument("--force", action="store_true", help="Force rerun of the target hop only.")
    return parser


def _parse_args(argv: Optional[Sequence[str]] = None) -> Tuple[argparse.Namespace, argparse.Namespace]:
    hop_parser = build_arg_parser()
    hop_args, pipeline_argv = hop_parser.parse_known_args(argv)

    pipeline_parser = run_pipeline.build_arg_parser()
    pipeline_args = pipeline_parser.parse_args(pipeline_argv)
    return hop_args, pipeline_args


def _arg_fingerprint_payload(
    pipeline_args: argparse.Namespace,
    runtime_brand_app_filters: Mapping[str, Sequence[str]],
    source_mode: str,
) -> Dict[str, object]:
    return {
        "source_mode": str(source_mode),
        "runtime_brand_app_filters": {
            str(k): [str(x) for x in runtime_brand_app_filters.get(k, [])]
            for k in sorted(runtime_brand_app_filters.keys())
        },
        "pipeline_args": {k: vars(pipeline_args)[k] for k in sorted(vars(pipeline_args).keys())},
    }


def _empty_join_diagnostics() -> JoinDiagnostics:
    return JoinDiagnostics(
        coverage_summary=pd.DataFrame(),
        time_range_summary=pd.DataFrame(),
        pattern_summary=pd.DataFrame(),
        sampled_merge_summary=pd.DataFrame(),
        random_samples={},
        commerce_joinable={},
    )


def _empty_profile() -> DataProfile:
    return DataProfile(
        table_profile=pd.DataFrame(),
        schema_profile=pd.DataFrame(),
        join_coverage=pd.DataFrame(),
    )


def _load_before_examples(before_examples_path: Path) -> pd.DataFrame:
    if not before_examples_path.exists():
        return pd.DataFrame()
    try:
        return pd.read_json(before_examples_path)
    except ValueError:
        return pd.DataFrame()


def _build_sample_config(args: argparse.Namespace, sample_mode: str) -> TrainSampleConfig:
    return TrainSampleConfig(
        mode=sample_mode,
        seed=int(args.train_sample_seed),
        frac=float(args.train_sample_frac),
        max_train_rows=int(args.train_max_train_rows),
        max_eval_rows=int(args.train_max_eval_rows),
        recent_days=int(args.train_recent_days),
        stratify_cols=tuple(run_pipeline._parse_csv_cols(args.train_stratify_cols)),
        group_col=str(args.train_group_col),
    )


def _load_model_bundle(rt: HopRuntime) -> Dict[str, object]:
    args = rt.pipeline_args
    if bool(args.skip_train) and rt.model_cfg.get("source") == "mlflow":
        loaded = load_model_from_mlflow(
            model_uri=rt.model_cfg.get("mlflow_model_uri", ""),
            registry_uri=rt.model_cfg.get("mlflow_registry_uri", "databricks"),
        )
        default_selected = "mlflow_sklearn_model"
    else:
        loaded = load_model_artifacts(artifact_dir=rt.artifacts_dir)
        default_selected = "hist_gradient_boosting_calibrated"

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

    summary_path = rt.reports_dir / "pipeline_summary.json"
    existing_summary: Dict[str, object] = {}
    if summary_path.exists():
        try:
            existing_summary = json.loads(summary_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            existing_summary = {}

    metrics = metadata.get("metrics") or existing_summary.get("metrics", {})
    selected_model = (
        (metrics.get("selected_model") if isinstance(metrics, Mapping) else None)
        or existing_summary.get("selected_model")
        or default_selected
    )

    return {
        "model": model,
        "feature_importance": feature_importance,
        "feature_columns": list(feature_columns),
        "class_labels": list(class_labels),
        "metrics": metrics if isinstance(metrics, Mapping) else {},
        "selected_model": str(selected_model),
    }


def _load_infer_input_df(rt: HopRuntime, sample_mode: str) -> Tuple[pd.DataFrame, Dict[str, object]]:
    labeled_path = rt.outputs_dir / "labeled_feature_table.parquet"
    if not labeled_path.exists():
        raise FileNotFoundError(f"Missing labeled feature table: {labeled_path}")

    if sample_mode == "off":
        return pd.read_parquet(labeled_path), {}

    sampled_df = rt.checkpoint.load_sampled_labeled_df()
    if not sampled_df.empty:
        qa_payload = {}
        qa_path = rt.outputs_dir / "sample_qa_report.json"
        if qa_path.exists():
            try:
                qa_payload = json.loads(qa_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                qa_payload = {}
        return sampled_df, qa_payload if isinstance(qa_payload, Mapping) else {}

    labeled_df = pd.read_parquet(labeled_path)
    sample_cfg = _build_sample_config(rt.pipeline_args, sample_mode)
    sample_result = build_train_eval_samples(labeled_df, config=sample_cfg)
    save_sample_outputs(sample_result, output_dir=rt.outputs_dir)
    rt.checkpoint.save_sampled_labeled_df(sample_result.sampled_df)

    sample_payload = {
        "config_used": sample_result.config_used,
        "fallback_applied": sample_result.fallback_applied,
        "qa_report": sample_result.qa_report,
        "train_rows": int(len(sample_result.sampled_train_df)),
        "eval_rows": int(len(sample_result.sampled_eval_df)),
        "sampled_total_rows": int(len(sample_result.sampled_df)),
        "source": "rebuilt_in_infer",
    }
    return sample_result.sampled_df, sample_payload


def _stage_load_tables(rt: HopRuntime) -> List[str]:
    args = rt.pipeline_args

    if rt.source_mode in {"databricks_sql", "databricks_pyspark"}:
        if rt.query_selection is None:
            raise ValueError(f"{rt.source_mode} configuration is not available")
        if rt.source_mode == "databricks_sql":
            if rt.databricks_cfg is None:
                raise ValueError("Databricks SQL configuration is not available")
            tables = load_tables_from_databricks_sql(
                config=rt.databricks_cfg,
                selection=rt.query_selection,
                columns_map=run_pipeline.COLUMNS_MAP,
                table_map=CATALOG_TABLE_MAP,
                column_aliases=COLUMN_ALIASES,
            )
        else:
            spark_recent_days = max(0, int(getattr(args, "databricks_spark_recent_days", 0)))
            spark_max_rows = max(0, int(getattr(args, "databricks_spark_max_rows_per_table", 0)))
            spark_explain = run_pipeline._parse_bool_flag(getattr(args, "databricks_spark_explain_pushdown", "false"))
            tables = load_tables_from_databricks_pyspark(
                selection=rt.query_selection,
                columns_map=run_pipeline.COLUMNS_MAP,
                catalog=str(args.databricks_catalog),
                database=str(args.databricks_database),
                table_map=CATALOG_TABLE_MAP,
                column_aliases=COLUMN_ALIASES,
                recent_days=spark_recent_days,
                max_rows_per_table=spark_max_rows,
                explain_pushdown=spark_explain,
            )
    else:
        tables = load_tables(
            rt.dataset_root,
            table_files=TABLE_FILES,
            columns_map=run_pipeline.COLUMNS_MAP,
            brand_app_ids=rt.runtime_brand_app_filters,
        )

    memory_optimize = run_pipeline._parse_bool_flag(args.memory_optimize)
    memory_float_downcast = run_pipeline._parse_bool_flag(args.memory_float_downcast)
    memory_validate_downcast = run_pipeline._parse_bool_flag(args.memory_validate_downcast)

    table_rows_before_opt = {k: int(len(v)) for k, v in tables.items() if isinstance(v, pd.DataFrame)}
    dtype_opt_summary = pd.DataFrame()

    outputs: List[str] = []
    if memory_optimize:
        tables, dtype_opt_summary = optimize_table_dict(
            tables,
            allow_float_downcast=memory_float_downcast,
            float_rtol=1e-6,
            cat_ratio_threshold=float(args.memory_cat_ratio_threshold),
            protect_columns=("brand_id",),
            validate=memory_validate_downcast,
        )
        for k, v in tables.items():
            if not isinstance(v, pd.DataFrame):
                continue
            before_n = table_rows_before_opt.get(k, int(len(v)))
            after_n = int(len(v))
            if before_n != after_n:
                raise AssertionError(
                    f"Row count changed after dtype optimization for table={k}: before={before_n}, after={after_n}"
                )

    outputs.extend(str(p) for p in rt.checkpoint.save_tables(tables))
    rt.checkpoint.write_dataframe("load_tables", "dtype_optimization.parquet", dtype_opt_summary)
    rt.checkpoint.write_json(
        "load_tables",
        "memory_settings.json",
        {
            "memory_optimize": bool(memory_optimize),
            "memory_float_downcast": bool(memory_float_downcast),
            "memory_cat_ratio_threshold": float(args.memory_cat_ratio_threshold),
            "memory_validate_downcast": bool(memory_validate_downcast),
        },
    )
    rt.checkpoint.write_json("load_tables", "table_rows.json", {k: int(len(v)) for k, v in tables.items() if isinstance(v, pd.DataFrame)})

    if isinstance(dtype_opt_summary, pd.DataFrame) and not dtype_opt_summary.empty:
        dtype_path = rt.outputs_dir / "memory_dtype_optimization.csv"
        dtype_opt_summary.to_csv(dtype_path, index=False)
        outputs.append(str(dtype_path))

    return outputs


def _stage_join_diagnostics(rt: HopRuntime) -> List[str]:
    tables = rt.checkpoint.load_tables()
    if not tables:
        raise RuntimeError("No checkpointed tables found. Run 'load_tables' first.")

    join_diag = build_purchase_item_join_diagnostics_from_tables(tables)
    join_md = rt.outputs_dir / "join_diagnostics.md"
    write_join_diagnostics_markdown(join_diag, join_md)

    outputs = [str(join_md)]
    outputs.extend(str(p) for p in rt.checkpoint.save_join_diagnostics(join_diag))
    return outputs


def _stage_profile(rt: HopRuntime) -> List[str]:
    tables = rt.checkpoint.load_tables()
    if not tables:
        raise RuntimeError("No checkpointed tables found. Run 'load_tables' first.")

    join_diag = rt.checkpoint.load_join_diagnostics()
    profile = profile_loaded_tables(tables=tables, table_files=TABLE_FILES)

    save_profile(profile, rt.reports_dir / "data_profile")
    coverage_notes_path = rt.outputs_dir / "coverage_notes.md"
    write_coverage_notes_markdown(profile.join_coverage, join_diag, coverage_notes_path)

    activity_enrichment_joinable = run_pipeline._compute_activity_enrichment_joinable(profile.join_coverage, threshold=0.80)
    profiling_report_path = rt.outputs_dir / "profiling_report.md"
    run_pipeline._write_profiling_report(
        profiling_report_path,
        join_diag_df=join_diag.coverage_summary,
        time_diag_df=join_diag.time_range_summary,
        commerce_joinable=join_diag.commerce_joinable,
    )

    zero_row_summary = run_pipeline._zero_row_tables_summary(profile.table_profile)
    if zero_row_summary:
        run_pipeline._log(f"Data warning | zero-row tables detected: {zero_row_summary}")

    outputs = [
        str(rt.reports_dir / "data_profile" / "table_profile.csv"),
        str(rt.reports_dir / "data_profile" / "schema_profile.csv"),
        str(rt.reports_dir / "data_profile" / "join_coverage.csv"),
        str(coverage_notes_path),
        str(profiling_report_path),
    ]
    outputs.extend(str(p) for p in rt.checkpoint.save_profile(profile, activity_enrichment_joinable))
    return outputs


def _stage_features(rt: HopRuntime) -> List[str]:
    tables = rt.checkpoint.load_tables()
    if not tables:
        raise RuntimeError("No checkpointed tables found. Run 'load_tables' first.")
    join_diag = rt.checkpoint.load_join_diagnostics()

    feature_df = build_feature_table(
        tables=tables,
        snapshot_freq=rt.pipeline_args.snapshot_freq,
        commerce_joinable_by_brand=join_diag.commerce_joinable,
    )
    if feature_df.empty:
        raise RuntimeError("Feature table is empty; check source timestamps and schema.")

    feature_path = rt.outputs_dir / "feature_table.parquet"
    write_parquet_chunked(feature_df, feature_path, chunk_rows=100_000)

    feature_sample_path = rt.outputs_dir / "feature_table_sample.csv"
    feature_df.to_csv(feature_sample_path, index=False)

    feature_defs_path = rt.outputs_dir / "feature_definitions.csv"
    feature_defs = feature_definitions(feature_df)
    feature_defs.to_csv(feature_defs_path, index=False)

    summary_path = rt.checkpoint.write_json(
        "features",
        "feature_summary.json",
        {
            "rows": int(len(feature_df)),
            "cols": int(len(feature_df.columns)),
            "snapshot_freq": str(rt.pipeline_args.snapshot_freq),
        },
    )

    return [str(feature_path), str(feature_sample_path), str(feature_defs_path), str(summary_path)]


def _stage_segments(rt: HopRuntime) -> List[str]:
    tables = rt.checkpoint.load_tables()
    if not tables:
        raise RuntimeError("No checkpointed tables found. Run 'load_tables' first.")

    join_diag = rt.checkpoint.load_join_diagnostics()
    _, activity_enrichment_joinable = rt.checkpoint.load_profile()

    segment_kpi_df = compute_segment_kpis(
        tables=tables,
        snapshot_freq=rt.pipeline_args.snapshot_freq,
        commerce_joinable_by_brand=join_diag.commerce_joinable,
        activity_enrichment_joinable_by_brand=activity_enrichment_joinable,
    )

    outputs: List[str] = []
    segment_parquet_path = rt.outputs_dir / "segment_kpis.parquet"
    segment_csv_path = rt.outputs_dir / "segment_kpis.csv"
    if not segment_kpi_df.empty:
        write_parquet_chunked(segment_kpi_df, segment_parquet_path, chunk_rows=100_000)
        segment_kpi_df.to_csv(segment_csv_path, index=False)
        outputs.extend([str(segment_parquet_path), str(segment_csv_path)])

    summary_path = rt.checkpoint.write_json(
        "segments",
        "segment_summary.json",
        {
            "rows": int(len(segment_kpi_df)),
            "cols": int(len(segment_kpi_df.columns)) if not segment_kpi_df.empty else 0,
        },
    )
    outputs.append(str(summary_path))
    return outputs


def _stage_labels(rt: HopRuntime) -> List[str]:
    feature_path = rt.outputs_dir / "feature_table.parquet"
    if not feature_path.exists():
        raise FileNotFoundError(f"Missing feature table: {feature_path}")

    feature_df = pd.read_parquet(feature_path)
    labeled_df = generate_weak_labels(feature_df)
    labeled_df = labeled_df.reset_index(drop=True)
    labeled_df["__row_id"] = np.arange(len(labeled_df), dtype=int)

    labeled_path = rt.outputs_dir / "labeled_feature_table.parquet"
    write_parquet_chunked(labeled_df, labeled_path, chunk_rows=100_000)

    summary_path = rt.checkpoint.write_json(
        "labels",
        "labels_summary.json",
        {
            "rows": int(len(labeled_df)),
            "cols": int(len(labeled_df.columns)),
        },
    )
    return [str(labeled_path), str(summary_path)]


def _stage_train(rt: HopRuntime) -> List[str]:
    args = rt.pipeline_args
    labeled_path = rt.outputs_dir / "labeled_feature_table.parquet"
    if not labeled_path.exists():
        raise FileNotFoundError(f"Missing labeled feature table: {labeled_path}")

    labeled_df = pd.read_parquet(labeled_path)
    sample_mode = str(args.train_sample_mode).strip().lower()
    train_input_df = labeled_df
    train_row_ids: Optional[Sequence[int]] = None
    eval_row_ids: Optional[Sequence[int]] = None
    sample_result = None

    outputs: List[str] = []
    if sample_mode != "off":
        sample_cfg = _build_sample_config(args, sample_mode)
        sample_result = build_train_eval_samples(labeled_df, config=sample_cfg)
        save_sample_outputs(sample_result, output_dir=rt.outputs_dir)

        train_input_df = sample_result.sampled_df
        train_row_ids = sample_result.train_row_ids
        eval_row_ids = sample_result.eval_row_ids

        sampled_path = rt.checkpoint.save_sampled_labeled_df(sample_result.sampled_df)
        outputs.append(str(sampled_path))

        sample_info_path = rt.checkpoint.write_json(
            "train",
            "sample_info.json",
            {
                "config_used": sample_result.config_used,
                "fallback_applied": sample_result.fallback_applied,
                "qa_report": sample_result.qa_report,
                "train_rows": int(len(sample_result.sampled_train_df)),
                "eval_rows": int(len(sample_result.sampled_eval_df)),
                "sampled_total_rows": int(len(sample_result.sampled_df)),
            },
        )
        outputs.append(str(sample_info_path))

    train_weight_classes = run_pipeline._parse_bool_flag(args.train_weight_classes)
    if args.skip_train:
        loaded = _load_model_bundle(rt)
        selected_model = str(loaded["selected_model"])
        metrics = loaded["metrics"]
        feature_columns = loaded["feature_columns"]
        class_labels = loaded["class_labels"]
    else:
        artifacts = train_models(
            train_input_df,
            artifact_dir=rt.artifacts_dir,
            train_row_ids=train_row_ids,
            eval_row_ids=eval_row_ids,
            sample_mode=sample_mode,
            n_jobs=int(args.n_jobs),
            weight_classes=train_weight_classes,
            group_col=str(args.train_group_col),
            quick_top_k_features=80 if sample_mode == "quick" else None,
        )
        selected_model = str(artifacts.metrics.get("selected_model"))
        metrics = artifacts.metrics
        feature_columns = artifacts.feature_columns
        class_labels = artifacts.class_labels

    summary_path = rt.checkpoint.write_json(
        "train",
        "train_summary.json",
        {
            "skip_train": bool(args.skip_train),
            "sample_mode": sample_mode,
            "selected_model": selected_model,
            "metrics": metrics,
            "feature_count": int(len(feature_columns)),
            "class_labels": list(class_labels),
            "sample_rows": int(len(train_input_df)),
        },
    )
    outputs.append(str(summary_path))

    for artifact_name in ["brand_health_model.joblib", "model_metadata.json", "feature_importance.json"]:
        fp = rt.artifacts_dir / artifact_name
        if fp.exists():
            outputs.append(str(fp))

    for sample_file in ["sample_train_indices.csv", "sample_eval_indices.csv", "sample_qa_report.json"]:
        fp = rt.outputs_dir / sample_file
        if fp.exists():
            outputs.append(str(fp))

    return outputs


def _stage_infer(rt: HopRuntime) -> List[str]:
    args = rt.pipeline_args
    sample_mode = str(args.train_sample_mode).strip().lower()

    before_examples = _load_before_examples(rt.outputs_dir / "examples_last4_with_segments.json")

    infer_input_df, sample_payload = _load_infer_input_df(rt, sample_mode)

    segment_kpi_path = rt.outputs_dir / "segment_kpis.parquet"
    segment_kpi_df = pd.read_parquet(segment_kpi_path) if segment_kpi_path.exists() else pd.DataFrame()

    loaded = _load_model_bundle(rt)
    model = loaded["model"]
    feature_importance = loaded["feature_importance"]
    feature_columns = loaded["feature_columns"]
    class_labels = loaded["class_labels"]
    metrics = loaded["metrics"]
    selected_model = loaded["selected_model"]

    pred_df = predict_with_drivers(
        feature_df=infer_input_df,
        model=model,
        feature_columns=feature_columns,
        class_labels=class_labels,
        feature_importance=feature_importance,
        segment_kpis_df=segment_kpi_df if not segment_kpi_df.empty else None,
        brand_primary_app_id_map=run_pipeline._brand_primary_app_id_map(run_pipeline.BRAND_APP_ID_FILTERS),
        top_n_drivers=5,
        top_n_actions=3,
        top_n_target_segments=3,
    )

    save_predictions(pred_df, output_dir=rt.outputs_dir)

    attribution_qa = dict(pred_df.attrs.get("attribution_qa", {}))
    attribution_path = rt.outputs_dir / "attribution_qa.json"
    attribution_path.write_text(json.dumps(attribution_qa, indent=2), encoding="utf-8")

    if sample_mode != "off":
        sample_metrics_payload = {
            "train_sample_mode": sample_mode,
            "config": {
                "train_sample_seed": int(args.train_sample_seed),
                "train_sample_frac": float(args.train_sample_frac),
                "train_max_train_rows": int(args.train_max_train_rows),
                "train_max_eval_rows": int(args.train_max_eval_rows),
                "train_recent_days": int(args.train_recent_days),
                "train_stratify_cols": run_pipeline._parse_csv_cols(args.train_stratify_cols),
                "train_group_col": str(args.train_group_col),
                "train_weight_classes": bool(run_pipeline._parse_bool_flag(args.train_weight_classes)),
                "n_jobs": int(args.n_jobs),
            },
            "sample_qa": sample_payload.get("qa_report", {}),
            "metrics": metrics,
            "selected_model": selected_model,
            "feature_count": len(feature_columns),
            "rows_scored": int(len(pred_df)),
        }
        (rt.outputs_dir / "model_metrics_sample.json").write_text(
            json.dumps(sample_metrics_payload, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

    ex = pred_df[pred_df["window_size"].astype(str) == "30d"].copy()
    if ex.empty:
        ex = pred_df.copy()
    examples = ex.sort_values("window_end_date").groupby("brand_id", as_index=False).tail(4)

    examples_last4_path = rt.outputs_dir / "examples_last4_windows.json"
    examples_with_segments_path = rt.outputs_dir / "examples_last4_with_segments.json"
    examples.to_json(examples_last4_path, orient="records", indent=2, date_format="iso")
    examples.to_json(examples_with_segments_path, orient="records", indent=2, date_format="iso")

    if sample_mode != "off":
        examples.to_json(
            rt.outputs_dir / "predictions_last_windows_sample.json",
            orient="records",
            indent=2,
            date_format="iso",
        )

    before_after_examples = run_pipeline._build_before_after_examples(
        before_examples=before_examples,
        after_examples=examples,
        n_per_brand=2,
    )
    before_after_path = rt.outputs_dir / "examples_before_after_2windows.json"
    before_after_path.write_text(
        json.dumps(before_after_examples, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    try:
        profile, activity_enrichment_joinable = rt.checkpoint.load_profile()
    except Exception:
        profile, activity_enrichment_joinable = _empty_profile(), {}

    try:
        join_diag = rt.checkpoint.load_join_diagnostics()
    except Exception:
        join_diag = _empty_join_diagnostics()

    feature_defs_path = rt.outputs_dir / "feature_definitions.csv"
    feature_defs_df = pd.read_csv(feature_defs_path) if feature_defs_path.exists() else pd.DataFrame()

    report_path = rt.reports_dir / args.report_name
    run_pipeline._write_markdown_report(
        report_path=report_path,
        table_profile=profile.table_profile,
        join_coverage=profile.join_coverage,
        feature_defs=feature_defs_df,
        thresholds=labeling_thresholds(),
        metrics=metrics,
        examples=examples,
        before_after_examples=before_after_examples,
        attribution_qa=attribution_qa,
    )

    dtype_opt_summary = rt.checkpoint.read_dataframe("load_tables", "dtype_optimization.parquet")
    dtype_opt_records = dtype_opt_summary.to_dict(orient="records") if not dtype_opt_summary.empty else []
    memory_cfg = rt.checkpoint.read_json("load_tables", "memory_settings.json", default={})
    memory_cfg = dict(memory_cfg) if isinstance(memory_cfg, Mapping) else {}

    memory_payload = {
        "memory_optimize": bool(memory_cfg.get("memory_optimize", run_pipeline._parse_bool_flag(args.memory_optimize))),
        "memory_float_downcast": bool(memory_cfg.get("memory_float_downcast", run_pipeline._parse_bool_flag(args.memory_float_downcast))),
        "memory_cat_ratio_threshold": float(memory_cfg.get("memory_cat_ratio_threshold", args.memory_cat_ratio_threshold)),
        "memory_validate_downcast": bool(memory_cfg.get("memory_validate_downcast", run_pipeline._parse_bool_flag(args.memory_validate_downcast))),
        "events": [],
        "dtype_optimization": dtype_opt_records,
    }
    memory_report_path = rt.outputs_dir / "memory_optimization_report.json"
    memory_report_path.write_text(
        json.dumps(memory_payload, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    if not dtype_opt_summary.empty:
        dtype_opt_summary.to_csv(rt.outputs_dir / "memory_dtype_optimization.csv", index=False)

    existing_summary: Dict[str, object] = {}
    summary_path = rt.reports_dir / "pipeline_summary.json"
    if summary_path.exists():
        try:
            existing_summary = json.loads(summary_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            existing_summary = {}

    catalog_publish_summary = existing_summary.get("catalog_publish") if isinstance(existing_summary.get("catalog_publish"), Mapping) else {
        "enabled": bool(rt.publish_cfg.get("enabled", False)),
        "table": str(rt.publish_cfg.get("table_name", "")),
        "write_mode": str(rt.publish_cfg.get("write_mode", "overwrite")),
        "rows_written": 0,
        "status": "disabled",
        "error": "",
    }

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
            "enabled": bool(memory_payload.get("memory_optimize", False)),
            "dtype_tables_optimized": int(len(dtype_opt_records)),
            "memory_events": [],
        },
    }
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    mlflow_model_input_sample: Optional[pd.DataFrame] = None
    if not args.skip_train and feature_columns:
        available_cols = [c for c in feature_columns if c in infer_input_df.columns]
        if available_cols:
            mlflow_model_input_sample = infer_input_df[available_cols].head(5).copy()

    run_pipeline._log_pipeline_to_mlflow(
        mlflow_cfg=rt.mlflow_cfg,
        args=args,
        source_mode=rt.source_mode,
        runtime_brand_app_filters=rt.runtime_brand_app_filters,
        selected_model=selected_model,
        pred_df=pred_df,
        metrics=metrics if isinstance(metrics, Mapping) else {},
        summary=summary,
        memory_payload=memory_payload,
        attribution_qa=attribution_qa,
        outputs_dir=rt.outputs_dir,
        reports_dir=rt.reports_dir,
        report_path=report_path,
        model=model,
        feature_columns=feature_columns,
        model_input_sample=mlflow_model_input_sample,
    )

    infer_summary_path = rt.checkpoint.write_json(
        "infer",
        "infer_summary.json",
        {
            "rows_scored": int(len(pred_df)),
            "selected_model": selected_model,
            "feature_count": int(len(feature_columns)),
            "train_sample_mode": sample_mode,
        },
    )

    outputs = [
        str(rt.outputs_dir / "predictions_with_drivers.jsonl"),
        str(rt.outputs_dir / "predictions_with_drivers.csv"),
        str(rt.outputs_dir / "predictions_with_drivers.parquet"),
        str(attribution_path),
        str(examples_last4_path),
        str(examples_with_segments_path),
        str(before_after_path),
        str(report_path),
        str(summary_path),
        str(memory_report_path),
        str(infer_summary_path),
    ]
    return outputs


def _stage_publish(rt: HopRuntime) -> List[str]:
    publish_cfg = dict(rt.publish_cfg)
    pred_path = rt.outputs_dir / "predictions_with_drivers.parquet"
    if not pred_path.exists():
        raise FileNotFoundError(f"Missing predictions parquet: {pred_path}")
    pred_df = pd.read_parquet(pred_path)

    catalog_publish_summary: Dict[str, object] = {
        "enabled": bool(publish_cfg.get("enabled", False)),
        "table": str(publish_cfg.get("table_name", "")),
        "write_mode": str(publish_cfg.get("write_mode", "overwrite")),
        "rows_written": 0,
        "status": "disabled",
        "error": "",
    }

    if bool(publish_cfg.get("enabled", False)):
        try:
            publish_result = publish_kpis_predicted_to_catalog(
                pred_df=pred_df,
                table_name=str(publish_cfg.get("table_name", "")),
                write_mode=str(publish_cfg.get("write_mode", "overwrite")),
                fail_on_cast_error=bool(publish_cfg.get("fail_on_cast_error", True)),
            )
            catalog_publish_summary["rows_written"] = int(publish_result.get("rows_written", 0))
            catalog_publish_summary["status"] = "success"
        except Exception as exc:
            catalog_publish_summary["status"] = "failed"
            catalog_publish_summary["error"] = str(exc)
            rt.checkpoint.write_json("publish", "publish_summary.json", catalog_publish_summary)
            raise

    summary_path = rt.reports_dir / "pipeline_summary.json"
    summary: Dict[str, object] = {}
    if summary_path.exists():
        try:
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            summary = {}
    summary["catalog_publish"] = catalog_publish_summary
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    publish_summary_path = rt.checkpoint.write_json("publish", "publish_summary.json", catalog_publish_summary)
    return [str(summary_path), str(publish_summary_path)]


STAGE_HANDLERS: Dict[str, StageHandler] = {
    "load_tables": _stage_load_tables,
    "join_diagnostics": _stage_join_diagnostics,
    "profile": _stage_profile,
    "features": _stage_features,
    "segments": _stage_segments,
    "labels": _stage_labels,
    "train": _stage_train,
    "infer": _stage_infer,
    "publish": _stage_publish,
}


class HopRunner:
    def __init__(self, runtime: HopRuntime, stage_handlers: Optional[Mapping[str, StageHandler]] = None) -> None:
        self.runtime = runtime
        self.stage_handlers = dict(stage_handlers or STAGE_HANDLERS)

    def run(self, target_hop: str) -> None:
        target = str(target_hop).strip()
        if target not in HOP_ORDER:
            raise ValueError(f"Unsupported hop: {target}")

        if self.runtime.hop_args.auto_upstream:
            for dep in self._dependency_closure(target):
                self._run_one(dep, force=False)
        else:
            self._validate_dependencies(target)

        self._run_one(target, force=bool(self.runtime.hop_args.force))

    def _dependency_closure(self, hop: str) -> List[str]:
        needed: set[str] = set()

        def dfs(stage: str) -> None:
            for dep in HOP_DEPENDENCIES.get(stage, ()):  # noqa: B023
                if dep in needed:
                    continue
                needed.add(dep)
                dfs(dep)

        dfs(hop)
        return [stage for stage in HOP_ORDER if stage in needed]

    def _validate_dependencies(self, hop: str) -> None:
        missing: List[str] = []
        for dep in self._dependency_closure(hop):
            if not self._is_stage_completed(dep):
                missing.append(dep)

        if missing:
            run_id = str(self.runtime.hop_args.run_id)
            cmd = f"python run_pipeline_hops.py <hop> --run-id {run_id}"
            missing_txt = ", ".join(missing)
            raise RuntimeError(
                f"Missing upstream hops for '{hop}': {missing_txt}. "
                f"Run missing hops first (e.g. {cmd}) or use --auto-upstream."
            )

    def _assert_fingerprint_compatible(self, stage: str, status: Mapping[str, object]) -> None:
        old = str(status.get("arg_fingerprint", "")).strip()
        if old and old != self.runtime.arg_fingerprint:
            raise ValueError(
                f"Run-id '{self.runtime.hop_args.run_id}' has incompatible args for stage '{stage}'. "
                "Use a new --run-id for a different configuration."
            )

    def _is_stage_completed(self, stage: str) -> bool:
        status = self.runtime.checkpoint.read_stage_status(stage)
        if not status:
            return False
        self._assert_fingerprint_compatible(stage, status)
        return bool(status.get("completed_at"))

    def _run_one(self, stage: str, force: bool) -> None:
        status = self.runtime.checkpoint.read_stage_status(stage)
        if status:
            self._assert_fingerprint_compatible(stage, status)
        if status and status.get("completed_at") and not force:
            run_pipeline._log(f"[{stage}] already completed for run-id={self.runtime.hop_args.run_id}; skip")
            return

        run_pipeline._log(f"[{stage}] start")
        started_at = utc_now_iso()
        started_clock = time.perf_counter()
        try:
            outputs = self.stage_handlers[stage](self.runtime)
        except Exception as exc:
            self.runtime.checkpoint.write_stage_status(
                stage,
                arg_fingerprint=self.runtime.arg_fingerprint,
                started_at=started_at,
                completed_at=None,
                outputs=[],
                error=str(exc),
            )
            raise

        completed_at = utc_now_iso()
        elapsed = time.perf_counter() - started_clock
        self.runtime.checkpoint.write_stage_status(
            stage,
            arg_fingerprint=self.runtime.arg_fingerprint,
            started_at=started_at,
            completed_at=completed_at,
            outputs=[str(x) for x in outputs],
            details={"elapsed_seconds": float(elapsed)},
        )
        run_pipeline._log(f"[{stage}] done in {elapsed:.2f}s")


def _build_runtime(hop_args: argparse.Namespace, pipeline_args: argparse.Namespace) -> HopRuntime:
    source_mode = str(pipeline_args.source_mode).strip().lower()
    publish_cfg = run_pipeline._resolve_publish_runtime(pipeline_args, source_mode=source_mode)
    runtime_brand_app_filters, databricks_cfg, query_selection = run_pipeline._resolve_source_runtime(pipeline_args)
    mlflow_cfg = run_pipeline._resolve_mlflow_runtime(pipeline_args)
    model_cfg = run_pipeline._resolve_model_runtime(pipeline_args)

    dataset_root = Path(pipeline_args.dataset_root)
    reports_dir = Path(pipeline_args.reports_dir)
    outputs_dir = Path(pipeline_args.outputs_dir)
    artifacts_dir = Path(pipeline_args.artifacts_dir)

    reports_dir.mkdir(parents=True, exist_ok=True)
    outputs_dir.mkdir(parents=True, exist_ok=True)
    artifacts_dir.mkdir(parents=True, exist_ok=True)

    run_pipeline._set_thread_limits(int(pipeline_args.n_jobs))

    checkpoint = CheckpointManager(
        checkpoint_root=Path(hop_args.checkpoint_root),
        run_id=str(hop_args.run_id),
    )
    checkpoint.ensure_base_dirs()

    fingerprint = build_arg_fingerprint(
        _arg_fingerprint_payload(
            pipeline_args=pipeline_args,
            runtime_brand_app_filters=runtime_brand_app_filters,
            source_mode=source_mode,
        )
    )

    return HopRuntime(
        hop_args=hop_args,
        pipeline_args=pipeline_args,
        source_mode=source_mode,
        runtime_brand_app_filters=runtime_brand_app_filters,
        databricks_cfg=databricks_cfg,
        query_selection=query_selection,
        mlflow_cfg=mlflow_cfg,
        model_cfg=model_cfg,
        publish_cfg=publish_cfg,
        dataset_root=dataset_root,
        reports_dir=reports_dir,
        outputs_dir=outputs_dir,
        artifacts_dir=artifacts_dir,
        checkpoint=checkpoint,
        arg_fingerprint=fingerprint,
    )


def main(argv: Optional[Sequence[str]] = None) -> None:
    hop_args, pipeline_args = _parse_args(argv)
    runtime = _build_runtime(hop_args, pipeline_args)

    run_pipeline._log(
        "Hop pipeline start | "
        f"run_id={hop_args.run_id} hop={hop_args.hop} auto_upstream={bool(hop_args.auto_upstream)} "
        f"force={bool(hop_args.force)} source_mode={runtime.source_mode}"
    )
    run_pipeline._log(
        f"Paths | dataset_root={runtime.dataset_root} reports_dir={runtime.reports_dir} "
        f"outputs_dir={runtime.outputs_dir} artifacts_dir={runtime.artifacts_dir} "
        f"checkpoint_run_dir={runtime.checkpoint.run_dir}"
    )

    runner = HopRunner(runtime)
    runner.run(str(hop_args.hop))
    run_pipeline._log(f"Hop pipeline finished | run_id={hop_args.run_id} hop={hop_args.hop}")


if __name__ == "__main__":
    main()
