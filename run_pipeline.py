from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Dict, List, Mapping, Optional, Sequence

import numpy as np
import pandas as pd

from src.data_load import (
    TABLE_FILES,
    build_purchase_item_join_diagnostics,
    profile_dataset,
    save_profile,
    load_tables,
    write_coverage_notes_markdown,
    write_join_diagnostics_markdown,
)
from src.features import build_feature_table, feature_definitions
from src.infer import load_model_artifacts, predict_with_drivers, save_predictions
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


def _brand_primary_app_id_map(brand_app_id_filters: Mapping[str, Sequence[int | str]]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for brand_id, app_ids in brand_app_id_filters.items():
        if not app_ids:
            continue
        out[str(brand_id)] = str(app_ids[0])
    return out


def _format_pct(v: float) -> str:
    return f"{v * 100:.2f}%"


def _parse_bool_flag(v) -> bool:
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    return s in {"1", "true", "yes", "y", "on"}


def _parse_csv_cols(v: str) -> List[str]:
    return [x.strip() for x in str(v).split(",") if x.strip()]


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



def main() -> None:
    parser = argparse.ArgumentParser(description="Brand Health Pipeline")
    parser.add_argument("--dataset-root", type=str, default="datasets")
    parser.add_argument("--reports-dir", type=str, default="reports")
    parser.add_argument("--outputs-dir", type=str, default="outputs")
    parser.add_argument("--artifacts-dir", type=str, default="artifacts")
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
    args = parser.parse_args()

    dataset_root = Path(args.dataset_root)
    reports_dir = Path(args.reports_dir)
    outputs_dir = Path(args.outputs_dir)
    artifacts_dir = Path(args.artifacts_dir)

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

    print(f"Using app_id filters: {BRAND_APP_ID_FILTERS}")
    print("[0/7] Building join diagnostics...")
    join_diag = build_purchase_item_join_diagnostics(dataset_root, brand_app_ids=BRAND_APP_ID_FILTERS)
    write_join_diagnostics_markdown(join_diag, outputs_dir / "join_diagnostics.md")

    print("[1/7] Profiling parquet datasets...")
    profile = profile_dataset(dataset_root, brand_app_ids=BRAND_APP_ID_FILTERS)
    save_profile(profile, reports_dir / "data_profile")
    write_coverage_notes_markdown(profile.join_coverage, join_diag, outputs_dir / "coverage_notes.md")
    activity_enrichment_joinable = _compute_activity_enrichment_joinable(profile.join_coverage, threshold=0.80)
    _write_profiling_report(
        outputs_dir / "profiling_report.md",
        join_diag_df=join_diag.coverage_summary,
        time_diag_df=join_diag.time_range_summary,
        commerce_joinable=join_diag.commerce_joinable,
    )

    print("[2/7] Loading tables for feature engineering...")
    columns_map = {
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
    tables = load_tables(
        dataset_root,
        table_files=TABLE_FILES,
        columns_map=columns_map,
        brand_app_ids=BRAND_APP_ID_FILTERS,
    )
    table_rows_before_opt = {k: int(len(v)) for k, v in tables.items() if isinstance(v, pd.DataFrame)}
    log_memory_rss("after_load_tables", sink=memory_events)

    dtype_opt_summary = pd.DataFrame()
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
            if isinstance(v, pd.DataFrame):
                before_n = table_rows_before_opt.get(k, int(len(v)))
                after_n = int(len(v))
                if before_n != after_n:
                    raise AssertionError(
                        f"Row count changed after dtype optimization for table={k}: before={before_n}, after={after_n}"
                    )
        if not dtype_opt_summary.empty:
            dtype_opt_summary.to_csv(outputs_dir / "memory_dtype_optimization.csv", index=False)
    log_memory_rss("after_optimize_tables", sink=memory_events)

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
    log_memory_rss("after_build_feature_table", sink=memory_events)

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
    log_memory_rss("after_build_segment_kpis", sink=memory_events)

    # Release raw event tables as soon as all derived frames are built.
    del tables
    collect_garbage("release_raw_tables", sink=memory_events)

    feature_defs = feature_definitions(feature_df)
    feature_defs.to_csv(outputs_dir / "feature_definitions.csv", index=False)

    print("[5/7] Generating weak labels...")
    labeled_df = generate_weak_labels(feature_df)
    labeled_df = labeled_df.reset_index(drop=True)
    labeled_df["__row_id"] = np.arange(len(labeled_df), dtype=int)
    write_parquet_chunked(labeled_df, outputs_dir / "labeled_feature_table.parquet", chunk_rows=100_000)
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

        # In sampled mode, full labeled_df can be released.
        del labeled_df
        collect_garbage("release_labeled_full_after_sampling", sink=memory_events)

    print("[6/7] Training and evaluating models...")
    log_memory_rss("before_train_stage", sink=memory_events)
    train_weight_classes = _parse_bool_flag(args.train_weight_classes)
    if args.skip_train:
        loaded = load_model_artifacts(artifact_dir=artifacts_dir)
        model = loaded["model"]
        metadata = loaded.get("metadata", {})
        feature_importance = loaded.get("feature_importance", {})
        feature_columns = metadata.get("feature_columns", [])
        class_labels = metadata.get("class_labels", ["AtRisk", "Healthy", "Warning"])

        existing_summary = {}
        summary_path = reports_dir / "pipeline_summary.json"
        if summary_path.exists():
            try:
                existing_summary = json.loads(summary_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                existing_summary = {}
        metrics = existing_summary.get("metrics", {})
        selected_model = existing_summary.get("selected_model", "hist_gradient_boosting_calibrated")
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
    log_memory_rss("after_train_stage", sink=memory_events)

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
    log_memory_rss("after_inference_stage", sink=memory_events)
    save_predictions(pred_df, output_dir=outputs_dir)
    attribution_qa = dict(pred_df.attrs.get("attribution_qa", {}))
    (outputs_dir / "attribution_qa.json").write_text(json.dumps(attribution_qa, indent=2), encoding="utf-8")

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
        "memory_optimization": {
            "enabled": bool(memory_optimize),
            "dtype_tables_optimized": int(len(dtype_opt_records)),
            "memory_events": memory_events,
        },
    }
    (reports_dir / "pipeline_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print("Done.")
    print(f"Report: {report_path}")
    print(f"Predictions: {outputs_dir / 'predictions_with_drivers.jsonl'}")


if __name__ == "__main__":
    main()
