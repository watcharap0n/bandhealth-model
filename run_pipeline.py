from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, List

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
from src.infer import predict_with_drivers, save_predictions
from src.labeling import generate_weak_labels, labeling_thresholds
from src.segments import compute_segment_kpis
from src.train import train_models

BRAND_APP_ID_FILTERS = {
    "c-vit": [1993744540760190],
    "see-chan": [838315041537793],
}


def _format_pct(v: float) -> str:
    return f"{v * 100:.2f}%"


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
                "predicted_health_statement": row.get("predicted_health_statement", row["predicted_health_class"]),
                "predicted_health_score": float(row["predicted_health_score"]),
                "confidence_band": row.get("confidence_band", "low"),
                "probabilities": {k.replace("prob_", ""): float(row[k]) for k in row.index if k.startswith("prob_")},
                "drivers": row["drivers"],
                "target_segments": row.get("target_segments", []),
                "suggested_actions": row["suggested_actions"],
            }
            lines.append(json.dumps(payload, ensure_ascii=False, indent=2))
            lines.append("```")
            lines.append("")

    report_path.write_text("\n".join(lines), encoding="utf-8")


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
    args = parser.parse_args()

    dataset_root = Path(args.dataset_root)
    reports_dir = Path(args.reports_dir)
    outputs_dir = Path(args.outputs_dir)
    artifacts_dir = Path(args.artifacts_dir)

    reports_dir.mkdir(parents=True, exist_ok=True)
    outputs_dir.mkdir(parents=True, exist_ok=True)
    artifacts_dir.mkdir(parents=True, exist_ok=True)

    print(f"Using app_id filters: {BRAND_APP_ID_FILTERS}")
    print("[0/7] Building join diagnostics...")
    join_diag = build_purchase_item_join_diagnostics(dataset_root, brand_app_ids=BRAND_APP_ID_FILTERS)
    write_join_diagnostics_markdown(join_diag, outputs_dir / "join_diagnostics.md")

    print("[1/7] Profiling parquet datasets...")
    profile = profile_dataset(dataset_root, brand_app_ids=BRAND_APP_ID_FILTERS)
    save_profile(profile, reports_dir / "data_profile")
    write_coverage_notes_markdown(profile.join_coverage, join_diag, outputs_dir / "coverage_notes.md")
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

    print("[3/7] Building features...")
    feature_df = build_feature_table(
        tables=tables,
        snapshot_freq=args.snapshot_freq,
        commerce_joinable_by_brand=join_diag.commerce_joinable,
    )
    if feature_df.empty:
        raise RuntimeError("Feature table is empty; check source timestamps and schema.")
    feature_df.to_parquet(outputs_dir / "feature_table.parquet", index=False)
    feature_df.to_csv(outputs_dir / "feature_table_sample.csv", index=False)

    print("[4/7] Building segment KPIs for Marketing Automation...")
    segment_kpi_df = compute_segment_kpis(tables=tables, snapshot_freq=args.snapshot_freq)
    if not segment_kpi_df.empty:
        segment_kpi_df.to_parquet(outputs_dir / "segment_kpis.parquet", index=False)
        segment_kpi_df.to_csv(outputs_dir / "segment_kpis.csv", index=False)

    feature_defs = feature_definitions(feature_df)
    feature_defs.to_csv(outputs_dir / "feature_definitions.csv", index=False)

    print("[5/7] Generating weak labels...")
    labeled_df = generate_weak_labels(feature_df)
    labeled_df.to_parquet(outputs_dir / "labeled_feature_table.parquet", index=False)

    print("[6/7] Training and evaluating models...")
    artifacts = train_models(labeled_df, artifact_dir=artifacts_dir)

    print("[7/7] Running inference + drivers + actions...")
    pred_df = predict_with_drivers(
        feature_df=labeled_df,
        model=artifacts.final_model,
        feature_columns=artifacts.feature_columns,
        class_labels=artifacts.class_labels,
        feature_importance=artifacts.feature_importance,
        segment_kpis_df=segment_kpi_df if not segment_kpi_df.empty else None,
        top_n_drivers=5,
        top_n_actions=3,
        top_n_target_segments=3,
    )
    save_predictions(pred_df, output_dir=outputs_dir)

    # Last 4 windows per brand (prefer 30d windows for concise dashboard snapshots).
    ex = pred_df[pred_df["window_size"].astype(str) == "30d"].copy()
    if ex.empty:
        ex = pred_df.copy()
    examples = ex.sort_values("window_end_date").groupby("brand_id", as_index=False).tail(4)
    examples.to_json(outputs_dir / "examples_last4_windows.json", orient="records", indent=2, date_format="iso")
    examples.to_json(outputs_dir / "examples_last4_with_segments.json", orient="records", indent=2, date_format="iso")

    # Final report.
    report_path = reports_dir / args.report_name
    _write_markdown_report(
        report_path=report_path,
        table_profile=profile.table_profile,
        join_coverage=profile.join_coverage,
        feature_defs=feature_defs,
        thresholds=labeling_thresholds(),
        metrics=artifacts.metrics,
        examples=examples,
    )

    # Also write compact summary JSON for backend usage.
    summary = {
        "join_coverage": profile.join_coverage.to_dict(orient="records"),
        "commerce_joinable": join_diag.commerce_joinable,
        "selected_model": artifacts.metrics.get("selected_model"),
        "metrics": artifacts.metrics,
        "feature_count": len(artifacts.feature_columns),
        "example_count": len(examples),
    }
    (reports_dir / "pipeline_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print("Done.")
    print(f"Report: {report_path}")
    print(f"Predictions: {outputs_dir / 'predictions_with_drivers.jsonl'}")


if __name__ == "__main__":
    main()
