from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd

from src.data_load import (
    TABLE_FILES,
    profile_dataset,
    save_profile,
    summarize_join_coverage,
    load_tables,
)
from src.features import build_feature_table, feature_definitions
from src.infer import predict_with_drivers, save_predictions
from src.labeling import generate_weak_labels, labeling_thresholds
from src.train import train_models


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
        weak = sub[sub["row_coverage"].fillna(0) < 0.5]
        if weak.empty:
            lines.append(f"- **{brand}**: key joins are mostly healthy.")
        else:
            rels = ", ".join([f"{r.left_table}->{r.right_table} ({_format_pct(float(r.row_coverage))})" for r in weak.itertuples()])
            lines.append(f"- **{brand}**: weak key coverage observed in {rels}; feature logic uses brand-level fallback aggregations.")
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
                "predicted_health_score": float(row["predicted_health_score"]),
                "probabilities": {k.replace("prob_", ""): float(row[k]) for k in row.index if k.startswith("prob_")},
                "drivers": row["drivers"],
                "suggested_actions": row["suggested_actions"],
            }
            lines.append(json.dumps(payload, ensure_ascii=False, indent=2))
            lines.append("```")
            lines.append("")

    report_path.write_text("\n".join(lines), encoding="utf-8")



def main() -> None:
    parser = argparse.ArgumentParser(description="Brand Health Pipeline")
    parser.add_argument("--dataset-root", type=str, default="datasets")
    parser.add_argument("--reports-dir", type=str, default="reports")
    parser.add_argument("--outputs-dir", type=str, default="outputs")
    parser.add_argument("--artifacts-dir", type=str, default="artifacts")
    parser.add_argument("--snapshot-freq", type=str, default="7D")
    args = parser.parse_args()

    dataset_root = Path(args.dataset_root)
    reports_dir = Path(args.reports_dir)
    outputs_dir = Path(args.outputs_dir)
    artifacts_dir = Path(args.artifacts_dir)

    reports_dir.mkdir(parents=True, exist_ok=True)
    outputs_dir.mkdir(parents=True, exist_ok=True)
    artifacts_dir.mkdir(parents=True, exist_ok=True)

    print("[1/6] Profiling parquet datasets...")
    profile = profile_dataset(dataset_root)
    save_profile(profile, reports_dir / "data_profile")

    print("[2/6] Loading tables for feature engineering...")
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
    tables = load_tables(dataset_root, table_files=TABLE_FILES, columns_map=columns_map)

    print("[3/6] Building features...")
    feature_df = build_feature_table(tables=tables, snapshot_freq=args.snapshot_freq)
    if feature_df.empty:
        raise RuntimeError("Feature table is empty; check source timestamps and schema.")
    feature_df.to_parquet(outputs_dir / "feature_table.parquet", index=False)
    feature_df.to_csv(outputs_dir / "feature_table_sample.csv", index=False)

    feature_defs = feature_definitions(feature_df)
    feature_defs.to_csv(outputs_dir / "feature_definitions.csv", index=False)

    print("[4/6] Generating weak labels...")
    labeled_df = generate_weak_labels(feature_df)
    labeled_df.to_parquet(outputs_dir / "labeled_feature_table.parquet", index=False)

    print("[5/6] Training and evaluating models...")
    artifacts = train_models(labeled_df, artifact_dir=artifacts_dir)

    print("[6/6] Running inference + drivers + actions...")
    pred_df = predict_with_drivers(
        feature_df=labeled_df,
        model=artifacts.final_model,
        feature_columns=artifacts.feature_columns,
        class_labels=artifacts.class_labels,
        feature_importance=artifacts.feature_importance,
        top_n_drivers=5,
        top_n_actions=3,
    )
    save_predictions(pred_df, outputs_dir=outputs_dir)

    # Last 4 windows per brand (prefer 30d windows for concise dashboard snapshots).
    ex = pred_df[pred_df["window_size"].astype(str) == "30d"].copy()
    if ex.empty:
        ex = pred_df.copy()
    examples = ex.sort_values("window_end_date").groupby("brand_id", as_index=False).tail(4)
    examples.to_json(outputs_dir / "examples_last4_windows.json", orient="records", indent=2, date_format="iso")

    # Final report.
    report_path = reports_dir / "brand_health_report.md"
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
