from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Mapping, Optional

import joblib
import numpy as np
import pandas as pd

from .drivers import attach_drivers
from .playbook import attach_actions
from .segments import SEGMENT_KEYS, SEGMENT_METRIC_SUFFIXES


CLASS_SCORE_MAP = {
    "Healthy": 90.0,
    "Warning": 60.0,
    "AtRisk": 25.0,
}


def load_model_artifacts(artifact_dir: str | Path):
    artifact_path = Path(artifact_dir)
    model = joblib.load(artifact_path / "brand_health_model.joblib")

    metadata_path = artifact_path / "model_metadata.json"
    importance_path = artifact_path / "feature_importance.json"

    metadata = json.loads(metadata_path.read_text(encoding="utf-8")) if metadata_path.exists() else {}
    importance = json.loads(importance_path.read_text(encoding="utf-8")) if importance_path.exists() else {}

    return {
        "model": model,
        "metadata": metadata,
        "feature_importance": importance,
    }


def _prepare_inference_frame(feature_df: pd.DataFrame, feature_columns) -> pd.DataFrame:
    df = feature_df.copy()

    dt = pd.to_datetime(df["window_end_date"], errors="coerce", utc=True)
    df["window_end_ordinal"] = dt.dt.date.map(lambda d: d.toordinal() if pd.notna(d) else 0)
    df["window_end_month"] = dt.dt.month.fillna(0).astype(float)
    df["window_end_week"] = dt.dt.isocalendar().week.astype(float)

    for col in feature_columns:
        if col not in df.columns:
            df[col] = 0.0

    X = df[feature_columns].copy()
    return X


def _canonical_driver_key(key: str) -> str:
    if key.startswith("model_"):
        k = key.replace("model_", "")
        if "active_users" in k:
            return "active_down"
        if "completion" in k:
            return "completion_down"
        if "redeem" in k:
            return "redeem_down"
        if "gmv" in k:
            return "gmv_down"
        if "transaction" in k:
            return "transactions_down"
        if "dormant" in k:
            return "dormant_up"
        if "efficiency" in k or "points" in k:
            return "efficiency_drop"
        if "sku" in k:
            return "sku_concentration_high"
    return key


def _driver_metric_suffix(driver_key: str) -> Optional[str]:
    mapping = {
        "active_down": "users",
        "dormant_up": "users",
        "completion_down": "activity_completion_rate",
        "redeem_down": "redeem_rate",
        "gmv_down": "gmv_net",
        "transactions_down": "transactions",
        "efficiency_drop": "activity_completion_rate",
    }
    return mapping.get(driver_key)


def _metric_label(metric_suffix: str) -> str:
    return {
        "users": "active_users",
        "activity_completion_rate": "completion_rate",
        "redeem_rate": "redeem_rate",
        "gmv_net": "gmv",
        "transactions": "transactions",
    }.get(metric_suffix, metric_suffix)


def _add_segment_deltas(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy().sort_values(["brand_id", "window_size", "window_end_date"]).reset_index(drop=True)
    group_cols = ["brand_id", "window_size"]

    for seg in SEGMENT_KEYS:
        for metric in SEGMENT_METRIC_SUFFIXES:
            col = f"seg_{seg}_{metric}"
            if col not in out.columns:
                continue
            g = out.groupby(group_cols, observed=True)[col]
            prev = g.shift(1)
            out[f"{col}_delta"] = (out[col] - prev).fillna(0.0)
            out[f"{col}_wow_pct"] = ((out[col] - prev) / prev.replace(0, np.nan)).replace([np.inf, -np.inf], np.nan).fillna(0.0)

    return out


def _confidence_row(row: pd.Series, class_labels) -> tuple[str, float, float]:
    probs = np.array([float(row.get(f"prob_{c}", 0.0)) for c in class_labels], dtype=float)
    if probs.size == 0:
        return "low", 0.0, 0.0

    top_idx = int(np.argmax(probs))
    top_prob = float(probs[top_idx])

    if len(probs) > 1:
        second_prob = float(np.partition(probs, -2)[-2])
    else:
        second_prob = 0.0

    margin = top_prob - second_prob
    if top_prob >= 0.75 and margin >= 0.25:
        return "high", top_prob, margin
    if top_prob >= 0.60 and margin >= 0.15:
        return "medium", top_prob, margin
    return "low", top_prob, margin


def _health_statement(health_class: str, score: float, confidence_band: str) -> str:
    near_threshold = min(abs(score - 70.0), abs(score - 45.0)) <= 5.0
    if confidence_band == "low" and near_threshold:
        return f"{health_class} (borderline)"
    return health_class


def _build_target_segments(row: Mapping, top_n: int = 3) -> list[dict]:
    drivers = row.get("drivers", [])
    if not isinstance(drivers, list) or not drivers:
        return []

    candidates: List[dict] = []

    for d in drivers:
        if not isinstance(d, Mapping):
            continue
        driver_key = _canonical_driver_key(str(d.get("key", "")))
        metric_suffix = _driver_metric_suffix(driver_key)
        if not metric_suffix:
            continue

        deltas = []
        for seg in SEGMENT_KEYS:
            delta_col = f"seg_{seg}_{metric_suffix}_delta"
            delta = float(row.get(delta_col, 0.0) or 0.0)
            deltas.append((seg, delta))

        # Prefer contributors aligned with driver direction to keep targeting actionable.
        expected_sign = 0
        if driver_key in {"active_down", "completion_down", "redeem_down", "gmv_down", "transactions_down", "efficiency_drop"}:
            expected_sign = -1
        elif driver_key in {"dormant_up"}:
            expected_sign = 1

        if expected_sign < 0:
            aligned = [(seg, delta) for seg, delta in deltas if delta < 0]
        elif expected_sign > 0:
            aligned = [(seg, delta) for seg, delta in deltas if delta > 0]
        else:
            aligned = []

        selected_deltas = aligned if aligned else [(seg, delta) for seg, delta in deltas if delta != 0]
        if not selected_deltas:
            continue

        denom = float(sum(abs(x[1]) for x in selected_deltas))
        if denom <= 0:
            continue

        top_seg = sorted(selected_deltas, key=lambda x: abs(x[1]), reverse=True)[:3]
        label = _metric_label(metric_suffix)
        driver_statement = str(d.get("statement", "")).strip()

        for seg, delta in top_seg:
            contribution = float(abs(delta) / denom)
            wow = float(row.get(f"seg_{seg}_{metric_suffix}_wow_pct", 0.0) or 0.0)
            seg_share = float(row.get(f"seg_{seg}_share", 0.0) or 0.0)
            direction = "drop" if delta < 0 else "increase"

            candidates.append(
                {
                    "segment_key": seg,
                    "contribution_share": contribution,
                    "reason_statement": f"Major contributor to {label} {direction}. Driver: {driver_statement}",
                    "evidence_metrics": {
                        f"{label}_delta_seg": float(delta),
                        f"{label}_wow_pct_seg": float(wow),
                        "segment_share": float(seg_share),
                        "driver_key": driver_key,
                    },
                    "_score": contribution * float(d.get("severity", 1.0) or 1.0),
                }
            )

    if not candidates:
        return []

    merged: Dict[str, dict] = {}
    score_sum: Dict[str, float] = {}
    for c in candidates:
        seg = c["segment_key"]
        score_sum[seg] = score_sum.get(seg, 0.0) + float(c.get("_score", 0.0))
        if seg not in merged or float(c.get("_score", 0.0)) > float(merged[seg].get("_score", 0.0)):
            merged[seg] = c

    ranked = sorted(score_sum.items(), key=lambda kv: kv[1], reverse=True)[:top_n]
    total_score = sum(v for _, v in ranked) or 1.0

    out = []
    for seg, score in ranked:
        obj = merged[seg]
        out.append(
            {
                "segment_key": seg,
                "contribution_share": float(score / total_score),
                "reason_statement": obj["reason_statement"],
                "evidence_metrics": obj["evidence_metrics"],
            }
        )

    return out


def predict_with_drivers(
    feature_df: pd.DataFrame,
    model,
    feature_columns,
    class_labels,
    feature_importance: Optional[Mapping[str, float]] = None,
    segment_kpis_df: Optional[pd.DataFrame] = None,
    top_n_drivers: int = 5,
    top_n_actions: int = 3,
    top_n_target_segments: int = 3,
) -> pd.DataFrame:
    df = feature_df.copy().reset_index(drop=True)
    X = _prepare_inference_frame(df, feature_columns)

    pred = model.predict(X)
    proba = model.predict_proba(X)

    prob_df = pd.DataFrame(proba, columns=[f"prob_{c}" for c in class_labels])

    score_map = {c: CLASS_SCORE_MAP.get(c, 50.0) for c in class_labels}
    score_arr = np.zeros(len(df), dtype=float)
    for c in class_labels:
        score_arr += prob_df[f"prob_{c}"].to_numpy() * score_map[c]

    out = pd.concat([df, prob_df], axis=1)
    out["predicted_health_class"] = pred
    out["predicted_health_score"] = score_arr

    if segment_kpis_df is not None and not segment_kpis_df.empty:
        keys = ["brand_id", "window_end_date", "window_size"]
        seg = segment_kpis_df.copy()
        if "window_size_days" in seg.columns:
            seg = seg.drop(columns=["window_size_days"])
        seg["window_end_date"] = pd.to_datetime(seg["window_end_date"], errors="coerce", utc=True)
        out["window_end_date"] = pd.to_datetime(out["window_end_date"], errors="coerce", utc=True)
        out = out.merge(seg, on=keys, how="left")

        seg_cols = [c for c in seg.columns if c not in keys]
        present_seg_cols = [c for c in seg_cols if c in out.columns]
        if present_seg_cols:
            out[present_seg_cols] = out[present_seg_cols].fillna(0.0)
        out = _add_segment_deltas(out)

    conf = out.apply(lambda r: _confidence_row(r, class_labels), axis=1)
    out["confidence_band"] = conf.apply(lambda x: x[0])
    out["confidence_top_probability"] = conf.apply(lambda x: x[1])
    out["confidence_margin"] = conf.apply(lambda x: x[2])
    out["predicted_health_statement"] = out.apply(
        lambda r: _health_statement(str(r.get("predicted_health_class", "")), float(r.get("predicted_health_score", 0.0)), str(r.get("confidence_band", "low"))),
        axis=1,
    )

    out = attach_drivers(out, feature_importance=feature_importance, top_n=top_n_drivers)
    out["target_segments"] = out.apply(lambda r: _build_target_segments(r.to_dict(), top_n=top_n_target_segments), axis=1)
    out = attach_actions(out, top_n=top_n_actions)

    def _to_payload(row: pd.Series) -> dict:
        probs = {c: float(row[f"prob_{c}"]) for c in class_labels}
        return {
            "brand_id": str(row.get("brand_id")),
            "window_end_date": str(row.get("window_end_date")),
            "window_size": str(row.get("window_size")),
            "predicted_health_class": str(row.get("predicted_health_class")),
            "predicted_health_statement": str(row.get("predicted_health_statement")),
            "predicted_health_score": float(row.get("predicted_health_score", 0.0)),
            "confidence_band": str(row.get("confidence_band", "low")),
            "probabilities": probs,
            "drivers": row.get("drivers", []),
            "target_segments": row.get("target_segments", []),
            "suggested_actions": row.get("suggested_actions", []),
        }

    out["payload"] = out.apply(_to_payload, axis=1)
    return out


def save_predictions(pred_df: pd.DataFrame, output_dir: str | Path) -> None:
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    save_cols = [
        "brand_id",
        "window_end_date",
        "window_size",
        "predicted_health_class",
        "predicted_health_statement",
        "predicted_health_score",
        "confidence_band",
        "drivers",
        "target_segments",
        "suggested_actions",
    ] + [c for c in pred_df.columns if c.startswith("prob_")]

    pred_df[save_cols].to_csv(out_dir / "predictions_with_drivers.csv", index=False)

    with (out_dir / "predictions_with_drivers.jsonl").open("w", encoding="utf-8") as f:
        for payload in pred_df["payload"]:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")
