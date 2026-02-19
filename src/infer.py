from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Mapping, Optional

import joblib
import numpy as np
import pandas as pd

from .drivers import attach_drivers
from .playbook import attach_actions


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



def predict_with_drivers(
    feature_df: pd.DataFrame,
    model,
    feature_columns,
    class_labels,
    feature_importance: Optional[Mapping[str, float]] = None,
    top_n_drivers: int = 5,
    top_n_actions: int = 3,
) -> pd.DataFrame:
    df = feature_df.copy().reset_index(drop=True)
    X = _prepare_inference_frame(df, feature_columns)

    pred = model.predict(X)
    proba = model.predict_proba(X)

    prob_df = pd.DataFrame(proba, columns=[f"prob_{c}" for c in class_labels])

    score_map = {c: CLASS_SCORE_MAP.get(c, 50.0) for c in class_labels}
    score_arr = np.zeros(len(df), dtype=float)
    for i, c in enumerate(class_labels):
        score_arr += prob_df[f"prob_{c}"].to_numpy() * score_map[c]

    out = pd.concat([df, prob_df], axis=1)
    out["predicted_health_class"] = pred
    out["predicted_health_score"] = score_arr

    # Deterministic drivers + action mapping.
    out = attach_drivers(out, feature_importance=feature_importance, top_n=top_n_drivers)
    out = attach_actions(out, top_n=top_n_actions)

    # Dashboard-friendly payload.
    def _to_payload(row: pd.Series) -> dict:
        probs = {c: float(row[f"prob_{c}"]) for c in class_labels}
        return {
            "brand_id": str(row.get("brand_id")),
            "window_end_date": str(row.get("window_end_date")),
            "window_size": str(row.get("window_size")),
            "predicted_health_class": str(row.get("predicted_health_class")),
            "predicted_health_score": float(row.get("predicted_health_score", 0.0)),
            "probabilities": probs,
            "drivers": row.get("drivers", []),
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
        "predicted_health_score",
        "drivers",
        "suggested_actions",
    ] + [c for c in pred_df.columns if c.startswith("prob_")]

    pred_df[save_cols].to_csv(out_dir / "predictions_with_drivers.csv", index=False)

    with (out_dir / "predictions_with_drivers.jsonl").open("w", encoding="utf-8") as f:
        for payload in pred_df["payload"]:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")
