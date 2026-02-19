from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Tuple

import numpy as np
import pandas as pd


HEALTH_CLASSES = ("Healthy", "Warning", "AtRisk")


@dataclass
class LabelingConfig:
    active_drop_warn: float = -0.10
    active_drop_risk: float = -0.20
    completion_drop_warn: float = -0.08
    completion_drop_risk: float = -0.15
    gmv_drop_warn: float = -0.10
    gmv_drop_risk: float = -0.20
    txn_drop_warn: float = -0.10
    txn_drop_risk: float = -0.20
    dormant_up_warn: float = 0.05
    dormant_up_risk: float = 0.10
    efficiency_penalty: float = 8.0



def _penalty_from_drop(val: float, warn_th: float, risk_th: float, warn_penalty: float, risk_penalty: float) -> float:
    if np.isnan(val):
        return 0.0
    if val <= risk_th:
        scale = min(abs(val / risk_th), 2.0)
        return risk_penalty * scale
    if val <= warn_th:
        scale = min(abs(val / warn_th), 2.0)
        return warn_penalty * scale
    return 0.0



def _penalty_from_rise(val: float, warn_th: float, risk_th: float, warn_penalty: float, risk_penalty: float) -> float:
    if np.isnan(val):
        return 0.0
    if val >= risk_th:
        scale = min(val / risk_th, 2.0)
        return risk_penalty * scale
    if val >= warn_th:
        scale = min(val / warn_th, 2.0)
        return warn_penalty * scale
    return 0.0



def _baseline_penalty(z: float, mild: float = -0.8, severe: float = -1.5, mild_penalty: float = 4.0, severe_penalty: float = 10.0) -> float:
    if np.isnan(z):
        return 0.0
    if z <= severe:
        return severe_penalty
    if z <= mild:
        return mild_penalty
    return 0.0



def generate_weak_labels(feature_df: pd.DataFrame, config: LabelingConfig = LabelingConfig()) -> pd.DataFrame:
    """Generate weakly supervised Brand Health labels from degradation heuristics."""
    df = feature_df.copy().sort_values(["brand_id", "window_size", "window_end_date"]).reset_index(drop=True)

    required = {
        "active_users_wow_pct": 0.0,
        "activity_completion_rate_wow_pct": 0.0,
        "gmv_net_wow_pct": 0.0,
        "transaction_count_wow_pct": 0.0,
        "dormant_share_wow_pct": 0.0,
        "activity_points_per_active_wow_pct": 0.0,
        "reward_efficiency_wow_pct": 0.0,
        "active_users_zscore": 0.0,
        "gmv_net_zscore": 0.0,
        "activity_completion_rate_zscore": 0.0,
        "transaction_count_zscore": 0.0,
        "dormant_share_zscore": 0.0,
    }
    for col, default in required.items():
        if col not in df.columns:
            df[col] = default

    scores = np.full(len(df), 100.0, dtype=float)

    # Core degradation components
    penalties = (
        df["active_users_wow_pct"].apply(
            lambda v: _penalty_from_drop(v, config.active_drop_warn, config.active_drop_risk, warn_penalty=14.0, risk_penalty=25.0)
        )
        + df["activity_completion_rate_wow_pct"].apply(
            lambda v: _penalty_from_drop(
                v,
                config.completion_drop_warn,
                config.completion_drop_risk,
                warn_penalty=12.0,
                risk_penalty=20.0,
            )
        )
        + df["gmv_net_wow_pct"].apply(
            lambda v: _penalty_from_drop(v, config.gmv_drop_warn, config.gmv_drop_risk, warn_penalty=14.0, risk_penalty=24.0)
        )
        + df["transaction_count_wow_pct"].apply(
            lambda v: _penalty_from_drop(v, config.txn_drop_warn, config.txn_drop_risk, warn_penalty=10.0, risk_penalty=18.0)
        )
        + df["dormant_share_wow_pct"].apply(
            lambda v: _penalty_from_rise(v, config.dormant_up_warn, config.dormant_up_risk, warn_penalty=9.0, risk_penalty=16.0)
        )
    )

    scores -= penalties.to_numpy(dtype=float)

    # Baseline-relative penalty (brand-specific trailing behavior encoded via z-scores).
    baseline_pen = (
        df["active_users_zscore"].apply(_baseline_penalty)
        + df["gmv_net_zscore"].apply(_baseline_penalty)
        + df["activity_completion_rate_zscore"].apply(_baseline_penalty)
        + df["transaction_count_zscore"].apply(_baseline_penalty)
        + df["dormant_share_zscore"].apply(lambda z: _baseline_penalty(-z))
    )
    scores -= baseline_pen.to_numpy(dtype=float)

    # Efficiency-drop heuristic: reward pressure rises while completion efficiency falls.
    if "activity_points_per_active_wow_pct" not in df.columns:
        df["activity_points_per_active_wow_pct"] = 0.0

    eff_drop = (
        (df["activity_points_per_active_wow_pct"] > 0.10)
        & (df["activity_completion_rate_wow_pct"] < -0.03)
    ) | (df["reward_efficiency_wow_pct"] < -0.08)
    scores -= np.where(eff_drop, config.efficiency_penalty, 0.0)

    # Clamp to [0, 100]
    scores = np.clip(scores, 0.0, 100.0)

    # Map score to classes.
    classes = np.where(scores >= 70, "Healthy", np.where(scores >= 45, "Warning", "AtRisk"))

    df["label_health_score"] = scores
    df["label_health_class"] = classes

    # Numeric target for optional downstream usage.
    class_to_int = {"AtRisk": 0, "Warning": 1, "Healthy": 2}
    df["label_health_class_int"] = df["label_health_class"].map(class_to_int).astype(int)

    return df



def labeling_thresholds() -> Dict[str, float]:
    cfg = LabelingConfig()
    return {
        "active_drop_warn": cfg.active_drop_warn,
        "active_drop_risk": cfg.active_drop_risk,
        "completion_drop_warn": cfg.completion_drop_warn,
        "completion_drop_risk": cfg.completion_drop_risk,
        "gmv_drop_warn": cfg.gmv_drop_warn,
        "gmv_drop_risk": cfg.gmv_drop_risk,
        "txn_drop_warn": cfg.txn_drop_warn,
        "txn_drop_risk": cfg.txn_drop_risk,
        "dormant_up_warn": cfg.dormant_up_warn,
        "dormant_up_risk": cfg.dormant_up_risk,
        "class_healthy_min": 70.0,
        "class_warning_min": 45.0,
    }
