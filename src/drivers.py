from __future__ import annotations

from typing import Dict, List, Mapping, Optional

import numpy as np
import pandas as pd


def _pct(v: float) -> str:
    return f"{v * 100:+.1f}%"


def _val(v: float) -> str:
    if abs(v) >= 1000:
        return f"{v:,.0f}"
    if abs(v) >= 100:
        return f"{v:,.1f}"
    return f"{v:.2f}"


def _add_driver(drivers: List[dict], key: str, statement: str, severity: float, metrics: Mapping[str, float]) -> None:
    drivers.append(
        {
            "key": key,
            "statement": statement,
            "severity": float(severity),
            "metrics": {k: float(v) for k, v in metrics.items()},
        }
    )



def build_metric_drivers(row: Mapping[str, float], top_n: int = 5) -> List[dict]:
    drivers: List[dict] = []

    window = str(row.get("window_size", "window"))

    active_wow = float(row.get("active_users_wow_pct", 0.0))
    if active_wow <= -0.08:
        _add_driver(
            drivers,
            "active_down",
            f"Active users {window} down {_pct(active_wow)} WoW",
            abs(active_wow),
            {"active_users_wow_pct": active_wow},
        )

    completion_wow = float(row.get("activity_completion_rate_wow_pct", 0.0))
    if completion_wow <= -0.06:
        _add_driver(
            drivers,
            "completion_down",
            f"Completion rate down {_pct(completion_wow)} WoW",
            abs(completion_wow),
            {"activity_completion_rate_wow_pct": completion_wow},
        )

    redeem_wow = float(row.get("activity_redeem_rate_wow_pct", 0.0))
    if redeem_wow <= -0.06:
        _add_driver(
            drivers,
            "redeem_down",
            f"Redemption proxy rate down {_pct(redeem_wow)} WoW",
            abs(redeem_wow),
            {"activity_redeem_rate_wow_pct": redeem_wow},
        )

    gmv_wow = float(row.get("gmv_net_wow_pct", 0.0))
    if gmv_wow <= -0.10:
        _add_driver(
            drivers,
            "gmv_down",
            f"GMV down {_pct(gmv_wow)} WoW",
            abs(gmv_wow),
            {"gmv_net_wow_pct": gmv_wow},
        )

    txn_wow = float(row.get("transaction_count_wow_pct", 0.0))
    if txn_wow <= -0.10:
        _add_driver(
            drivers,
            "transactions_down",
            f"Transactions down {_pct(txn_wow)} WoW",
            abs(txn_wow),
            {"transaction_count_wow_pct": txn_wow},
        )

    dormant_wow = float(row.get("dormant_share_wow_pct", 0.0))
    if dormant_wow >= 0.06:
        _add_driver(
            drivers,
            "dormant_up",
            f"Dormant share up {_pct(dormant_wow)} WoW",
            abs(dormant_wow),
            {"dormant_share_wow_pct": dormant_wow},
        )

    reward_eff_wow = float(row.get("reward_efficiency_wow_pct", 0.0))
    points_wow = float(row.get("activity_points_per_active_wow_pct", 0.0))
    if reward_eff_wow <= -0.08 or (points_wow > 0.1 and completion_wow < -0.03):
        _add_driver(
            drivers,
            "efficiency_drop",
            "Reward intensity increased while conversion efficiency declined",
            max(abs(reward_eff_wow), abs(points_wow), abs(completion_wow)),
            {
                "reward_efficiency_wow_pct": reward_eff_wow,
                "activity_points_per_active_wow_pct": points_wow,
                "activity_completion_rate_wow_pct": completion_wow,
            },
        )

    sku_top = float(row.get("sku_top_share", 0.0))
    if sku_top >= 0.55:
        _add_driver(
            drivers,
            "sku_concentration_high",
            f"SKU mix concentration is high (top SKU share {_pct(sku_top)})",
            abs(sku_top),
            {"sku_top_share": sku_top},
        )

    # Keep strongest metric rules.
    drivers = sorted(drivers, key=lambda x: x["severity"], reverse=True)
    return drivers[:top_n]



def build_model_importance_drivers(
    row: Mapping[str, float],
    feature_importance: Optional[Mapping[str, float]] = None,
    top_k: int = 3,
) -> List[dict]:
    if not feature_importance:
        return []

    rows: List[dict] = []
    important = sorted(feature_importance.items(), key=lambda kv: abs(kv[1]), reverse=True)

    used = 0
    for feat, imp in important:
        if used >= top_k:
            break
        if feat not in row:
            continue

        try:
            val = float(row.get(feat, 0.0))
        except (TypeError, ValueError):
            continue

        # Only surface interpretable change-like features.
        if feat.endswith("_wow_pct") and abs(val) >= 0.05:
            direction = "down" if val < 0 else "up"
            rows.append(
                {
                    "key": f"model_{feat}",
                    "statement": f"Model-highlighted signal: {feat} {direction} {_pct(val)}",
                    "severity": float(abs(val) * abs(float(imp))),
                    "metrics": {feat: val, "importance": float(imp)},
                }
            )
            used += 1
        elif feat.endswith("_zscore") and abs(val) >= 1.0:
            rows.append(
                {
                    "key": f"model_{feat}",
                    "statement": f"Model-highlighted anomaly: {feat}={_val(val)}",
                    "severity": float(abs(val) * abs(float(imp))),
                    "metrics": {feat: val, "importance": float(imp)},
                }
            )
            used += 1

    return sorted(rows, key=lambda x: x["severity"], reverse=True)



def build_drivers(
    row: Mapping[str, float],
    feature_importance: Optional[Mapping[str, float]] = None,
    top_n: int = 5,
) -> List[dict]:
    metric_drivers = build_metric_drivers(row, top_n=top_n)
    model_drivers = build_model_importance_drivers(row, feature_importance=feature_importance, top_k=3)

    merged: Dict[str, dict] = {}
    for d in metric_drivers + model_drivers:
        if d["key"] not in merged:
            merged[d["key"]] = d

    out = sorted(merged.values(), key=lambda x: x.get("severity", 0.0), reverse=True)
    return out[:top_n]



def attach_drivers(
    predictions_df: pd.DataFrame,
    feature_importance: Optional[Mapping[str, float]] = None,
    top_n: int = 5,
) -> pd.DataFrame:
    df = predictions_df.copy()
    df["drivers"] = df.apply(lambda r: build_drivers(r.to_dict(), feature_importance=feature_importance, top_n=top_n), axis=1)
    return df
