from __future__ import annotations

from typing import Dict, List, Mapping, Optional

import numpy as np
import pandas as pd

from .driver_mapping import canonical_driver_key, infer_metric_family_from_key

DRIVER_KEY_TH: Dict[str, str] = {
    "active_down": "ผู้ใช้งานลดลง",
    "completion_drop": "อัตราการทำกิจกรรมสำเร็จลดลง",
    "redeem_down": "อัตราการแลกรับลดลง",
    "gmv_down": "GMV ลดลง",
    "transactions_down": "จำนวนธุรกรรมลดลง",
    "dormant_up": "สัดส่วนผู้ใช้ไม่เคลื่อนไหวเพิ่มขึ้น",
    "efficiency_drop": "ประสิทธิภาพรางวัลลดลง",
    "sku_concentration_high": "ความกระจุกตัวของ SKU สูง",
}

LABEL_TH: Dict[str, str] = {
    "Active users": "ผู้ใช้งานที่แอคทีฟ",
    "Completion rate": "อัตราการทำกิจกรรมสำเร็จ",
    "Redemption proxy rate": "อัตราการแลกรับ (proxy)",
    "GMV": "GMV",
    "Transactions": "จำนวนธุรกรรม",
    "Dormant share": "สัดส่วนผู้ใช้ไม่เคลื่อนไหว",
}


def _pct(v: float) -> str:
    return f"{v * 100:+.1f}%"


def _val(v: float) -> str:
    if abs(v) >= 1000:
        return f"{v:,.0f}"
    if abs(v) >= 100:
        return f"{v:,.1f}"
    return f"{v:.2f}"


def _direction(v: float, eps: float = 1e-4) -> str:
    if v > eps:
        return "up"
    if v < -eps:
        return "down"
    return "flat"


def _i18n(en: str, th: Optional[str] = None) -> dict:
    return {"en": str(en), "th": str(th if th is not None else en)}


def _direction_th(direction: str) -> str:
    return {"up": "เพิ่มขึ้น", "down": "ลดลง", "flat": "ทรงตัว"}.get(direction, direction)


def _driver_key_i18n(key: str) -> dict:
    en = str(key)
    th = DRIVER_KEY_TH.get(key, en)
    return _i18n(en, th)


def _add_driver(
    drivers: List[dict],
    key: str,
    statement_en: str,
    statement_th: Optional[str],
    severity: float,
    metrics: Mapping[str, float],
    direction: str,
    metric_family: Optional[str] = None,
) -> None:
    ckey = canonical_driver_key(key)
    mf = metric_family or infer_metric_family_from_key(ckey)
    drivers.append(
        {
            "key": ckey,
            "key_i18n": _driver_key_i18n(ckey),
            "statement": statement_en,
            "statement_i18n": _i18n(statement_en, statement_th),
            "severity": float(severity),
            "direction": direction,
            "direction_i18n": _i18n(direction, _direction_th(direction)),
            "metric_family": mf,
            "metrics": {k: float(v) for k, v in metrics.items()},
        }
    )


def _trend_statement(label: str, value: float, suffix: str = "WoW") -> tuple[str, str]:
    d = _direction(value)
    if label.startswith("Active users "):
        window = label.replace("Active users ", "").strip()
        label_th = f"ผู้ใช้งานที่แอคทีฟ {window}"
    else:
        label_th = LABEL_TH.get(label, label)
    d_th = _direction_th(d)
    if d == "flat":
        return (
            f"{label} flat ({_pct(value)}) {suffix}",
            f"{label_th} {d_th} ({_pct(value)}) {suffix}",
        )
    return (
        f"{label} {d} {_pct(value)} {suffix}",
        f"{label_th} {d_th} {_pct(value)} {suffix}",
    )


def _model_signal_statement(feat: str, value: float) -> tuple[str, str]:
    d = _direction(value)
    if feat.endswith("_wow_pct"):
        if d == "flat":
            return (
                f"Model-highlighted signal: {feat} flat ({_pct(value)})",
                f"สัญญาณที่โมเดลเน้น: {feat} ทรงตัว ({_pct(value)})",
            )
        return (
            f"Model-highlighted signal: {feat} {d} {_pct(value)}",
            f"สัญญาณที่โมเดลเน้น: {feat} {_direction_th(d)} {_pct(value)}",
        )
    if feat.endswith("_zscore"):
        if d == "flat":
            return (
                f"Model-highlighted anomaly: {feat}={_val(value)} (near baseline)",
                f"ความผิดปกติที่โมเดลเน้น: {feat}={_val(value)} (ใกล้ค่า baseline)",
            )
        return (
            f"Model-highlighted anomaly: {feat}={_val(value)} ({d} vs baseline)",
            f"ความผิดปกติที่โมเดลเน้น: {feat}={_val(value)} ({_direction_th(d)} เทียบ baseline)",
        )
    return (
        f"Model-highlighted signal: {feat}={_val(value)}",
        f"สัญญาณที่โมเดลเน้น: {feat}={_val(value)}",
    )



def build_metric_drivers(row: Mapping[str, float], top_n: int = 5) -> List[dict]:
    drivers: List[dict] = []

    window = str(row.get("window_size", "window"))

    active_wow = float(row.get("active_users_wow_pct", 0.0))
    if active_wow <= -0.08:
        s_en, s_th = _trend_statement(f"Active users {window}", active_wow, suffix="WoW")
        _add_driver(
            drivers,
            "active_down",
            s_en,
            s_th,
            abs(active_wow),
            {"active_users_wow_pct": active_wow},
            direction=_direction(active_wow),
            metric_family="active_users",
        )

    completion_wow = float(row.get("activity_completion_rate_wow_pct", 0.0))
    if completion_wow <= -0.06:
        s_en, s_th = _trend_statement("Completion rate", completion_wow, suffix="WoW")
        _add_driver(
            drivers,
            "completion_drop",
            s_en,
            s_th,
            abs(completion_wow),
            {"activity_completion_rate_wow_pct": completion_wow},
            direction=_direction(completion_wow),
            metric_family="activity_completion_rate",
        )

    redeem_wow = float(row.get("activity_redeem_rate_wow_pct", 0.0))
    if redeem_wow <= -0.06:
        s_en, s_th = _trend_statement("Redemption proxy rate", redeem_wow, suffix="WoW")
        _add_driver(
            drivers,
            "redeem_down",
            s_en,
            s_th,
            abs(redeem_wow),
            {"activity_redeem_rate_wow_pct": redeem_wow},
            direction=_direction(redeem_wow),
            metric_family="redeem_rate",
        )

    gmv_wow = float(row.get("gmv_net_wow_pct", 0.0))
    if gmv_wow <= -0.10:
        s_en, s_th = _trend_statement("GMV", gmv_wow, suffix="WoW")
        _add_driver(
            drivers,
            "gmv_down",
            s_en,
            s_th,
            abs(gmv_wow),
            {"gmv_net_wow_pct": gmv_wow},
            direction=_direction(gmv_wow),
            metric_family="gmv_net",
        )

    txn_wow = float(row.get("transaction_count_wow_pct", 0.0))
    if txn_wow <= -0.10:
        s_en, s_th = _trend_statement("Transactions", txn_wow, suffix="WoW")
        _add_driver(
            drivers,
            "transactions_down",
            s_en,
            s_th,
            abs(txn_wow),
            {"transaction_count_wow_pct": txn_wow},
            direction=_direction(txn_wow),
            metric_family="transaction_count",
        )

    dormant_wow = float(row.get("dormant_share_wow_pct", 0.0))
    if dormant_wow >= 0.06:
        s_en, s_th = _trend_statement("Dormant share", dormant_wow, suffix="WoW")
        _add_driver(
            drivers,
            "dormant_up",
            s_en,
            s_th,
            abs(dormant_wow),
            {"dormant_share_wow_pct": dormant_wow},
            direction=_direction(dormant_wow),
            metric_family="dormant_share",
        )

    reward_eff_wow = float(row.get("reward_efficiency_wow_pct", 0.0))
    points_wow = float(row.get("activity_points_per_active_wow_pct", 0.0))
    if reward_eff_wow <= -0.08 or (points_wow > 0.1 and completion_wow < -0.03):
        _add_driver(
            drivers,
            "efficiency_drop",
            "Reward intensity increased while conversion efficiency declined",
            "ความเข้มข้นของรางวัลเพิ่มขึ้น แต่ประสิทธิภาพการเปลี่ยนเป็นการกระทำลดลง",
            max(abs(reward_eff_wow), abs(points_wow), abs(completion_wow)),
            {
                "reward_efficiency_wow_pct": reward_eff_wow,
                "activity_points_per_active_wow_pct": points_wow,
                "activity_completion_rate_wow_pct": completion_wow,
            },
            direction="down",
            metric_family="reward_efficiency",
        )

    sku_top = float(row.get("sku_top_share", 0.0))
    if sku_top >= 0.55:
        _add_driver(
            drivers,
            "sku_concentration_high",
            f"SKU mix concentration is high (top SKU share {_pct(sku_top)})",
            f"ความกระจุกตัวของ SKU สูง (สัดส่วน SKU อันดับ 1 {_pct(sku_top)})",
            abs(sku_top),
            {"sku_top_share": sku_top},
            direction="up",
            metric_family="sku_concentration",
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
            key = canonical_driver_key(f"model_{feat}")
            direction = _direction(val)
            s_en, s_th = _model_signal_statement(feat, val)
            rows.append(
                {
                    "key": key,
                    "key_i18n": _driver_key_i18n(key),
                    "statement": s_en,
                    "statement_i18n": _i18n(s_en, s_th),
                    "severity": float(abs(val) * abs(float(imp))),
                    "direction": direction,
                    "direction_i18n": _i18n(direction, _direction_th(direction)),
                    "metric_family": infer_metric_family_from_key(key),
                    "metrics": {feat: val, "importance": float(imp)},
                }
            )
            used += 1
        elif feat.endswith("_zscore") and abs(val) >= 1.0:
            key = canonical_driver_key(f"model_{feat}")
            direction = _direction(val)
            s_en, s_th = _model_signal_statement(feat, val)
            rows.append(
                {
                    "key": key,
                    "key_i18n": _driver_key_i18n(key),
                    "statement": s_en,
                    "statement_i18n": _i18n(s_en, s_th),
                    "severity": float(abs(val) * abs(float(imp))),
                    "direction": direction,
                    "direction_i18n": _i18n(direction, _direction_th(direction)),
                    "metric_family": infer_metric_family_from_key(key),
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
