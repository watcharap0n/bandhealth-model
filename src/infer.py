from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Dict, List, Mapping, Optional, Sequence, Tuple

import joblib
import numpy as np
import pandas as pd

from .driver_mapping import (
    COMMERCE_METRIC_FAMILIES,
    METRIC_FAMILY_CONFIG,
    canonical_driver_key,
    infer_metric_family_from_key,
)
from .drivers import attach_drivers
from .playbook import attach_actions, build_actions_i18n
from .segments import ACTIVITY_SEGMENT_KEYS, COMMERCE_SEGMENT_KEYS, SEGMENT_KEYS, SEGMENT_METRIC_SUFFIXES


CLASS_SCORE_MAP = {
    "Healthy": 90.0,
    "Warning": 60.0,
    "AtRisk": 25.0,
}

HEALTH_CLASS_TH = {
    "Healthy": "สุขภาพแบรนด์ดี",
    "Warning": "เริ่มน่ากังวล",
    "AtRisk": "เสี่ยงสูง",
}

CONFIDENCE_BAND_TH = {
    "high": "สูง",
    "medium": "กลาง",
    "low": "ต่ำ",
}

METRIC_FAMILY_LABELS = {
    "active_users": ("Active users", "ผู้ใช้งานที่แอคทีฟ"),
    "gmv_net": ("GMV", "GMV"),
    "transaction_count": ("Transactions", "จำนวนธุรกรรม"),
    "dormant_share": ("Dormant share", "สัดส่วนผู้ใช้ไม่เคลื่อนไหว"),
    "activity_completion_rate": ("Completion rate", "อัตราการทำกิจกรรมสำเร็จ"),
    "reward_efficiency": ("Reward efficiency", "ประสิทธิภาพรางวัล"),
    "redeem_rate": ("Redeem rate", "อัตราการแลกรับ"),
    "sku_concentration": ("SKU concentration", "ความกระจุกตัวของ SKU"),
}

SEGMENT_LABEL_TH = {
    "new_users_0_7d": "ผู้ใช้ใหม่ 0-7 วัน",
    "active_0_7d": "ผู้ใช้งานแอคทีฟ 0-7 วัน",
    "engaged_no_redeem": "มีส่วนร่วมสูงแต่ยังไม่แลก",
    "redeemers": "กลุ่มผู้แลกรับ",
    "recently_lapsed_8_14d": "ผู้ใช้ที่เพิ่งหลุด 8-14 วัน",
    "dormant_15_30d": "ผู้ใช้ไม่เคลื่อนไหว 15-30 วัน",
    "dormant_31_60d": "ผู้ใช้ไม่เคลื่อนไหว 31-60 วัน",
    "dormant_60d_plus": "ผู้ใช้ไม่เคลื่อนไหวมากกว่า 60 วัน",
    "non_redeemers": "ผู้ใช้ที่ยังไม่แลกรับ",
    "buyers": "ผู้ซื้อ",
    "repeat_buyers": "ผู้ซื้อซ้ำ",
    "high_aov_buyers": "ผู้ซื้อ AOV สูง",
    "discount_sensitive": "ผู้ใช้ไวต่อส่วนลด",
    "sku_affinity_top1": "ผู้ใช้ที่ชอบ SKU อันดับ 1",
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


def _direction_from_value(v: float, eps: float = 1e-9) -> str:
    if pd.isna(v):
        return "flat"
    if v > eps:
        return "up"
    if v < -eps:
        return "down"
    return "flat"


def _statement_direction(statement: str) -> Optional[str]:
    s = str(statement or "").lower()
    if " down " in f" {s} ":
        return "down"
    if " up " in f" {s} ":
        return "up"
    if " flat " in f" {s} ":
        return "flat"
    return None


def _metric_family_from_driver(driver: Mapping) -> Optional[str]:
    mf = str(driver.get("metric_family", "") or "").strip()
    if mf:
        return mf
    key = canonical_driver_key(str(driver.get("key", "")))
    return infer_metric_family_from_key(key)


def _add_total_metric_deltas(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy().sort_values(["brand_id", "window_size", "window_end_date"]).reset_index(drop=True)
    group_cols = ["brand_id", "window_size"]

    for fam, cfg in METRIC_FAMILY_CONFIG.items():
        col = cfg.get("total_col")
        if not col or col not in out.columns:
            continue
        prev_col = f"{col}_prev_window"
        delta_col = f"{col}_delta_window"
        wow_col = f"{col}_wow_pct_window"

        g = out.groupby(group_cols, observed=True)[col]
        prev = g.shift(1)
        delta = out[col] - prev
        wow = np.where(prev != 0, delta / prev, np.nan)
        wow = pd.Series(wow, index=out.index, dtype=float)

        out[prev_col] = prev
        out[delta_col] = delta
        out[wow_col] = wow

    return out


def _add_segment_deltas(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy().sort_values(["brand_id", "window_size", "window_end_date"]).reset_index(drop=True)
    group_cols = ["brand_id", "window_size"]

    for seg in SEGMENT_KEYS:
        for metric in SEGMENT_METRIC_SUFFIXES:
            col = f"seg_{seg}_{metric}"
            if col not in out.columns:
                continue
            prev_col = f"{col}_prev"
            delta_col = f"{col}_delta"
            wow_col = f"{col}_wow_pct"
            cold_col = f"{col}_cold_start_increase"

            g = out.groupby(group_cols, observed=True)[col]
            prev = g.shift(1)
            delta = out[col] - prev
            wow = np.where(prev != 0, delta / prev, np.nan)
            wow = pd.Series(wow, index=out.index, dtype=float)
            cold_start = (prev == 0) & (out[col] > 0)

            out[prev_col] = prev
            out[delta_col] = delta
            out[wow_col] = wow
            out[cold_col] = cold_start.fillna(False).astype(bool)

    return out


def _driver_signal_value(driver: Mapping, row: Mapping, metric_family: Optional[str]) -> float:
    metrics = driver.get("metrics", {})
    if isinstance(metrics, Mapping):
        for k, v in metrics.items():
            if k == "importance":
                continue
            try:
                return float(v)
            except (TypeError, ValueError):
                continue

    if metric_family and metric_family in METRIC_FAMILY_CONFIG:
        cfg = METRIC_FAMILY_CONFIG[metric_family]
        total_col = cfg.get("total_col")
        if total_col:
            v = row.get(f"{total_col}_delta_window", np.nan)
            if pd.notna(v):
                return float(v)
        wow_col = cfg.get("total_wow_col")
        if wow_col and wow_col in row:
            try:
                return float(row.get(wow_col, np.nan))
            except (TypeError, ValueError):
                pass
    return 0.0


def _driver_sign_mismatch(driver: Mapping, row: Mapping) -> bool:
    metric_family = _metric_family_from_driver(driver)
    if not metric_family:
        return False
    signal = _driver_signal_value(driver, row, metric_family)
    expected = _direction_from_value(signal)
    stated = str(driver.get("direction", "") or "").strip().lower()
    if not stated:
        stated = _statement_direction(str(driver.get("statement", ""))) or "flat"

    if expected == "flat":
        return False
    if stated == "flat":
        return False
    return expected != stated


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


def _i18n(en: str, th: Optional[str] = None) -> dict:
    return {"en": str(en), "th": str(th if th is not None else en)}


def _direction_th(direction: str) -> str:
    return {"up": "เพิ่มขึ้น", "down": "ลดลง", "flat": "ทรงตัว"}.get(direction, direction)


def _health_class_i18n(health_class: str) -> dict:
    en = str(health_class)
    th = HEALTH_CLASS_TH.get(en, en)
    return _i18n(en, th)


def _confidence_band_i18n(conf_band: str) -> dict:
    en = str(conf_band)
    th = CONFIDENCE_BAND_TH.get(en, en)
    return _i18n(en, th)


def _health_statement_i18n(health_class: str, score: float, confidence_band: str) -> dict:
    stmt_en = _health_statement(health_class, score, confidence_band)
    class_th = HEALTH_CLASS_TH.get(str(health_class), str(health_class))
    near_threshold = min(abs(score - 70.0), abs(score - 45.0)) <= 5.0
    if confidence_band == "low" and near_threshold:
        return _i18n(stmt_en, f"{class_th} (ใกล้เส้นแบ่ง)")
    return _i18n(stmt_en, class_th)


def _metric_family_label(metric_family: str) -> tuple[str, str]:
    if metric_family in METRIC_FAMILY_LABELS:
        return METRIC_FAMILY_LABELS[metric_family]
    return metric_family, metric_family


def _segment_label_i18n(segment_key: str) -> dict:
    en = str(segment_key)
    th = SEGMENT_LABEL_TH.get(en, en)
    return _i18n(en, th)


def _segment_candidates(metric_family: str, commerce_joinable: bool) -> Sequence[str]:
    if metric_family in COMMERCE_METRIC_FAMILIES:
        return COMMERCE_SEGMENT_KEYS if commerce_joinable else ()
    return ACTIVITY_SEGMENT_KEYS


def _is_big_brand(row: Mapping) -> bool:
    active = float(row.get("active_users", 0.0) or 0.0)
    return active >= 1000


def _tiny_delta(metric_family: str, delta_total: float, row: Mapping) -> bool:
    if metric_family in {"active_users", "transaction_count", "gmv_net"}:
        base = abs(float(row.get(METRIC_FAMILY_CONFIG[metric_family]["total_col"], 0.0) or 0.0))
        eps = max(1.0, base * 0.01)
        return abs(delta_total) < eps
    return abs(delta_total) < 0.01


def _segment_confidence(
    metric_family: str,
    segment_share_now: float,
    delta_total: float,
    commerce_joinable: bool,
    note: str,
) -> str:
    conf = "high"
    if segment_share_now < 0.05 or _tiny_delta(metric_family, delta_total, {}):
        conf = "medium"
    if metric_family in COMMERCE_METRIC_FAMILIES and not commerce_joinable:
        conf = "low"
    if note in {"cold_start", "noisy"}:
        conf = "medium" if conf == "high" else conf
    return conf


def _format_metric_family_label(metric_family: str) -> str:
    en, _ = _metric_family_label(metric_family)
    return en


def _build_target_segments_for_row(
    row: Mapping,
    top_k_per_family: int = 3,
    min_share: float = 0.02,
    qa_counter: Optional[Counter] = None,
) -> tuple[list[dict], list[str]]:
    qa = qa_counter if qa_counter is not None else Counter()
    warnings: List[str] = []

    drivers = row.get("drivers", [])
    if not isinstance(drivers, list) or not drivers:
        return [], warnings

    commerce_joinable = bool(float(row.get("commerce_joinable", 0.0) or 0.0))
    min_count = 30 if _is_big_brand(row) else 5

    primary_family_driver: Dict[str, Mapping] = {}
    invalid_families: set = set()

    for d in drivers:
        if not isinstance(d, Mapping):
            continue
        metric_family = _metric_family_from_driver(d)
        if not metric_family:
            continue

        if _driver_sign_mismatch(d, row):
            invalid_families.add(metric_family)
            qa["driver_sign_mismatch"] += 1
            warnings.append(f"driver_sign_mismatch:{metric_family}:{d.get('key')}")
            continue

        if metric_family not in primary_family_driver:
            primary_family_driver[metric_family] = d

    out: List[dict] = []

    for metric_family, driver in primary_family_driver.items():
        if metric_family in invalid_families:
            continue
        cfg = METRIC_FAMILY_CONFIG.get(metric_family)
        if not cfg:
            continue

        if metric_family in COMMERCE_METRIC_FAMILIES and not commerce_joinable:
            qa["segments_dropped_commerce_mode"] += 1
            warnings.append(f"commerce_mode_block:{metric_family}")
            continue

        total_col = cfg.get("total_col", "")
        seg_metric = cfg.get("segment_metric", "")
        if not total_col or not seg_metric:
            continue

        delta_total = float(row.get(f"{total_col}_delta_window", 0.0) or 0.0)
        direction = _direction_from_value(delta_total)
        if direction == "flat" or _tiny_delta(metric_family, delta_total, row):
            qa["metric_family_noisy"] += 1
            warnings.append(f"noisy_metric:{metric_family}")
            continue

        eligible: List[dict] = []
        for seg in _segment_candidates(metric_family, commerce_joinable=commerce_joinable):
            seg_now_col = f"seg_{seg}_{seg_metric}"
            seg_prev_col = f"{seg_now_col}_prev"
            seg_delta_col = f"{seg_now_col}_delta"
            seg_wow_col = f"{seg_now_col}_wow_pct"
            seg_cold_col = f"{seg_now_col}_cold_start_increase"
            seg_share_col = f"seg_{seg}_share"
            seg_count_col = f"seg_{seg}_users"

            if seg_now_col not in row or seg_delta_col not in row:
                continue

            seg_now = float(row.get(seg_now_col, 0.0) or 0.0)
            seg_prev = row.get(seg_prev_col, np.nan)
            seg_delta = float(row.get(seg_delta_col, 0.0) or 0.0)
            seg_share = float(row.get(seg_share_col, 0.0) or 0.0)
            seg_count = int(round(float(row.get(seg_count_col, 0.0) or 0.0)))

            if seg_share <= 0.0 or seg_count <= 0:
                qa["segments_dropped_zero_presence"] += 1
                continue

            if not (seg_share >= min_share or seg_count >= min_count):
                qa["segments_dropped_min_presence"] += 1
                continue

            if direction == "down" and seg_delta >= 0:
                qa["segments_dropped_sign_mismatch"] += 1
                continue
            if direction == "up" and seg_delta <= 0:
                qa["segments_dropped_sign_mismatch"] += 1
                continue

            if pd.isna(seg_prev):
                qa["segments_dropped_no_prev"] += 1
                continue

            note = "stable"
            wow_pct_seg = row.get(seg_wow_col, np.nan)
            cold_start = bool(row.get(seg_cold_col, False))
            if (not pd.isna(seg_prev)) and float(seg_prev) == 0:
                if seg_now > 0:
                    wow_pct_seg = None
                    note = "cold_start"
                else:
                    qa["segments_dropped_noisy"] += 1
                    continue
            elif pd.isna(wow_pct_seg):
                wow_pct_seg = None

            driver_stmt_i18n = driver.get("statement_i18n", _i18n(str(driver.get("statement", "")).strip()))
            if not isinstance(driver_stmt_i18n, Mapping):
                driver_stmt_i18n = _i18n(str(driver.get("statement", "")).strip())

            eligible.append(
                {
                    "metric_family": metric_family,
                    "segment_key": seg,
                    "direction": direction,
                    "delta_seg": seg_delta,
                    "delta_total": delta_total,
                    "segment_share_now": seg_share,
                    "segment_count_now": seg_count,
                    "wow_pct_seg": wow_pct_seg,
                    "note": note,
                    "cold_start_increase": cold_start or note == "cold_start",
                    "driver_statement": str(driver.get("statement", "")).strip(),
                    "driver_statement_i18n": driver_stmt_i18n,
                }
            )

        if not eligible:
            qa["metric_family_no_eligible_segments"] += 1
            warnings.append(f"no_eligible_segments:{metric_family}")
            continue

        denom = float(sum(abs(x["delta_seg"]) for x in eligible))
        if denom <= 0:
            qa["metric_family_zero_denom"] += 1
            warnings.append(f"zero_denom:{metric_family}")
            continue

        top = sorted(eligible, key=lambda x: abs(x["delta_seg"]), reverse=True)[:top_k_per_family]
        family_label_en, family_label_th = _metric_family_label(metric_family)

        for e in top:
            seg_conf = _segment_confidence(
                metric_family=metric_family,
                segment_share_now=e["segment_share_now"],
                delta_total=e["delta_total"],
                commerce_joinable=commerce_joinable,
                note=e["note"],
            )
            seg_label = _segment_label_i18n(e["segment_key"])
            reason_en = (
                f"{family_label_en} {e['direction']} driven by `{e['segment_key']}` "
                f"(driver: {e['driver_statement']})"
            )
            reason_th = (
                f"{family_label_th} {_direction_th(e['direction'])} โดยมีแรงขับหลักจาก `{seg_label['th']}` "
                f"(driver: {e['driver_statement_i18n'].get('th', e['driver_statement'])})"
            )
            out.append(
                {
                    "metric_family": metric_family,
                    "metric_family_i18n": _i18n(family_label_en, family_label_th),
                    "segment_key": e["segment_key"],
                    "segment_label_i18n": seg_label,
                    "direction": e["direction"],
                    "direction_i18n": _i18n(e["direction"], _direction_th(e["direction"])),
                    "contribution_share": float(abs(e["delta_seg"]) / denom),
                    "reason_statement": reason_en,
                    "reason_statement_i18n": _i18n(reason_en, reason_th),
                    "evidence_metrics": {
                        "delta_seg": float(e["delta_seg"]),
                        "delta_total": float(e["delta_total"]),
                        "segment_share_now": float(e["segment_share_now"]),
                        "segment_count_now": int(e["segment_count_now"]),
                        "wow_pct_seg": None if e["wow_pct_seg"] is None else float(e["wow_pct_seg"]),
                        "cold_start_increase": bool(e["cold_start_increase"]),
                        "note": e["note"],
                    },
                    "segment_confidence": seg_conf,
                }
            )

    return out, warnings


def _validate_target_segments_row(row: Mapping, target_segments: Sequence[Mapping], qa_counter: Optional[Counter] = None) -> tuple[list[dict], list[str]]:
    qa = qa_counter if qa_counter is not None else Counter()
    warnings: List[str] = []
    commerce_joinable = bool(float(row.get("commerce_joinable", 0.0) or 0.0))

    out: List[dict] = []
    for s in target_segments:
        if not isinstance(s, Mapping):
            continue
        metric_family = str(s.get("metric_family", "")).strip()
        segment_key = str(s.get("segment_key", "")).strip()
        direction = str(s.get("direction", "")).strip()
        ev = s.get("evidence_metrics", {}) if isinstance(s.get("evidence_metrics", {}), Mapping) else {}

        seg_share = float(ev.get("segment_share_now", 0.0) or 0.0)
        seg_count = int(ev.get("segment_count_now", 0) or 0)
        delta_seg = float(ev.get("delta_seg", 0.0) or 0.0)

        if seg_share <= 0.0 or seg_count <= 0:
            qa["segments_dropped_validation_presence"] += 1
            warnings.append(f"drop_presence:{metric_family}:{segment_key}")
            continue

        sign = _direction_from_value(delta_seg)
        if sign != "flat" and direction in {"up", "down"} and sign != direction:
            qa["segments_dropped_validation_direction"] += 1
            warnings.append(f"drop_direction:{metric_family}:{segment_key}")
            continue

        if (metric_family in COMMERCE_METRIC_FAMILIES or segment_key in COMMERCE_SEGMENT_KEYS) and not commerce_joinable:
            qa["segments_dropped_validation_commerce"] += 1
            warnings.append(f"drop_commerce_nonjoinable:{metric_family}:{segment_key}")
            continue

        out.append(dict(s))

    return out, warnings


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

    out = _add_total_metric_deltas(out)
    out = _add_segment_deltas(out)

    conf = out.apply(lambda r: _confidence_row(r, class_labels), axis=1)
    out["confidence_band"] = conf.apply(lambda x: x[0])
    out["confidence_top_probability"] = conf.apply(lambda x: x[1])
    out["confidence_margin"] = conf.apply(lambda x: x[2])
    out["predicted_health_statement"] = out.apply(
        lambda r: _health_statement(
            str(r.get("predicted_health_class", "")),
            float(r.get("predicted_health_score", 0.0)),
            str(r.get("confidence_band", "low")),
        ),
        axis=1,
    )
    out["predicted_health_class_i18n"] = out["predicted_health_class"].apply(lambda x: _health_class_i18n(str(x)))
    out["confidence_band_i18n"] = out["confidence_band"].apply(lambda x: _confidence_band_i18n(str(x)))
    out["predicted_health_statement_i18n"] = out.apply(
        lambda r: _health_statement_i18n(
            str(r.get("predicted_health_class", "")),
            float(r.get("predicted_health_score", 0.0)),
            str(r.get("confidence_band", "low")),
        ),
        axis=1,
    )

    out = attach_drivers(out, feature_importance=feature_importance, top_n=top_n_drivers)

    qa = Counter()
    targets: List[list] = []
    warns_col: List[list] = []
    for _, row in out.iterrows():
        t, w = _build_target_segments_for_row(
            row.to_dict(),
            top_k_per_family=top_n_target_segments,
            qa_counter=qa,
        )
        t, w2 = _validate_target_segments_row(row.to_dict(), t, qa_counter=qa)
        targets.append(t)
        warns_col.append(w + w2)
        qa["rows_total"] += 1
        qa["segments_output_total"] += len(t)
        if t:
            qa["rows_with_target_segments"] += 1
        qa["segments_dropped_total"] = (
            qa["segments_dropped_sign_mismatch"]
            + qa["segments_dropped_zero_presence"]
            + qa["segments_dropped_min_presence"]
            + qa["segments_dropped_no_prev"]
            + qa["segments_dropped_noisy"]
            + qa["segments_dropped_commerce_mode"]
            + qa["segments_dropped_validation_presence"]
            + qa["segments_dropped_validation_direction"]
            + qa["segments_dropped_validation_commerce"]
        )

    out["target_segments"] = targets
    out["attribution_warnings"] = warns_col
    out = attach_actions(out, top_n=top_n_actions)
    out["suggested_actions_i18n"] = out["suggested_actions"].apply(lambda acts: build_actions_i18n(acts if isinstance(acts, list) else []))

    def _to_payload(row: pd.Series) -> dict:
        probs = {c: float(row[f"prob_{c}"]) for c in class_labels}
        return {
            "brand_id": str(row.get("brand_id")),
            "window_end_date": str(row.get("window_end_date")),
            "window_size": str(row.get("window_size")),
            "predicted_health_class": str(row.get("predicted_health_class")),
            "predicted_health_class_i18n": row.get("predicted_health_class_i18n", _i18n(str(row.get("predicted_health_class", "")))),
            "predicted_health_statement": str(row.get("predicted_health_statement")),
            "predicted_health_statement_i18n": row.get("predicted_health_statement_i18n", _i18n(str(row.get("predicted_health_statement", "")))),
            "predicted_health_score": float(row.get("predicted_health_score", 0.0)),
            "confidence_band": str(row.get("confidence_band", "low")),
            "confidence_band_i18n": row.get("confidence_band_i18n", _i18n(str(row.get("confidence_band", "low")))),
            "probabilities": probs,
            "drivers": row.get("drivers", []),
            "target_segments": row.get("target_segments", []),
            "suggested_actions": row.get("suggested_actions", []),
            "suggested_actions_i18n": row.get("suggested_actions_i18n", []),
            "attribution_warnings": row.get("attribution_warnings", []),
        }

    out["payload"] = out.apply(_to_payload, axis=1)
    out.attrs["attribution_qa"] = dict(qa)
    return out


def save_predictions(pred_df: pd.DataFrame, output_dir: str | Path) -> None:
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    save_cols = [
        "brand_id",
        "window_end_date",
        "window_size",
        "predicted_health_class",
        "predicted_health_class_i18n",
        "predicted_health_statement",
        "predicted_health_statement_i18n",
        "predicted_health_score",
        "confidence_band",
        "confidence_band_i18n",
        "drivers",
        "target_segments",
        "suggested_actions",
        "suggested_actions_i18n",
        "attribution_warnings",
    ] + [c for c in pred_df.columns if c.startswith("prob_")]

    pred_df[save_cols].to_csv(out_dir / "predictions_with_drivers.csv", index=False)

    with (out_dir / "predictions_with_drivers.jsonl").open("w", encoding="utf-8") as f:
        for payload in pred_df["payload"]:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")
