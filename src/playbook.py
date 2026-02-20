from __future__ import annotations

from typing import Dict, List, Mapping, Optional, Sequence

from .driver_mapping import canonical_driver_key, infer_metric_family_from_key

ACTION_MAP: Dict[str, List[str]] = {
    "active_users": [
        "Launch dormant-user reactivation campaigns with segmented incentives.",
        "Reduce message fatigue via tighter frequency caps and send-time optimization.",
        "Retarget recently lapsed cohorts with low-friction missions.",
    ],
    "activity_completion_rate": [
        "Audit mission completion friction (steps, UX, rules) and simplify top drop-off flows.",
        "Shift reward mix toward lower-friction, faster-win activities.",
        "A/B test mission copy and CTA clarity on the highest-volume activities.",
    ],
    "redeem_rate": [
        "Increase redemption visibility with in-app reminders and redemption-specific triggers.",
        "Rebalance point thresholds to reduce perceived redemption effort.",
        "Promote expiring-value nudges to accelerate redemption intent.",
    ],
    "gmv_net": [
        "Run short-cycle conversion pushes for high-intent audiences.",
        "Bundle offers to lift average order value and repeat basket behavior.",
        "Check checkout/payment friction and recover abandoned purchase attempts.",
    ],
    "transaction_count": [
        "Introduce repeat-purchase triggers based on product replenishment cadence.",
        "Activate personalized offer ladders for near-churn buyers.",
        "Prioritize retention promotions for buyers with recent order decline.",
    ],
    "dormant_share": [
        "Create dormant-tier win-back journeys with escalating incentives.",
        "Use triggered education content to reintroduce loyalty value.",
        "Suppress low-intent users from broad blasts and target by propensity.",
    ],
    "reward_efficiency": [
        "Recalibrate reward economics: trim low-yield rewards and raise completion-linked value.",
        "Prioritize activities with best completion-per-point efficiency.",
        "Set guardrails so point inflation does not outpace engagement conversion.",
    ],
    "sku_concentration": [
        "Diversify reward/product mix to reduce dependence on top SKU(s).",
        "Promote under-indexed SKUs through personalized recommendations.",
        "Use rotation strategy for featured SKUs to broaden basket composition.",
    ],
}


COMMERCE_SEGMENT_KEYS = {
    "buyers",
    "repeat_buyers",
    "high_aov_buyers",
    "discount_sensitive",
    "sku_affinity_top1",
}


def _segment_objects(target_segments: Optional[Sequence[Mapping]]) -> List[dict]:
    if not target_segments:
        return []
    out: List[dict] = []
    for s in target_segments:
        if not isinstance(s, Mapping):
            continue
        k = str(s.get("segment_key", "")).strip()
        if not k:
            continue
        out.append(
            {
                "segment_key": k,
                "metric_family": str(s.get("metric_family", "")).strip(),
                "direction": str(s.get("direction", "")).strip(),
                "contribution_share": float(s.get("contribution_share", 0.0) or 0.0),
            }
        )
    out.sort(key=lambda x: x["contribution_share"], reverse=True)
    return out


def _segment_specific_actions(
    metric_family: str,
    seg_key: str,
    direction: str,
    commerce_joinable: bool,
    row: Optional[Mapping] = None,
) -> List[str]:
    actions: List[str] = []
    active_wow = float(row.get("active_users_wow_pct", 0.0)) if row else 0.0

    if metric_family == "active_users" and direction == "down" and seg_key in {"recently_lapsed_8_14d", "dormant_15_30d", "dormant_31_60d", "dormant_60d_plus"}:
        actions.extend(
            [
                f"Trigger winback rewards for `{seg_key}` with short expiry and capped frequency.",
                f"Set reactivation journeys for `{seg_key}` using low-friction missions first.",
            ]
        )
    if metric_family == "active_users" and direction == "down" and seg_key in {"new_users_0_7d"}:
        actions.append("Boost first-7-day activation missions for `new_users_0_7d` with immediate low-friction rewards.")

    if metric_family == "dormant_share" and direction == "up" and seg_key in {"dormant_15_30d", "dormant_31_60d", "dormant_60d_plus", "recently_lapsed_8_14d"}:
        actions.extend(
            [
                f"Apply reactivation + frequency caps specifically for `{seg_key}`.",
                f"Use low-friction missions for `{seg_key}` before high-effort offers.",
            ]
        )

    if metric_family == "activity_completion_rate" and direction == "down" and seg_key in {"active_0_7d", "non_redeemers"}:
        actions.append(f"Reduce mission friction for `{seg_key}` and A/B test mission flow copy.")

    if metric_family == "gmv_net" and direction == "down" and active_wow >= -0.05 and commerce_joinable:
        actions.append("Active base is stable; run purchase triggers for `buyers_recent`-like cohorts (active/redeemers).")
    if metric_family == "gmv_net" and direction == "down" and seg_key in {"buyers", "repeat_buyers", "high_aov_buyers"} and commerce_joinable:
        actions.append(f"Launch basket-building offers for `{seg_key}` to recover GMV quickly.")

    if metric_family == "transaction_count" and direction == "down" and seg_key in {"buyers", "repeat_buyers"} and commerce_joinable:
        actions.append(f"Trigger repeat-purchase journeys for `{seg_key}` using replenishment cadence.")
        actions.append("Offer replenishment coupon to `repeat_buyers` cohort; test 7d expiry.")

    if metric_family == "redeem_rate" and direction == "down" and seg_key in {"non_redeemers", "active_0_7d"}:
        actions.append(f"Send redemption-intent nudges to `{seg_key}` with easier point thresholds and expiry reminders.")

    if not commerce_joinable and seg_key in COMMERCE_SEGMENT_KEYS:
        return []

    return actions



def map_drivers_to_actions(
    drivers: Sequence[Mapping],
    target_segments: Optional[Sequence[Mapping]] = None,
    row: Optional[Mapping] = None,
    top_n: int = 3,
) -> List[str]:
    actions: List[str] = []
    seen: set = set()
    commerce_joinable = bool(float(row.get("commerce_joinable", 0.0))) if row is not None else False
    seg_objs = _segment_objects(target_segments)

    for seg in seg_objs:
        seg_key = seg["segment_key"]
        mf = seg["metric_family"]
        direction = seg["direction"]
        if not commerce_joinable and seg_key in COMMERCE_SEGMENT_KEYS:
            continue
        for action in _segment_specific_actions(
            metric_family=mf,
            seg_key=seg_key,
            direction=direction,
            commerce_joinable=commerce_joinable,
            row=row,
        ):
            if action not in seen:
                seen.add(action)
                actions.append(action)
            if len(actions) >= top_n:
                return actions

    for d in drivers:
        key = canonical_driver_key(str(d.get("key", "")))
        mf = str(d.get("metric_family", "")).strip() or infer_metric_family_from_key(key) or ""
        if mf in {"gmv_net", "transaction_count", "sku_concentration"} and not commerce_joinable:
            continue
        for action in ACTION_MAP.get(mf, []):
            if action not in seen:
                seen.add(action)
                actions.append(action)
            if len(actions) >= top_n:
                return actions
        for action in ACTION_MAP.get(key, []):
            if action not in seen:
                seen.add(action)
                actions.append(action)
            if len(actions) >= top_n:
                return actions

    if not actions:
        for d in drivers:
            key = canonical_driver_key(str(d.get("key", "")))
            mf = str(d.get("metric_family", "")).strip() or infer_metric_family_from_key(key) or ""
            if mf in {"gmv_net", "transaction_count", "sku_concentration"} and not commerce_joinable:
                continue
            for action in ACTION_MAP.get(mf, []):
                if action not in seen:
                    seen.add(action)
                    actions.append(action)
                if len(actions) >= top_n:
                    return actions

    # Fallback generic actions.
    if not actions:
        actions = [
            "Monitor the next 1-2 windows and trigger targeted interventions for the weakest KPI.",
            "Run controlled experiments on campaign cadence and reward mix.",
            "Review segment-level funnel drops and prioritize high-impact fixes.",
        ][:top_n]

    return actions



def attach_actions(predictions_df, top_n: int = 3):
    df = predictions_df.copy()
    df["suggested_actions"] = df.apply(
        lambda r: map_drivers_to_actions(
            r.get("drivers", []),
            target_segments=r.get("target_segments", []),
            row=r.to_dict(),
            top_n=top_n,
        ),
        axis=1,
    )
    return df
