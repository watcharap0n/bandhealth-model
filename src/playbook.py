from __future__ import annotations

from typing import Dict, Iterable, List, Mapping, Optional, Sequence


ACTION_MAP: Dict[str, List[str]] = {
    "active_down": [
        "Launch dormant-user reactivation campaigns with segmented incentives.",
        "Reduce message fatigue via tighter frequency caps and send-time optimization.",
        "Retarget recently lapsed cohorts with low-friction missions.",
    ],
    "completion_down": [
        "Audit mission completion friction (steps, UX, rules) and simplify top drop-off flows.",
        "Shift reward mix toward lower-friction, faster-win activities.",
        "A/B test mission copy and CTA clarity on the highest-volume activities.",
    ],
    "redeem_down": [
        "Increase redemption visibility with in-app reminders and redemption-specific triggers.",
        "Rebalance point thresholds to reduce perceived redemption effort.",
        "Promote expiring-value nudges to accelerate redemption intent.",
    ],
    "gmv_down": [
        "Run short-cycle conversion pushes for high-intent audiences.",
        "Bundle offers to lift average order value and repeat basket behavior.",
        "Check checkout/payment friction and recover abandoned purchase attempts.",
    ],
    "transactions_down": [
        "Introduce repeat-purchase triggers based on product replenishment cadence.",
        "Activate personalized offer ladders for near-churn buyers.",
        "Prioritize retention promotions for buyers with recent order decline.",
    ],
    "dormant_up": [
        "Create dormant-tier win-back journeys with escalating incentives.",
        "Use triggered education content to reintroduce loyalty value.",
        "Suppress low-intent users from broad blasts and target by propensity.",
    ],
    "efficiency_drop": [
        "Recalibrate reward economics: trim low-yield rewards and raise completion-linked value.",
        "Prioritize activities with best completion-per-point efficiency.",
        "Set guardrails so point inflation does not outpace engagement conversion.",
    ],
    "sku_concentration_high": [
        "Diversify reward/product mix to reduce dependence on top SKU(s).",
        "Promote under-indexed SKUs through personalized recommendations.",
        "Use rotation strategy for featured SKUs to broaden basket composition.",
    ],
}


def _normalize_driver_key(key: str) -> str:
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


def _segment_keys(target_segments: Optional[Sequence[Mapping]]) -> List[str]:
    if not target_segments:
        return []
    out: List[str] = []
    for s in target_segments:
        k = str(s.get("segment_key", "")).strip()
        if k and k not in out:
            out.append(k)
    return out


def _segment_specific_actions(driver_key: str, seg_key: str, row: Optional[Mapping] = None) -> List[str]:
    actions: List[str] = []
    active_wow = float(row.get("active_users_wow_pct", 0.0)) if row else 0.0

    if driver_key == "active_down" and seg_key in {"recently_lapsed_8_14d", "dormant_15_30d"}:
        actions.extend(
            [
                f"Trigger winback rewards for `{seg_key}` with short expiry and capped frequency.",
                f"Set reactivation journeys for `{seg_key}` using low-friction missions first.",
            ]
        )
    if driver_key == "active_down" and seg_key == "engaged_no_redeem":
        actions.append("Send redemption-intent nudges to `engaged_no_redeem` with easier point thresholds.")

    if driver_key == "dormant_up" and seg_key in {"dormant_15_30d", "recently_lapsed_8_14d"}:
        actions.extend(
            [
                f"Apply reactivation + frequency caps specifically for `{seg_key}`.",
                f"Use low-friction missions for `{seg_key}` before high-effort offers.",
            ]
        )

    if driver_key == "completion_down" and seg_key in {"active_0_7d", "engaged_no_redeem"}:
        actions.append(f"Reduce mission friction for `{seg_key}` and A/B test mission flow copy.")

    if driver_key == "gmv_down" and active_wow >= -0.05:
        actions.append("Active base is stable; run purchase triggers for `buyers_recent`-like cohorts (active/redeemers).")
    if driver_key == "gmv_down" and seg_key in {"active_0_7d", "redeemers"}:
        actions.append(f"Launch basket-building offers for `{seg_key}` to recover GMV quickly.")

    if driver_key == "transactions_down" and seg_key in {"active_0_7d", "redeemers"}:
        actions.append(f"Trigger repeat-purchase journeys for `{seg_key}` using replenishment cadence.")

    return actions



def map_drivers_to_actions(
    drivers: Sequence[Mapping],
    target_segments: Optional[Sequence[Mapping]] = None,
    row: Optional[Mapping] = None,
    top_n: int = 3,
) -> List[str]:
    actions: List[str] = []
    seen: set = set()
    seg_keys = _segment_keys(target_segments)

    for d in drivers:
        key = _normalize_driver_key(str(d.get("key", "")))
        for seg_key in seg_keys:
            for action in _segment_specific_actions(key, seg_key, row=row):
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
