from __future__ import annotations

from typing import Dict, Iterable, List, Mapping, Sequence


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



def map_drivers_to_actions(drivers: Sequence[Mapping], top_n: int = 3) -> List[str]:
    actions: List[str] = []
    seen: set = set()

    for d in drivers:
        key = _normalize_driver_key(str(d.get("key", "")))
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
    df["suggested_actions"] = df["drivers"].apply(lambda ds: map_drivers_to_actions(ds, top_n=top_n))
    return df
