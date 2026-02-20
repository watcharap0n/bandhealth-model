from __future__ import annotations

from typing import Dict, Optional


DRIVER_ALIAS_MAP: Dict[str, str] = {
    "completion_down": "completion_drop",
}


# Strict mapping from driver keys to metric families used for attribution.
DRIVER_METRIC_MAP: Dict[str, str] = {
    "active_down": "active_users",
    "gmv_down": "gmv_net",
    "transactions_down": "transaction_count",
    "dormant_up": "dormant_share",
    "completion_drop": "activity_completion_rate",
    "efficiency_drop": "reward_efficiency",
    "redeem_down": "redeem_rate",
    "sku_concentration_high": "sku_concentration",
    "model_active_users_wow_pct": "active_users",
    "model_active_users_zscore": "active_users",
    "model_gmv_net_wow_pct": "gmv_net",
    "model_gmv_net_zscore": "gmv_net",
    "model_transaction_count_wow_pct": "transaction_count",
    "model_transaction_count_zscore": "transaction_count",
    "model_dormant_share_wow_pct": "dormant_share",
    "model_dormant_share_zscore": "dormant_share",
    "model_activity_completion_rate_wow_pct": "activity_completion_rate",
    "model_activity_completion_rate_zscore": "activity_completion_rate",
    "model_reward_efficiency_wow_pct": "reward_efficiency",
    "model_reward_efficiency_zscore": "reward_efficiency",
    "model_activity_redeem_rate_wow_pct": "redeem_rate",
    "model_activity_redeem_rate_zscore": "redeem_rate",
}


METRIC_FAMILY_CONFIG: Dict[str, dict] = {
    "active_users": {
        "total_col": "active_users",
        "total_wow_col": "active_users_wow_pct",
        "segment_metric": "users",
        "commerce_related": False,
    },
    "gmv_net": {
        "total_col": "gmv_net",
        "total_wow_col": "gmv_net_wow_pct",
        "segment_metric": "gmv_net",
        "commerce_related": True,
    },
    "transaction_count": {
        "total_col": "transaction_count",
        "total_wow_col": "transaction_count_wow_pct",
        "segment_metric": "transactions",
        "commerce_related": True,
    },
    "dormant_share": {
        "total_col": "dormant_share",
        "total_wow_col": "dormant_share_wow_pct",
        "segment_metric": "dormant_share",
        "commerce_related": False,
    },
    "activity_completion_rate": {
        "total_col": "activity_completion_rate",
        "total_wow_col": "activity_completion_rate_wow_pct",
        "segment_metric": "activity_completion_rate",
        "commerce_related": False,
    },
    "reward_efficiency": {
        "total_col": "reward_efficiency",
        "total_wow_col": "reward_efficiency_wow_pct",
        # Segment-level proxy where reward-efficiency is not directly observable.
        "segment_metric": "activity_completion_rate",
        "commerce_related": False,
    },
    "redeem_rate": {
        "total_col": "activity_redeem_rate",
        "total_wow_col": "activity_redeem_rate_wow_pct",
        "segment_metric": "redeem_rate",
        "commerce_related": False,
    },
    "sku_concentration": {
        "total_col": "sku_top_share",
        "total_wow_col": "sku_top_share_wow_pct",
        "segment_metric": "users",
        "commerce_related": True,
    },
}


COMMERCE_METRIC_FAMILIES = {"gmv_net", "transaction_count", "sku_concentration"}


def canonical_driver_key(key: str) -> str:
    k = str(key or "").strip()
    if k in DRIVER_ALIAS_MAP:
        return DRIVER_ALIAS_MAP[k]
    return k


def infer_metric_family_from_key(key: str) -> Optional[str]:
    ck = canonical_driver_key(key)
    if ck in DRIVER_METRIC_MAP:
        return DRIVER_METRIC_MAP[ck]

    # Controlled fallback for model keys not explicitly listed above.
    if ck.startswith("model_"):
        feat = ck.replace("model_", "")
        if "active_users" in feat:
            return "active_users"
        if "gmv_net" in feat:
            return "gmv_net"
        if "transaction_count" in feat:
            return "transaction_count"
        if "dormant_share" in feat:
            return "dormant_share"
        if "activity_completion_rate" in feat:
            return "activity_completion_rate"
        if "reward_efficiency" in feat:
            return "reward_efficiency"
        if "activity_redeem_rate" in feat:
            return "redeem_rate"
        if "sku" in feat:
            return "sku_concentration"
    return None
