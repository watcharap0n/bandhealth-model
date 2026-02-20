# Brand Health Modeling Report

## 1) Data Joins + Coverage Summary

### Table row counts

| brand_id | table                | rows    | columns |
| -------- | -------------------- | ------- | ------- |
| c-vit    | activity_transaction | 10000   | 10      |
| c-vit    | purchase             | 10000   | 14      |
| c-vit    | purchase_items       | 8827    | 21      |
| c-vit    | user_device          | 10000   | 10      |
| c-vit    | user_identity        | 10000   | 5       |
| c-vit    | user_info            | 10000   | 8       |
| c-vit    | user_view            | 10000   | 5       |
| c-vit    | user_visitor         | 10000   | 11      |
| see-chan | activity_transaction | 178594  | 10      |
| see-chan | purchase             | 1404759 | 14      |
| see-chan | purchase_items       | 1982120 | 21      |
| see-chan | user_device          | 79984   | 10      |
| see-chan | user_identity        | 141220  | 5       |
| see-chan | user_info            | 141220  | 8       |
| see-chan | user_view            | 173494  | 5       |
| see-chan | user_visitor         | 446324  | 11      |

### Join key coverage

| brand_id | left_table           | right_table    | key            | left_rows | right_rows | left_unique | right_unique | overlap_unique | row_coverage       | left_unique_norm | right_unique_norm | overlap_unique_norm | row_coverage_norm  |
| -------- | -------------------- | -------------- | -------------- | --------- | ---------- | ----------- | ------------ | -------------- | ------------------ | ---------------- | ----------------- | ------------------- | ------------------ |
| c-vit    | activity_transaction | user_identity  | user_id        | 10000     | 10000      | 181         | 10000        | 9              | 0.0408             | 181              | 10000             | 9                   | 0.0408             |
| c-vit    | activity_transaction | user_view      | user_id        | 10000     | 10000      | 181         | 10000        | 10             | 0.028              | 181              | 10000             | 10                  | 0.028              |
| c-vit    | activity_transaction | user_visitor   | user_id        | 10000     | 10000      | 181         | 242          | 4              | 0.1084             | 181              | 242               | 4                   | 0.1084             |
| c-vit    | purchase             | purchase_items | transaction_id | 10000     | 8827       | 10000       | 5326         | 0              | 0.0                | 10000            | 5326              | 0                   | 0.0                |
| c-vit    | purchase             | purchase_items | user_id        | 10000     | 8827       | 1250        | 1            | 0              | 0.0                | 1250             | 1                 | 0                   | 0.0                |
| c-vit    | purchase             | user_identity  | user_id        | 10000     | 10000      | 1250        | 10000        | 82             | 0.0439             | 1250             | 10000             | 82                  | 0.0439             |
| c-vit    | purchase             | user_info      | user_id        | 10000     | 10000      | 1250        | 10000        | 70             | 0.0308             | 1250             | 10000             | 70                  | 0.0308             |
| c-vit    | user_view            | user_visitor   | user_id        | 10000     | 10000      | 10000       | 242          | 0              | 0.0                | 10000            | 242               | 0                   | 0.0                |
| see-chan | activity_transaction | user_identity  | user_id        | 178594    | 141220     | 140795      | 141220       | 140792         | 0.9999832021232516 | 140795           | 141220            | 140792              | 0.9999832021232516 |
| see-chan | activity_transaction | user_view      | user_id        | 178594    | 173494     | 140795      | 173494       | 140792         | 0.9999832021232516 | 140795           | 173494            | 140792              | 0.9999832021232516 |
| see-chan | activity_transaction | user_visitor   | user_id        | 178594    | 446324     | 140795      | 172144       | 139453         | 0.9924857498012252 | 140795           | 172144            | 139453              | 0.9924857498012252 |
| see-chan | purchase             | purchase_items | transaction_id | 1404759   | 1982120    | 1399583     | 1378053      | 1378053        | 0.9839175260667488 | 1399566          | 1378053           | 1378053             | 0.9839294905061196 |
| see-chan | purchase             | purchase_items | user_id        | 1404759   | 1982120    | 10359       | 19           | 19             | 0.0020581521535755 | 10359            | 19                | 19                  | 0.0020581521535755 |
| see-chan | purchase             | user_identity  | user_id        | 1404759   | 141220     | 10359       | 141220       | 10359          | 1.0                | 10359            | 141220            | 10359               | 1.0                |
| see-chan | purchase             | user_info      | user_id        | 1404759   | 141220     | 10359       | 141220       | 10359          | 1.0                | 10359            | 141220            | 10359               | 1.0                |
| see-chan | user_view            | user_visitor   | user_id        | 173494    | 446324     | 173494      | 172144       | 172144         | 0.992218751080729  | 173494           | 172144            | 172144              | 0.992218751080729  |

### Coverage notes

- **c-vit**: canonical `purchase↔purchase_items` join key is `transaction_id` (raw=0.0000, normalized=0.0000, not valid).
- **c-vit**: `purchase_items.user_id` coverage is low (raw=0.0000, normalized=0.0000); ignored for purchase↔items joins.
- **c-vit**: additional low-coverage relations: activity_transaction->user_identity.user_id (0.0408), activity_transaction->user_view.user_id (0.0280), activity_transaction->user_visitor.user_id (0.1084), purchase->user_identity.user_id (0.0439), purchase->user_info.user_id (0.0308), user_view->user_visitor.user_id (0.0000).

- **see-chan**: canonical `purchase↔purchase_items` join key is `transaction_id` (raw=0.9839, normalized=0.9839, valid).
- **see-chan**: `purchase_items.user_id` coverage is low (raw=0.0021, normalized=0.0021); ignored for purchase↔items joins.

## 2) Feature Table Schema

Feature names and meanings are exported to `outputs/feature_definitions.csv`.

| feature_name                                             | meaning                                            |
| -------------------------------------------------------- | -------------------------------------------------- |
| brand_id                                                 | identifier                                         |
| window_end_date                                          | identifier                                         |
| window_size                                              | identifier                                         |
| window_size_days                                         | identifier                                         |
| recency_days_mean                                        | aggregated KPI metric                              |
| recency_days_median                                      | aggregated KPI metric                              |
| recency_days_p90                                         | aggregated KPI metric                              |
| active_users                                             | aggregated KPI metric                              |
| new_users                                                | aggregated KPI metric                              |
| returning_users                                          | aggregated KPI metric                              |
| dormant_share                                            | ratio/share feature                                |
| total_events                                             | aggregated KPI metric                              |
| total_logins                                             | aggregated KPI metric                              |
| total_views                                              | aggregated KPI metric                              |
| avg_events_per_active                                    | aggregated KPI metric                              |
| session_proxy_login_to_view                              | aggregated KPI metric                              |
| activity_events                                          | aggregated KPI metric                              |
| activity_completed_events                                | aggregated KPI metric                              |
| activity_completion_rate                                 | rate/proportion feature                            |
| activity_points_sum                                      | aggregated KPI metric                              |
| activity_points_per_active                               | aggregated KPI metric                              |
| activity_redeem_count                                    | aggregated KPI metric                              |
| activity_redeem_rate                                     | rate/proportion feature                            |
| activity_redeem_per_active                               | aggregated KPI metric                              |
| activity_type_entropy                                    | distribution entropy                               |
| activity_type_top_share                                  | ratio/share feature                                |
| activity_type_login_count                                | aggregated KPI metric                              |
| activity_type_login_completion_rate                      | rate/proportion feature                            |
| activity_type_product_consume_uniquecode_count           | aggregated KPI metric                              |
| activity_type_product_consume_uniquecode_completion_rate | rate/proportion feature                            |
| activity_type_registration_count                         | aggregated KPI metric                              |
| activity_type_registration_completion_rate               | rate/proportion feature                            |
| activity_name_entropy                                    | distribution entropy                               |
| activity_name_top_share                                  | ratio/share feature                                |
| redeem_user_low_share                                    | ratio/share feature                                |
| redeem_user_med_share                                    | ratio/share feature                                |
| redeem_user_high_share                                   | ratio/share feature                                |
| transaction_count                                        | aggregated KPI metric                              |
| transaction_success_rate                                 | rate/proportion feature                            |
| gmv_net                                                  | aggregated KPI metric                              |
| gmv_sell                                                 | aggregated KPI metric                              |
| discount_rate                                            | rate/proportion feature                            |
| aov_net                                                  | aggregated KPI metric                              |
| items_per_transaction                                    | aggregated KPI metric                              |
| paid_delay_hours                                         | aggregated KPI metric                              |
| delivered_rate                                           | rate/proportion feature                            |
| shipped_rate                                             | rate/proportion feature                            |
| buyers                                                   | aggregated KPI metric                              |
| repeat_purchase_rate                                     | rate/proportion feature                            |
| sku_unique                                               | aggregated KPI metric                              |
| sku_top_share                                            | ratio/share feature                                |
| sku_entropy                                              | distribution entropy                               |
| status_success_rate                                      | rate/proportion feature                            |
| rfm_recency_mean                                         | RFM-based user distribution or transition metric   |
| rfm_frequency_mean                                       | RFM-based user distribution or transition metric   |
| rfm_monetary_mean                                        | RFM-based user distribution or transition metric   |
| rfm_score_mean                                           | RFM-based user distribution or transition metric   |
| rfm_transition_up_share                                  | RFM-based user distribution or transition metric   |
| rfm_transition_down_share                                | RFM-based user distribution or transition metric   |
| rfm_dormant_share                                        | RFM-based user distribution or transition metric   |
| rfm_recency_tier_1_pct                                   | RFM-based user distribution or transition metric   |
| rfm_recency_tier_2_pct                                   | RFM-based user distribution or transition metric   |
| rfm_recency_tier_3_pct                                   | RFM-based user distribution or transition metric   |
| rfm_recency_tier_4_pct                                   | RFM-based user distribution or transition metric   |
| rfm_recency_tier_5_pct                                   | RFM-based user distribution or transition metric   |
| rfm_frequency_tier_1_pct                                 | RFM-based user distribution or transition metric   |
| rfm_frequency_tier_2_pct                                 | RFM-based user distribution or transition metric   |
| rfm_frequency_tier_3_pct                                 | RFM-based user distribution or transition metric   |
| rfm_frequency_tier_4_pct                                 | RFM-based user distribution or transition metric   |
| rfm_frequency_tier_5_pct                                 | RFM-based user distribution or transition metric   |
| rfm_monetary_tier_1_pct                                  | RFM-based user distribution or transition metric   |
| rfm_monetary_tier_2_pct                                  | RFM-based user distribution or transition metric   |
| rfm_monetary_tier_3_pct                                  | RFM-based user distribution or transition metric   |
| rfm_monetary_tier_4_pct                                  | RFM-based user distribution or transition metric   |
| rfm_monetary_tier_5_pct                                  | RFM-based user distribution or transition metric   |
| rfm_score_tier_1_pct                                     | RFM-based user distribution or transition metric   |
| rfm_score_tier_2_pct                                     | RFM-based user distribution or transition metric   |
| rfm_score_tier_3_pct                                     | RFM-based user distribution or transition metric   |
| rfm_score_tier_4_pct                                     | RFM-based user distribution or transition metric   |
| rfm_score_tier_5_pct                                     | RFM-based user distribution or transition metric   |
| commerce_joinable                                        | aggregated KPI metric                              |
| activity_type_referral_count                             | aggregated KPI metric                              |
| activity_type_referral_completion_rate                   | rate/proportion feature                            |
| activity_type_upload_receipts_count                      | aggregated KPI metric                              |
| activity_type_upload_receipts_completion_rate            | rate/proportion feature                            |
| status_voided_rate                                       | rate/proportion feature                            |
| status_pending_rate                                      | rate/proportion feature                            |
| status_reject_rate                                       | rate/proportion feature                            |
| new_user_share                                           | ratio/share feature                                |
| returning_user_share                                     | ratio/share feature                                |
| gmv_per_active                                           | aggregated KPI metric                              |
| transactions_per_active                                  | aggregated KPI metric                              |
| points_per_completion                                    | aggregated KPI metric                              |
| reward_efficiency                                        | aggregated KPI metric                              |
| active_users_wow_pct                                     | week-over-week percent change                      |
| active_users_mom_pct                                     | month-over-month percent change (4 weekly windows) |
| active_users_zscore                                      | rolling z-score vs trailing baseline               |
| active_users_volatility                                  | rolling std/mean over trailing windows             |
| gmv_net_wow_pct                                          | week-over-week percent change                      |
| gmv_net_mom_pct                                          | month-over-month percent change (4 weekly windows) |
| gmv_net_zscore                                           | rolling z-score vs trailing baseline               |
| gmv_net_volatility                                       | rolling std/mean over trailing windows             |
| transaction_count_wow_pct                                | week-over-week percent change                      |
| transaction_count_mom_pct                                | month-over-month percent change (4 weekly windows) |
| transaction_count_zscore                                 | rolling z-score vs trailing baseline               |
| transaction_count_volatility                             | rolling std/mean over trailing windows             |
| activity_completion_rate_wow_pct                         | week-over-week percent change                      |
| activity_completion_rate_mom_pct                         | month-over-month percent change (4 weekly windows) |
| activity_completion_rate_zscore                          | rolling z-score vs trailing baseline               |
| activity_completion_rate_volatility                      | rolling std/mean over trailing windows             |
| activity_redeem_rate_wow_pct                             | week-over-week percent change                      |
| activity_redeem_rate_mom_pct                             | month-over-month percent change (4 weekly windows) |
| activity_redeem_rate_zscore                              | rolling z-score vs trailing baseline               |
| activity_redeem_rate_volatility                          | rolling std/mean over trailing windows             |
| reward_efficiency_wow_pct                                | week-over-week percent change                      |
| reward_efficiency_mom_pct                                | month-over-month percent change (4 weekly windows) |
| reward_efficiency_zscore                                 | rolling z-score vs trailing baseline               |
| reward_efficiency_volatility                             | rolling std/mean over trailing windows             |
| dormant_share_wow_pct                                    | week-over-week percent change                      |
| dormant_share_mom_pct                                    | month-over-month percent change (4 weekly windows) |
| dormant_share_zscore                                     | rolling z-score vs trailing baseline               |
| dormant_share_volatility                                 | rolling std/mean over trailing windows             |

## 3) Weak Labeling Logic

Brand health score starts at 100 and subtracts penalties from multi-metric degradation signals:
- Active users WoW drop
- Completion rate WoW drop
- GMV and transaction WoW drop
- Dormant share WoW increase
- Efficiency drop (points/reward pressure up while conversion efficiency down)
- Additional baseline-relative penalties from rolling z-scores

Threshold config: `{
  "active_drop_warn": -0.1,
  "active_drop_risk": -0.2,
  "completion_drop_warn": -0.08,
  "completion_drop_risk": -0.15,
  "gmv_drop_warn": -0.1,
  "gmv_drop_risk": -0.2,
  "txn_drop_warn": -0.1,
  "txn_drop_risk": -0.2,
  "dormant_up_warn": 0.05,
  "dormant_up_risk": 0.1,
  "class_healthy_min": 70.0,
  "class_warning_min": 45.0
}`

Class mapping:
- Healthy: score >= 70
- Warning: 45 <= score < 70
- AtRisk: score < 45

## 4) Model Selection + Evaluation

Selected model based on time-split macro F1: **hgb_calibrated**

### Time-based split results

| model          | macro_f1            | balanced_accuracy  | calibration_ece     | brier                |
| -------------- | ------------------- | ------------------ | ------------------- | -------------------- |
| logistic       | 0.49822831158093667 | 0.4970106322098733 | 0.17189567063510822 | 0.1577434334632189   |
| hgb            | 0.8850937873064154  | 0.8810540345170326 | 0.2587043766387233  | 0.016315063380478907 |
| hgb_calibrated | 0.8860380030241233  | 0.8746724496250113 | 0.15558165545200014 | 0.025104786425384443 |

### Cross-brand holdout (train other brand, test holdout)

| holdout_brand | macro_f1           | balanced_accuracy  | note |
| ------------- | ------------------ | ------------------ | ---- |
| c-vit         | 0.6604169887002952 | 0.7036059205679459 | ok   |
| see-chan      | 0.7234418155355226 | 0.7033708353006941 | ok   |

### GroupKFold by brand

| fold | macro_f1           | balanced_accuracy  |
| ---- | ------------------ | ------------------ |
| 1    | 0.6604169887002952 | 0.7036059205679459 |
| 2    | 0.7234418155355226 | 0.7033708353006941 |

## 5) Example Output (Last 4 Windows Per Brand)

### Brand: c-vit

```json
{
  "brand_id": "c-vit",
  "window_end_date": "2026-01-25T00:00:00.000Z",
  "window_size": "30d",
  "predicted_health_class": "Healthy",
  "predicted_health_statement": "Healthy",
  "predicted_health_score": 85.3504902843,
  "confidence_band": "high",
  "probabilities": {
    "AtRisk": 0.0031703971,
    "Healthy": 0.8487151394,
    "Warning": 0.14811446350000002
  },
  "drivers": [
    {
      "key": "model_reward_efficiency_zscore",
      "statement": "Model-highlighted anomaly: reward_efficiency_zscore=1.10 (up vs baseline)",
      "severity": 0.0013218029,
      "direction": "up",
      "metric_family": "reward_efficiency",
      "metrics": {
        "reward_efficiency_zscore": 1.1039320711,
        "importance": -0.0011973589
      }
    },
    {
      "key": "model_reward_efficiency_wow_pct",
      "statement": "Model-highlighted signal: reward_efficiency_wow_pct up +10.6%",
      "severity": 0.0002618538,
      "direction": "up",
      "metric_family": "reward_efficiency",
      "metrics": {
        "reward_efficiency_wow_pct": 0.10599078340000001,
        "importance": -0.0024705340000000004
      }
    }
  ],
  "target_segments": [],
  "suggested_actions": [
    "Recalibrate reward economics: trim low-yield rewards and raise completion-linked value.",
    "Prioritize activities with best completion-per-point efficiency.",
    "Set guardrails so point inflation does not outpace engagement conversion."
  ]
}
```

```json
{
  "brand_id": "c-vit",
  "window_end_date": "2026-02-01T00:00:00.000Z",
  "window_size": "30d",
  "predicted_health_class": "Healthy",
  "predicted_health_statement": "Healthy",
  "predicted_health_score": 85.9089264316,
  "confidence_band": "high",
  "probabilities": {
    "AtRisk": 0.0033669898,
    "Healthy": 0.8675590358,
    "Warning": 0.1290739744
  },
  "drivers": [
    {
      "key": "model_active_users_wow_pct",
      "statement": "Model-highlighted signal: active_users_wow_pct up +13.4%",
      "severity": 0.0306463218,
      "direction": "up",
      "metric_family": "active_users",
      "metrics": {
        "active_users_wow_pct": 0.1343283582,
        "importance": 0.2281448401
      }
    },
    {
      "key": "model_reward_efficiency_zscore",
      "statement": "Model-highlighted anomaly: reward_efficiency_zscore=1.10 (up vs baseline)",
      "severity": 0.0013175803000000002,
      "direction": "up",
      "metric_family": "reward_efficiency",
      "metrics": {
        "reward_efficiency_zscore": 1.1004054403,
        "importance": -0.0011973589
      }
    }
  ],
  "target_segments": [
    {
      "metric_family": "active_users",
      "segment_key": "redeemers",
      "direction": "up",
      "contribution_share": 1.0,
      "reason_statement": "Active users up driven by `redeemers` (driver: Model-highlighted signal: active_users_wow_pct up +13.4%)",
      "evidence_metrics": {
        "delta_seg": 1.0,
        "delta_total": 9.0,
        "segment_share_now": 0.0276243094,
        "segment_count_now": 5,
        "wow_pct_seg": 0.25,
        "cold_start_increase": false,
        "note": "stable"
      },
      "segment_confidence": "medium"
    }
  ],
  "suggested_actions": [
    "Launch dormant-user reactivation campaigns with segmented incentives.",
    "Reduce message fatigue via tighter frequency caps and send-time optimization.",
    "Retarget recently lapsed cohorts with low-friction missions."
  ]
}
```

```json
{
  "brand_id": "c-vit",
  "window_end_date": "2026-02-08T00:00:00.000Z",
  "window_size": "30d",
  "predicted_health_class": "Healthy",
  "predicted_health_statement": "Healthy",
  "predicted_health_score": 76.2878595054,
  "confidence_band": "medium",
  "probabilities": {
    "AtRisk": 0.0659912751,
    "Healthy": 0.6199184711,
    "Warning": 0.3140902538
  },
  "drivers": [
    {
      "key": "active_down",
      "statement": "Active users 30d down -18.4% WoW",
      "severity": 0.1842105263,
      "direction": "down",
      "metric_family": "active_users",
      "metrics": {
        "active_users_wow_pct": -0.1842105263
      }
    },
    {
      "key": "model_active_users_wow_pct",
      "statement": "Model-highlighted signal: active_users_wow_pct down -18.4%",
      "severity": 0.042026681100000005,
      "direction": "down",
      "metric_family": "active_users",
      "metrics": {
        "active_users_wow_pct": -0.1842105263,
        "importance": 0.2281448401
      }
    },
    {
      "key": "model_reward_efficiency_zscore",
      "statement": "Model-highlighted anomaly: reward_efficiency_zscore=1.04 (up vs baseline)",
      "severity": 0.0012471647,
      "direction": "up",
      "metric_family": "reward_efficiency",
      "metrics": {
        "reward_efficiency_zscore": 1.041596399,
        "importance": -0.0011973589
      }
    }
  ],
  "target_segments": [
    {
      "metric_family": "active_users",
      "segment_key": "dormant_60d_plus",
      "direction": "down",
      "contribution_share": 0.5,
      "reason_statement": "Active users down driven by `dormant_60d_plus` (driver: Active users 30d down -18.4% WoW)",
      "evidence_metrics": {
        "delta_seg": -2.0,
        "delta_total": -14.0,
        "segment_share_now": 0.9613259669,
        "segment_count_now": 174,
        "wow_pct_seg": -0.0113636364,
        "cold_start_increase": false,
        "note": "stable"
      },
      "segment_confidence": "high"
    },
    {
      "metric_family": "active_users",
      "segment_key": "non_redeemers",
      "direction": "down",
      "contribution_share": 0.5,
      "reason_statement": "Active users down driven by `non_redeemers` (driver: Active users 30d down -18.4% WoW)",
      "evidence_metrics": {
        "delta_seg": -2.0,
        "delta_total": -14.0,
        "segment_share_now": 0.9613259669,
        "segment_count_now": 174,
        "wow_pct_seg": -0.0113636364,
        "cold_start_increase": false,
        "note": "stable"
      },
      "segment_confidence": "high"
    }
  ],
  "suggested_actions": [
    "Trigger winback rewards for `dormant_60d_plus` with short expiry and capped frequency.",
    "Set reactivation journeys for `dormant_60d_plus` using low-friction missions first.",
    "Launch dormant-user reactivation campaigns with segmented incentives."
  ]
}
```

```json
{
  "brand_id": "c-vit",
  "window_end_date": "2026-02-15T00:00:00.000Z",
  "window_size": "30d",
  "predicted_health_class": "Warning",
  "predicted_health_statement": "Warning",
  "predicted_health_score": 51.3953028469,
  "confidence_band": "medium",
  "probabilities": {
    "AtRisk": 0.2814209462,
    "Healthy": 0.0415011988,
    "Warning": 0.6770778551000001
  },
  "drivers": [
    {
      "key": "active_down",
      "statement": "Active users 30d down -24.2% WoW",
      "severity": 0.2419354839,
      "direction": "down",
      "metric_family": "active_users",
      "metrics": {
        "active_users_wow_pct": -0.2419354839
      }
    },
    {
      "key": "model_dormant_share_zscore",
      "statement": "Model-highlighted anomaly: dormant_share_zscore=2.11 (up vs baseline)",
      "severity": 0.0914251293,
      "direction": "up",
      "metric_family": "dormant_share",
      "metrics": {
        "dormant_share_zscore": 2.1106124424,
        "importance": 0.0433168721
      }
    },
    {
      "key": "model_active_users_wow_pct",
      "statement": "Model-highlighted signal: active_users_wow_pct down -24.2%",
      "severity": 0.055196332300000005,
      "direction": "down",
      "metric_family": "active_users",
      "metrics": {
        "active_users_wow_pct": -0.2419354839,
        "importance": 0.2281448401
      }
    },
    {
      "key": "model_active_users_zscore",
      "statement": "Model-highlighted anomaly: active_users_zscore=-2.09 (down vs baseline)",
      "severity": 0.047027131800000004,
      "direction": "down",
      "metric_family": "active_users",
      "metrics": {
        "active_users_zscore": -2.0949193662,
        "importance": 0.0224481823
      }
    }
  ],
  "target_segments": [],
  "suggested_actions": [
    "Launch dormant-user reactivation campaigns with segmented incentives.",
    "Reduce message fatigue via tighter frequency caps and send-time optimization.",
    "Retarget recently lapsed cohorts with low-friction missions."
  ]
}
```

### Brand: see-chan

```json
{
  "brand_id": "see-chan",
  "window_end_date": "2026-01-07T00:00:00.000Z",
  "window_size": "30d",
  "predicted_health_class": "Healthy",
  "predicted_health_statement": "Healthy (borderline)",
  "predicted_health_score": 68.3058572321,
  "confidence_band": "low",
  "probabilities": {
    "AtRisk": 0.2326813401,
    "Healthy": 0.5483234712,
    "Warning": 0.2189951887
  },
  "drivers": [
    {
      "key": "active_down",
      "statement": "Active users 30d down -12.7% WoW",
      "severity": 0.12731099240000002,
      "direction": "down",
      "metric_family": "active_users",
      "metrics": {
        "active_users_wow_pct": -0.12731099240000002
      }
    },
    {
      "key": "model_dormant_share_zscore",
      "statement": "Model-highlighted anomaly: dormant_share_zscore=1.31 (up vs baseline)",
      "severity": 0.0568713697,
      "direction": "up",
      "metric_family": "dormant_share",
      "metrics": {
        "dormant_share_zscore": 1.3129149663,
        "importance": 0.0433168721
      }
    },
    {
      "key": "model_active_users_zscore",
      "statement": "Model-highlighted anomaly: active_users_zscore=-1.42 (down vs baseline)",
      "severity": 0.0318706803,
      "direction": "down",
      "metric_family": "active_users",
      "metrics": {
        "active_users_zscore": -1.4197443642,
        "importance": 0.0224481823
      }
    },
    {
      "key": "model_active_users_wow_pct",
      "statement": "Model-highlighted signal: active_users_wow_pct down -12.7%",
      "severity": 0.029045346000000003,
      "direction": "down",
      "metric_family": "active_users",
      "metrics": {
        "active_users_wow_pct": -0.12731099240000002,
        "importance": 0.2281448401
      }
    }
  ],
  "target_segments": [
    {
      "metric_family": "active_users",
      "segment_key": "dormant_31_60d",
      "direction": "down",
      "contribution_share": 0.37184218,
      "reason_statement": "Active users down driven by `dormant_31_60d` (driver: Active users 30d down -12.7% WoW)",
      "evidence_metrics": {
        "delta_seg": -9170.0,
        "delta_total": -3932.0,
        "segment_share_now": 0.14440533090000002,
        "segment_count_now": 24531,
        "wow_pct_seg": -0.2720987508,
        "cold_start_increase": false,
        "note": "stable"
      },
      "segment_confidence": "high"
    },
    {
      "metric_family": "active_users",
      "segment_key": "active_0_7d",
      "direction": "down",
      "contribution_share": 0.3614614168,
      "reason_statement": "Active users down driven by `active_0_7d` (driver: Active users 30d down -12.7% WoW)",
      "evidence_metrics": {
        "delta_seg": -8914.0,
        "delta_total": -3932.0,
        "segment_share_now": 0.0062810521,
        "segment_count_now": 1067,
        "wow_pct_seg": -0.8930968841,
        "cold_start_increase": false,
        "note": "stable"
      },
      "segment_confidence": "medium"
    },
    {
      "metric_family": "active_users",
      "segment_key": "new_users_0_7d",
      "direction": "down",
      "contribution_share": 0.2031142289,
      "reason_statement": "Active users down driven by `new_users_0_7d` (driver: Active users 30d down -12.7% WoW)",
      "evidence_metrics": {
        "delta_seg": -5009.0,
        "delta_total": -3932.0,
        "segment_share_now": 0.0015481881,
        "segment_count_now": 263,
        "wow_pct_seg": -0.9501138088000001,
        "cold_start_increase": false,
        "note": "stable"
      },
      "segment_confidence": "medium"
    },
    {
      "metric_family": "dormant_share",
      "segment_key": "non_redeemers",
      "direction": "up",
      "contribution_share": 1.0,
      "reason_statement": "Dormant share up driven by `non_redeemers` (driver: Model-highlighted anomaly: dormant_share_zscore=1.31 (up vs baseline))",
      "evidence_metrics": {
        "delta_seg": 0.0138822951,
        "delta_total": 0.023428904,
        "segment_share_now": 1.0,
        "segment_count_now": 169876,
        "wow_pct_seg": 0.015027354500000001,
        "cold_start_increase": false,
        "note": "stable"
      },
      "segment_confidence": "high"
    }
  ],
  "suggested_actions": [
    "Trigger winback rewards for `dormant_31_60d` with short expiry and capped frequency.",
    "Set reactivation journeys for `dormant_31_60d` using low-friction missions first.",
    "Boost first-7-day activation missions for `new_users_0_7d` with immediate low-friction rewards."
  ]
}
```

```json
{
  "brand_id": "see-chan",
  "window_end_date": "2026-01-14T00:00:00.000Z",
  "window_size": "30d",
  "predicted_health_class": "AtRisk",
  "predicted_health_statement": "AtRisk",
  "predicted_health_score": 29.3690503798,
  "confidence_band": "high",
  "probabilities": {
    "AtRisk": 0.8992620424000001,
    "Healthy": 0.028107395400000002,
    "Warning": 0.0726305622
  },
  "drivers": [
    {
      "key": "active_down",
      "statement": "Active users 30d down -44.6% WoW",
      "severity": 0.4463696064,
      "direction": "down",
      "metric_family": "active_users",
      "metrics": {
        "active_users_wow_pct": -0.4463696064
      }
    },
    {
      "key": "gmv_down",
      "statement": "GMV down -23.0% WoW",
      "severity": 0.2295684274,
      "direction": "down",
      "metric_family": "gmv_net",
      "metrics": {
        "gmv_net_wow_pct": -0.2295684274
      }
    },
    {
      "key": "transactions_down",
      "statement": "Transactions down -22.4% WoW",
      "severity": 0.22383860790000001,
      "direction": "down",
      "metric_family": "transaction_count",
      "metrics": {
        "transaction_count_wow_pct": -0.22383860790000001
      }
    },
    {
      "key": "model_active_users_wow_pct",
      "statement": "Model-highlighted signal: active_users_wow_pct down -44.6%",
      "severity": 0.10183692250000001,
      "direction": "down",
      "metric_family": "active_users",
      "metrics": {
        "active_users_wow_pct": -0.4463696064,
        "importance": 0.2281448401
      }
    },
    {
      "key": "dormant_up",
      "statement": "Dormant share up +8.5% WoW",
      "severity": 0.0845419109,
      "direction": "up",
      "metric_family": "dormant_share",
      "metrics": {
        "dormant_share_wow_pct": 0.0845419109
      }
    }
  ],
  "target_segments": [
    {
      "metric_family": "active_users",
      "segment_key": "recently_lapsed_8_14d",
      "direction": "down",
      "contribution_share": 0.7269394217,
      "reason_statement": "Active users down driven by `recently_lapsed_8_14d` (driver: Active users 30d down -44.6% WoW)",
      "evidence_metrics": {
        "delta_seg": -8724.0,
        "delta_total": -12031.0,
        "segment_share_now": 0.004663714,
        "segment_count_now": 795,
        "wow_pct_seg": -0.9164828238,
        "cold_start_increase": false,
        "note": "stable"
      },
      "segment_confidence": "medium"
    },
    {
      "metric_family": "active_users",
      "segment_key": "dormant_15_30d",
      "direction": "down",
      "contribution_share": 0.2730605783,
      "reason_statement": "Active users down driven by `dormant_15_30d` (driver: Active users 30d down -44.6% WoW)",
      "evidence_metrics": {
        "delta_seg": -3277.0,
        "delta_total": -12031.0,
        "segment_share_now": 0.0777109671,
        "segment_count_now": 13247,
        "wow_pct_seg": -0.1983175986,
        "cold_start_increase": false,
        "note": "stable"
      },
      "segment_confidence": "high"
    },
    {
      "metric_family": "dormant_share",
      "segment_key": "non_redeemers",
      "direction": "up",
      "contribution_share": 1.0,
      "reason_statement": "Dormant share up driven by `non_redeemers` (driver: Dormant share up +8.5% WoW)",
      "evidence_metrics": {
        "delta_seg": 0.0487003441,
        "delta_total": 0.0711278607,
        "segment_share_now": 1.0,
        "segment_count_now": 170465,
        "wow_pct_seg": 0.051936842500000004,
        "cold_start_increase": false,
        "note": "stable"
      },
      "segment_confidence": "high"
    }
  ],
  "suggested_actions": [
    "Trigger winback rewards for `recently_lapsed_8_14d` with short expiry and capped frequency.",
    "Set reactivation journeys for `recently_lapsed_8_14d` using low-friction missions first.",
    "Trigger winback rewards for `dormant_15_30d` with short expiry and capped frequency."
  ]
}
```

```json
{
  "brand_id": "see-chan",
  "window_end_date": "2026-01-21T00:00:00.000Z",
  "window_size": "30d",
  "predicted_health_class": "Healthy",
  "predicted_health_statement": "Healthy",
  "predicted_health_score": 83.6664580922,
  "confidence_band": "high",
  "probabilities": {
    "AtRisk": 0.024804862900000002,
    "Healthy": 0.8178209432,
    "Warning": 0.15737419390000001
  },
  "drivers": [
    {
      "key": "active_down",
      "statement": "Active users 30d down -8.9% WoW",
      "severity": 0.08899611310000001,
      "direction": "down",
      "metric_family": "active_users",
      "metrics": {
        "active_users_wow_pct": -0.08899611310000001
      }
    },
    {
      "key": "model_dormant_share_zscore",
      "statement": "Model-highlighted anomaly: dormant_share_zscore=1.52 (up vs baseline)",
      "severity": 0.06602947590000001,
      "direction": "up",
      "metric_family": "dormant_share",
      "metrics": {
        "dormant_share_zscore": 1.5243361915,
        "importance": 0.0433168721
      }
    },
    {
      "key": "model_active_users_wow_pct",
      "statement": "Model-highlighted signal: active_users_wow_pct down -8.9%",
      "severity": 0.020304004,
      "direction": "down",
      "metric_family": "active_users",
      "metrics": {
        "active_users_wow_pct": -0.08899611310000001,
        "importance": 0.2281448401
      }
    },
    {
      "key": "model_transaction_count_wow_pct",
      "statement": "Model-highlighted signal: transaction_count_wow_pct down -5.7%",
      "severity": 0.004920812100000001,
      "direction": "down",
      "metric_family": "transaction_count",
      "metrics": {
        "transaction_count_wow_pct": -0.056942941500000004,
        "importance": 0.0864165412
      }
    }
  ],
  "target_segments": [
    {
      "metric_family": "active_users",
      "segment_key": "dormant_15_30d",
      "direction": "down",
      "contribution_share": 0.7101405923,
      "reason_statement": "Active users down driven by `dormant_15_30d` (driver: Active users 30d down -8.9% WoW)",
      "evidence_metrics": {
        "delta_seg": -2374.0,
        "delta_total": -1328.0,
        "segment_share_now": 0.0636678241,
        "segment_count_now": 10873,
        "wow_pct_seg": -0.1792103873,
        "cold_start_increase": false,
        "note": "stable"
      },
      "segment_confidence": "high"
    },
    {
      "metric_family": "active_users",
      "segment_key": "dormant_31_60d",
      "direction": "down",
      "contribution_share": 0.20729883340000002,
      "reason_statement": "Active users down driven by `dormant_31_60d` (driver: Active users 30d down -8.9% WoW)",
      "evidence_metrics": {
        "delta_seg": -693.0,
        "delta_total": -1328.0,
        "segment_share_now": 0.1858212757,
        "segment_count_now": 31734,
        "wow_pct_seg": -0.0213710797,
        "cold_start_increase": false,
        "note": "stable"
      },
      "segment_confidence": "high"
    },
    {
      "metric_family": "active_users",
      "segment_key": "new_users_0_7d",
      "direction": "down",
      "contribution_share": 0.08256057430000001,
      "reason_statement": "Active users down driven by `new_users_0_7d` (driver: Active users 30d down -8.9% WoW)",
      "evidence_metrics": {
        "delta_seg": -276.0,
        "delta_total": -1328.0,
        "segment_share_now": 0.0018327995000000001,
        "segment_count_now": 313,
        "wow_pct_seg": -0.4685908319,
        "cold_start_increase": false,
        "note": "stable"
      },
      "segment_confidence": "medium"
    }
  ],
  "suggested_actions": [
    "Trigger winback rewards for `dormant_15_30d` with short expiry and capped frequency.",
    "Set reactivation journeys for `dormant_15_30d` using low-friction missions first.",
    "Trigger winback rewards for `dormant_31_60d` with short expiry and capped frequency."
  ]
}
```

```json
{
  "brand_id": "see-chan",
  "window_end_date": "2026-01-28T00:00:00.000Z",
  "window_size": "30d",
  "predicted_health_class": "AtRisk",
  "predicted_health_statement": "AtRisk",
  "predicted_health_score": 30.5346240343,
  "confidence_band": "high",
  "probabilities": {
    "AtRisk": 0.867228239,
    "Healthy": 0.02958708,
    "Warning": 0.10318468110000001
  },
  "drivers": [
    {
      "key": "active_down",
      "statement": "Active users 30d down -56.5% WoW",
      "severity": 0.5650286891,
      "direction": "down",
      "metric_family": "active_users",
      "metrics": {
        "active_users_wow_pct": -0.5650286891
      }
    },
    {
      "key": "model_active_users_wow_pct",
      "statement": "Model-highlighted signal: active_users_wow_pct down -56.5%",
      "severity": 0.1289083799,
      "direction": "down",
      "metric_family": "active_users",
      "metrics": {
        "active_users_wow_pct": -0.5650286891,
        "importance": 0.2281448401
      }
    },
    {
      "key": "model_gmv_net_wow_pct",
      "statement": "Model-highlighted signal: gmv_net_wow_pct up +7.5%",
      "severity": 0.0126109275,
      "direction": "up",
      "metric_family": "gmv_net",
      "metrics": {
        "gmv_net_wow_pct": 0.0754885396,
        "importance": 0.1670575099
      }
    },
    {
      "key": "model_transaction_count_wow_pct",
      "statement": "Model-highlighted signal: transaction_count_wow_pct up +12.5%",
      "severity": 0.0108055575,
      "direction": "up",
      "metric_family": "transaction_count",
      "metrics": {
        "transaction_count_wow_pct": 0.1250403841,
        "importance": 0.0864165412
      }
    }
  ],
  "target_segments": [
    {
      "metric_family": "active_users",
      "segment_key": "dormant_15_30d",
      "direction": "down",
      "contribution_share": 0.7924546985,
      "reason_statement": "Active users down driven by `dormant_15_30d` (driver: Active users 30d down -56.5% WoW)",
      "evidence_metrics": {
        "delta_seg": -8003.0,
        "delta_total": -7681.0,
        "segment_share_now": 0.0167295238,
        "segment_count_now": 2870,
        "wow_pct_seg": -0.7360434103,
        "cold_start_increase": false,
        "note": "stable"
      },
      "segment_confidence": "medium"
    },
    {
      "metric_family": "active_users",
      "segment_key": "dormant_31_60d",
      "direction": "down",
      "contribution_share": 0.2069511833,
      "reason_statement": "Active users down driven by `dormant_31_60d` (driver: Active users 30d down -56.5% WoW)",
      "evidence_metrics": {
        "delta_seg": -2090.0,
        "delta_total": -7681.0,
        "segment_share_now": 0.1727979108,
        "segment_count_now": 29644,
        "wow_pct_seg": -0.0658599609,
        "cold_start_increase": false,
        "note": "stable"
      },
      "segment_confidence": "high"
    },
    {
      "metric_family": "active_users",
      "segment_key": "recently_lapsed_8_14d",
      "direction": "down",
      "contribution_share": 0.0005941182,
      "reason_statement": "Active users down driven by `recently_lapsed_8_14d` (driver: Active users 30d down -56.5% WoW)",
      "evidence_metrics": {
        "delta_seg": -6.0,
        "delta_total": -7681.0,
        "segment_share_now": 0.0077293898,
        "segment_count_now": 1326,
        "wow_pct_seg": -0.0045045045,
        "cold_start_increase": false,
        "note": "stable"
      },
      "segment_confidence": "medium"
    }
  ],
  "suggested_actions": [
    "Trigger winback rewards for `dormant_15_30d` with short expiry and capped frequency.",
    "Set reactivation journeys for `dormant_15_30d` using low-friction missions first.",
    "Trigger winback rewards for `dormant_31_60d` with short expiry and capped frequency."
  ]
}
```

## 6) Attribution QA

| metric                             | value |
| ---------------------------------- | ----- |
| metric_family_no_eligible_segments | 235   |
| metric_family_noisy                | 546   |
| rows_total                         | 1632  |
| rows_with_target_segments          | 1132  |
| segments_dropped_commerce_mode     | 1516  |
| segments_dropped_min_presence      | 2147  |
| segments_dropped_sign_mismatch     | 7338  |
| segments_dropped_total             | 13297 |
| segments_dropped_zero_presence     | 2296  |
| segments_output_total              | 4125  |

## 7) Before/After Target Segments (2 windows per brand)

```json
{
  "brand_id": "c-vit",
  "window_end_date": "2026-02-08T00:00:00.000Z",
  "window_size": "30d",
  "before": {
    "predicted_health_class": "Healthy",
    "target_segments": [
      {
        "segment_key": "new_users_0_7d",
        "contribution_share": 0.9945175805,
        "reason_statement": "Major contributor to active_users drop. Driver: Active users 30d down -18.4% WoW",
        "evidence_metrics": {
          "active_users_delta_seg": -20.0,
          "active_users_wow_pct_seg": -1.0,
          "segment_share": 0.0,
          "driver_key": "active_down"
        }
      },
      {
        "segment_key": "recently_lapsed_8_14d",
        "contribution_share": 0.0054824195,
        "reason_statement": "Major contributor to completion_rate drop. Driver: Model-highlighted anomaly: reward_efficiency_zscore=1.04",
        "evidence_metrics": {
          "completion_rate_delta_seg": -1.0,
          "completion_rate_wow_pct_seg": -1.0,
          "segment_share": 0.2794117647,
          "driver_key": "efficiency_drop"
        }
      }
    ]
  },
  "after": {
    "predicted_health_class": "Healthy",
    "target_segments": [
      {
        "metric_family": "active_users",
        "segment_key": "dormant_60d_plus",
        "direction": "down",
        "contribution_share": 0.5,
        "reason_statement": "Active users down driven by `dormant_60d_plus` (driver: Active users 30d down -18.4% WoW)",
        "evidence_metrics": {
          "delta_seg": -2.0,
          "delta_total": -14.0,
          "segment_share_now": 0.9613259669,
          "segment_count_now": 174,
          "wow_pct_seg": -0.0113636364,
          "cold_start_increase": false,
          "note": "stable"
        },
        "segment_confidence": "high"
      },
      {
        "metric_family": "active_users",
        "segment_key": "non_redeemers",
        "direction": "down",
        "contribution_share": 0.5,
        "reason_statement": "Active users down driven by `non_redeemers` (driver: Active users 30d down -18.4% WoW)",
        "evidence_metrics": {
          "delta_seg": -2.0,
          "delta_total": -14.0,
          "segment_share_now": 0.9613259669,
          "segment_count_now": 174,
          "wow_pct_seg": -0.0113636364,
          "cold_start_increase": false,
          "note": "stable"
        },
        "segment_confidence": "high"
      }
    ]
  }
}
```

```json
{
  "brand_id": "c-vit",
  "window_end_date": "2026-02-15T00:00:00.000Z",
  "window_size": "30d",
  "before": {
    "predicted_health_class": "Warning",
    "target_segments": [
      {
        "segment_key": "recently_lapsed_8_14d",
        "contribution_share": 0.7368421053,
        "reason_statement": "Major contributor to active_users drop. Driver: Active users 30d down -24.2% WoW",
        "evidence_metrics": {
          "active_users_delta_seg": -14.0,
          "active_users_wow_pct_seg": -0.7368421053,
          "segment_share": 0.1020408163,
          "driver_key": "active_down"
        }
      },
      {
        "segment_key": "active_0_7d",
        "contribution_share": 0.2631578947,
        "reason_statement": "Major contributor to active_users drop. Driver: Active users 30d down -24.2% WoW",
        "evidence_metrics": {
          "active_users_delta_seg": -5.0,
          "active_users_wow_pct_seg": -1.0,
          "segment_share": 0.0,
          "driver_key": "active_down"
        }
      }
    ]
  },
  "after": {
    "predicted_health_class": "Warning",
    "target_segments": []
  }
}
```

```json
{
  "brand_id": "see-chan",
  "window_end_date": "2026-01-21T00:00:00.000Z",
  "window_size": "30d",
  "before": {
    "predicted_health_class": "Healthy",
    "target_segments": [
      {
        "segment_key": "dormant_15_30d",
        "contribution_share": 0.6020240171,
        "reason_statement": "Major contributor to active_users drop. Driver: Active users 30d down -8.9% WoW",
        "evidence_metrics": {
          "active_users_delta_seg": -2374.0,
          "active_users_wow_pct_seg": -0.1792509816,
          "segment_share": 0.7908330302,
          "driver_key": "active_down"
        }
      },
      {
        "segment_key": "recently_lapsed_8_14d",
        "contribution_share": 0.2583398677,
        "reason_statement": "Major contributor to active_users increase. Driver: Model-highlighted anomaly: dormant_share_zscore=1.52",
        "evidence_metrics": {
          "active_users_delta_seg": 536.0,
          "active_users_wow_pct_seg": 0.6708385482,
          "segment_share": 0.0971262277,
          "driver_key": "dormant_up"
        }
      },
      {
        "segment_key": "active_0_7d",
        "contribution_share": 0.1396361152,
        "reason_statement": "Major contributor to active_users increase. Driver: Model-highlighted anomaly: dormant_share_zscore=1.52",
        "evidence_metrics": {
          "active_users_delta_seg": 272.0,
          "active_users_wow_pct_seg": 0.2863157895,
          "segment_share": 0.0889050564,
          "driver_key": "dormant_up"
        }
      }
    ]
  },
  "after": {
    "predicted_health_class": "Healthy",
    "target_segments": [
      {
        "metric_family": "active_users",
        "segment_key": "dormant_15_30d",
        "direction": "down",
        "contribution_share": 0.7101405923,
        "reason_statement": "Active users down driven by `dormant_15_30d` (driver: Active users 30d down -8.9% WoW)",
        "evidence_metrics": {
          "delta_seg": -2374.0,
          "delta_total": -1328.0,
          "segment_share_now": 0.0636678241,
          "segment_count_now": 10873,
          "wow_pct_seg": -0.1792103873,
          "cold_start_increase": false,
          "note": "stable"
        },
        "segment_confidence": "high"
      },
      {
        "metric_family": "active_users",
        "segment_key": "dormant_31_60d",
        "direction": "down",
        "contribution_share": 0.20729883340000002,
        "reason_statement": "Active users down driven by `dormant_31_60d` (driver: Active users 30d down -8.9% WoW)",
        "evidence_metrics": {
          "delta_seg": -693.0,
          "delta_total": -1328.0,
          "segment_share_now": 0.1858212757,
          "segment_count_now": 31734,
          "wow_pct_seg": -0.0213710797,
          "cold_start_increase": false,
          "note": "stable"
        },
        "segment_confidence": "high"
      },
      {
        "metric_family": "active_users",
        "segment_key": "new_users_0_7d",
        "direction": "down",
        "contribution_share": 0.08256057430000001,
        "reason_statement": "Active users down driven by `new_users_0_7d` (driver: Active users 30d down -8.9% WoW)",
        "evidence_metrics": {
          "delta_seg": -276.0,
          "delta_total": -1328.0,
          "segment_share_now": 0.0018327995000000001,
          "segment_count_now": 313,
          "wow_pct_seg": -0.4685908319,
          "cold_start_increase": false,
          "note": "stable"
        },
        "segment_confidence": "medium"
      }
    ]
  }
}
```

```json
{
  "brand_id": "see-chan",
  "window_end_date": "2026-01-28T00:00:00.000Z",
  "window_size": "30d",
  "before": {
    "predicted_health_class": "AtRisk",
    "target_segments": [
      {
        "segment_key": "dormant_15_30d",
        "contribution_share": 0.9746090831,
        "reason_statement": "Major contributor to active_users drop. Driver: Active users 30d down -56.5% WoW",
        "evidence_metrics": {
          "active_users_delta_seg": -7997.0,
          "active_users_wow_pct_seg": -0.7356945722,
          "segment_share": 0.4341190692,
          "driver_key": "active_down"
        }
      },
      {
        "segment_key": "recently_lapsed_8_14d",
        "contribution_share": 0.0253909169,
        "reason_statement": "Major contributor to transactions drop. Driver: Model-highlighted signal: transaction_count_wow_pct up +12.5%",
        "evidence_metrics": {
          "transactions_delta_seg": -33.0,
          "transactions_wow_pct_seg": -0.4647887324,
          "segment_share": 0.2000604412,
          "driver_key": "transactions_down"
        }
      }
    ]
  },
  "after": {
    "predicted_health_class": "AtRisk",
    "target_segments": [
      {
        "metric_family": "active_users",
        "segment_key": "dormant_15_30d",
        "direction": "down",
        "contribution_share": 0.7924546985,
        "reason_statement": "Active users down driven by `dormant_15_30d` (driver: Active users 30d down -56.5% WoW)",
        "evidence_metrics": {
          "delta_seg": -8003.0,
          "delta_total": -7681.0,
          "segment_share_now": 0.0167295238,
          "segment_count_now": 2870,
          "wow_pct_seg": -0.7360434103,
          "cold_start_increase": false,
          "note": "stable"
        },
        "segment_confidence": "medium"
      },
      {
        "metric_family": "active_users",
        "segment_key": "dormant_31_60d",
        "direction": "down",
        "contribution_share": 0.2069511833,
        "reason_statement": "Active users down driven by `dormant_31_60d` (driver: Active users 30d down -56.5% WoW)",
        "evidence_metrics": {
          "delta_seg": -2090.0,
          "delta_total": -7681.0,
          "segment_share_now": 0.1727979108,
          "segment_count_now": 29644,
          "wow_pct_seg": -0.0658599609,
          "cold_start_increase": false,
          "note": "stable"
        },
        "segment_confidence": "high"
      },
      {
        "metric_family": "active_users",
        "segment_key": "recently_lapsed_8_14d",
        "direction": "down",
        "contribution_share": 0.0005941182,
        "reason_statement": "Active users down driven by `recently_lapsed_8_14d` (driver: Active users 30d down -56.5% WoW)",
        "evidence_metrics": {
          "delta_seg": -6.0,
          "delta_total": -7681.0,
          "segment_share_now": 0.0077293898,
          "segment_count_now": 1326,
          "wow_pct_seg": -0.0045045045,
          "cold_start_increase": false,
          "note": "stable"
        },
        "segment_confidence": "medium"
      }
    ]
  }
}
```
