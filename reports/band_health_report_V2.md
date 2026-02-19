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

| brand_id | left_table           | right_table    | key            | left_rows | right_rows | left_unique | right_unique | overlap_unique | row_coverage          |
| -------- | -------------------- | -------------- | -------------- | --------- | ---------- | ----------- | ------------ | -------------- | --------------------- |
| c-vit    | activity_transaction | user_identity  | user_id        | 10000     | 10000      | 181         | 10000        | 9              | 0.0408                |
| c-vit    | activity_transaction | user_view      | user_id        | 10000     | 10000      | 181         | 10000        | 10             | 0.028                 |
| c-vit    | activity_transaction | user_visitor   | user_id        | 10000     | 10000      | 181         | 242          | 4              | 0.1084                |
| c-vit    | purchase             | purchase_items | transaction_id | 10000     | 8827       | 10000       | 5326         | 0              | 0.0                   |
| c-vit    | purchase             | purchase_items | user_id        | 10000     | 8827       | 1250        | 1            | 0              | 0.0                   |
| c-vit    | purchase             | user_identity  | user_id        | 10000     | 10000      | 1250        | 10000        | 82             | 0.0439                |
| c-vit    | purchase             | user_info      | user_id        | 10000     | 10000      | 1250        | 10000        | 70             | 0.0308                |
| c-vit    | user_view            | user_visitor   | user_id        | 10000     | 10000      | 10000       | 242          | 0              | 0.0                   |
| see-chan | activity_transaction | user_identity  | user_id        | 178594    | 141220     | 140795      | 141220       | 140792         | 0.9999832021232516    |
| see-chan | activity_transaction | user_view      | user_id        | 178594    | 173494     | 140795      | 173494       | 140792         | 0.9999832021232516    |
| see-chan | activity_transaction | user_visitor   | user_id        | 178594    | 446324     | 140795      | 172144       | 139453         | 0.9924857498012252    |
| see-chan | purchase             | purchase_items | transaction_id | 1404759   | 1982120    | 1399583     | 1378053      | 1378053        | 0.9839175260667489    |
| see-chan | purchase             | purchase_items | user_id        | 1404759   | 1982120    | 10359       | 19           | 19             | 0.0020581521535755717 |
| see-chan | purchase             | user_identity  | user_id        | 1404759   | 141220     | 10359       | 141220       | 10359          | 1.0                   |
| see-chan | purchase             | user_info      | user_id        | 1404759   | 141220     | 10359       | 141220       | 10359          | 1.0                   |
| see-chan | user_view            | user_visitor   | user_id        | 173494    | 446324     | 173494      | 172144       | 172144         | 0.992218751080729     |

### Coverage notes

- **c-vit**: weak key coverage observed in activity_transaction->user_identity (4.08%), activity_transaction->user_view (2.80%), activity_transaction->user_visitor (10.84%), purchase->purchase_items (0.00%), purchase->purchase_items (0.00%), purchase->user_identity (4.39%), purchase->user_info (3.08%), user_view->user_visitor (0.00%); feature logic uses brand-level fallback aggregations.
- **see-chan**: weak key coverage observed in purchase->purchase_items (0.21%); feature logic uses brand-level fallback aggregations.

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
  "window_end_date": "2026-01-25 00:00:00+00:00",
  "window_size": "30d",
  "predicted_health_class": "Healthy",
  "predicted_health_score": 85.3504902842963,
  "probabilities": {
    "AtRisk": 0.0031703971093804176,
    "Healthy": 0.8487151394374873,
    "Warning": 0.1481144634531322
  },
  "drivers": [
    {
      "key": "model_reward_efficiency_zscore",
      "statement": "Model-highlighted anomaly: reward_efficiency_zscore=1.10",
      "severity": 0.0013218029221820702,
      "metrics": {
        "reward_efficiency_zscore": 1.1039320711075211,
        "importance": -0.0011973589288478321
      }
    },
    {
      "key": "model_reward_efficiency_wow_pct",
      "statement": "Model-highlighted signal: reward_efficiency_wow_pct up +10.6%",
      "severity": 0.0002618538336543813,
      "metrics": {
        "reward_efficiency_wow_pct": 0.10599078341013835,
        "importance": -0.0024705339957826387
      }
    }
  ],
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
  "window_end_date": "2026-02-01 00:00:00+00:00",
  "window_size": "30d",
  "predicted_health_class": "Healthy",
  "predicted_health_score": 85.90892643155102,
  "probabilities": {
    "AtRisk": 0.003366989790721984,
    "Healthy": 0.867559035807543,
    "Warning": 0.12907397440173504
  },
  "drivers": [
    {
      "key": "model_active_users_wow_pct",
      "statement": "Model-highlighted signal: active_users_wow_pct up +13.4%",
      "severity": 0.030646321803926638,
      "metrics": {
        "active_users_wow_pct": 0.13432835820895517,
        "importance": 0.2281448400958984
      }
    },
    {
      "key": "model_reward_efficiency_zscore",
      "statement": "Model-highlighted anomaly: reward_efficiency_zscore=1.10",
      "severity": 0.0013175802793117248,
      "metrics": {
        "reward_efficiency_zscore": 1.100405440313187,
        "importance": -0.0011973589288478321
      }
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
  "window_end_date": "2026-02-08 00:00:00+00:00",
  "window_size": "30d",
  "predicted_health_class": "Healthy",
  "predicted_health_score": 76.28785950540521,
  "probabilities": {
    "AtRisk": 0.06599127508928683,
    "Healthy": 0.619918471117675,
    "Warning": 0.3140902537930383
  },
  "drivers": [
    {
      "key": "active_down",
      "statement": "Active users 30d down -18.4% WoW",
      "severity": 0.1842105263157895,
      "metrics": {
        "active_users_wow_pct": -0.1842105263157895
      }
    },
    {
      "key": "model_active_users_wow_pct",
      "statement": "Model-highlighted signal: active_users_wow_pct down -18.4%",
      "severity": 0.04202668107029708,
      "metrics": {
        "active_users_wow_pct": -0.1842105263157895,
        "importance": 0.2281448400958984
      }
    },
    {
      "key": "model_reward_efficiency_zscore",
      "statement": "Model-highlighted anomaly: reward_efficiency_zscore=1.04",
      "severity": 0.0012471647485557287,
      "metrics": {
        "reward_efficiency_zscore": 1.0415963989643628,
        "importance": -0.0011973589288478321
      }
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
  "window_end_date": "2026-02-15 00:00:00+00:00",
  "window_size": "30d",
  "predicted_health_class": "Warning",
  "predicted_health_score": 51.3953028469059,
  "probabilities": {
    "AtRisk": 0.2814209461760006,
    "Healthy": 0.04150119876886372,
    "Warning": 0.6770778550551357
  },
  "drivers": [
    {
      "key": "active_down",
      "statement": "Active users 30d down -24.2% WoW",
      "severity": 0.24193548387096775,
      "metrics": {
        "active_users_wow_pct": -0.24193548387096775
      }
    },
    {
      "key": "model_dormant_share_zscore",
      "statement": "Model-highlighted anomaly: dormant_share_zscore=2.11",
      "severity": 0.09142512930612001,
      "metrics": {
        "dormant_share_zscore": 2.1106124423657717,
        "importance": 0.043316872141453966
      }
    },
    {
      "key": "model_active_users_wow_pct",
      "statement": "Model-highlighted signal: active_users_wow_pct down -24.2%",
      "severity": 0.05519633228126574,
      "metrics": {
        "active_users_wow_pct": -0.24193548387096775,
        "importance": 0.2281448400958984
      }
    },
    {
      "key": "model_active_users_zscore",
      "statement": "Model-highlighted anomaly: active_users_zscore=-2.09",
      "severity": 0.047027131756661116,
      "metrics": {
        "active_users_zscore": -2.0949193661947816,
        "importance": 0.02244818226206069
      }
    }
  ],
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
  "window_end_date": "2026-01-07 00:00:00+00:00",
  "window_size": "30d",
  "predicted_health_class": "Healthy",
  "predicted_health_score": 68.30585723207474,
  "probabilities": {
    "AtRisk": 0.23268134010634559,
    "Healthy": 0.5483234711932284,
    "Warning": 0.2189951887004259
  },
  "drivers": [
    {
      "key": "active_down",
      "statement": "Active users 30d down -12.7% WoW",
      "severity": 0.12731099239112842,
      "metrics": {
        "active_users_wow_pct": -0.12731099239112842
      }
    },
    {
      "key": "model_dormant_share_zscore",
      "statement": "Model-highlighted anomaly: dormant_share_zscore=1.31",
      "severity": 0.05687136972679211,
      "metrics": {
        "dormant_share_zscore": 1.3129149662763064,
        "importance": 0.043316872141453966
      }
    },
    {
      "key": "model_active_users_zscore",
      "statement": "Model-highlighted anomaly: active_users_zscore=-1.42",
      "severity": 0.03187068025199764,
      "metrics": {
        "active_users_zscore": -1.419744364151113,
        "importance": 0.02244818226206069
      }
    },
    {
      "key": "model_active_users_wow_pct",
      "statement": "Model-highlighted signal: active_users_wow_pct down -12.7%",
      "severity": 0.02904534600152413,
      "metrics": {
        "active_users_wow_pct": -0.12731099239112842,
        "importance": 0.2281448400958984
      }
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
  "brand_id": "see-chan",
  "window_end_date": "2026-01-14 00:00:00+00:00",
  "window_size": "30d",
  "predicted_health_class": "AtRisk",
  "predicted_health_score": 29.369050379757407,
  "probabilities": {
    "AtRisk": 0.8992620423517659,
    "Healthy": 0.02810739540230728,
    "Warning": 0.07263056224592673
  },
  "drivers": [
    {
      "key": "active_down",
      "statement": "Active users 30d down -44.6% WoW",
      "severity": 0.44636960635179757,
      "metrics": {
        "active_users_wow_pct": -0.44636960635179757
      }
    },
    {
      "key": "gmv_down",
      "statement": "GMV down -23.0% WoW",
      "severity": 0.22956842737083594,
      "metrics": {
        "gmv_net_wow_pct": -0.22956842737083594
      }
    },
    {
      "key": "transactions_down",
      "statement": "Transactions down -22.4% WoW",
      "severity": 0.22383860787879284,
      "metrics": {
        "transaction_count_wow_pct": -0.22383860787879284
      }
    },
    {
      "key": "model_active_users_wow_pct",
      "statement": "Model-highlighted signal: active_users_wow_pct down -44.6%",
      "severity": 0.10183692246479996,
      "metrics": {
        "active_users_wow_pct": -0.44636960635179757,
        "importance": 0.2281448400958984
      }
    },
    {
      "key": "dormant_up",
      "statement": "Dormant share up +8.5% WoW",
      "severity": 0.0845419109115273,
      "metrics": {
        "dormant_share_wow_pct": 0.0845419109115273
      }
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
  "brand_id": "see-chan",
  "window_end_date": "2026-01-21 00:00:00+00:00",
  "window_size": "30d",
  "predicted_health_class": "Healthy",
  "predicted_health_score": 83.66645809216831,
  "probabilities": {
    "AtRisk": 0.024804862941783246,
    "Healthy": 0.8178209431710242,
    "Warning": 0.15737419388719256
  },
  "drivers": [
    {
      "key": "active_down",
      "statement": "Active users 30d down -8.9% WoW",
      "severity": 0.08899611312156552,
      "metrics": {
        "active_users_wow_pct": -0.08899611312156552
      }
    },
    {
      "key": "model_dormant_share_zscore",
      "statement": "Model-highlighted anomaly: dormant_share_zscore=1.52",
      "severity": 0.06602947590682176,
      "metrics": {
        "dormant_share_zscore": 1.5243361914775002,
        "importance": 0.043316872141453966
      }
    },
    {
      "key": "model_active_users_wow_pct",
      "statement": "Model-highlighted signal: active_users_wow_pct down -8.9%",
      "severity": 0.02030400399727605,
      "metrics": {
        "active_users_wow_pct": -0.08899611312156552,
        "importance": 0.2281448400958984
      }
    },
    {
      "key": "model_transaction_count_wow_pct",
      "statement": "Model-highlighted signal: transaction_count_wow_pct down -5.7%",
      "severity": 0.0049208120500090265,
      "metrics": {
        "transaction_count_wow_pct": -0.05694294149165291,
        "importance": 0.08641654120959581
      }
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
  "brand_id": "see-chan",
  "window_end_date": "2026-01-28 00:00:00+00:00",
  "window_size": "30d",
  "predicted_health_class": "AtRisk",
  "predicted_health_score": 30.534624034264723,
  "probabilities": {
    "AtRisk": 0.8672282389881542,
    "Healthy": 0.029587079961670495,
    "Warning": 0.10318468105017535
  },
  "drivers": [
    {
      "key": "active_down",
      "statement": "Active users 30d down -56.5% WoW",
      "severity": 0.5650286891275562,
      "metrics": {
        "active_users_wow_pct": -0.5650286891275562
      }
    },
    {
      "key": "model_active_users_wow_pct",
      "statement": "Model-highlighted signal: active_users_wow_pct down -56.5%",
      "severity": 0.1289083799306014,
      "metrics": {
        "active_users_wow_pct": -0.5650286891275562,
        "importance": 0.2281448400958984
      }
    },
    {
      "key": "model_gmv_net_wow_pct",
      "statement": "Model-highlighted signal: gmv_net_wow_pct up +7.5%",
      "severity": 0.012610927453434064,
      "metrics": {
        "gmv_net_wow_pct": 0.07548853961496804,
        "importance": 0.16705750989165433
      }
    },
    {
      "key": "model_transaction_count_wow_pct",
      "statement": "Model-highlighted signal: transaction_count_wow_pct up +12.5%",
      "severity": 0.010805557507397288,
      "metrics": {
        "transaction_count_wow_pct": 0.12504038412263396,
        "importance": 0.08641654120959581
      }
    }
  ],
  "suggested_actions": [
    "Launch dormant-user reactivation campaigns with segmented incentives.",
    "Reduce message fatigue via tighter frequency caps and send-time optimization.",
    "Retarget recently lapsed cohorts with low-friction missions."
  ]
}
```
