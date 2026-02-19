# Brand Health Modeling Report

## 1) Data Joins + Coverage Summary

### Table row counts

| brand_id | table                | rows     | columns |
| -------- | -------------------- | -------- | ------- |
| c-vit    | activity_transaction | 10000    | 10      |
| c-vit    | purchase             | 10000    | 14      |
| c-vit    | purchase_items       | 8827     | 21      |
| c-vit    | user_device          | 10000    | 10      |
| c-vit    | user_identity        | 10000    | 5       |
| c-vit    | user_info            | 10000    | 8       |
| c-vit    | user_view            | 10000    | 5       |
| c-vit    | user_visitor         | 10000    | 11      |
| see-chan | activity_transaction | 20937218 | 10      |
| see-chan | purchase             | 1404759  | 14      |
| see-chan | purchase_items       | 1982120  | 21      |
| see-chan | user_device          | 105411   | 10      |
| see-chan | user_identity        | 342690   | 5       |
| see-chan | user_info            | 342690   | 8       |
| see-chan | user_view            | 400239   | 5       |
| see-chan | user_visitor         | 5409254  | 11      |

### Join key coverage

| brand_id | left_table           | right_table    | key            | left_rows | right_rows | left_unique | right_unique | overlap_unique | row_coverage       |
| -------- | -------------------- | -------------- | -------------- | --------- | ---------- | ----------- | ------------ | -------------- | ------------------ |
| c-vit    | activity_transaction | user_identity  | user_id        | 10000     | 10000      | 181         | 10000        | 9              | 0.0408             |
| c-vit    | activity_transaction | user_view      | user_id        | 10000     | 10000      | 181         | 10000        | 10             | 0.028              |
| c-vit    | activity_transaction | user_visitor   | user_id        | 10000     | 10000      | 181         | 242          | 4              | 0.1084             |
| c-vit    | purchase             | purchase_items | transaction_id | 10000     | 8827       | 10000       | 5326         | 0              | 0.0                |
| c-vit    | purchase             | purchase_items | user_id        | 10000     | 8827       | 1250        | 1            | 0              | 0.0                |
| c-vit    | purchase             | user_identity  | user_id        | 10000     | 10000      | 1250        | 10000        | 82             | 0.0439             |
| c-vit    | purchase             | user_info      | user_id        | 10000     | 10000      | 1250        | 10000        | 70             | 0.0308             |
| c-vit    | user_view            | user_visitor   | user_id        | 10000     | 10000      | 10000       | 242          | 0              | 0.0                |
| see-chan | activity_transaction | user_identity  | user_id        | 20937218  | 342690     | 342532      | 342690       | 342207         | 0.9999375752786258 |
| see-chan | activity_transaction | user_view      | user_id        | 20937218  | 400239     | 342532      | 400239       | 342207         | 0.9999375752786258 |
| see-chan | activity_transaction | user_visitor   | user_id        | 20937218  | 5409254    | 342532      | 398842       | 340857         | 0.999873096798247  |
| see-chan | purchase             | purchase_items | transaction_id | 1404759   | 1982120    | 1399583     | 1378053      | 1378053        | 0.9839175260667488 |
| see-chan | purchase             | purchase_items | user_id        | 1404759   | 1982120    | 10359       | 19           | 19             | 0.0020581521535755 |
| see-chan | purchase             | user_identity  | user_id        | 1404759   | 342690     | 10359       | 342690       | 10359          | 1.0                |
| see-chan | purchase             | user_info      | user_id        | 1404759   | 342690     | 10359       | 342690       | 10359          | 1.0                |
| see-chan | user_view            | user_visitor   | user_id        | 400239    | 5409254    | 400239      | 398842       | 398842         | 0.9965095855226502 |

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

Selected model based on time-split macro F1: **hgb**

### Time-based split results

| model          | macro_f1           | balanced_accuracy  | calibration_ece     | brier               |
| -------------- | ------------------ | ------------------ | ------------------- | ------------------- |
| logistic       | 0.29012142026853   | 0.386616671336152  | 0.3522829112112435  | 0.1641568069476274  |
| hgb            | 0.8269513921469741 | 0.8328547110862071 | 0.24215028824351134 | 0.03535610464610793 |
| hgb_calibrated | 0.8108728879715595 | 0.8023178688468918 | 0.09849538620693865 | 0.03145969218821867 |

### Cross-brand holdout (train other brand, test holdout)

| holdout_brand | macro_f1           | balanced_accuracy  | note |
| ------------- | ------------------ | ------------------ | ---- |
| c-vit         | 0.7674475963773792 | 0.7445574772157051 | ok   |
| see-chan      | 0.759668983583908  | 0.7366665898635208 | ok   |

### GroupKFold by brand

| fold | macro_f1           | balanced_accuracy  |
| ---- | ------------------ | ------------------ |
| 1    | 0.759668983583908  | 0.7366665898635208 |
| 2    | 0.7674475963773792 | 0.7445574772157051 |

## 5) Example Output (Last 4 Windows Per Brand)

### Brand: c-vit

```json
{
  "brand_id": "c-vit",
  "window_end_date": "2026-01-25 00:00:00+00:00",
  "window_size": "30d",
  "predicted_health_class": "Healthy",
  "predicted_health_score": 89.99997134931544,
  "probabilities": {
    "AtRisk": 6.757125969002432e-10,
    "Healthy": 0.999999045765513,
    "Warning": 9.535587744466172e-07
  },
  "drivers": [
    {
      "key": "model_reward_efficiency_zscore",
      "statement": "Model-highlighted anomaly: reward_efficiency_zscore=1.10",
      "severity": 0.013729031659924108,
      "metrics": {
        "reward_efficiency_zscore": 1.1039320711075211,
        "importance": -0.012436482297457344
      }
    },
    {
      "key": "model_reward_efficiency_wow_pct",
      "statement": "Model-highlighted signal: reward_efficiency_wow_pct up +10.6%",
      "severity": 2.8825839248453684e-05,
      "metrics": {
        "reward_efficiency_wow_pct": 0.10599078341013835,
        "importance": 0.00027196552682236713
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
  "predicted_health_score": 89.9999766684962,
  "probabilities": {
    "AtRisk": 4.3516030653254656e-10,
    "Healthy": 0.9999992227908935,
    "Warning": 7.767739463240911e-07
  },
  "drivers": [
    {
      "key": "model_active_users_wow_pct",
      "statement": "Model-highlighted signal: active_users_wow_pct up +13.4%",
      "severity": 0.026321218308144267,
      "metrics": {
        "active_users_wow_pct": 0.13432835820895517,
        "importance": 0.19594684740507406
      }
    },
    {
      "key": "model_reward_efficiency_zscore",
      "statement": "Model-highlighted anomaly: reward_efficiency_zscore=1.10",
      "severity": 0.013685172778480706,
      "metrics": {
        "reward_efficiency_zscore": 1.100405440313187,
        "importance": -0.012436482297457344
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
  "predicted_health_score": 89.96000352073663,
  "probabilities": {
    "AtRisk": 3.988983497370605e-07,
    "Healthy": 0.9986672494059624,
    "Warning": 0.0013323516956878318
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
      "severity": 0.03609547189040838,
      "metrics": {
        "active_users_wow_pct": -0.1842105263157895,
        "importance": 0.19594684740507406
      }
    },
    {
      "key": "model_reward_efficiency_zscore",
      "statement": "Model-highlighted anomaly: reward_efficiency_zscore=1.04",
      "severity": 0.012953795176815615,
      "metrics": {
        "reward_efficiency_zscore": 1.0415963989643628,
        "importance": -0.012436482297457344
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
  "predicted_health_score": 59.99690046720949,
  "probabilities": {
    "AtRisk": 0.00015421923110291882,
    "Healthy": 7.660467660333824e-05,
    "Warning": 0.9997691760922937
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
      "key": "model_active_users_zscore",
      "statement": "Model-highlighted anomaly: active_users_zscore=-2.09",
      "severity": 0.14139310852452655,
      "metrics": {
        "active_users_zscore": -2.0949193661947816,
        "importance": 0.06749334165608172
      }
    },
    {
      "key": "model_active_users_wow_pct",
      "statement": "Model-highlighted signal: active_users_wow_pct down -24.2%",
      "severity": 0.04740649533993727,
      "metrics": {
        "active_users_wow_pct": -0.24193548387096775,
        "importance": 0.19594684740507406
      }
    },
    {
      "key": "model_dormant_share_zscore",
      "statement": "Model-highlighted anomaly: dormant_share_zscore=2.11",
      "severity": 0.03717187442671582,
      "metrics": {
        "dormant_share_zscore": 2.1106124423657717,
        "importance": 0.017611890122778823
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
  "window_end_date": "2026-01-22 00:00:00+00:00",
  "window_size": "30d",
  "predicted_health_class": "Healthy",
  "predicted_health_score": 89.99248394072767,
  "probabilities": {
    "AtRisk": 6.621196706141446e-08,
    "Healthy": 0.9997495419382174,
    "Warning": 0.0002503918498155149
  },
  "drivers": [
    {
      "key": "model_active_users_zscore",
      "statement": "Model-highlighted anomaly: active_users_zscore=-1.45",
      "severity": 0.09754817639402837,
      "metrics": {
        "active_users_zscore": -1.445300736346612,
        "importance": 0.06749334165608172
      }
    },
    {
      "key": "model_active_users_wow_pct",
      "statement": "Model-highlighted signal: active_users_wow_pct down -6.6%",
      "severity": 0.012880655384142488,
      "metrics": {
        "active_users_wow_pct": -0.06573545609292075,
        "importance": 0.19594684740507406
      }
    },
    {
      "key": "model_transaction_count_wow_pct",
      "statement": "Model-highlighted signal: transaction_count_wow_pct down -5.8%",
      "severity": 0.004174894747596899,
      "metrics": {
        "transaction_count_wow_pct": -0.05801315913346372,
        "importance": 0.07196461647593148
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
  "window_end_date": "2026-01-29 00:00:00+00:00",
  "window_size": "30d",
  "predicted_health_class": "Warning",
  "predicted_health_score": 59.99372308785113,
  "probabilities": {
    "AtRisk": 0.00025320312466691413,
    "Healthy": 8.617324048232114e-05,
    "Warning": 0.9996606236348508
  },
  "drivers": [
    {
      "key": "active_down",
      "statement": "Active users 30d down -32.3% WoW",
      "severity": 0.32324098387010347,
      "metrics": {
        "active_users_wow_pct": -0.32324098387010347
      }
    },
    {
      "key": "model_active_users_wow_pct",
      "statement": "Model-highlighted signal: active_users_wow_pct down -32.3%",
      "severity": 0.06333805174146118,
      "metrics": {
        "active_users_wow_pct": -0.32324098387010347,
        "importance": 0.19594684740507406
      }
    },
    {
      "key": "model_gmv_net_wow_pct",
      "statement": "Model-highlighted signal: gmv_net_wow_pct up +11.2%",
      "severity": 0.01797824156992172,
      "metrics": {
        "gmv_net_wow_pct": 0.11181573400200184,
        "importance": 0.16078454191071048
      }
    },
    {
      "key": "model_transaction_count_wow_pct",
      "statement": "Model-highlighted signal: transaction_count_wow_pct up +15.8%",
      "severity": 0.011337345940102378,
      "metrics": {
        "transaction_count_wow_pct": 0.1575405594483248,
        "importance": 0.07196461647593148
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
  "window_end_date": "2026-02-05 00:00:00+00:00",
  "window_size": "30d",
  "predicted_health_class": "Healthy",
  "predicted_health_score": 89.99977476208701,
  "probabilities": {
    "AtRisk": 7.411352495315797e-09,
    "Healthy": 0.999992500716145,
    "Warning": 7.4918725025510125e-06
  },
  "drivers": [
    {
      "key": "model_active_users_zscore",
      "statement": "Model-highlighted anomaly: active_users_zscore=-1.02",
      "severity": 0.06861122430836732,
      "metrics": {
        "active_users_zscore": -1.0165628582739594,
        "importance": 0.06749334165608172
      }
    },
    {
      "key": "model_active_users_wow_pct",
      "statement": "Model-highlighted signal: active_users_wow_pct up +15.3%",
      "severity": 0.029976190449489494,
      "metrics": {
        "active_users_wow_pct": 0.15298123366854055,
        "importance": 0.19594684740507406
      }
    },
    {
      "key": "model_gmv_net_wow_pct",
      "statement": "Model-highlighted signal: gmv_net_wow_pct down -6.3%",
      "severity": 0.010136530167350475,
      "metrics": {
        "gmv_net_wow_pct": -0.06304418351970464,
        "importance": 0.16078454191071048
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
  "window_end_date": "2026-02-12 00:00:00+00:00",
  "window_size": "30d",
  "predicted_health_class": "AtRisk",
  "predicted_health_score": 25.00313705515164,
  "probabilities": {
    "AtRisk": 0.9999201817529186,
    "Healthy": 1.1447216793109295e-05,
    "Warning": 6.837103028824537e-05
  },
  "drivers": [
    {
      "key": "transactions_down",
      "statement": "Transactions down -21.9% WoW",
      "severity": 0.21892178322751898,
      "metrics": {
        "transaction_count_wow_pct": -0.21892178322751898
      }
    },
    {
      "key": "gmv_down",
      "statement": "GMV down -21.4% WoW",
      "severity": 0.21380103290550478,
      "metrics": {
        "gmv_net_wow_pct": -0.21380103290550478
      }
    },
    {
      "key": "active_down",
      "statement": "Active users 30d down -11.3% WoW",
      "severity": 0.11283565689169694,
      "metrics": {
        "active_users_wow_pct": -0.11283565689169694
      }
    },
    {
      "key": "model_gmv_net_wow_pct",
      "statement": "Model-highlighted signal: gmv_net_wow_pct down -21.4%",
      "severity": 0.03437590113574832,
      "metrics": {
        "gmv_net_wow_pct": -0.21380103290550478,
        "importance": 0.16078454191071048
      }
    },
    {
      "key": "model_active_users_wow_pct",
      "statement": "Model-highlighted signal: active_users_wow_pct down -11.3%",
      "severity": 0.022109791242808634,
      "metrics": {
        "active_users_wow_pct": -0.11283565689169694,
        "importance": 0.19594684740507406
      }
    }
  ],
  "suggested_actions": [
    "Introduce repeat-purchase triggers based on product replenishment cadence.",
    "Activate personalized offer ladders for near-churn buyers.",
    "Prioritize retention promotions for buyers with recent order decline."
  ]
}
```
