# Join Diagnostics

Diagnostic normalized key columns used: `transaction_id_norm`, `user_id_norm`.

## Coverage Before/After Normalization

brand_id left_table    right_table            key  left_unique  right_unique  overlap_unique  row_coverage  left_unique_norm  right_unique_norm  overlap_unique_norm  row_coverage_norm
   c-vit   purchase purchase_items transaction_id        10000          5326               0      0.000000             10000               5326                    0           0.000000
   c-vit   purchase purchase_items        user_id         1250             1               0      0.000000              1250                  1                    0           0.000000
see-chan   purchase purchase_items transaction_id      1399583       1378053         1378053      0.983918           1399566            1378053              1378053           0.983929
see-chan   purchase purchase_items        user_id        10359            19              19      0.002058             10359                 19                   19           0.002058

## Time Range Overlap Check

brand_id             purchase_overall_min             purchase_overall_max       purchase_items_overall_min       purchase_items_overall_max  time_range_overlap     purchase_create_datetime_min     purchase_create_datetime_max       purchase_paid_datetime_min       purchase_paid_datetime_max purchase_items_create_datetime_min purchase_items_create_datetime_max purchase_items_paid_datetime_min purchase_items_paid_datetime_max purchase_items_delivered_datetime_min purchase_items_delivered_datetime_max
   c-vit 2021-08-31 09:54:55.280000+00:00 2024-06-16 20:39:37.433000+00:00 2023-09-09 12:30:35.430000+00:00 2024-03-05 18:03:38.485000+00:00                True 2021-08-31 09:54:55.280000+00:00 2024-06-16 20:39:37.433000+00:00 2021-08-31 09:54:55.280000+00:00 2024-06-16 20:39:37.433000+00:00   2023-09-09 12:30:35.430000+00:00   2024-03-05 18:03:38.485000+00:00 2023-09-09 12:30:35.430000+00:00 2024-03-05 18:03:38.485000+00:00                                  None                                  None
see-chan        2023-02-23 00:00:00+00:00        2026-02-02 00:00:00+00:00        2023-04-06 00:00:00+00:00        2026-02-02 00:00:00+00:00                True        2023-02-23 00:00:00+00:00        2026-02-02 00:00:00+00:00        2023-02-23 00:00:00+00:00        2026-02-02 00:00:00+00:00          2023-04-06 00:00:00+00:00          2026-02-02 00:00:00+00:00        2023-04-06 00:00:00+00:00        2026-02-02 00:00:00+00:00                                  None                                  None

## Brand: c-vit

- `transaction_id` normalized row coverage: 0.0000
- time range overlap: True
- decision: commerce_joinable=false
- "Join quality below threshold (<0.80); commerce join is not valid for this brand."

### transaction_id_norm pattern/length

brand_id          table  len_min  len_median  len_max                                                                   prefix_samples  share_alnum_dash_underscore  share_numeric_only  share_has_dash  share_has_underscore
   c-vit       purchase     33.0        33.0     33.0 ["smkx", "sml9", "smlb", "smlz", "smma", "smmq", "smms", "smnb", "smnd", "smns"]                          1.0                 0.0        0.555000              0.355000
   c-vit purchase_items     33.0        33.0     33.0 ["sme1", "sme2", "sme3", "sme4", "sme5", "sme7", "smee", "smef", "smeh", "smei"]                          1.0                 0.0        0.571429              0.369888

### sampled merge on transaction_id_norm

brand_id  sampled_merge_match_rate_norm_sample_vs_sample  sampled_merge_match_rate_norm_left_sample_vs_full_right  sample_size_left  sample_size_right
   c-vit                                             0.0                                                      0.0              5000               5000

### random transaction_id samples (20 each table)

purchase:
- SmIInUwfNrBnM528fVRsAC4nURtqlAl7k
- SmNa0ZlO2Rs6UPBjKySLQ8CmHfGky3zwV
- SmIwzdJu2yRjVgI8m1YHdh8_Eiq7Xh1AV
- SmOAvXFIokBxMJFpJd7eOSDG1G1KXdR2k
- SmOnYG6Xm7kKqjGO46ZydhCIpoEyNcWBF
- SmEqjCKZSb3_UCAshmyYct-J5PUw9gS0F
- SmKGsouE0uV9G45vy2VrPC7PH3kut4l6k
- SmGWZrt9nORFOPdleV7H703U-mjKJS0IF
- SmDnIJFuaCoJSbsOVG5ZsG6Ue3botZAAV
- SmHkFntD37kKD6vNeub4Q0Bc_N4qsFNmk
- SmLZRD4pQJgQ3GoOF-Iyep4OHWnVhmSpk
- SmIepy5uyu3atUklJT-hdlEwfd1n_jq0k
- SmOWqI4mLyF9_JYt90jKedCym4AwILzs-
- SmMg6Qv1dUByeepcX4JBAW27u0-SfHb4F
- SmDywKIazxcvb84_K6EIOd5c95asiEwzF
- SmOrGcTU2ZNlIa6G8EdS844V3j70UG41F
- SmH2UZhDiCJhO8Cg9IhfA_6JKnmZ5IOI-
- SmOtovaabigfr2GyA-eQdt4KenemWD0-F
- SmHcxwAdV7kMP8BUR_9ctp5bDLTX4eTc-
- SmOAJKSFMCwMMriJMEdDuCAtY87ruS1KF
purchase_items:
- SmEjhc426C3LkWScI3RSfW3z0SNUWkUYF
- SmG9v8yEfO3By2chvF88fl4Jw1rZt0hoV
- SmFnq7O0iVJ6XJFDe-7P8tCzQ23AC9_MV
- SmFQW7UQV1sjJf3_zP7atG24pmrsELUtk
- SmEK5CNgyVJK8lrvw2TGAdBFBpO8PGkJV
- SmFcMQnANtZihz4mH7sTQ8ADPFfDjQ5sk
- SmEJEYJduAJtNoGrI-tg840W-WYqvhMX-
- SmFcqbg0gzFJUpRa8xP_dK0y2gqa0cISV
- SmEH0OS74R73mfIXrpqYc_AW3nTsL22Kk
- SmFLFAhW5m7iHfsYrgOFsl4INCs4nbvyk
- SmFMo5q9Y_74QmC4OxLRsWDvq84Yw53Ik
- SmFMo0EboHoR1ULQLilTtK6DyLBrazgmV
- SmFEp9LejvB25PMySbsDQK9yVaS8tgtpF
- SmF6y9rPQlVY_CMn8KK7O47WN75e6do9F
- SmEcX_gXGUJXI-GOwduYO_DXWuyyvfSiV
- SmFMnfgH5bow7nSbxOwcvSEif491Oz5Ok
- SmFT5ptKNRJwGihF5mNc9K8K1Tr3UGIzF
- SmFYSoQAaZR8AKVcusZJcW2ZPFMS60-NF
- SmFWN_LQY53l8fMR7NJzOl1xtYmdZzb6F
- SmE7Yd55flkkfpdgfpevf44kNa7vmndg-

## Brand: see-chan

- `transaction_id` normalized row coverage: 0.9839
- time range overlap: True
- decision: commerce_joinable=true

### transaction_id_norm pattern/length

brand_id          table  len_min  len_median  len_max                                                                   prefix_samples  share_alnum_dash_underscore  share_numeric_only  share_has_dash  share_has_underscore
see-chan       purchase      1.0        14.0    108.0 ["2601", "5824", "2602", "5821", "1072", "1080", "5823", "1071", "1079", "1065"]                          1.0            0.357858    4.911917e-05                   0.0
see-chan purchase_items      4.0        14.0     18.0 ["2601", "2512", "5821", "1059", "1076", "1068", "1067", "1061", "1069", "5819"]                          1.0            0.357169    5.045103e-07                   0.0

### sampled merge on transaction_id_norm

brand_id  sampled_merge_match_rate_norm_sample_vs_sample  sampled_merge_match_rate_norm_left_sample_vs_full_right  sample_size_left  sample_size_right
see-chan                                          0.0052                                                   0.9838              5000               5000

### random transaction_id samples (20 each table)

purchase:
- 948995044276329
- 580169762922859464
- 250107U9N7M269
- 581634649333597201
- 250303MFGT7KCY
- 2506203HCE3APN
- 579753810234083346
- 260107EA776XYC
- 581118757420893903
- 251124M2WVHSKB
- 260106BKYUGT5S
- 250901CA76SHUM
- 1005865828986316
- 250222RWECKW7M
- 25042568VJGBBK
- 582337111488955881
- 2512124WJ5G5RY
- 250126F9Q1NG8R
- 2505064Q925AH1
- 250105NH9WNQ74
purchase_items:
- 955382596734632
- 250917PD9DQQTM
- 580098795455875092
- 581437484121556771
- 251019FXXP5VJR
- 250210SE1F3GPY
- 250508B25H1PH8
- 581482883026421339
- 580219354768836489
- 578495310556923659
- 581430121057585076
- 250210SE5A3A6F
- 251023T02NUWVT
- 250429HM94KF37
- 241212M0N2Q32W
- 250510G0FURSQV
- 251010N8U5KAUM
- 974732657263864
- 2505174H2DUKGS
- 251010PRV3FDWC
