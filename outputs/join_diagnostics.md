# Join Diagnostics

Diagnostic normalized key columns used: `transaction_id_norm`, `user_id_norm`.

## Coverage Before/After Normalization

brand_id left_table    right_table            key  left_unique  right_unique  overlap_unique  row_coverage  left_unique_norm  right_unique_norm  overlap_unique_norm  row_coverage_norm
   c-vit   purchase purchase_items transaction_id      2741452       2741452         2741452      1.000000           2741452            2741452              2741452           1.000000
   c-vit   purchase purchase_items        user_id       118703        118703          118703      1.000000            118703             118703               118703           1.000000
see-chan   purchase purchase_items transaction_id      1399583       1378053         1378053      0.983918           1399566            1378053              1378053           0.983929
see-chan   purchase purchase_items        user_id        10359            19              19      0.002058             10359                 19                   19           0.002058

## Time Range Overlap Check

brand_id             purchase_overall_min             purchase_overall_max       purchase_items_overall_min       purchase_items_overall_max  time_range_overlap     purchase_create_datetime_min     purchase_create_datetime_max       purchase_paid_datetime_min       purchase_paid_datetime_max purchase_items_create_datetime_min purchase_items_create_datetime_max purchase_items_paid_datetime_min purchase_items_paid_datetime_max purchase_items_delivered_datetime_min purchase_items_delivered_datetime_max
   c-vit 2021-05-17 18:16:06.873000+00:00 2024-06-17 00:16:03.019000+00:00 2021-05-17 18:16:06.873000+00:00 2024-06-17 00:16:03.019000+00:00                True 2021-05-17 18:16:06.873000+00:00 2024-06-17 00:16:03.019000+00:00 2021-05-17 18:16:06.873000+00:00 2024-06-17 00:16:03.019000+00:00   2021-05-17 18:16:06.873000+00:00   2024-06-17 00:16:03.019000+00:00 2021-05-17 18:16:06.873000+00:00 2024-06-17 00:16:03.019000+00:00                                  None                                  None
see-chan        2023-02-23 00:00:00+00:00        2026-02-02 00:00:00+00:00        2023-04-06 00:00:00+00:00        2026-02-02 00:00:00+00:00                True        2023-02-23 00:00:00+00:00        2026-02-02 00:00:00+00:00        2023-02-23 00:00:00+00:00        2026-02-02 00:00:00+00:00          2023-04-06 00:00:00+00:00          2026-02-02 00:00:00+00:00        2023-04-06 00:00:00+00:00        2026-02-02 00:00:00+00:00                                  None                                  None

## Brand: c-vit

- `transaction_id` normalized row coverage: 1.0000
- time range overlap: True
- decision: commerce_joinable=true

### transaction_id_norm pattern/length

brand_id          table  len_min  len_median  len_max                                                                   prefix_samples  share_alnum_dash_underscore  share_numeric_only  share_has_dash  share_has_underscore
   c-vit       purchase     33.0        33.0     33.0 ["smkx", "sml9", "smlb", "smlz", "smma", "smmq", "smms", "smnb", "smnd", "smns"]                          1.0                 0.0        0.555121              0.367175
   c-vit purchase_items     33.0        33.0     33.0 ["sme1", "sme2", "sme3", "sme4", "sme5", "sme7", "smee", "smef", "smeh", "smei"]                          1.0                 0.0        0.555454              0.366958

### sampled merge on transaction_id_norm

brand_id  sampled_merge_match_rate_norm_sample_vs_sample  sampled_merge_match_rate_norm_left_sample_vs_full_right  sample_size_left  sample_size_right
   c-vit                                          0.0038                                                      1.0              5000               5000

### random transaction_id samples (20 each table)

purchase:
- SmJqdxvXZrsXJjZlam7R8S2NYDD3rt8dV
- SmE1Kql2ILsKSP_1m5I3tt8Vi5Rmqfgu-
- SmFD7SMhxsocxV4ZtMv9e89xqYG11Kyb-
- SmNUAGKYHX3xUx0hryFXfK20DUEPaQYZk
- SmNESuHy-TBEkDHtHm1Kfh2zu2SVpUEOk
- SmH67OEM587pEMV5RCoK74EkwfflcXIck
- SmFPIT0UpHccubYLcNEHPK2H2cbz0ytF-
- SmJhxpscseV4QniSvLJZ8xDo5f791VPK-
- SmK9gtumnaNHJsY7d--y9hDknk1cVCaPV
- SmDPzR0lRqR7HhaLRGot9lACMj3PDYV2k
- SmEdBQbRapsK5BpSMnbROdBGw-tFITHA-
- SmIZYY2W483XoElTj53ncO5kUbUdKfScF
- SmDTfOs7C-NIakZETd2QNW9ORUNmpfe7-
- SmDpkgyiEMRQTZiyxd1IAO2lT2C-RxjzV
- SmKOFKKjBcNMy54I7vdWAt6EYXPvSwJwF
- SmEW_eTBD5ZTvZVjnwveAC2FBkOm9jrIF
- SmIWtPrJTewx2VM6uSHufxBlsJQCjsPQk
- SmE3NzceHa-FRVesnxr37dEsDnGRXofN-
- SmOOJMwguLcpASLcT8nQ9C4cVIU65NS-k
- SmKftlCWezg5cPh-w-nMcG1GyovLTocq-
purchase_items:
- SmKPSNp1kpRQkSgugNQuOl60B8h6pZHC-
- SmJdwixs1lgpQL5sTbz7N8EpsOfwyRptF
- SmP-lc_xjgVaFfM69-2e8G4-jqKmA0wCF
- SmLQ7Tb6_QNfyTTWBO1SeCCXtcBpceLdk
- SmFiV9r-m-JMDJtSz3Sms4AXoUqBA8gDk
- SmHspXE6B1gyvSO3a7d2Ph1ryw7f5ZaGV
- SmH45bkD1_3XAeh3arfmsp1VU5LlI01A-
- SmHCWiLU5lNDxQbZTT889t3rjPIjXH6N-
- SmGVy80eA-sAT69Qzp22flAElFvNn1v8-
- SmDKQtgYZbRn-_zpQ_US8p1gwS6Yk_lGk
- SmHRNbmv36Bv6H2UsKN-uC55Irn8iGzU-
- SmHCN6LmRqFPPhG3bRjCs8BhBMR78s9pk
- SmE1xdvAqoghnIAuRCEetSBGsbirBO5tF
- SmD0j6_hrFBxqsAoCq0ofG0LcUJbdA1MV
- SmODA7cR-NNa9RX1JnDnf87Sxu24WwaAV
- SmHTwPSk33wTuwLH60j8Pp24XJINvqcPF
- SmDmNOWfrsBg6mkoQUBF7tDyw6Rr-51ek
- SmHSFlkHq-ohxX8IotoYet0K4lFkEBKY-
- SmD0iV-ybVo9SRutxDqAvt5Sx12kPYJLV
- SmLAXXoiIRBNLMxj6ImTOt9RnHCCGUzs-

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
