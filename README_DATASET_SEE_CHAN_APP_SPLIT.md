# Dataset Split README: `datasets/see-chan` (by `app_id`)

Generated at: `2026-02-20 09:50:27`

## App ID Mapping

- `c-vit` -> `1993744540760190`
- `see-chan` -> `838315041537793`

## Scope

- This report includes **all parquet files** under `datasets/see-chan/`.
- Split is computed using the `app_id` column in each table.

## Table-Level Split

| table | total_rows | c-vit_rows | c-vit_pct | see-chan_rows | see-chan_pct | other_or_null_rows | unique_app_ids |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| activity_transaction | 20,937,218 | 20,758,624 | 99.1470% | 178,594 | 0.8530% | 0 | 2 |
| purchase | 4,146,211 | 2,741,452 | 66.1195% | 1,404,759 | 33.8805% | 0 | 2 |
| purchase_items | 15,270,148 | 13,288,028 | 87.0196% | 1,982,120 | 12.9804% | 0 | 2 |
| user_device | 105,411 | 25,427 | 24.1218% | 79,984 | 75.8782% | 0 | 2 |
| user_identity | 342,690 | 201,470 | 58.7907% | 141,220 | 41.2093% | 0 | 2 |
| user_info | 342,690 | 201,470 | 58.7907% | 141,220 | 41.2093% | 0 | 2 |
| user_view | 400,239 | 226,745 | 56.6524% | 173,494 | 43.3476% | 0 | 2 |
| user_visitor | 5,409,254 | 4,962,930 | 91.7489% | 446,324 | 8.2511% | 0 | 2 |

## Overall Split (sum across all tables)

| total_rows | c-vit_rows | c-vit_pct | see-chan_rows | see-chan_pct | other_or_null_rows |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 46,953,861 | 42,406,146 | 90.3145% | 4,547,715 | 9.6855% | 0 |

## Notes

- `other_or_null_rows = 0` means every row is mapped to one of the two known app_ids.
- Large c-vit volume inside `datasets/see-chan` is expected and is now handled by app_id-based splitting.