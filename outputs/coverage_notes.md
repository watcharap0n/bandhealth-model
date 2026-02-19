# Coverage Notes

## c-vit

- Canonical join key for `purchase ↔ purchase_items` is `transaction_id`.
- transaction_id normalized coverage: 0.0000 (invalid)
- user_id normalized coverage: 0.0000 (unreliable for this relationship; ignored for joins)

## see-chan

- Canonical join key for `purchase ↔ purchase_items` is `transaction_id`.
- transaction_id normalized coverage: 0.9839 (valid)
- user_id normalized coverage: 0.0021 (unreliable for this relationship; ignored for joins)
- purchase↔items join by transaction_id is valid for see-chan.
- purchase_items.user_id is unreliable and must be ignored for joins.
