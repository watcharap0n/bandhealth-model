from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import pandas as pd

from src.data_load import load_tables


class DataLoadTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.dataset_root = Path(self._tmp.name) / "datasets"
        self.dataset_root.mkdir(parents=True, exist_ok=True)

    def test_load_tables_skips_zero_byte_parquet_and_keeps_cvit_rows_from_other_subsets(self) -> None:
        (self.dataset_root / "c-vit").mkdir()
        (self.dataset_root / "c-vit" / "purchase.parquet").write_bytes(b"")

        source_subset = self.dataset_root / "all"
        source_subset.mkdir()
        pd.DataFrame(
            [
                {"app_id": 1993744540760190, "transaction_id": "tx-1", "user_id": "u-1"},
                {"app_id": 838315041537793, "transaction_id": "tx-2", "user_id": "u-2"},
            ]
        ).to_parquet(source_subset / "purchase.parquet", index=False)

        tables = load_tables(
            dataset_root=self.dataset_root,
            table_files=("purchase.parquet",),
            columns_map={"purchase": ["app_id", "transaction_id", "user_id"]},
            brand_app_ids={"c-vit": [1993744540760190]},
        )

        purchase = tables["purchase"]
        self.assertEqual(len(purchase), 1)
        self.assertEqual(purchase["brand_id"].tolist(), ["c-vit"])
        self.assertEqual(purchase["transaction_id"].tolist(), ["tx-1"])


if __name__ == "__main__":
    unittest.main()

