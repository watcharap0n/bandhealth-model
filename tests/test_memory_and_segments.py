from __future__ import annotations

import unittest

import pandas as pd

from src.memory_opt import optimize_dataframe_dtypes
from src.segments import _to_datetime as segments_to_datetime


class MemoryAndSegmentsTests(unittest.TestCase):
    def test_optimize_does_not_categorize_datetime_like_object_columns(self) -> None:
        df = pd.DataFrame(
            {
                "activity_datetime": pd.Series(
                    [
                        "2021-01-01 00:00:00",
                        "2021-01-01 00:00:00",
                        "2021-01-02 00:00:00",
                    ],
                    dtype="object",
                ),
                "user_type": pd.Series(
                    [
                        "member",
                        "member",
                        "guest",
                    ],
                    dtype="object",
                ),
            }
        )

        out, _ = optimize_dataframe_dtypes(df, table_name="activity_transaction", cat_ratio_threshold=0.9)

        self.assertNotEqual(str(out["activity_datetime"].dtype), "category")
        self.assertEqual(str(out["user_type"].dtype), "category")

    def test_segments_to_datetime_handles_categorical_input(self) -> None:
        series = pd.Series(pd.Categorical(["2021-01-01 00:00:00", "2021-01-03 00:00:00", None]))

        dt = segments_to_datetime(series)

        self.assertIsInstance(dt.dtype, pd.DatetimeTZDtype)
        self.assertEqual(str(dt.dropna().min()), "2021-01-01 00:00:00+00:00")
        self.assertEqual(str(dt.dropna().max()), "2021-01-03 00:00:00+00:00")


if __name__ == "__main__":
    unittest.main()
