from __future__ import annotations

import unittest

import pandas as pd

from src.features import _build_window_dates, _to_datetime


class FeatureDatetimeTests(unittest.TestCase):
    def test_to_datetime_handles_categorical_input(self) -> None:
        series = pd.Series(
            pd.Categorical(
                ["2021-01-01 00:00:00", "2021-01-03 00:00:00", None]
            )
        )

        dt = _to_datetime(series)

        self.assertIsInstance(dt.dtype, pd.DatetimeTZDtype)
        self.assertEqual(str(dt.dropna().min()), "2021-01-01 00:00:00+00:00")
        self.assertEqual(str(dt.dropna().max()), "2021-01-03 00:00:00+00:00")

    def test_build_window_dates_handles_categorical_datetime_columns(self) -> None:
        activity = pd.DataFrame(
            {
                "brand_id": ["c-vit", "c-vit"],
                "activity_datetime": pd.Categorical(
                    ["2021-01-01 10:00:00", "2021-01-20 12:00:00"]
                ),
            }
        )

        windows = _build_window_dates(
            brand_tables={"activity_transaction": activity},
            window_sizes=(7, 30),
            freq="7D",
        )

        self.assertGreater(len(windows), 0)
        self.assertEqual(str(windows[-1]), "2021-01-15 00:00:00+00:00")


if __name__ == "__main__":
    unittest.main()
