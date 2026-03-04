from __future__ import annotations

import unittest

import pandas as pd

from src.train import _safe_calibration_cv


class TrainCalibrationTests(unittest.TestCase):
    def test_safe_calibration_cv_drops_to_two_when_min_class_count_is_two(self) -> None:
        y = pd.Series(["Healthy", "Healthy", "Warning", "Warning", "AtRisk", "AtRisk"])
        self.assertEqual(_safe_calibration_cv(y, preferred_cv=3), 2)

    def test_safe_calibration_cv_skips_when_any_class_has_only_one_row(self) -> None:
        y = pd.Series(["Healthy", "Healthy", "Warning", "AtRisk"])
        self.assertEqual(_safe_calibration_cv(y, preferred_cv=3), 0)

    def test_safe_calibration_cv_skips_when_only_one_class_exists(self) -> None:
        y = pd.Series(["Healthy", "Healthy", "Healthy"])
        self.assertEqual(_safe_calibration_cv(y, preferred_cv=3), 0)


if __name__ == "__main__":
    unittest.main()
