from __future__ import annotations

import unittest

import numpy as np

from src.infer import _coerce_attr_values


class InferHelpersTests(unittest.TestCase):
    def test_coerce_attr_values_handles_numpy_arrays(self) -> None:
        values = np.array(["f1", "f2", "f3"])
        self.assertEqual(_coerce_attr_values(values), ["f1", "f2", "f3"])

    def test_coerce_attr_values_handles_none(self) -> None:
        self.assertEqual(_coerce_attr_values(None), [])


if __name__ == "__main__":
    unittest.main()
