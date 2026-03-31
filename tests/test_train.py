import unittest

import src.train as train


class LegacyOneHotEncoder:
    def __init__(self, handle_unknown=None, sparse=None, sparse_output=None):
        if sparse_output is not None:
            raise TypeError("unexpected keyword argument 'sparse_output'")
        self.handle_unknown = handle_unknown
        self.sparse = sparse


class LegacyHistGradientBoostingClassifier:
    def __init__(self, class_weight=None, **kwargs):
        if class_weight is not None:
            raise TypeError("unexpected keyword argument 'class_weight'")
        self.kwargs = kwargs


class LegacyCalibratedClassifierCV:
    def __init__(self, estimator=None, base_estimator=None, method=None, cv=None):
        if estimator is not None:
            raise TypeError("unexpected keyword argument 'estimator'")
        self.base_estimator = base_estimator
        self.method = method
        self.cv = cv


class TrainCompatibilityTests(unittest.TestCase):
    def test_make_onehot_encoder_falls_back_for_legacy_sklearn(self):
        original = train.OneHotEncoder
        train.OneHotEncoder = LegacyOneHotEncoder
        try:
            encoder = train._make_onehot_encoder()
        finally:
            train.OneHotEncoder = original

        self.assertIsInstance(encoder, LegacyOneHotEncoder)
        self.assertEqual(encoder.handle_unknown, "ignore")
        self.assertFalse(encoder.sparse)

    def test_make_hgb_classifier_falls_back_for_legacy_sklearn(self):
        original = train.HistGradientBoostingClassifier
        train.HistGradientBoostingClassifier = LegacyHistGradientBoostingClassifier
        try:
            model = train._make_hgb_classifier(hgb_max_iter=220, class_weight="balanced")
        finally:
            train.HistGradientBoostingClassifier = original

        self.assertIsInstance(model, LegacyHistGradientBoostingClassifier)
        self.assertEqual(model.kwargs["max_iter"], 220)
        self.assertTrue(model.kwargs["early_stopping"])

    def test_make_calibrated_classifier_falls_back_for_legacy_sklearn(self):
        original = train.CalibratedClassifierCV
        train.CalibratedClassifierCV = LegacyCalibratedClassifierCV
        try:
            model = train._make_calibrated_classifier(estimator="dummy", method="sigmoid", cv=3)
        finally:
            train.CalibratedClassifierCV = original

        self.assertIsInstance(model, LegacyCalibratedClassifierCV)
        self.assertEqual(model.base_estimator, "dummy")
        self.assertEqual(model.method, "sigmoid")
        self.assertEqual(model.cv, 3)


if __name__ == "__main__":
    unittest.main()
