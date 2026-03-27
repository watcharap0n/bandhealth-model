from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import joblib
import numpy as np

from src.infer import _coerce_attr_values, load_model_from_release_manifest


class InferHelpersTests(unittest.TestCase):
    def test_coerce_attr_values_handles_numpy_arrays(self) -> None:
        values = np.array(["f1", "f2", "f3"])
        self.assertEqual(_coerce_attr_values(values), ["f1", "f2", "f3"])

    def test_coerce_attr_values_handles_none(self) -> None:
        self.assertEqual(_coerce_attr_values(None), [])

    def test_load_model_from_release_manifest_uses_bundle_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            bundle_dir = Path(tmp) / "bundle-1"
            bundle_dir.mkdir(parents=True, exist_ok=True)

            joblib.dump({"kind": "dummy-model"}, bundle_dir / "brand_health_model.joblib")
            (bundle_dir / "model_metadata.json").write_text(
                json.dumps({"feature_columns": ["f1", "f2"], "class_labels": ["Healthy", "Warning"]}),
                encoding="utf-8",
            )
            (bundle_dir / "feature_importance.json").write_text(
                json.dumps({"f1": 0.5, "f2": 0.2}),
                encoding="utf-8",
            )

            manifest_path = Path(tmp) / "model_release_manifest.json"
            manifest_path.write_text(
                json.dumps(
                    {
                        "artifact_uri": str(bundle_dir),
                        "feature_columns": ["f1", "f2"],
                        "class_labels": ["Healthy", "Warning"],
                    }
                ),
                encoding="utf-8",
            )

            loaded = load_model_from_release_manifest(manifest_path)

        self.assertEqual(loaded["model"]["kind"], "dummy-model")
        self.assertEqual(loaded["feature_columns"], ["f1", "f2"])
        self.assertEqual(loaded["class_labels"], ["Healthy", "Warning"])
        self.assertEqual(loaded["feature_importance"]["f1"], 0.5)


if __name__ == "__main__":
    unittest.main()
