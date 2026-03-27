from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import joblib
import pandas as pd

from src.mlops_runtime import (
    build_data_validation_report,
    package_model_artifact_bundle,
    resolve_storage_path,
)


class MlopsRuntimeTests(unittest.TestCase):
    def test_resolve_storage_path_supports_dbfs_uri(self) -> None:
        path = resolve_storage_path("dbfs:/tmp/brand-health/model")
        self.assertEqual(path, Path("/dbfs/tmp/brand-health/model"))

    def test_build_data_validation_report_flags_missing_columns_and_app_id_violation(self) -> None:
        tables = {
            "activity_transaction": pd.DataFrame(
                [
                    {
                        "app_id": "999",
                        "user_id": "u1",
                        "transaction_id": "t1",
                    }
                ]
            )
        }
        columns_map = {
            "activity_transaction": ["app_id", "user_id", "transaction_id", "activity_datetime"],
        }

        report = build_data_validation_report(
            tables=tables,
            columns_map=columns_map,
            runtime_brand_app_filters={"brand-a": ["123"]},
            null_rate_threshold=0.95,
            row_count_delta_threshold=0.50,
            previous_snapshot_manifest={
                "source_tables": [
                    {
                        "table": "activity_transaction",
                        "rows": 10,
                    }
                ]
            },
            commerce_joinable={"brand-a": False},
            activity_enrichment_joinable={"brand-a": False},
        )

        self.assertEqual(report["status"], "failed")
        error_types = {item["type"] for item in report["errors"]}
        self.assertIn("missing_required_columns", error_types)
        self.assertIn("app_id_whitelist_violation", error_types)
        warning_types = {item["type"] for item in report["warnings"]}
        self.assertIn("row_count_drift", warning_types)
        self.assertIn("commerce_joinability_shortfall", warning_types)
        self.assertIn("activity_joinability_shortfall", warning_types)

    def test_package_model_artifact_bundle_copies_files_and_writes_aliases(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            artifact_dir = tmp_path / "artifacts"
            artifact_dir.mkdir(parents=True, exist_ok=True)

            joblib.dump({"kind": "dummy-model"}, artifact_dir / "brand_health_model.joblib")
            (artifact_dir / "model_metadata.json").write_text(
                json.dumps({"feature_columns": ["f1"], "class_labels": ["Healthy"]}),
                encoding="utf-8",
            )
            (artifact_dir / "feature_importance.json").write_text(
                json.dumps({"f1": 0.9}),
                encoding="utf-8",
            )

            bundle = package_model_artifact_bundle(
                artifact_dir=artifact_dir,
                bundle_root=tmp_path / "registry",
                model_version="20260327-1",
                training_snapshot_uri="/tmp/training_snapshot.json",
                feature_columns=["f1"],
                class_labels=["Healthy"],
                metrics={"selected_model": "hgb"},
                selected_model="hgb",
                status="approved",
                code_version="abc123",
            )

            manifest = bundle["manifest"]
            self.assertEqual(manifest["model_version"], "20260327-1")
            self.assertEqual(manifest["status"], "approved")
            self.assertTrue((tmp_path / "registry" / "latest_candidate.json").exists())
            self.assertTrue((tmp_path / "registry" / "production_manifest.json").exists())
            self.assertTrue((tmp_path / "registry" / "20260327-1" / "brand_health_model.joblib").exists())


if __name__ == "__main__":
    unittest.main()
