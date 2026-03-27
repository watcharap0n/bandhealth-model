from __future__ import annotations

import unittest

import run_pipeline


class RunPipelineMLflowTests(unittest.TestCase):
    def test_mlflow_disabled_by_default(self) -> None:
        parser = run_pipeline.build_arg_parser()
        args = parser.parse_args([])

        cfg = run_pipeline._resolve_mlflow_runtime(args)

        self.assertEqual(
            cfg,
            {
                "enabled": False,
                "experiment": None,
                "run_name": None,
                "log_outputs": True,
            },
        )

    def test_mlflow_enabled_requires_experiment(self) -> None:
        parser = run_pipeline.build_arg_parser()
        args = parser.parse_args(["--mlflow-enable", "true"])

        with self.assertRaises(ValueError) as ctx:
            run_pipeline._resolve_mlflow_runtime(args)

        self.assertIn("mlflow_experiment", str(ctx.exception))

    def test_mlflow_enabled_generates_default_run_name(self) -> None:
        parser = run_pipeline.build_arg_parser()
        args = parser.parse_args(
            [
                "--mlflow-enable",
                "true",
                "--mlflow-experiment",
                "/Shared/brand-health",
            ]
        )

        cfg = run_pipeline._resolve_mlflow_runtime(args)

        self.assertTrue(cfg["enabled"])
        self.assertEqual(cfg["experiment"], "/Shared/brand-health")
        self.assertTrue(str(cfg["run_name"]).startswith("brand-health-"))
        self.assertTrue(cfg["log_outputs"])

    def test_model_runtime_defaults_to_artifacts_and_databricks_registry(self) -> None:
        parser = run_pipeline.build_arg_parser()
        args = parser.parse_args([])

        cfg = run_pipeline._resolve_model_runtime(args)

        self.assertEqual(
            cfg,
            {
                "source": "artifacts",
                "mlflow_model_uri": "",
                "mlflow_registry_uri": "databricks",
                "model_release_manifest": "",
            },
        )

    def test_skip_train_mlflow_requires_model_uri(self) -> None:
        parser = run_pipeline.build_arg_parser()
        args = parser.parse_args(["--skip-train", "--model-source", "mlflow"])

        with self.assertRaises(ValueError) as ctx:
            run_pipeline._resolve_model_runtime(args)

        self.assertIn("mlflow_model_uri", str(ctx.exception))

    def test_skip_train_mlflow_accepts_model_uri(self) -> None:
        parser = run_pipeline.build_arg_parser()
        args = parser.parse_args(
            [
                "--skip-train",
                "--model-source",
                "mlflow",
                "--mlflow-model-uri",
                "runs:/abc123/model",
            ]
        )

        cfg = run_pipeline._resolve_model_runtime(args)

        self.assertEqual(cfg["source"], "mlflow")
        self.assertEqual(cfg["mlflow_model_uri"], "runs:/abc123/model")
        self.assertEqual(cfg["mlflow_registry_uri"], "databricks")

    def test_skip_train_artifact_bundle_requires_release_manifest(self) -> None:
        parser = run_pipeline.build_arg_parser()
        args = parser.parse_args(["--skip-train", "--model-source", "artifact_bundle"])

        with self.assertRaises(ValueError) as ctx:
            run_pipeline._resolve_model_runtime(args)

        self.assertIn("model_release_manifest", str(ctx.exception))

    def test_publish_runtime_defaults_to_merge(self) -> None:
        parser = run_pipeline.build_arg_parser()
        args = parser.parse_args([])

        cfg = run_pipeline._resolve_publish_runtime(args, source_mode="parquet")

        self.assertEqual(cfg["write_mode"], "merge")

    def test_azure_blob_runtime_defaults_disabled(self) -> None:
        parser = run_pipeline.build_arg_parser()
        args = parser.parse_args([])

        cfg = run_pipeline._resolve_azure_blob_runtime(args)

        self.assertEqual(
            cfg,
            {
                "enabled": False,
                "sas_url": "",
                "training_prefix": "training-set",
            },
        )


if __name__ == "__main__":
    unittest.main()
