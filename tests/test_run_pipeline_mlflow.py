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


if __name__ == "__main__":
    unittest.main()
