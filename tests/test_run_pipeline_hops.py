from __future__ import annotations

import argparse
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

import run_pipeline
import run_pipeline_hops


class RunPipelineHopsTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.tmp_path = Path(self._tmp.name)

    def _make_runtime(
        self,
        *,
        hop: str,
        run_id: str = "run-1",
        auto_upstream: bool = False,
        force: bool = False,
        pipeline_overrides: list[str] | None = None,
    ) -> run_pipeline_hops.HopRuntime:
        hop_args = argparse.Namespace(
            hop=hop,
            run_id=run_id,
            checkpoint_root=str(self.tmp_path / "checkpoints"),
            auto_upstream=bool(auto_upstream),
            force=bool(force),
        )

        pipeline_argv = [
            "--dataset-root",
            str(self.tmp_path / "datasets"),
            "--reports-dir",
            str(self.tmp_path / "reports"),
            "--outputs-dir",
            str(self.tmp_path / "outputs"),
            "--artifacts-dir",
            str(self.tmp_path / "artifacts"),
        ]
        if pipeline_overrides:
            pipeline_argv.extend(pipeline_overrides)

        pipeline_args = run_pipeline.build_arg_parser().parse_args(pipeline_argv)
        return run_pipeline_hops._build_runtime(hop_args, pipeline_args)

    def _noop_handlers(self):
        def _noop(_: run_pipeline_hops.HopRuntime):
            return []

        return {stage: _noop for stage in run_pipeline_hops.HOP_ORDER}

    def test_parse_args_supports_subcommand_and_pipeline_flags(self) -> None:
        hop_args, pipeline_args = run_pipeline_hops._parse_args(
            [
                "infer",
                "--run-id",
                "abc123",
                "--auto-upstream",
                "--dataset-root",
                "datasets-custom",
                "--skip-train",
            ]
        )

        self.assertEqual(hop_args.hop, "infer")
        self.assertEqual(hop_args.run_id, "abc123")
        self.assertTrue(hop_args.auto_upstream)
        self.assertEqual(pipeline_args.dataset_root, "datasets-custom")
        self.assertTrue(pipeline_args.skip_train)

    def test_strict_mode_fails_when_dependencies_missing(self) -> None:
        runtime = self._make_runtime(hop="infer", auto_upstream=False)
        runner = run_pipeline_hops.HopRunner(runtime, stage_handlers=self._noop_handlers())

        with self.assertRaises(RuntimeError) as ctx:
            runner.run("infer")

        msg = str(ctx.exception)
        self.assertIn("Missing upstream hops", msg)
        self.assertIn("labels", msg)
        self.assertIn("segments", msg)

    def test_auto_upstream_executes_dependencies_in_order(self) -> None:
        runtime = self._make_runtime(hop="infer", auto_upstream=True)
        executed: list[str] = []

        def make_handler(stage: str):
            def _handler(_: run_pipeline_hops.HopRuntime):
                executed.append(stage)
                return []

            return _handler

        handlers = {stage: make_handler(stage) for stage in run_pipeline_hops.HOP_ORDER}
        runner = run_pipeline_hops.HopRunner(runtime, stage_handlers=handlers)
        runner.run("infer")

        self.assertEqual(
            executed,
            [
                "load_tables",
                "join_diagnostics",
                "profile",
                "features",
                "segments",
                "labels",
                "infer",
            ],
        )

    def test_stage_marker_created_and_resume_skips_completed_stage(self) -> None:
        runtime = self._make_runtime(hop="load_tables", auto_upstream=False)
        call_count = {"load_tables": 0}

        handlers = self._noop_handlers()

        def _load_handler(rt: run_pipeline_hops.HopRuntime):
            call_count["load_tables"] += 1
            marker_path = rt.checkpoint.stage_dir("load_tables") / "dummy.txt"
            marker_path.parent.mkdir(parents=True, exist_ok=True)
            marker_path.write_text("ok", encoding="utf-8")
            return [str(marker_path)]

        handlers["load_tables"] = _load_handler

        runner = run_pipeline_hops.HopRunner(runtime, stage_handlers=handlers)
        runner.run("load_tables")
        runner.run("load_tables")

        self.assertEqual(call_count["load_tables"], 1)

        status = runtime.checkpoint.read_stage_status("load_tables")
        assert status is not None
        self.assertTrue(status.get("started_at"))
        self.assertTrue(status.get("completed_at"))
        self.assertIsInstance(status.get("outputs"), list)

    def test_force_reruns_target_only(self) -> None:
        counts = {"load_tables": 0, "join_diagnostics": 0, "features": 0, "labels": 0}

        def make_handler(stage: str):
            def _handler(_: run_pipeline_hops.HopRuntime):
                if stage in counts:
                    counts[stage] += 1
                return []

            return _handler

        handlers = {stage: make_handler(stage) for stage in run_pipeline_hops.HOP_ORDER}

        runtime1 = self._make_runtime(hop="labels", run_id="force-run", auto_upstream=True, force=False)
        run_pipeline_hops.HopRunner(runtime1, stage_handlers=handlers).run("labels")

        runtime2 = self._make_runtime(hop="labels", run_id="force-run", auto_upstream=True, force=True)
        run_pipeline_hops.HopRunner(runtime2, stage_handlers=handlers).run("labels")

        self.assertEqual(counts["load_tables"], 1)
        self.assertEqual(counts["join_diagnostics"], 1)
        self.assertEqual(counts["features"], 1)
        self.assertEqual(counts["labels"], 2)

    def test_arg_fingerprint_mismatch_fails_on_same_run_id(self) -> None:
        handlers = self._noop_handlers()

        runtime1 = self._make_runtime(hop="load_tables", run_id="fp-run", auto_upstream=False)
        run_pipeline_hops.HopRunner(runtime1, stage_handlers=handlers).run("load_tables")

        runtime2 = self._make_runtime(
            hop="load_tables",
            run_id="fp-run",
            auto_upstream=False,
            pipeline_overrides=["--snapshot-freq", "30D"],
        )

        with self.assertRaises(ValueError) as ctx:
            run_pipeline_hops.HopRunner(runtime2, stage_handlers=handlers).run("load_tables")

        self.assertIn("incompatible args", str(ctx.exception))

    def test_train_failure_then_rerun_train_without_rerunning_load_tables(self) -> None:
        counts = {"load_tables": 0, "train": 0}
        fail_once = {"value": True}

        def make_handler(stage: str):
            def _handler(_: run_pipeline_hops.HopRuntime):
                if stage == "load_tables":
                    counts["load_tables"] += 1
                if stage == "train":
                    counts["train"] += 1
                    if fail_once["value"]:
                        fail_once["value"] = False
                        raise RuntimeError("train failed")
                return []

            return _handler

        handlers = {stage: make_handler(stage) for stage in run_pipeline_hops.HOP_ORDER}
        runtime = self._make_runtime(hop="train", run_id="train-rerun", auto_upstream=True)
        runner = run_pipeline_hops.HopRunner(runtime, stage_handlers=handlers)

        with self.assertRaises(RuntimeError):
            runner.run("train")

        runner.run("train")

        self.assertEqual(counts["load_tables"], 1)
        self.assertEqual(counts["train"], 2)

    def test_infer_prefers_sampled_labeled_checkpoint_when_sample_mode_on(self) -> None:
        runtime = self._make_runtime(
            hop="infer",
            run_id="sampled-infer",
            auto_upstream=False,
            pipeline_overrides=["--train_sample_mode", "quick"],
        )

        labeled_df = pd.DataFrame(
            {
                "brand_id": ["a", "a", "b"],
                "window_end_date": ["2025-01-01", "2025-01-08", "2025-01-15"],
                "window_size": ["30d", "30d", "30d"],
                "predicted_health_class": ["Healthy", "Warning", "AtRisk"],
                "__row_id": [0, 1, 2],
            }
        )
        (runtime.outputs_dir).mkdir(parents=True, exist_ok=True)
        labeled_path = runtime.outputs_dir / "labeled_feature_table.parquet"
        labeled_df.to_parquet(labeled_path, index=False)

        sampled_df = labeled_df.head(1).copy()
        runtime.checkpoint.save_sampled_labeled_df(sampled_df)

        (runtime.outputs_dir / "sample_qa_report.json").write_text(
            json.dumps({"qa_report": {"representative_pass": True}}, indent=2),
            encoding="utf-8",
        )

        with patch("run_pipeline_hops.build_train_eval_samples", side_effect=AssertionError("should not be called")):
            infer_df, payload = run_pipeline_hops._load_infer_input_df(runtime, sample_mode="quick")

        self.assertEqual(len(infer_df), 1)
        self.assertIn("qa_report", payload)

    def test_publish_failure_then_rerun_publish_only(self) -> None:
        counts = {"infer": 0, "publish": 0}
        fail_once = {"value": True}

        def make_handler(stage: str):
            def _handler(_: run_pipeline_hops.HopRuntime):
                if stage == "infer":
                    counts["infer"] += 1
                if stage == "publish":
                    counts["publish"] += 1
                    if fail_once["value"]:
                        fail_once["value"] = False
                        raise RuntimeError("publish failed")
                return []

            return _handler

        handlers = {stage: make_handler(stage) for stage in run_pipeline_hops.HOP_ORDER}
        runtime = self._make_runtime(hop="publish", run_id="publish-rerun", auto_upstream=True)
        runner = run_pipeline_hops.HopRunner(runtime, stage_handlers=handlers)

        with self.assertRaises(RuntimeError):
            runner.run("publish")

        runner.run("publish")

        self.assertEqual(counts["infer"], 1)
        self.assertEqual(counts["publish"], 2)


if __name__ == "__main__":
    unittest.main()
