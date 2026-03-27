from __future__ import annotations

import argparse
from pathlib import Path


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Submit the Brand Health weekly retrain pipeline to Azure ML.")
    parser.add_argument("--subscription-id", type=str, required=True)
    parser.add_argument("--resource-group", type=str, required=True)
    parser.add_argument("--workspace-name", type=str, required=True)
    parser.add_argument("--compute", type=str, required=True, help="Azure ML compute cluster name.")
    parser.add_argument("--environment", type=str, required=True, help="Registered Azure ML environment name:version.")
    parser.add_argument("--training-snapshot-manifest", type=str, required=True)
    parser.add_argument("--model-bundle-root", type=str, required=True)
    parser.add_argument("--production-manifest-out", type=str, required=True)
    parser.add_argument("--experiment-name", type=str, default="brand-health-weekly-retrain")
    parser.add_argument("--min-macro-f1", type=float, default=0.55)
    parser.add_argument("--min-balanced-accuracy", type=float, default=0.55)
    return parser


def main() -> None:
    try:
        from azure.identity import DefaultAzureCredential
        from azure.ai.ml import MLClient, Input, command, dsl
    except ImportError as exc:
        raise RuntimeError(
            "Azure ML dependencies are not installed. Install azure-ai-ml and azure-identity to use this scaffold."
        ) from exc

    args = _build_parser().parse_args()
    repo_root = Path(__file__).resolve().parents[1]

    ml_client = MLClient(
        DefaultAzureCredential(),
        subscription_id=args.subscription_id,
        resource_group_name=args.resource_group,
        workspace_name=args.workspace_name,
    )

    validate_component = command(
        name="brand_health_validate_snapshot",
        display_name="Validate Brand Health Snapshot",
        code=str(repo_root),
        command=(
            "python azureml/components/validate_snapshot.py "
            "--snapshot-manifest ${{inputs.snapshot_manifest}} "
            "--output-json ${{outputs.validation_json}}"
        ),
        inputs={
            "snapshot_manifest": Input(type="uri_file"),
        },
        outputs={"validation_json": {"type": "uri_file"}},
        environment=args.environment,
        compute=args.compute,
    )

    train_component = command(
        name="brand_health_train_from_snapshot",
        display_name="Train Brand Health From Snapshot",
        code=str(repo_root),
        command=(
            "python azureml/components/train_from_snapshot.py "
            "--snapshot-manifest ${{inputs.snapshot_manifest}} "
            "--artifact-dir ${{outputs.artifact_dir}} "
            "--model-bundle-root ${{inputs.model_bundle_root}} "
            "--candidate-manifest-out ${{outputs.candidate_manifest}} "
            "--output-json ${{outputs.train_json}}"
        ),
        inputs={
            "snapshot_manifest": Input(type="uri_file"),
            "model_bundle_root": Input(type="uri_folder"),
        },
        outputs={
            "artifact_dir": {"type": "uri_folder"},
            "candidate_manifest": {"type": "uri_file"},
            "train_json": {"type": "uri_file"},
        },
        environment=args.environment,
        compute=args.compute,
    )

    promote_component = command(
        name="brand_health_promote_model",
        display_name="Promote Brand Health Candidate",
        code=str(repo_root),
        command=(
            "python azureml/components/promote_model.py "
            "--candidate-manifest ${{inputs.candidate_manifest}} "
            "--production-manifest-out ${{outputs.production_manifest}} "
            "--min-macro-f1 ${{inputs.min_macro_f1}} "
            "--min-balanced-accuracy ${{inputs.min_balanced_accuracy}} "
            "--output-json ${{outputs.promote_json}}"
        ),
        inputs={
            "candidate_manifest": Input(type="uri_file"),
            "min_macro_f1": args.min_macro_f1,
            "min_balanced_accuracy": args.min_balanced_accuracy,
        },
        outputs={
            "promote_json": {"type": "uri_file"},
            "production_manifest": {"type": "uri_file"},
        },
        environment=args.environment,
        compute=args.compute,
    )

    @dsl.pipeline(
        compute=args.compute,
        description="Weekly retrain, evaluate, and promote pipeline for Brand Health.",
    )
    def weekly_retrain_pipeline(snapshot_manifest: str, model_bundle_root: str):
        validate_job = validate_component(snapshot_manifest=snapshot_manifest)
        train_job = train_component(
            snapshot_manifest=snapshot_manifest,
            model_bundle_root=model_bundle_root,
        )
        train_job.outputs.artifact_dir.mode = "rw_mount"
        train_job.outputs.candidate_manifest.mode = "rw_mount"
        promote_job = promote_component(
            candidate_manifest=train_job.outputs.candidate_manifest,
        )
        promote_job.outputs.promote_json.mode = "rw_mount"
        promote_job.outputs.production_manifest.mode = "rw_mount"
        validate_job.outputs.validation_json.mode = "rw_mount"
        return {
            "validation_json": validate_job.outputs.validation_json,
            "train_json": train_job.outputs.train_json,
            "promote_json": promote_job.outputs.promote_json,
            "production_manifest": promote_job.outputs.production_manifest,
        }

    pipeline_job = weekly_retrain_pipeline(
        snapshot_manifest=args.training_snapshot_manifest,
        model_bundle_root=args.model_bundle_root,
    )
    pipeline_job.experiment_name = args.experiment_name
    pipeline_job.outputs.production_manifest.path = args.production_manifest_out

    created = ml_client.jobs.create_or_update(pipeline_job)
    print(f"Submitted Azure ML pipeline job: {created.name}")


if __name__ == "__main__":
    main()
