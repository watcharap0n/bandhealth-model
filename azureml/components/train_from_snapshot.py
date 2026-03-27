from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Mapping

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.append(str(REPO_ROOT))

from src.mlops_runtime import (  # noqa: E402
    package_model_artifact_bundle,
    read_json_manifest,
    resolve_manifest_asset_path,
    resolve_storage_path,
    utc_now_iso,
)
from src.train import train_models  # noqa: E402


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Train Brand Health model from a curated snapshot.")
    parser.add_argument("--snapshot-manifest", type=str, required=True)
    parser.add_argument("--artifact-dir", type=str, required=True)
    parser.add_argument("--model-bundle-root", type=str, required=True)
    parser.add_argument("--sample-mode", type=str, default="off", choices=["off", "quick", "smart"])
    parser.add_argument("--n-jobs", type=int, default=4)
    parser.add_argument("--group-col", type=str, default="brand_id")
    parser.add_argument("--weight-classes", type=str, default="true")
    parser.add_argument("--candidate-manifest-out", type=str, required=True)
    parser.add_argument("--output-json", type=str, required=True)
    return parser


def _find_asset(manifest: Mapping[str, object], asset_name: str) -> Path:
    manifest_path = str(manifest.get("__manifest_path__", "")).strip()
    for item in manifest.get("exported_assets", []):
        if not isinstance(item, Mapping):
            continue
        if str(item.get("name", "")) != asset_name:
            continue
        raw_path = str(item.get("path", "")).strip()
        if raw_path:
            if manifest_path:
                return resolve_manifest_asset_path(manifest_path, raw_path)
            return resolve_storage_path(raw_path)
    raise FileNotFoundError(f"Snapshot asset not found: {asset_name}")


def _parse_bool_flag(raw: object) -> bool:
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


def main() -> None:
    args = _build_parser().parse_args()
    manifest = read_json_manifest(args.snapshot_manifest)
    manifest["__manifest_path__"] = str(args.snapshot_manifest)

    labeled_path = _find_asset(manifest, "labeled_feature_table")
    labeled_df = pd.read_parquet(labeled_path)

    artifact_dir = resolve_storage_path(args.artifact_dir)
    artifact_dir.mkdir(parents=True, exist_ok=True)

    artifacts = train_models(
        labeled_df,
        artifact_dir=artifact_dir,
        sample_mode=str(args.sample_mode).strip().lower(),
        n_jobs=int(args.n_jobs),
        weight_classes=_parse_bool_flag(args.weight_classes),
        group_col=str(args.group_col),
        quick_top_k_features=80 if str(args.sample_mode).strip().lower() == "quick" else None,
    )

    model_version = f"aml-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}"
    bundle = package_model_artifact_bundle(
        artifact_dir=artifact_dir,
        bundle_root=args.model_bundle_root,
        model_version=model_version,
        training_snapshot_uri=str(resolve_storage_path(args.snapshot_manifest)),
        feature_columns=artifacts.feature_columns,
        class_labels=artifacts.class_labels,
        metrics=artifacts.metrics,
        selected_model=str(artifacts.metrics.get("selected_model", "")),
        status="candidate",
        code_version=manifest.get("code_version", "unknown"),
        extra_tags={"origin": "azureml_training_component"},
    )

    candidate_manifest_out = Path(args.candidate_manifest_out)
    candidate_manifest_out.parent.mkdir(parents=True, exist_ok=True)
    candidate_manifest_out.write_text(
        json.dumps(bundle["manifest"], indent=2, ensure_ascii=False, default=str),
        encoding="utf-8",
    )

    payload = {
        "trained_at": utc_now_iso(),
        "snapshot_manifest": str(args.snapshot_manifest),
        "artifact_dir": str(artifact_dir),
        "model_version": model_version,
        "selected_model": str(artifacts.metrics.get("selected_model", "")),
        "candidate_manifest_path": str(bundle["manifest_path"]),
        "candidate_manifest_out": str(candidate_manifest_out),
        "artifact_uri": str(bundle["manifest"].get("artifact_uri", "")),
        "metrics": artifacts.metrics,
        "feature_count": len(artifacts.feature_columns),
    }
    output_path = Path(args.output_json)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


if __name__ == "__main__":
    main()
