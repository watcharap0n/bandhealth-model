from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Dict, List


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.append(str(REPO_ROOT))

from src.mlops_runtime import read_json_manifest, resolve_manifest_asset_path, utc_now_iso  # noqa: E402


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Validate a Brand Health training snapshot manifest.")
    parser.add_argument("--snapshot-manifest", type=str, default=None)
    parser.add_argument("--snapshot-root", type=str, default=None)
    parser.add_argument("--output-json", type=str, required=True)
    return parser


def _resolve_manifest_input(snapshot_manifest: str | None, snapshot_root: str | None) -> Path:
    manifest_raw = str(snapshot_manifest or "").strip()
    if manifest_raw:
        return Path(manifest_raw)
    root_raw = str(snapshot_root or "").strip()
    if root_raw:
        return Path(root_raw) / "snapshot_manifest.json"
    raise ValueError("Either --snapshot-manifest or --snapshot-root is required")


def main() -> None:
    args = _build_parser().parse_args()
    manifest_path = _resolve_manifest_input(args.snapshot_manifest, args.snapshot_root)
    manifest = read_json_manifest(manifest_path)

    errors: List[Dict[str, object]] = []
    warnings: List[Dict[str, object]] = []

    if str(manifest.get("snapshot_kind", "")) != "training_snapshot":
        errors.append({"type": "snapshot_kind", "message": "expected snapshot_kind=training_snapshot"})

    exported_assets = manifest.get("exported_assets", [])
    if not isinstance(exported_assets, list) or not exported_assets:
        errors.append({"type": "exported_assets", "message": "manifest must include exported_assets"})
        exported_assets = []

    asset_names = {str(item.get("name", "")) for item in exported_assets if isinstance(item, dict)}
    for required_asset in ("labeled_feature_table", "feature_definitions", "data_validation_report"):
        if required_asset not in asset_names:
            errors.append({"type": "missing_asset", "asset": required_asset})

    for item in exported_assets:
        if not isinstance(item, dict):
            continue
        raw_path = str(item.get("path", "")).strip()
        if not raw_path:
            errors.append({"type": "asset_path", "asset": item.get("name"), "message": "missing path"})
            continue
        resolved = resolve_manifest_asset_path(manifest_path, raw_path)
        if not resolved.exists():
            errors.append({"type": "asset_missing", "asset": item.get("name"), "path": raw_path})

    validation_report = manifest.get("validation_report", {})
    if isinstance(validation_report, dict) and int(validation_report.get("warning_count", 0) or 0) > 0:
        warnings.append(
            {
                "type": "validation_warning_count",
                "warning_count": int(validation_report.get("warning_count", 0) or 0),
            }
        )

    payload = {
        "validated_at": utc_now_iso(),
        "snapshot_manifest": str(manifest_path),
        "status": "failed" if errors else "passed",
        "error_count": len(errors),
        "warning_count": len(warnings),
        "errors": errors,
        "warnings": warnings,
    }
    output_path = Path(args.output_json)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")

    if errors:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
