from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence

import pandas as pd


DEFAULT_MANIFEST_VERSION = "1.0"
SNAPSHOT_KINDS = {"training_snapshot", "scoring_snapshot"}
MODEL_ARTIFACT_FILES = (
    "brand_health_model.joblib",
    "model_metadata.json",
    "feature_importance.json",
)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def resolve_storage_path(path_or_uri: str | Path) -> Path:
    raw = str(path_or_uri).strip()
    if not raw:
        raise ValueError("path_or_uri is required")
    if raw.startswith("dbfs:/"):
        suffix = raw.replace("dbfs:/", "", 1).lstrip("/")
        return Path("/dbfs") / suffix
    if raw.startswith("file://"):
        return Path(raw.replace("file://", "", 1))
    return Path(raw)


def normalize_app_id_value(value: object) -> str:
    raw = str(value or "").strip()
    if not raw or raw.lower() == "nan":
        return ""
    try:
        return str(int(float(raw)))
    except (TypeError, ValueError):
        pass
    if raw.endswith(".0"):
        return raw[:-2]
    return raw


def file_sha256(path: str | Path) -> str:
    fp = resolve_storage_path(path)
    digest = hashlib.sha256()
    with fp.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def dataframe_schema_hash(df: pd.DataFrame) -> str:
    payload = [
        {
            "name": str(col),
            "dtype": str(df[col].dtype),
        }
        for col in df.columns
    ]
    canonical = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def current_code_version(cwd: str | Path | None = None) -> str:
    workdir = str(cwd) if cwd is not None else None
    try:
        proc = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=workdir,
            check=True,
            capture_output=True,
            text=True,
        )
    except Exception:
        return "unknown"
    return str(proc.stdout).strip() or "unknown"


def read_json_manifest(path: str | Path) -> Dict[str, Any]:
    manifest_path = resolve_storage_path(path)
    return json.loads(manifest_path.read_text(encoding="utf-8"))


def find_latest_snapshot_manifest(
    snapshot_root: str | Path,
    snapshot_kind: str,
    *,
    exclude_run_id: Optional[str] = None,
) -> Optional[Path]:
    kind = str(snapshot_kind).strip()
    if kind not in SNAPSHOT_KINDS:
        raise ValueError(f"Unsupported snapshot_kind: {snapshot_kind}")

    root = resolve_storage_path(snapshot_root) / kind
    if not root.exists():
        return None

    candidates: List[Path] = []
    for manifest_path in root.glob("*/snapshot_manifest.json"):
        if exclude_run_id and manifest_path.parent.name == str(exclude_run_id):
            continue
        candidates.append(manifest_path)
    if not candidates:
        return None
    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0]


def summarize_tables_for_contracts(
    tables: Mapping[str, pd.DataFrame],
    columns_map: Mapping[str, Sequence[str]],
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for table_name in sorted(tables.keys()):
        df = tables.get(table_name)
        if not isinstance(df, pd.DataFrame):
            continue
        required_cols = [str(col) for col in columns_map.get(table_name, ())]
        missing_required = [col for col in required_cols if col not in df.columns]
        null_rates = {
            str(col): float(df[col].isna().mean())
            for col in required_cols
            if col in df.columns
        }
        distinct_app_ids = []
        if "app_id" in df.columns:
            seen: set[str] = set()
            for value in df["app_id"].dropna().tolist():
                key = normalize_app_id_value(value)
                if not key or key in seen:
                    continue
                seen.add(key)
                distinct_app_ids.append(key)
                if len(distinct_app_ids) >= 20:
                    break

        out.append(
            {
                "table": str(table_name),
                "rows": int(len(df)),
                "cols": int(len(df.columns)),
                "required_columns": required_cols,
                "missing_required_columns": missing_required,
                "schema_hash": dataframe_schema_hash(df),
                "null_rates": null_rates,
                "app_id_sample": distinct_app_ids,
                "distinct_app_id_count": int(df["app_id"].nunique(dropna=True)) if "app_id" in df.columns else 0,
            }
        )
    return out


def build_data_validation_report(
    *,
    tables: Mapping[str, pd.DataFrame],
    columns_map: Mapping[str, Sequence[str]],
    runtime_brand_app_filters: Mapping[str, Sequence[str]],
    null_rate_threshold: float,
    row_count_delta_threshold: float,
    previous_snapshot_manifest: Optional[Mapping[str, Any]] = None,
    join_coverage: Optional[pd.DataFrame] = None,
    commerce_joinable: Optional[Mapping[str, bool]] = None,
    commerce_join_threshold: float = 0.80,
    activity_enrichment_joinable: Optional[Mapping[str, bool]] = None,
    activity_join_threshold: float = 0.80,
) -> Dict[str, Any]:
    table_contracts = summarize_tables_for_contracts(tables=tables, columns_map=columns_map)
    allowed_app_ids = {
        normalize_app_id_value(app_id)
        for app_ids in runtime_brand_app_filters.values()
        for app_id in app_ids
        if normalize_app_id_value(app_id)
    }

    errors: List[Dict[str, Any]] = []
    warnings: List[Dict[str, Any]] = []

    previous_rows: Dict[str, int] = {}
    if isinstance(previous_snapshot_manifest, Mapping):
        for item in previous_snapshot_manifest.get("source_tables", []):
            if not isinstance(item, Mapping):
                continue
            key = str(item.get("table", "")).strip()
            if not key:
                continue
            try:
                previous_rows[key] = int(item.get("rows", 0))
            except (TypeError, ValueError):
                continue

    for summary in table_contracts:
        table_name = str(summary.get("table", ""))
        missing_required = list(summary.get("missing_required_columns", []))
        if missing_required:
            errors.append(
                {
                    "type": "missing_required_columns",
                    "table": table_name,
                    "columns": missing_required,
                }
            )

        null_rates = dict(summary.get("null_rates", {}))
        high_null_columns = [
            {
                "column": str(col),
                "null_rate": float(rate),
            }
            for col, rate in null_rates.items()
            if float(rate) > float(null_rate_threshold)
        ]
        if high_null_columns:
            warnings.append(
                {
                    "type": "high_null_rate",
                    "table": table_name,
                    "columns": high_null_columns,
                }
            )

        current_rows = int(summary.get("rows", 0))
        prev_rows = previous_rows.get(table_name)
        if prev_rows is not None:
            drift_ratio = abs(current_rows - prev_rows) / max(prev_rows, 1)
            if drift_ratio > float(row_count_delta_threshold):
                warnings.append(
                    {
                        "type": "row_count_drift",
                        "table": table_name,
                        "previous_rows": int(prev_rows),
                        "current_rows": int(current_rows),
                        "delta_ratio": float(drift_ratio),
                    }
                )

        df = tables.get(table_name)
        if isinstance(df, pd.DataFrame) and "app_id" in df.columns and allowed_app_ids:
            violations: List[str] = []
            seen: set[str] = set()
            for value in df["app_id"].dropna().tolist():
                key = normalize_app_id_value(value)
                if not key or key in allowed_app_ids or key in seen:
                    continue
                seen.add(key)
                violations.append(key)
                if len(violations) >= 10:
                    break
            if violations:
                errors.append(
                    {
                        "type": "app_id_whitelist_violation",
                        "table": table_name,
                        "invalid_app_ids": violations,
                    }
                )

    if isinstance(commerce_joinable, Mapping):
        for brand_id, is_joinable in sorted(commerce_joinable.items()):
            if bool(is_joinable):
                continue
            warnings.append(
                {
                    "type": "commerce_joinability_shortfall",
                    "brand_id": str(brand_id),
                    "threshold": float(commerce_join_threshold),
                }
            )

    if isinstance(activity_enrichment_joinable, Mapping):
        for brand_id, is_joinable in sorted(activity_enrichment_joinable.items()):
            if bool(is_joinable):
                continue
            warnings.append(
                {
                    "type": "activity_joinability_shortfall",
                    "brand_id": str(brand_id),
                    "threshold": float(activity_join_threshold),
                }
            )

    joinability_metrics: List[Dict[str, Any]] = []
    if isinstance(join_coverage, pd.DataFrame) and not join_coverage.empty:
        p2i = join_coverage[
            (join_coverage["left_table"] == "purchase")
            & (join_coverage["right_table"] == "purchase_items")
            & (join_coverage["key"] == "transaction_id")
        ]
        for row in p2i.itertuples():
            cov = getattr(row, "row_coverage_norm", None)
            if cov is None or pd.isna(cov):
                cov = getattr(row, "row_coverage", None)
            joinability_metrics.append(
                {
                    "brand_id": str(getattr(row, "brand_id")),
                    "purchase_items_transaction_id_coverage": float(cov) if cov is not None and not pd.isna(cov) else None,
                }
            )

    return {
        "status": "failed" if errors else "passed",
        "error_count": int(len(errors)),
        "warning_count": int(len(warnings)),
        "thresholds": {
            "null_rate_threshold": float(null_rate_threshold),
            "row_count_delta_threshold": float(row_count_delta_threshold),
            "commerce_join_threshold": float(commerce_join_threshold),
            "activity_join_threshold": float(activity_join_threshold),
        },
        "table_contracts": table_contracts,
        "joinability_metrics": joinability_metrics,
        "errors": errors,
        "warnings": warnings,
    }


def assert_validation_report(report: Mapping[str, Any], *, fail_on_warning: bool = False) -> None:
    error_count = int(report.get("error_count", 0) or 0)
    warning_count = int(report.get("warning_count", 0) or 0)
    if error_count > 0:
        raise ValueError(
            f"Data validation failed with {error_count} error(s). "
            f"See validation report for details."
        )
    if fail_on_warning and warning_count > 0:
        raise ValueError(
            f"Data validation failed because warnings are treated as fatal ({warning_count} warning(s))."
        )


def copy_assets_to_snapshot(
    *,
    snapshot_root: str | Path,
    snapshot_kind: str,
    run_id: str,
    files: Sequence[Mapping[str, Any]],
    manifest_payload: Mapping[str, Any],
) -> Dict[str, Any]:
    kind = str(snapshot_kind).strip()
    if kind not in SNAPSHOT_KINDS:
        raise ValueError(f"Unsupported snapshot_kind: {snapshot_kind}")
    snapshot_dir = resolve_storage_path(snapshot_root) / kind / str(run_id)
    snapshot_dir.mkdir(parents=True, exist_ok=True)

    exported_assets: List[Dict[str, Any]] = []
    for item in files:
        src = resolve_storage_path(str(item.get("source_path", "")))
        name = str(item.get("name", src.name)).strip() or src.name
        dest_name = str(item.get("dest_name", src.name)).strip() or src.name
        dest = snapshot_dir / dest_name
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dest)
        asset_payload = {
            "name": name,
            "path": str(dest),
            "sha256": file_sha256(dest),
            "bytes": int(dest.stat().st_size),
        }
        for key in ("rows", "cols", "schema_hash", "content_type"):
            if key in item and item.get(key) is not None:
                asset_payload[key] = item.get(key)
        exported_assets.append(asset_payload)

    manifest = dict(manifest_payload)
    manifest["manifest_version"] = str(manifest.get("manifest_version") or DEFAULT_MANIFEST_VERSION)
    manifest["snapshot_kind"] = kind
    manifest["run_id"] = str(run_id)
    manifest["exported_assets"] = exported_assets
    manifest_path = snapshot_dir / "snapshot_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False, default=str), encoding="utf-8")

    latest_alias = snapshot_dir.parent / "latest.json"
    latest_alias.write_text(json.dumps(manifest, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    return {
        "snapshot_dir": snapshot_dir,
        "manifest": manifest,
        "manifest_path": manifest_path,
        "latest_alias_path": latest_alias,
    }


def package_model_artifact_bundle(
    *,
    artifact_dir: str | Path,
    bundle_root: str | Path,
    model_version: str,
    training_snapshot_uri: str,
    feature_columns: Sequence[str],
    class_labels: Sequence[str],
    metrics: Mapping[str, Any],
    selected_model: str,
    status: str = "candidate",
    approved_at: Optional[str] = None,
    code_version: str = "unknown",
    extra_tags: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    src_dir = resolve_storage_path(artifact_dir)
    root = resolve_storage_path(bundle_root)
    bundle_dir = root / str(model_version)
    bundle_dir.mkdir(parents=True, exist_ok=True)

    copied_files: List[Dict[str, Any]] = []
    for artifact_name in MODEL_ARTIFACT_FILES:
        src = src_dir / artifact_name
        if not src.exists():
            raise FileNotFoundError(f"Missing model artifact for bundle packaging: {src}")
        dest = bundle_dir / artifact_name
        shutil.copy2(src, dest)
        copied_files.append(
            {
                "name": artifact_name,
                "path": str(dest),
                "sha256": file_sha256(dest),
                "bytes": int(dest.stat().st_size),
            }
        )

    manifest = {
        "manifest_version": DEFAULT_MANIFEST_VERSION,
        "model_version": str(model_version),
        "artifact_uri": str(bundle_dir),
        "training_snapshot_uri": str(training_snapshot_uri),
        "feature_columns": [str(col) for col in feature_columns],
        "class_labels": [str(label) for label in class_labels],
        "metrics": dict(metrics),
        "selected_model": str(selected_model),
        "status": str(status),
        "created_at": utc_now_iso(),
        "approved_at": str(approved_at) if approved_at else (utc_now_iso() if str(status) == "approved" else None),
        "code_version": str(code_version or "unknown"),
        "artifacts": copied_files,
        "tags": dict(extra_tags or {}),
    }
    manifest_path = bundle_dir / "model_release_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False, default=str), encoding="utf-8")

    latest_candidate = root / "latest_candidate.json"
    latest_candidate.parent.mkdir(parents=True, exist_ok=True)
    latest_candidate.write_text(json.dumps(manifest, indent=2, ensure_ascii=False, default=str), encoding="utf-8")

    production_manifest_path = None
    if str(status) == "approved":
        production_manifest_path = root / "production_manifest.json"
        production_manifest_path.write_text(
            json.dumps(manifest, indent=2, ensure_ascii=False, default=str),
            encoding="utf-8",
        )

    return {
        "bundle_dir": bundle_dir,
        "manifest": manifest,
        "manifest_path": manifest_path,
        "latest_candidate_path": latest_candidate,
        "production_manifest_path": production_manifest_path,
    }
