from __future__ import annotations

import hashlib
import json
import mimetypes
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence
from urllib.parse import parse_qsl, quote, urlsplit, urlunsplit

import pandas as pd
import requests


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


def resolve_manifest_asset_path(manifest_path: str | Path, asset_path: str | Path) -> Path:
    raw_asset = str(asset_path).strip()
    if not raw_asset:
        raise ValueError("asset_path is required")
    asset = Path(raw_asset)
    if asset.is_absolute():
        return asset
    if raw_asset.startswith("dbfs:/") or raw_asset.startswith("file://"):
        return resolve_storage_path(raw_asset)
    manifest_fp = resolve_storage_path(manifest_path)
    return (manifest_fp.parent / asset).resolve()


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


def _blob_url_for_path(container_sas_url: str, blob_path: str) -> str:
    split = urlsplit(str(container_sas_url).strip())
    if split.scheme not in {"http", "https"}:
        raise ValueError("container_sas_url must be an http(s) URL")
    base_path = split.path.rstrip("/")
    blob_suffix = quote(str(blob_path).lstrip("/"), safe="/")
    combined_path = f"{base_path}/{blob_suffix}"
    return urlunsplit((split.scheme, split.netloc, combined_path, split.query, ""))


def upload_file_to_blob_sas(
    *,
    container_sas_url: str,
    local_path: str | Path,
    blob_path: str,
    content_type: Optional[str] = None,
    timeout_seconds: int = 300,
) -> Dict[str, Any]:
    src = resolve_storage_path(local_path)
    if not src.exists():
        raise FileNotFoundError(f"Upload source file does not exist: {src}")
    target_url = _blob_url_for_path(container_sas_url, blob_path)
    guessed_content_type = content_type or mimetypes.guess_type(str(src.name))[0] or "application/octet-stream"
    split = urlsplit(str(container_sas_url).strip())
    version = dict(parse_qsl(split.query)).get("sv", "2024-11-04")
    headers = {
        "x-ms-blob-type": "BlockBlob",
        "x-ms-version": version,
        "Content-Type": guessed_content_type,
        "Content-Length": str(int(src.stat().st_size)),
    }
    with src.open("rb") as fh:
        response = requests.put(target_url, headers=headers, data=fh, timeout=timeout_seconds)
    response.raise_for_status()
    return {
        "blob_path": str(blob_path),
        "url": target_url.split("?", 1)[0],
        "bytes": int(src.stat().st_size),
        "content_type": guessed_content_type,
    }


def upload_training_set_to_blob(
    *,
    container_sas_url: str,
    run_id: str,
    local_files: Sequence[Mapping[str, Any]],
    prefix: str = "training-set",
    manifest_name: str = "snapshot_manifest.json",
    base_manifest: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    normalized_prefix = str(prefix).strip().strip("/")
    if not normalized_prefix:
        raise ValueError("prefix is required")

    uploaded_assets: List[Dict[str, Any]] = []
    for item in local_files:
        source_path = resolve_storage_path(str(item.get("source_path", "")))
        dest_name = str(item.get("dest_name", source_path.name)).strip() or source_path.name
        blob_path = f"{normalized_prefix}/{dest_name}"
        uploaded = upload_file_to_blob_sas(
            container_sas_url=container_sas_url,
            local_path=source_path,
            blob_path=blob_path,
            content_type=str(item.get("content_type", "")).strip() or None,
        )
        asset_payload = {
            "name": str(item.get("name", dest_name)),
            "path": dest_name,
            "blob_path": uploaded["blob_path"],
            "blob_url": uploaded["url"],
            "bytes": uploaded["bytes"],
            "content_type": uploaded["content_type"],
        }
        for key in ("rows", "cols", "schema_hash"):
            if key in item and item.get(key) is not None:
                asset_payload[key] = item.get(key)
        uploaded_assets.append(asset_payload)

    manifest_payload = {
        "manifest_version": DEFAULT_MANIFEST_VERSION,
        "snapshot_kind": "training_snapshot",
        "run_id": str(run_id),
        "snapshot_date": utc_now_iso(),
        "storage_type": "azure_blob_sas",
        "prefix": normalized_prefix,
        "exported_assets": uploaded_assets,
    }
    if isinstance(base_manifest, Mapping):
        for key, value in base_manifest.items():
            if key in {"exported_assets", "manifest_blob_url"}:
                continue
            manifest_payload[key] = value
        manifest_payload["exported_assets"] = uploaded_assets
        manifest_payload["storage_type"] = "azure_blob_sas"
        manifest_payload["prefix"] = normalized_prefix

    tmp_dir = Path("/tmp") / "brand-health-upload-manifests"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    tmp_manifest = tmp_dir / f"{str(run_id).strip() or 'run'}-{manifest_name}"
    tmp_manifest.write_text(json.dumps(manifest_payload, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    uploaded_manifest = upload_file_to_blob_sas(
        container_sas_url=container_sas_url,
        local_path=tmp_manifest,
        blob_path=f"{normalized_prefix}/{manifest_name}",
        content_type="application/json",
    )
    manifest_payload["manifest_blob_url"] = uploaded_manifest["url"]

    return {
        "manifest": manifest_payload,
        "manifest_blob_url": uploaded_manifest["url"],
        "uploaded_assets": uploaded_assets,
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
