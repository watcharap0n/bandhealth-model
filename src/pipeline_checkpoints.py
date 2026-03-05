from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Mapping, Optional, Sequence, Tuple

import pandas as pd

from .data_load import DataProfile, JoinDiagnostics
from .memory_opt import write_parquet_chunked


class CheckpointManager:
    """Run-scoped checkpoint storage for hop-based pipeline execution."""

    def __init__(self, checkpoint_root: str | Path, run_id: str) -> None:
        root = Path(checkpoint_root)
        self.checkpoint_root = root
        self.run_id = str(run_id).strip()
        if not self.run_id:
            raise ValueError("run_id is required")
        self.run_dir = root / self.run_id

    def ensure_base_dirs(self) -> None:
        (self.run_dir / "status").mkdir(parents=True, exist_ok=True)

    def stage_dir(self, stage: str) -> Path:
        return self.run_dir / str(stage)

    def status_path(self, stage: str) -> Path:
        return self.run_dir / "status" / f"{stage}.json"

    def read_stage_status(self, stage: str) -> Optional[Dict[str, object]]:
        path = self.status_path(stage)
        if not path.exists():
            return None
        return json.loads(path.read_text(encoding="utf-8"))

    def stage_completed(self, stage: str) -> bool:
        status = self.read_stage_status(stage)
        if not status:
            return False
        return bool(status.get("completed_at"))

    def write_stage_status(
        self,
        stage: str,
        *,
        arg_fingerprint: str,
        started_at: str,
        completed_at: Optional[str],
        outputs: Sequence[str],
        details: Optional[Mapping[str, object]] = None,
        error: str = "",
    ) -> Path:
        path = self.status_path(stage)
        path.parent.mkdir(parents=True, exist_ok=True)
        payload: Dict[str, object] = {
            "stage": str(stage),
            "arg_fingerprint": str(arg_fingerprint),
            "started_at": str(started_at),
            "completed_at": str(completed_at) if completed_at else None,
            "updated_at": utc_now_iso(),
            "outputs": [str(x) for x in outputs],
            "error": str(error or ""),
        }
        if details is not None:
            payload["details"] = dict(details)
        path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
        return path

    def write_dataframe(self, stage: str, name: str, df: pd.DataFrame) -> Path:
        path = self.stage_dir(stage) / str(name)
        path.parent.mkdir(parents=True, exist_ok=True)
        write_parquet_chunked(df, path, chunk_rows=100_000)
        return path

    def read_dataframe(self, stage: str, name: str) -> pd.DataFrame:
        path = self.stage_dir(stage) / str(name)
        if not path.exists():
            return pd.DataFrame()
        return pd.read_parquet(path)

    def write_json(self, stage: str, name: str, payload: Mapping[str, object] | List[object]) -> Path:
        path = self.stage_dir(stage) / str(name)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
        return path

    def read_json(self, stage: str, name: str, default: Optional[object] = None):
        path = self.stage_dir(stage) / str(name)
        if not path.exists():
            return default
        return json.loads(path.read_text(encoding="utf-8"))

    def save_tables(self, tables: Mapping[str, pd.DataFrame]) -> List[Path]:
        tables_dir = self.stage_dir("load_tables") / "tables"
        tables_dir.mkdir(parents=True, exist_ok=True)

        paths: List[Path] = []
        manifest: List[Dict[str, object]] = []
        for table_name in sorted(tables.keys()):
            value = tables.get(table_name)
            if not isinstance(value, pd.DataFrame):
                continue
            path = tables_dir / f"{table_name}.parquet"
            write_parquet_chunked(value, path, chunk_rows=100_000)
            paths.append(path)
            manifest.append(
                {
                    "table": str(table_name),
                    "path": str(path.relative_to(self.run_dir)),
                    "rows": int(len(value)),
                    "cols": int(len(value.columns)),
                }
            )

        manifest_path = self.write_json("load_tables", "tables_manifest.json", manifest)
        paths.append(manifest_path)
        return paths

    def load_tables(self) -> Dict[str, pd.DataFrame]:
        manifest = self.read_json("load_tables", "tables_manifest.json", default=[])
        out: Dict[str, pd.DataFrame] = {}

        if isinstance(manifest, list) and manifest:
            for item in manifest:
                if not isinstance(item, Mapping):
                    continue
                table_name = str(item.get("table", "")).strip()
                rel_path = str(item.get("path", "")).strip()
                if not table_name or not rel_path:
                    continue
                path = self.run_dir / rel_path
                if not path.exists():
                    raise FileNotFoundError(f"Missing checkpoint parquet for table '{table_name}': {path}")
                out[table_name] = pd.read_parquet(path)
            return out

        tables_dir = self.stage_dir("load_tables") / "tables"
        if not tables_dir.exists():
            return out
        for fp in sorted(tables_dir.glob("*.parquet")):
            out[fp.stem] = pd.read_parquet(fp)
        return out

    def save_join_diagnostics(self, diagnostics: JoinDiagnostics) -> List[Path]:
        paths = [
            self.write_dataframe("join_diagnostics", "coverage_summary.parquet", diagnostics.coverage_summary),
            self.write_dataframe("join_diagnostics", "time_range_summary.parquet", diagnostics.time_range_summary),
            self.write_dataframe("join_diagnostics", "pattern_summary.parquet", diagnostics.pattern_summary),
            self.write_dataframe("join_diagnostics", "sampled_merge_summary.parquet", diagnostics.sampled_merge_summary),
            self.write_json("join_diagnostics", "random_samples.json", diagnostics.random_samples),
            self.write_json("join_diagnostics", "commerce_joinable.json", diagnostics.commerce_joinable),
        ]
        return paths

    def load_join_diagnostics(self) -> JoinDiagnostics:
        random_samples = self.read_json("join_diagnostics", "random_samples.json", default={}) or {}
        commerce_joinable = self.read_json("join_diagnostics", "commerce_joinable.json", default={}) or {}
        return JoinDiagnostics(
            coverage_summary=self.read_dataframe("join_diagnostics", "coverage_summary.parquet"),
            time_range_summary=self.read_dataframe("join_diagnostics", "time_range_summary.parquet"),
            pattern_summary=self.read_dataframe("join_diagnostics", "pattern_summary.parquet"),
            sampled_merge_summary=self.read_dataframe("join_diagnostics", "sampled_merge_summary.parquet"),
            random_samples=dict(random_samples) if isinstance(random_samples, Mapping) else {},
            commerce_joinable=dict(commerce_joinable) if isinstance(commerce_joinable, Mapping) else {},
        )

    def save_profile(
        self,
        profile: DataProfile,
        activity_enrichment_joinable: Mapping[str, bool],
    ) -> List[Path]:
        paths = [
            self.write_dataframe("profile", "table_profile.parquet", profile.table_profile),
            self.write_dataframe("profile", "schema_profile.parquet", profile.schema_profile),
            self.write_dataframe("profile", "join_coverage.parquet", profile.join_coverage),
            self.write_json("profile", "activity_enrichment_joinable.json", dict(activity_enrichment_joinable)),
        ]
        return paths

    def load_profile(self) -> Tuple[DataProfile, Dict[str, bool]]:
        activity_map = self.read_json("profile", "activity_enrichment_joinable.json", default={}) or {}
        profile = DataProfile(
            table_profile=self.read_dataframe("profile", "table_profile.parquet"),
            schema_profile=self.read_dataframe("profile", "schema_profile.parquet"),
            join_coverage=self.read_dataframe("profile", "join_coverage.parquet"),
        )
        activity_enrichment = {
            str(k): bool(v)
            for k, v in dict(activity_map).items()
        } if isinstance(activity_map, Mapping) else {}
        return profile, activity_enrichment

    def sampled_labeled_path(self) -> Path:
        return self.stage_dir("train") / "sampled_labeled_feature_table.parquet"

    def save_sampled_labeled_df(self, sampled_df: pd.DataFrame) -> Path:
        return self.write_dataframe("train", "sampled_labeled_feature_table.parquet", sampled_df)

    def load_sampled_labeled_df(self) -> pd.DataFrame:
        path = self.sampled_labeled_path()
        if not path.exists():
            return pd.DataFrame()
        return pd.read_parquet(path)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_arg_fingerprint(payload: Mapping[str, object]) -> str:
    canonical = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
