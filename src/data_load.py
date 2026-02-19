from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

TABLE_FILES: Tuple[str, ...] = (
    "activity_transaction.parquet",
    "purchase.parquet",
    "purchase_items.parquet",
    "user_device.parquet",
    "user_identity.parquet",
    "user_info.parquet",
    "user_view.parquet",
    "user_visitor.parquet",
)

DEFAULT_KEY_RELATIONS: Tuple[Tuple[str, str, str], ...] = (
    ("purchase", "purchase_items", "transaction_id"),
    ("purchase", "purchase_items", "user_id"),
    ("purchase", "user_identity", "user_id"),
    ("purchase", "user_info", "user_id"),
    ("activity_transaction", "user_identity", "user_id"),
    ("activity_transaction", "user_view", "user_id"),
    ("activity_transaction", "user_visitor", "user_id"),
    ("user_view", "user_visitor", "user_id"),
)


@dataclass
class DataProfile:
    table_profile: pd.DataFrame
    schema_profile: pd.DataFrame
    join_coverage: pd.DataFrame


# -----------------------------
# Loading helpers
# -----------------------------

def list_brands(dataset_root: str | Path) -> List[str]:
    root = Path(dataset_root)
    return sorted([p.name for p in root.iterdir() if p.is_dir()])


def _available_columns(parquet_path: str | Path) -> List[str]:
    schema = pq.read_schema(parquet_path)
    return list(schema.names)


def safe_read_parquet(
    parquet_path: str | Path,
    columns: Optional[Sequence[str]] = None,
    add_missing_requested_columns: bool = True,
) -> pd.DataFrame:
    parquet_path = Path(parquet_path)
    available = _available_columns(parquet_path)

    selected = None
    missing_requested: List[str] = []
    if columns is not None:
        selected = [c for c in columns if c in available]
        missing_requested = [c for c in columns if c not in available]

    df = pd.read_parquet(parquet_path, columns=selected)

    if add_missing_requested_columns and missing_requested:
        for col in missing_requested:
            df[col] = np.nan

    return df


def _filter_brand_app_id(
    df: pd.DataFrame,
    brand_id: str,
    brand_app_ids: Optional[Mapping[str, Sequence[int | str]]] = None,
) -> pd.DataFrame:
    if not brand_app_ids or df.empty or "app_id" not in df.columns:
        return df

    allowed = brand_app_ids.get(brand_id)
    if not allowed:
        return df

    allowed_ids = {int(x) for x in allowed}
    app_numeric = pd.to_numeric(df["app_id"], errors="coerce").astype("Int64")
    return df.loc[app_numeric.isin(list(allowed_ids))].copy()


def load_tables(
    dataset_root: str | Path,
    table_files: Sequence[str] = TABLE_FILES,
    columns_map: Optional[Mapping[str, Sequence[str]]] = None,
    brand_app_ids: Optional[Mapping[str, Sequence[int | str]]] = None,
) -> Dict[str, pd.DataFrame]:
    """Load all tables across brands; add brand_id to each row.

    Returns a dict keyed by table stem (e.g., "purchase") with brand-concatenated frames.
    """
    root = Path(dataset_root)
    brands = list_brands(root)

    out: Dict[str, List[pd.DataFrame]] = {}
    for file_name in table_files:
        table_name = Path(file_name).stem
        out[table_name] = []
        for brand_id in brands:
            fp = root / brand_id / file_name
            req_cols = None if columns_map is None else columns_map.get(table_name)
            df = safe_read_parquet(fp, columns=req_cols)
            df = _filter_brand_app_id(df, brand_id=brand_id, brand_app_ids=brand_app_ids)
            df["brand_id"] = brand_id
            out[table_name].append(df)

    merged: Dict[str, pd.DataFrame] = {}
    for k, frames in out.items():
        if not frames:
            merged[k] = pd.DataFrame()
            continue
        non_empty = [f for f in frames if not f.empty]
        if non_empty:
            merged[k] = pd.concat(non_empty, ignore_index=True)
        else:
            merged[k] = frames[0].iloc[0:0].copy()
    return merged


# -----------------------------
# Profiling
# -----------------------------

def _detect_datetime_columns(df: pd.DataFrame) -> List[str]:
    dt_cols: List[str] = []
    for col in df.columns:
        name = col.lower()
        if any(tok in name for tok in ("date", "time", "datetime", "access")):
            dt_cols.append(col)
    return dt_cols


def profile_dataset(
    dataset_root: str | Path,
    table_files: Sequence[str] = TABLE_FILES,
    key_relations: Sequence[Tuple[str, str, str]] = DEFAULT_KEY_RELATIONS,
    brand_app_ids: Optional[Mapping[str, Sequence[int | str]]] = None,
) -> DataProfile:
    root = Path(dataset_root)
    brands = list_brands(root)

    table_rows: List[dict] = []
    schema_rows: List[dict] = []

    for brand_id in brands:
        for file_name in table_files:
            table_name = Path(file_name).stem
            fp = root / brand_id / file_name
            df = safe_read_parquet(fp)
            df = _filter_brand_app_id(df, brand_id=brand_id, brand_app_ids=brand_app_ids)

            table_rows.append(
                {
                    "brand_id": brand_id,
                    "table": table_name,
                    "rows": int(len(df)),
                    "columns": int(df.shape[1]),
                }
            )

            dt_cols = set(_detect_datetime_columns(df))
            for col in df.columns:
                s = df[col]
                missing_count = int(s.isna().sum())
                row = {
                    "brand_id": brand_id,
                    "table": table_name,
                    "column": col,
                    "dtype": str(s.dtype),
                    "missing_count": missing_count,
                    "missing_pct": float(missing_count / len(df)) if len(df) else 0.0,
                }

                if col in dt_cols:
                    dt = pd.to_datetime(s, errors="coerce", utc=True)
                    row["time_min"] = dt.min()
                    row["time_max"] = dt.max()
                else:
                    row["time_min"] = pd.NaT
                    row["time_max"] = pd.NaT

                schema_rows.append(row)

    table_profile = pd.DataFrame(table_rows).sort_values(["brand_id", "table"]).reset_index(drop=True)
    schema_profile = pd.DataFrame(schema_rows).sort_values(["brand_id", "table", "column"]).reset_index(drop=True)
    join_coverage = validate_join_coverage(
        root,
        table_files=table_files,
        key_relations=key_relations,
        brand_app_ids=brand_app_ids,
    )

    return DataProfile(table_profile=table_profile, schema_profile=schema_profile, join_coverage=join_coverage)


def validate_join_coverage(
    dataset_root: str | Path,
    table_files: Sequence[str] = TABLE_FILES,
    key_relations: Sequence[Tuple[str, str, str]] = DEFAULT_KEY_RELATIONS,
    brand_app_ids: Optional[Mapping[str, Sequence[int | str]]] = None,
) -> pd.DataFrame:
    root = Path(dataset_root)
    brands = list_brands(root)

    table_name_to_file = {Path(x).stem: x for x in table_files}
    rows: List[dict] = []

    for brand_id in brands:
        key_frames: Dict[str, pd.DataFrame] = {}

        needed: Dict[str, set] = {}
        for left, right, key in key_relations:
            needed.setdefault(left, set()).add(key)
            needed.setdefault(right, set()).add(key)

        for table_name, cols in needed.items():
            if table_name not in table_name_to_file:
                continue
            fp = root / brand_id / table_name_to_file[table_name]
            read_cols = sorted(set(cols).union({"app_id"}))
            df = safe_read_parquet(fp, columns=read_cols)
            df = _filter_brand_app_id(df, brand_id=brand_id, brand_app_ids=brand_app_ids)
            key_frames[table_name] = df

        for left_table, right_table, key in key_relations:
            left_df = key_frames.get(left_table, pd.DataFrame())
            right_df = key_frames.get(right_table, pd.DataFrame())

            if key not in left_df.columns or key not in right_df.columns:
                rows.append(
                    {
                        "brand_id": brand_id,
                        "left_table": left_table,
                        "right_table": right_table,
                        "key": key,
                        "left_rows": len(left_df),
                        "right_rows": len(right_df),
                        "left_unique": 0,
                        "right_unique": 0,
                        "overlap_unique": 0,
                        "row_coverage": np.nan,
                    }
                )
                continue

            left_key = left_df[key].dropna().astype(str)
            right_key = right_df[key].dropna().astype(str)

            right_unique = set(right_key.unique())
            left_unique_vals = set(left_key.unique())
            overlap_unique = len(left_unique_vals.intersection(right_unique))

            coverage = float(left_key.isin(right_unique).mean()) if len(left_key) else np.nan
            rows.append(
                {
                    "brand_id": brand_id,
                    "left_table": left_table,
                    "right_table": right_table,
                    "key": key,
                    "left_rows": int(len(left_df)),
                    "right_rows": int(len(right_df)),
                    "left_unique": int(len(left_unique_vals)),
                    "right_unique": int(len(right_unique)),
                    "overlap_unique": int(overlap_unique),
                    "row_coverage": coverage,
                }
            )

    return pd.DataFrame(rows).sort_values(["brand_id", "left_table", "right_table", "key"]).reset_index(drop=True)


def save_profile(profile: DataProfile, output_dir: str | Path) -> None:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    profile.table_profile.to_csv(out / "table_profile.csv", index=False)
    profile.schema_profile.to_csv(out / "schema_profile.csv", index=False)
    profile.join_coverage.to_csv(out / "join_coverage.csv", index=False)

    # Lightweight summary JSON for quick inspection.
    summary = {
        "tables": profile.table_profile.to_dict(orient="records"),
        "join_coverage": profile.join_coverage.to_dict(orient="records"),
    }
    with (out / "profile_summary.json").open("w", encoding="utf-8") as f:
        json.dump(summary, f, default=str, indent=2)


def summarize_join_coverage(join_df: pd.DataFrame) -> str:
    if join_df.empty:
        return "No join coverage data available."

    lines: List[str] = []
    for brand_id, sub in join_df.groupby("brand_id"):
        lines.append(f"Brand {brand_id}:")
        for _, row in sub.iterrows():
            cov = row.get("row_coverage", np.nan)
            cov_text = "NA" if pd.isna(cov) else f"{cov:.2%}"
            lines.append(
                "  "
                f"{row['left_table']}.{row['key']} -> {row['right_table']}.{row['key']} "
                f"coverage={cov_text} overlap_unique={int(row['overlap_unique'])}"
            )
    return "\n".join(lines)
