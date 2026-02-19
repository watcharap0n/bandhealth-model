from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from .id_utils import normalize_id

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


@dataclass
class JoinDiagnostics:
    coverage_summary: pd.DataFrame
    time_range_summary: pd.DataFrame
    pattern_summary: pd.DataFrame
    sampled_merge_summary: pd.DataFrame
    random_samples: Dict[str, Dict[str, List[str]]]
    commerce_joinable: Dict[str, bool]


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
                        "left_unique_norm": 0,
                        "right_unique_norm": 0,
                        "overlap_unique_norm": 0,
                        "row_coverage_norm": np.nan,
                    }
                )
                continue

            left_key = left_df[key].dropna().astype(str)
            right_key = right_df[key].dropna().astype(str)

            right_unique = set(right_key.unique())
            left_unique_vals = set(left_key.unique())
            overlap_unique = len(left_unique_vals.intersection(right_unique))

            coverage = float(left_key.isin(right_unique).mean()) if len(left_key) else np.nan

            left_norm = normalize_id(left_df[key]).dropna()
            right_norm = normalize_id(right_df[key]).dropna()
            right_norm_unique = set(right_norm.unique())
            left_norm_unique = set(left_norm.unique())
            overlap_norm = len(left_norm_unique.intersection(right_norm_unique))
            coverage_norm = float(left_norm.isin(right_norm_unique).mean()) if len(left_norm) else np.nan

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
                    "left_unique_norm": int(len(left_norm_unique)),
                    "right_unique_norm": int(len(right_norm_unique)),
                    "overlap_unique_norm": int(overlap_norm),
                    "row_coverage_norm": coverage_norm,
                }
            )

    return pd.DataFrame(rows).sort_values(["brand_id", "left_table", "right_table", "key"]).reset_index(drop=True)


def _coverage_metrics(left: pd.Series, right: pd.Series) -> Tuple[int, int, int, float]:
    l = left.dropna().astype(str)
    r = right.dropna().astype(str)
    l_u = set(l.unique())
    r_u = set(r.unique())
    overlap = len(l_u.intersection(r_u))
    coverage = float(l.isin(r_u).mean()) if len(l) else np.nan
    return len(l_u), len(r_u), overlap, coverage


def _time_range_summary(df: pd.DataFrame, cols: Sequence[str]) -> Dict[str, Optional[pd.Timestamp]]:
    result: Dict[str, Optional[pd.Timestamp]] = {"overall_min": None, "overall_max": None}
    mins: List[pd.Timestamp] = []
    maxs: List[pd.Timestamp] = []
    for col in cols:
        if col not in df.columns:
            result[f"{col}_min"] = None
            result[f"{col}_max"] = None
            continue
        dt = pd.to_datetime(df[col], errors="coerce", utc=True)
        mn = dt.min() if dt.notna().any() else None
        mx = dt.max() if dt.notna().any() else None
        result[f"{col}_min"] = mn
        result[f"{col}_max"] = mx
        if mn is not None:
            mins.append(mn)
        if mx is not None:
            maxs.append(mx)

    result["overall_min"] = min(mins) if mins else None
    result["overall_max"] = max(maxs) if maxs else None
    return result


def _length_stats(values: pd.Series) -> Dict[str, float]:
    s = values.dropna().astype(str)
    if s.empty:
        return {"len_min": np.nan, "len_median": np.nan, "len_max": np.nan}
    lens = s.str.len()
    return {
        "len_min": float(lens.min()),
        "len_median": float(lens.median()),
        "len_max": float(lens.max()),
    }


def _prefix_samples(values: pd.Series, n: int = 10, prefix_len: int = 4) -> List[str]:
    s = values.dropna().astype(str)
    if s.empty:
        return []
    return s.str[:prefix_len].dropna().drop_duplicates().head(n).tolist()


def _regex_profile(values: pd.Series) -> Dict[str, float]:
    s = values.dropna().astype(str)
    if s.empty:
        return {
            "share_alnum_dash_underscore": 0.0,
            "share_numeric_only": 0.0,
            "share_has_dash": 0.0,
            "share_has_underscore": 0.0,
        }

    return {
        "share_alnum_dash_underscore": float(s.str.match(r"^[a-z0-9_-]+$").mean()),
        "share_numeric_only": float(s.str.match(r"^[0-9]+$").mean()),
        "share_has_dash": float(s.str.contains(r"-", regex=True).mean()),
        "share_has_underscore": float(s.str.contains(r"_", regex=True).mean()),
    }


def _random_samples(values: pd.Series, n: int = 20, seed: int = 42) -> List[str]:
    s = values.dropna().astype(str).drop_duplicates()
    if s.empty:
        return []
    k = min(n, len(s))
    return s.sample(n=k, random_state=seed).tolist()


def _sampled_match_rates(left: pd.Series, right: pd.Series, sample_n: int = 5000) -> Tuple[float, float]:
    l = left.dropna().astype(str)
    r = right.dropna().astype(str)
    if l.empty or r.empty:
        return np.nan, np.nan

    l_sample = l.sample(n=min(sample_n, len(l)), random_state=42)
    r_sample = r.sample(n=min(sample_n, len(r)), random_state=42)
    r_sample_set = set(r_sample.unique())
    r_full_set = set(r.unique())
    sample_vs_sample = float(l_sample.isin(r_sample_set).mean())
    left_vs_full = float(l_sample.isin(r_full_set).mean())
    return sample_vs_sample, left_vs_full


def build_purchase_item_join_diagnostics(
    dataset_root: str | Path,
    brand_app_ids: Optional[Mapping[str, Sequence[int | str]]] = None,
    sample_n: int = 5000,
    random_n: int = 20,
) -> JoinDiagnostics:
    root = Path(dataset_root)
    brands = list_brands(root)

    coverage_rows: List[dict] = []
    time_rows: List[dict] = []
    pattern_rows: List[dict] = []
    sampled_rows: List[dict] = []
    random_samples: Dict[str, Dict[str, List[str]]] = {}
    joinable_map: Dict[str, bool] = {}

    for brand_id in brands:
        purchase = safe_read_parquet(
            root / brand_id / "purchase.parquet",
            columns=["app_id", "transaction_id", "user_id", "create_datetime", "paid_datetime"],
        )
        purchase_items = safe_read_parquet(
            root / brand_id / "purchase_items.parquet",
            columns=[
                "app_id",
                "transaction_id",
                "user_id",
                "create_datetime",
                "paid_datetime",
                "delivered_datetime",
            ],
        )
        purchase = _filter_brand_app_id(purchase, brand_id=brand_id, brand_app_ids=brand_app_ids)
        purchase_items = _filter_brand_app_id(purchase_items, brand_id=brand_id, brand_app_ids=brand_app_ids)

        purchase["transaction_id_norm"] = normalize_id(purchase.get("transaction_id"))
        purchase["user_id_norm"] = normalize_id(purchase.get("user_id"))
        purchase_items["transaction_id_norm"] = normalize_id(purchase_items.get("transaction_id"))
        purchase_items["user_id_norm"] = normalize_id(purchase_items.get("user_id"))

        for key in ("transaction_id", "user_id"):
            l_u, r_u, overlap_raw, cov_raw = _coverage_metrics(purchase.get(key, pd.Series(dtype=str)), purchase_items.get(key, pd.Series(dtype=str)))
            l_un, r_un, overlap_norm, cov_norm = _coverage_metrics(
                purchase.get(f"{key}_norm", pd.Series(dtype=str)),
                purchase_items.get(f"{key}_norm", pd.Series(dtype=str)),
            )
            coverage_rows.append(
                {
                    "brand_id": brand_id,
                    "left_table": "purchase",
                    "right_table": "purchase_items",
                    "key": key,
                    "left_unique": l_u,
                    "right_unique": r_u,
                    "overlap_unique": overlap_raw,
                    "row_coverage": cov_raw,
                    "left_unique_norm": l_un,
                    "right_unique_norm": r_un,
                    "overlap_unique_norm": overlap_norm,
                    "row_coverage_norm": cov_norm,
                }
            )

        p_time = _time_range_summary(purchase, ["create_datetime", "paid_datetime"])
        pi_time = _time_range_summary(purchase_items, ["create_datetime", "paid_datetime", "delivered_datetime"])
        p_min, p_max = p_time.get("overall_min"), p_time.get("overall_max")
        i_min, i_max = pi_time.get("overall_min"), pi_time.get("overall_max")
        time_overlap = bool(p_min is not None and p_max is not None and i_min is not None and i_max is not None and max(p_min, i_min) <= min(p_max, i_max))
        time_rows.append(
            {
                "brand_id": brand_id,
                "purchase_overall_min": p_min,
                "purchase_overall_max": p_max,
                "purchase_items_overall_min": i_min,
                "purchase_items_overall_max": i_max,
                "time_range_overlap": time_overlap,
                **{f"purchase_{k}": v for k, v in p_time.items() if k not in {"overall_min", "overall_max"}},
                **{f"purchase_items_{k}": v for k, v in pi_time.items() if k not in {"overall_min", "overall_max"}},
            }
        )

        p_tx_norm = purchase["transaction_id_norm"]
        pi_tx_norm = purchase_items["transaction_id_norm"]
        tx_cov_norm = coverage_rows[-2]["row_coverage_norm"] if len(coverage_rows) >= 2 else np.nan
        joinable = bool((not pd.isna(tx_cov_norm)) and tx_cov_norm >= 0.80 and time_overlap)
        joinable_map[brand_id] = joinable

        pattern_rows.append(
            {
                "brand_id": brand_id,
                "table": "purchase",
                **_length_stats(p_tx_norm),
                "prefix_samples": json.dumps(_prefix_samples(p_tx_norm, n=10)),
                **_regex_profile(p_tx_norm),
            }
        )
        pattern_rows.append(
            {
                "brand_id": brand_id,
                "table": "purchase_items",
                **_length_stats(pi_tx_norm),
                "prefix_samples": json.dumps(_prefix_samples(pi_tx_norm, n=10)),
                **_regex_profile(pi_tx_norm),
            }
        )

        sampled_sample_rate, sampled_left_full_rate = _sampled_match_rates(p_tx_norm, pi_tx_norm, sample_n=sample_n)
        sampled_rows.append(
            {
                "brand_id": brand_id,
                "sampled_merge_match_rate_norm_sample_vs_sample": sampled_sample_rate,
                "sampled_merge_match_rate_norm_left_sample_vs_full_right": sampled_left_full_rate,
                "sample_size_left": int(min(sample_n, p_tx_norm.dropna().shape[0])),
                "sample_size_right": int(min(sample_n, pi_tx_norm.dropna().shape[0])),
            }
        )

        random_samples[brand_id] = {
            "purchase_transaction_id_samples": _random_samples(purchase.get("transaction_id", pd.Series(dtype=str)), n=random_n),
            "purchase_items_transaction_id_samples": _random_samples(purchase_items.get("transaction_id", pd.Series(dtype=str)), n=random_n),
        }

    return JoinDiagnostics(
        coverage_summary=pd.DataFrame(coverage_rows),
        time_range_summary=pd.DataFrame(time_rows),
        pattern_summary=pd.DataFrame(pattern_rows),
        sampled_merge_summary=pd.DataFrame(sampled_rows),
        random_samples=random_samples,
        commerce_joinable=joinable_map,
    )


def write_join_diagnostics_markdown(diagnostics: JoinDiagnostics, output_path: str | Path) -> None:
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    lines: List[str] = []
    lines.append("# Join Diagnostics")
    lines.append("")
    lines.append("Diagnostic normalized key columns used: `transaction_id_norm`, `user_id_norm`.")
    lines.append("")
    lines.append("## Coverage Before/After Normalization")
    lines.append("")
    if diagnostics.coverage_summary.empty:
        lines.append("No diagnostics available.")
    else:
        lines.append(diagnostics.coverage_summary.to_string(index=False))
    lines.append("")
    lines.append("## Time Range Overlap Check")
    lines.append("")
    lines.append(diagnostics.time_range_summary.to_string(index=False) if not diagnostics.time_range_summary.empty else "No time summary.")
    lines.append("")

    for brand_id in sorted(diagnostics.commerce_joinable.keys()):
        joinable = diagnostics.commerce_joinable.get(brand_id, False)
        trow = diagnostics.time_range_summary.loc[diagnostics.time_range_summary["brand_id"] == brand_id]
        tx_row = diagnostics.coverage_summary[
            (diagnostics.coverage_summary["brand_id"] == brand_id)
            & (diagnostics.coverage_summary["key"] == "transaction_id")
        ]
        cov_norm = float(tx_row["row_coverage_norm"].iloc[0]) if not tx_row.empty else np.nan
        overlap = bool(trow["time_range_overlap"].iloc[0]) if not trow.empty else False

        lines.append(f"## Brand: {brand_id}")
        lines.append("")
        lines.append(f"- `transaction_id` normalized row coverage: {cov_norm:.4f}" if not pd.isna(cov_norm) else "- `transaction_id` normalized row coverage: NA")
        lines.append(f"- time range overlap: {overlap}")
        lines.append(f"- decision: commerce_joinable={str(joinable).lower()}")
        if not overlap:
            lines.append('- "These tables represent different export periods; commerce join is not valid for this brand."')
        elif not joinable:
            lines.append('- "Join quality below threshold (<0.80); commerce join is not valid for this brand."')
        lines.append("")

        pat = diagnostics.pattern_summary.loc[diagnostics.pattern_summary["brand_id"] == brand_id]
        if not pat.empty:
            lines.append("### transaction_id_norm pattern/length")
            lines.append("")
            lines.append(pat.to_string(index=False))
            lines.append("")

        sampled = diagnostics.sampled_merge_summary.loc[diagnostics.sampled_merge_summary["brand_id"] == brand_id]
        if not sampled.empty:
            lines.append("### sampled merge on transaction_id_norm")
            lines.append("")
            lines.append(sampled.to_string(index=False))
            lines.append("")

        rs = diagnostics.random_samples.get(brand_id, {})
        lines.append("### random transaction_id samples (20 each table)")
        lines.append("")
        lines.append("purchase:")
        for v in rs.get("purchase_transaction_id_samples", []):
            lines.append(f"- {v}")
        lines.append("purchase_items:")
        for v in rs.get("purchase_items_transaction_id_samples", []):
            lines.append(f"- {v}")
        lines.append("")

    out.write_text("\n".join(lines), encoding="utf-8")


def write_coverage_notes_markdown(
    join_coverage: pd.DataFrame,
    diagnostics: JoinDiagnostics,
    output_path: str | Path,
) -> None:
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    lines: List[str] = []
    lines.append("# Coverage Notes")
    lines.append("")

    for brand_id in sorted(join_coverage["brand_id"].dropna().astype(str).unique().tolist()):
        lines.append(f"## {brand_id}")
        lines.append("")
        sub = join_coverage[
            (join_coverage["brand_id"] == brand_id)
            & (join_coverage["left_table"] == "purchase")
            & (join_coverage["right_table"] == "purchase_items")
        ]
        tx = sub[sub["key"] == "transaction_id"]
        uid = sub[sub["key"] == "user_id"]

        tx_cov = float(tx["row_coverage_norm"].iloc[0]) if not tx.empty else np.nan
        uid_cov = float(uid["row_coverage_norm"].iloc[0]) if not uid.empty else np.nan
        tx_ok = (not pd.isna(tx_cov)) and tx_cov >= 0.80

        lines.append(f"- Canonical join key for `purchase ↔ purchase_items` is `transaction_id`.")
        if pd.isna(tx_cov):
            lines.append("- transaction_id normalized coverage: NA")
        else:
            lines.append(f"- transaction_id normalized coverage: {tx_cov:.4f} ({'valid' if tx_ok else 'invalid'})")
        if pd.isna(uid_cov):
            lines.append("- user_id normalized coverage: NA")
        else:
            lines.append(f"- user_id normalized coverage: {uid_cov:.4f} (unreliable for this relationship; ignored for joins)")

        if brand_id == "see-chan":
            lines.append("- purchase↔items join by transaction_id is valid for see-chan.")
            lines.append("- purchase_items.user_id is unreliable and must be ignored for joins.")
        lines.append("")

    out.write_text("\n".join(lines), encoding="utf-8")

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
