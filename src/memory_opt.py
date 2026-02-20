from __future__ import annotations

import gc
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, List, Mapping, MutableMapping, Optional, Sequence, Tuple

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

try:
    import psutil
except Exception:  # pragma: no cover - optional dependency
    psutil = None


@dataclass
class OptimizeSummary:
    table_name: str
    rows: int
    cols: int
    bytes_before: int
    bytes_after: int
    reduced_bytes: int
    reduced_pct: float
    conversions: List[Dict[str, object]]


def get_rss_mb() -> float:
    if psutil is None:
        return float("nan")
    try:
        proc = psutil.Process()
        return float(proc.memory_info().rss / (1024 * 1024))
    except Exception:
        return float("nan")


def log_memory_rss(stage: str, sink: Optional[List[Dict[str, object]]] = None) -> Dict[str, object]:
    rss = get_rss_mb()
    rss_value: Optional[float] = float(rss) if np.isfinite(rss) else None
    row = {
        "stage": str(stage),
        "rss_mb": rss_value,
    }
    if sink is not None:
        sink.append(row)
    if rss_value is not None:
        print(f"[memory] {row['stage']}: rss={rss_value:.2f} MB")
    else:
        print(f"[memory] {row['stage']}: rss=NA (psutil unavailable)")
    return row


def collect_garbage(stage: str, sink: Optional[List[Dict[str, object]]] = None) -> None:
    log_memory_rss(f"{stage}:before_gc", sink=sink)
    gc.collect()
    log_memory_rss(f"{stage}:after_gc", sink=sink)


def _best_int_dtype(min_val: int, max_val: int):
    for dt in (np.int8, np.int16, np.int32, np.int64):
        info = np.iinfo(dt)
        if min_val >= info.min and max_val <= info.max:
            return dt
    return np.int64


def _object_equal_with_na(left: pd.Series, right: pd.Series) -> bool:
    if len(left) != len(right):
        return False
    l = left.astype("object")
    r = right.astype("object")
    l_na = l.isna()
    r_na = r.isna()
    if not bool((l_na == r_na).all()):
        return False
    if l_na.all():
        return True
    return bool((l[~l_na] == r[~r_na]).all())


def optimize_dataframe_dtypes(
    df: pd.DataFrame,
    table_name: str = "",
    allow_float_downcast: bool = True,
    float_rtol: float = 1e-6,
    cat_ratio_threshold: float = 0.5,
    protect_columns: Optional[Sequence[str]] = None,
    validate: bool = True,
) -> Tuple[pd.DataFrame, OptimizeSummary]:
    if df.empty:
        summary = OptimizeSummary(
            table_name=table_name,
            rows=0,
            cols=len(df.columns),
            bytes_before=0,
            bytes_after=0,
            reduced_bytes=0,
            reduced_pct=0.0,
            conversions=[],
        )
        return df, summary

    out = df
    protected = set(protect_columns or [])
    before_bytes = int(out.memory_usage(deep=True).sum())
    conversions: List[Dict[str, object]] = []

    for col in out.columns:
        if col in protected:
            continue

        s = out[col]
        before_dtype = str(s.dtype)
        conv_reason = ""
        changed = False

        if pd.api.types.is_integer_dtype(s):
            mn = int(s.min()) if s.notna().any() else 0
            mx = int(s.max()) if s.notna().any() else 0
            best_dt = _best_int_dtype(mn, mx)
            target_dtype = best_dt
            if s.isna().any():
                nullable_map = {
                    np.int8: "Int8",
                    np.int16: "Int16",
                    np.int32: "Int32",
                    np.int64: "Int64",
                }
                target_dtype = nullable_map.get(best_dt, "Int64")
            if str(s.dtype) != str(target_dtype):
                new_s = s.astype(target_dtype)
                if validate:
                    left = pd.to_numeric(s, errors="coerce").astype("Int64")
                    right = pd.to_numeric(new_s, errors="coerce").astype("Int64")
                    if not left.equals(right):
                        raise AssertionError(f"Integer downcast validation failed for {table_name}.{col}")
                out[col] = new_s
                changed = True
                conv_reason = "int_downcast"

        elif pd.api.types.is_float_dtype(s) and allow_float_downcast and str(s.dtype) == "float64":
            new_s = s.astype(np.float32)
            if validate:
                ok = np.allclose(
                    s.to_numpy(dtype=np.float64, copy=False),
                    new_s.to_numpy(dtype=np.float64, copy=False),
                    rtol=float_rtol,
                    atol=float_rtol,
                    equal_nan=True,
                )
                if not ok:
                    new_s = None
            if new_s is not None:
                out[col] = new_s
                changed = True
                conv_reason = "float32_downcast"

        elif pd.api.types.is_object_dtype(s):
            n = len(s)
            if n > 0:
                nunique = int(s.nunique(dropna=True))
                ratio = float(nunique / max(1, n))
                if ratio < float(cat_ratio_threshold):
                    new_s = s.astype("category")
                    if validate:
                        if not _object_equal_with_na(s, new_s.astype("object")):
                            raise AssertionError(f"Category conversion validation failed for {table_name}.{col}")
                    out[col] = new_s
                    changed = True
                    conv_reason = f"category_ratio<{cat_ratio_threshold}"

        if changed:
            conversions.append(
                {
                    "column": str(col),
                    "before_dtype": before_dtype,
                    "after_dtype": str(out[col].dtype),
                    "reason": conv_reason,
                }
            )

    after_bytes = int(out.memory_usage(deep=True).sum())
    reduced = int(max(before_bytes - after_bytes, 0))
    reduced_pct = float(reduced / before_bytes) if before_bytes > 0 else 0.0

    summary = OptimizeSummary(
        table_name=table_name,
        rows=int(len(out)),
        cols=int(len(out.columns)),
        bytes_before=before_bytes,
        bytes_after=after_bytes,
        reduced_bytes=reduced,
        reduced_pct=reduced_pct,
        conversions=conversions,
    )
    return out, summary


def optimize_table_dict(
    tables: Mapping[str, pd.DataFrame],
    allow_float_downcast: bool = True,
    float_rtol: float = 1e-6,
    cat_ratio_threshold: float = 0.5,
    protect_columns: Optional[Sequence[str]] = None,
    validate: bool = True,
) -> Tuple[Dict[str, pd.DataFrame], pd.DataFrame]:
    out: Dict[str, pd.DataFrame] = {}
    rows: List[Dict[str, object]] = []

    for table_name, df in tables.items():
        if not isinstance(df, pd.DataFrame):
            continue
        opt_df, summary = optimize_dataframe_dtypes(
            df=df,
            table_name=table_name,
            allow_float_downcast=allow_float_downcast,
            float_rtol=float_rtol,
            cat_ratio_threshold=cat_ratio_threshold,
            protect_columns=protect_columns,
            validate=validate,
        )
        out[table_name] = opt_df
        rows.append(asdict(summary))

    summary_df = pd.DataFrame(rows)
    return out, summary_df


def write_parquet_chunked(
    df: pd.DataFrame,
    output_path: str | Path,
    chunk_rows: int = 100_000,
    compression: str = "snappy",
) -> None:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    if len(df) == 0:
        empty = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_table(empty, path, compression=compression)
        return

    writer: Optional[pq.ParquetWriter] = None
    try:
        for start in range(0, len(df), max(1, int(chunk_rows))):
            chunk = df.iloc[start : start + int(chunk_rows)]
            table = pa.Table.from_pandas(chunk, preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(path, table.schema, compression=compression)
            writer.write_table(table)
    finally:
        if writer is not None:
            writer.close()
