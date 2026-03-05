from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List, Mapping, Optional, Sequence

import pandas as pd

from src.databricks_sql import (
    CATALOG_TABLE_MAP,
    COLUMN_ALIASES,
    QuerySelection,
    canonicalize_table_frame,
)


PRIMARY_TIME_COLUMNS: Dict[str, Sequence[str]] = {
    "activity_transaction": ("activity_datetime",),
    "purchase": ("paid_datetime", "create_datetime"),
    "purchase_items": ("paid_datetime", "create_datetime", "delivered_datetime"),
    "user_view": ("join_datetime", "inactive_datetime"),
    "user_visitor": ("visit_datetime", "visit_end_datetime"),
    "user_device": ("lastaccess",),
}


def load_tables_from_databricks_pyspark(
    selection: QuerySelection,
    columns_map: Mapping[str, Sequence[str]],
    *,
    catalog: str = "projects_prd",
    database: str = "datacleansing",
    table_map: Optional[Mapping[str, str]] = None,
    column_aliases: Optional[Mapping[str, Mapping[str, str]]] = None,
    spark_session: Optional[Any] = None,
    recent_days: int = 0,
    max_rows_per_table: int = 0,
    explain_pushdown: bool = False,
) -> Dict[str, pd.DataFrame]:
    spark = _resolve_spark_session(spark_session)
    spark_fns = _resolve_spark_functions()
    app_ids = _normalize_app_ids(selection.app_ids)
    if not app_ids:
        raise ValueError("Databricks PySpark source mode requires at least one app_id")

    table_name_map = dict(CATALOG_TABLE_MAP)
    if table_map:
        table_name_map.update({str(k): str(v) for k, v in table_map.items()})
    alias_map = {str(k): dict(v) for k, v in (column_aliases or COLUMN_ALIASES).items()}

    out: Dict[str, pd.DataFrame] = {}
    for canonical_table, requested_cols in columns_map.items():
        source_table = table_name_map.get(canonical_table, canonical_table)
        full_name = f"{catalog}.{database}.{source_table}"
        sdf = spark.table(full_name)
        if "app_id" not in sdf.columns:
            raise ValueError(f"{full_name} is missing required column: app_id")

        select_cols = _existing_select_columns(
            requested_cols=requested_cols,
            alias_map=alias_map.get(canonical_table, {}),
            available_columns=sdf.columns,
        )
        app_id_expr = _normalized_app_id_expression(spark_fns)
        filtered = sdf.filter(app_id_expr.isin(app_ids))
        filtered = _apply_time_filters(
            sdf=filtered,
            canonical_table=canonical_table,
            selection=selection,
            recent_days=int(recent_days),
            spark_fns=spark_fns,
        )
        projected = filtered.select(*select_cols)
        if max_rows_per_table and int(max_rows_per_table) > 0:
            projected = projected.limit(int(max_rows_per_table))
        if explain_pushdown:
            projected.explain(True)

        pdf = projected.toPandas()
        out[canonical_table] = canonicalize_table_frame(
            canonical_table=canonical_table,
            df=pdf,
            requested_columns=requested_cols,
            brand_aliases=selection.brand_aliases,
            alias_map=alias_map.get(canonical_table, {}),
        )
    return out


def _resolve_spark_session(spark_session: Optional[Any]) -> Any:
    if spark_session is not None:
        return spark_session
    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:
        raise RuntimeError("pyspark is required for databricks_pyspark source mode") from exc

    active = SparkSession.getActiveSession()
    if active is not None:
        return active
    return SparkSession.builder.getOrCreate()


def _resolve_spark_functions() -> Any:
    try:
        from pyspark.sql import functions as F
    except ImportError as exc:
        raise RuntimeError("pyspark is required for databricks_pyspark source mode") from exc
    return F


def _normalize_app_ids(app_ids: Sequence[str]) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for raw in app_ids:
        key = str(raw).strip()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(key)
    return out


def _existing_select_columns(
    requested_cols: Sequence[str],
    alias_map: Mapping[str, str],
    available_columns: Sequence[str],
) -> List[str]:
    available = {str(c) for c in available_columns}
    needed = list(dict.fromkeys([*(str(c) for c in requested_cols), *(str(k) for k in alias_map.keys()), "app_id"]))
    cols = [c for c in needed if c in available]
    if "app_id" not in cols:
        raise ValueError("Databricks source table is missing required column: app_id")
    return cols


def _normalized_app_id_expression(spark_fns: Any):
    return spark_fns.coalesce(
        spark_fns.col("app_id").cast("bigint").cast("string"),
        spark_fns.regexp_replace(spark_fns.trim(spark_fns.col("app_id").cast("string")), r"\.0+$", ""),
        spark_fns.trim(spark_fns.col("app_id").cast("string")),
    )


def _apply_time_filters(
    sdf: Any,
    canonical_table: str,
    selection: QuerySelection,
    recent_days: int,
    spark_fns: Any,
):
    ts_expr = _primary_time_expression(canonical_table=canonical_table, available_columns=sdf.columns, spark_fns=spark_fns)
    if ts_expr is None:
        return sdf

    out = sdf
    if selection.start_date is not None:
        start_ts = datetime.combine(selection.start_date, datetime.min.time()).strftime("%Y-%m-%d %H:%M:%S")
        out = out.filter(ts_expr >= spark_fns.to_timestamp(spark_fns.lit(start_ts)))
    if selection.end_date is not None:
        end_exclusive = datetime.combine(selection.end_date + timedelta(days=1), datetime.min.time()).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        out = out.filter(ts_expr < spark_fns.to_timestamp(spark_fns.lit(end_exclusive)))
    if recent_days > 0:
        out = out.filter(ts_expr >= spark_fns.to_timestamp(spark_fns.date_sub(spark_fns.current_date(), int(recent_days))))
    return out


def _primary_time_expression(canonical_table: str, available_columns: Sequence[str], spark_fns: Any):
    available = {str(c) for c in available_columns}
    time_cols = [c for c in PRIMARY_TIME_COLUMNS.get(canonical_table, ()) if c in available]
    if not time_cols:
        return None
    exprs = [spark_fns.to_timestamp(spark_fns.col(c)) for c in time_cols]
    if len(exprs) == 1:
        return exprs[0]
    return spark_fns.coalesce(*exprs)
