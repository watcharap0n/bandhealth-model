from __future__ import annotations

import json
import uuid
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

MERGE_KEY_COLUMNS: Tuple[str, ...] = ("brand_id", "window_end_date", "window_size")


def publish_kpis_predicted_to_catalog(
    pred_df: pd.DataFrame,
    table_name: str,
    write_mode: str = "overwrite",
    fail_on_cast_error: bool = True,
    spark_session: Optional[Any] = None,
) -> Dict[str, Any]:
    spark = _resolve_active_spark_session(spark_session)
    target_df = spark.table(str(table_name))
    table_schema = target_df.schema

    aligned_pdf = _align_pred_df_to_table_schema(
        pred_df=pred_df,
        table_schema=table_schema,
        fail_on_cast_error=bool(fail_on_cast_error),
    )
    table_rows_after = _write_to_table(
        aligned_pdf=aligned_pdf,
        table_name=str(table_name),
        spark=spark,
        table_schema=table_schema,
        write_mode=str(write_mode),
    )
    return {
        "table_name": str(table_name),
        "write_mode": str(write_mode),
        "rows_written": int(len(aligned_pdf)),
        "columns_written": int(len(aligned_pdf.columns)),
        "table_rows_after_write": int(table_rows_after),
    }


def _align_pred_df_to_table_schema(
    pred_df: pd.DataFrame,
    table_schema: Any,
    fail_on_cast_error: bool = True,
) -> pd.DataFrame:
    fields = _schema_fields(table_schema)
    target_columns = [name for name, _ in fields]
    out = pred_df.copy()

    for col_name in target_columns:
        if col_name not in out.columns:
            out[col_name] = pd.NA

    out = out[target_columns]

    cast_errors: List[str] = []
    for col_name, dtype_name in fields:
        series = out[col_name]
        try:
            casted, failed_count = _cast_series_to_dtype(series, dtype_name)
        except Exception as exc:
            raise ValueError(f"Failed to cast column '{col_name}' to {dtype_name}: {exc}") from exc
        out[col_name] = casted
        if bool(fail_on_cast_error) and failed_count > 0:
            cast_errors.append(f"{col_name}({dtype_name}) failed={failed_count}")

    if cast_errors:
        raise ValueError(
            "Type casting produced invalid values for one or more columns: " + ", ".join(cast_errors)
        )
    return out


def _write_to_table(
    aligned_pdf: pd.DataFrame,
    table_name: str,
    spark: Any,
    table_schema: Any,
    write_mode: str = "overwrite",
) -> int:
    mode = str(write_mode).strip().lower()
    if mode == "overwrite":
        return _write_overwrite_to_table(
            aligned_pdf=aligned_pdf,
            table_name=table_name,
            spark=spark,
            table_schema=table_schema,
        )
    if mode == "merge":
        return _write_merge_to_table(
            aligned_pdf=aligned_pdf,
            table_name=table_name,
            spark=spark,
            table_schema=table_schema,
        )
    raise ValueError("publish-kpis-write-mode must be one of: overwrite, merge")


def _write_overwrite_to_table(
    aligned_pdf: pd.DataFrame,
    table_name: str,
    spark: Any,
    table_schema: Any,
) -> int:
    sdf = spark.createDataFrame(aligned_pdf, schema=table_schema)
    sdf.write.mode("overwrite").saveAsTable(str(table_name))
    return int(spark.table(str(table_name)).count())


def _write_merge_to_table(
    aligned_pdf: pd.DataFrame,
    table_name: str,
    spark: Any,
    table_schema: Any,
) -> int:
    key_columns = _resolve_merge_key_columns(aligned_pdf=aligned_pdf, table_schema=table_schema)
    _validate_merge_keys_unique(aligned_pdf=aligned_pdf, key_columns=key_columns)

    sdf = spark.createDataFrame(aligned_pdf, schema=table_schema)
    temp_view_name = _temp_view_name(table_name)
    target_columns = [name for name, _ in _schema_fields(table_schema)]
    merge_sql = _build_merge_sql(
        table_name=table_name,
        temp_view_name=temp_view_name,
        target_columns=target_columns,
        key_columns=key_columns,
    )

    try:
        sdf.createOrReplaceTempView(temp_view_name)
        spark.sql(merge_sql)
    finally:
        catalog = getattr(spark, "catalog", None)
        if catalog is not None and hasattr(catalog, "dropTempView"):
            try:
                catalog.dropTempView(temp_view_name)
            except Exception:
                pass
    return int(spark.table(str(table_name)).count())


def _resolve_merge_key_columns(
    aligned_pdf: pd.DataFrame,
    table_schema: Any,
) -> List[str]:
    schema_cols = {name for name, _ in _schema_fields(table_schema)}
    missing = [col for col in MERGE_KEY_COLUMNS if col not in aligned_pdf.columns or col not in schema_cols]
    if missing:
        raise ValueError(
            "publish-kpis-write-mode=merge requires key columns in both source and target schema: "
            + ", ".join(missing)
        )
    return list(MERGE_KEY_COLUMNS)


def _validate_merge_keys_unique(
    aligned_pdf: pd.DataFrame,
    key_columns: Sequence[str],
) -> None:
    dup_mask = aligned_pdf.duplicated(list(key_columns), keep=False)
    if not bool(dup_mask.any()):
        return

    sample_rows = (
        aligned_pdf.loc[dup_mask, list(key_columns)]
        .drop_duplicates()
        .head(5)
        .to_dict(orient="records")
    )
    raise ValueError(
        "publish-kpis-write-mode=merge requires unique source rows per merge key. "
        f"Duplicate keys found for columns {list(key_columns)}; sample={sample_rows}"
    )


def _build_merge_sql(
    table_name: str,
    temp_view_name: str,
    target_columns: Sequence[str],
    key_columns: Sequence[str],
) -> str:
    match_clause = " AND ".join(f"target.`{col}` <=> source.`{col}`" for col in key_columns)
    update_clause = ", ".join(f"target.`{col}` = source.`{col}`" for col in target_columns)
    insert_columns = ", ".join(f"`{col}`" for col in target_columns)
    insert_values = ", ".join(f"source.`{col}`" for col in target_columns)
    return (
        f"MERGE INTO {table_name} AS target "
        f"USING {temp_view_name} AS source "
        f"ON {match_clause} "
        f"WHEN MATCHED THEN UPDATE SET {update_clause} "
        f"WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})"
    )


def _temp_view_name(table_name: str) -> str:
    table_slug = "".join(ch if ch.isalnum() else "_" for ch in str(table_name)).strip("_").lower()
    return f"tmp_publish_{table_slug}_{uuid.uuid4().hex[:12]}"


def _resolve_active_spark_session(spark_session: Optional[Any]) -> Any:
    if spark_session is not None:
        return spark_session

    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:
        raise RuntimeError("pyspark is required to publish predictions to Unity Catalog") from exc

    active = SparkSession.getActiveSession()
    if active is None:
        raise RuntimeError("No active Spark session found; run this publish step in Databricks Spark runtime")
    return active


def _schema_fields(table_schema: Any) -> List[Tuple[str, str]]:
    fields: Sequence[Any]
    if hasattr(table_schema, "fields"):
        fields = list(table_schema.fields)
    else:
        fields = list(table_schema)

    out: List[Tuple[str, str]] = []
    for field in fields:
        name = str(getattr(field, "name", "")).strip()
        if not name:
            continue
        dtype_obj = getattr(field, "dataType", None)
        out.append((name, _dtype_name(dtype_obj)))
    return out


def _dtype_name(dtype_obj: Any) -> str:
    if dtype_obj is None:
        return "string"
    if hasattr(dtype_obj, "typeName") and callable(dtype_obj.typeName):
        try:
            return str(dtype_obj.typeName()).strip().lower()
        except Exception:
            pass
    if hasattr(dtype_obj, "simpleString") and callable(dtype_obj.simpleString):
        try:
            raw = str(dtype_obj.simpleString()).strip().lower()
            if raw.startswith("decimal"):
                return "decimal"
            return raw
        except Exception:
            pass
    raw = str(dtype_obj).strip().lower()
    if raw.startswith("decimal"):
        return "decimal"
    return raw


def _cast_series_to_dtype(series: pd.Series, dtype_name: str) -> Tuple[pd.Series, int]:
    dtype = str(dtype_name).strip().lower()
    if dtype in {"string", "char", "varchar"}:
        return _cast_to_string(series), 0
    if dtype in {"double", "float", "real", "decimal"}:
        return _cast_to_float(series)
    if dtype in {"bigint", "long", "int", "integer", "smallint", "tinyint"}:
        return _cast_to_int(series)
    if dtype in {"boolean", "bool"}:
        return _cast_to_boolean(series)
    if dtype in {"timestamp", "timestamp_ntz"}:
        return _cast_to_timestamp(series)
    if dtype == "date":
        return _cast_to_date(series)
    return _cast_to_string(series), 0


def _cast_to_string(series: pd.Series) -> pd.Series:
    return series.apply(_stringify_value).astype("string")


def _cast_to_float(series: pd.Series) -> Tuple[pd.Series, int]:
    raw = series.copy()
    numeric = pd.to_numeric(raw, errors="coerce")
    failed = int(((~_is_nullish(raw)) & numeric.isna()).sum())
    return numeric.astype("Float64"), failed


def _cast_to_int(series: pd.Series) -> Tuple[pd.Series, int]:
    raw = series.copy()
    numeric = pd.to_numeric(raw, errors="coerce")
    non_integer = numeric.notna() & (numeric % 1 != 0)
    failed = int(((~_is_nullish(raw)) & numeric.isna()).sum()) + int(non_integer.sum())
    if bool(non_integer.any()):
        numeric = numeric.copy()
        numeric.loc[non_integer] = pd.NA
    return numeric.astype("Int64"), failed


def _cast_to_boolean(series: pd.Series) -> Tuple[pd.Series, int]:
    parsed = series.apply(_parse_bool_or_na).astype("boolean")
    failed = int(((~_is_nullish(series)) & parsed.isna()).sum())
    return parsed, failed


def _cast_to_timestamp(series: pd.Series) -> Tuple[pd.Series, int]:
    raw = series.copy()
    ts = pd.to_datetime(raw, errors="coerce", utc=True)
    failed = int(((~_is_nullish(raw)) & ts.isna()).sum())
    ts_utc_naive = ts.dt.tz_convert("UTC").dt.tz_localize(None)
    return ts_utc_naive, failed


def _cast_to_date(series: pd.Series) -> Tuple[pd.Series, int]:
    raw = series.copy()
    dt = pd.to_datetime(raw, errors="coerce", utc=True)
    failed = int(((~_is_nullish(raw)) & dt.isna()).sum())
    return dt.dt.date, failed


def _stringify_value(value: Any) -> Any:
    if _is_scalar_na(value):
        return pd.NA
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, ensure_ascii=False)
    return str(value)


def _parse_bool_or_na(value: Any) -> Any:
    if _is_scalar_na(value):
        return pd.NA
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and not pd.isna(value):
        if float(value) == 1.0:
            return True
        if float(value) == 0.0:
            return False
        return pd.NA

    raw = str(value).strip().lower()
    if raw in {"true", "t", "1", "yes", "y", "on"}:
        return True
    if raw in {"false", "f", "0", "no", "n", "off"}:
        return False
    return pd.NA


def _is_nullish(series: pd.Series) -> pd.Series:
    as_str = series.astype("string")
    return series.isna() | as_str.str.strip().str.lower().isin({"", "none", "null", "nan"})


def _is_scalar_na(value: Any) -> bool:
    try:
        flag = pd.isna(value)
    except Exception:
        return False
    if isinstance(flag, (bool, np.bool_)):
        return bool(flag)
    return False
