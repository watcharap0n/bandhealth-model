from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple
from urllib.parse import urljoin

import pandas as pd
import requests


CATALOG_TABLE_MAP: Dict[str, str] = {
    "activity_transaction": "activity_transaction",
    "purchase": "purchase_transaction",
    "purchase_items": "purchase_transactionitems",
    "user_device": "user_device",
    "user_identity": "user_identity",
    "user_info": "userinfo",
    "user_view": "user_view",
    "user_visitor": "user_visitor",
}

COLUMN_ALIASES: Dict[str, Dict[str, str]] = {
    "purchase": {
        "created_datetime": "create_datetime",
        "payment_datetime": "paid_datetime",
        "status": "transaction_status",
    },
    "purchase_items": {
        "created_datetime": "create_datetime",
        "payment_datetime": "paid_datetime",
        "shipped": "is_shiped",
    },
    "user_info": {
        "dob": "dateofbirth",
    },
}

PRIMARY_TIME_EXPRESSIONS: Dict[str, str] = {
    "activity_transaction": "to_timestamp(activity_datetime)",
    "purchase": "coalesce(to_timestamp(paid_datetime), to_timestamp(create_datetime))",
    "purchase_items": "coalesce(to_timestamp(paid_datetime), to_timestamp(create_datetime), to_timestamp(delivered_datetime))",
    "user_view": "coalesce(to_timestamp(join_datetime), to_timestamp(inactive_datetime))",
    "user_visitor": "coalesce(to_timestamp(visit_datetime), to_timestamp(visit_end_datetime))",
    "user_device": "to_timestamp(lastaccess)",
}


@dataclass(frozen=True)
class DatabricksSQLConfig:
    host: str
    token: str
    warehouse_id: str
    catalog: str = "projects_prd"
    database: str = "datacleansing"
    wait_timeout: str = "30s"
    poll_interval_seconds: int = 2

    @property
    def base_url(self) -> str:
        return str(self.host).rstrip("/")


@dataclass(frozen=True)
class QuerySelection:
    app_ids: Tuple[str, ...]
    brand_aliases: Dict[str, str]
    start_date: Optional[date] = None
    end_date: Optional[date] = None


@dataclass(frozen=True)
class StatementResult:
    rows: List[List[Any]]
    columns: List[str]
    statement_id: str


class DatabricksStatementError(RuntimeError):
    def __init__(self, statement_id: str, state: str, message: str) -> None:
        super().__init__(f"Databricks statement failed (statement_id={statement_id}, state={state}): {message}")
        self.statement_id = statement_id
        self.state = state
        self.message = message


def group_app_ids_by_brand(
    app_ids: Sequence[str],
    brand_aliases: Optional[Mapping[str, str]] = None,
) -> Dict[str, List[str]]:
    out: Dict[str, List[str]] = {}
    alias_map = {str(k): str(v) for k, v in (brand_aliases or {}).items()}
    for app_id in app_ids:
        key = str(app_id).strip()
        if not key:
            continue
        brand_id = alias_map.get(key, key)
        out.setdefault(brand_id, []).append(key)
    return out


def build_table_statement(
    canonical_table: str,
    source_table: str,
    catalog: str,
    database: str,
    selected_columns: Sequence[str],
    selection: QuerySelection,
) -> Tuple[str, List[Dict[str, str]]]:
    cols = [str(c) for c in selected_columns if str(c).strip()]
    if "app_id" not in cols:
        cols = cols + ["app_id"]

    app_params: List[Dict[str, str]] = []
    placeholders: List[str] = []
    for idx, app_id in enumerate(selection.app_ids):
        name = f"app_id_{idx}"
        placeholders.append(f":{name}")
        app_params.append({"name": name, "value": str(app_id), "type": "STRING"})

    quoted_cols = ", ".join(cols)
    table_ref = f"{catalog}.{database}.{source_table}"
    where_parts = [f"CAST(app_id AS STRING) IN ({', '.join(placeholders)})"]
    params = list(app_params)

    time_expr = PRIMARY_TIME_EXPRESSIONS.get(canonical_table)
    if time_expr and selection.start_date is not None:
        start_dt = datetime.combine(selection.start_date, datetime.min.time())
        where_parts.append(f"{time_expr} >= to_timestamp(:start_ts)")
        params.append({"name": "start_ts", "value": start_dt.strftime("%Y-%m-%d %H:%M:%S"), "type": "STRING"})
    if time_expr and selection.end_date is not None:
        end_dt_exclusive = datetime.combine(selection.end_date + timedelta(days=1), datetime.min.time())
        where_parts.append(f"{time_expr} < to_timestamp(:end_ts_exclusive)")
        params.append(
            {
                "name": "end_ts_exclusive",
                "value": end_dt_exclusive.strftime("%Y-%m-%d %H:%M:%S"),
                "type": "STRING",
            }
        )

    sql = f"SELECT {quoted_cols} FROM {table_ref} WHERE {' AND '.join(where_parts)}"
    return sql, params


def load_tables_from_databricks_sql(
    config: DatabricksSQLConfig,
    selection: QuerySelection,
    columns_map: Mapping[str, Sequence[str]],
    table_map: Optional[Mapping[str, str]] = None,
    column_aliases: Optional[Mapping[str, Mapping[str, str]]] = None,
    http_session: Optional[Any] = None,
) -> Dict[str, pd.DataFrame]:
    table_name_map = dict(CATALOG_TABLE_MAP)
    if table_map:
        table_name_map.update({str(k): str(v) for k, v in table_map.items()})

    alias_map = {str(k): dict(v) for k, v in (column_aliases or COLUMN_ALIASES).items()}
    session = http_session or requests.Session()

    out: Dict[str, pd.DataFrame] = {}
    for canonical_table, requested_cols in columns_map.items():
        source_table = table_name_map.get(canonical_table, canonical_table)
        available_columns = _fetch_remote_columns(
            config=config,
            source_table=source_table,
            http_session=session,
        )
        select_cols = _existing_select_columns(
            requested_cols=requested_cols,
            alias_map=alias_map.get(canonical_table, {}),
            available_columns=available_columns,
        )
        statement, params = build_table_statement(
            canonical_table=canonical_table,
            source_table=source_table,
            catalog=config.catalog,
            database=config.database,
            selected_columns=select_cols,
            selection=selection,
        )
        result = execute_statement(
            config=config,
            statement=statement,
            parameters=params,
            http_session=session,
        )
        pdf = pd.DataFrame(result.rows, columns=result.columns)
        out[canonical_table] = canonicalize_table_frame(
            canonical_table=canonical_table,
            df=pdf,
            requested_columns=requested_cols,
            brand_aliases=selection.brand_aliases,
            alias_map=alias_map.get(canonical_table, {}),
        )
    return out


def execute_statement(
    config: DatabricksSQLConfig,
    statement: str,
    parameters: Optional[Sequence[Mapping[str, str]]] = None,
    http_session: Optional[Any] = None,
) -> StatementResult:
    session = http_session or requests.Session()
    headers = {
        "Authorization": f"Bearer {config.token}",
        "Content-Type": "application/json",
    }
    payload: Dict[str, Any] = {
        "statement": statement,
        "warehouse_id": config.warehouse_id,
        "catalog": config.catalog,
        "schema": config.database,
        "wait_timeout": config.wait_timeout,
        "on_wait_timeout": "CONTINUE",
        "format": "JSON_ARRAY",
        "disposition": "EXTERNAL_LINKS",
    }
    if parameters:
        payload["parameters"] = list(parameters)

    submit_url = f"{config.base_url}/api/2.0/sql/statements"
    response = session.post(submit_url, headers=headers, json=payload, timeout=60)
    response.raise_for_status()
    payload_doc = response.json()

    final_doc = _poll_to_terminal(
        config=config,
        statement_doc=payload_doc,
        headers=headers,
        http_session=session,
    )
    statement_id = str(final_doc.get("statement_id") or payload_doc.get("statement_id") or "")
    status = dict(final_doc.get("status") or {})
    state = str(status.get("state") or "UNKNOWN")
    if state != "SUCCEEDED":
        error = dict(final_doc.get("error") or status.get("error") or {})
        message = str(error.get("message") or error.get("error_message") or status.get("error_message") or "Unknown error")
        raise DatabricksStatementError(statement_id=statement_id, state=state, message=message)

    columns = _manifest_columns(final_doc)
    rows = _collect_all_rows(
        doc=final_doc,
        config=config,
        headers=headers,
        http_session=session,
        columns=columns,
    )
    return StatementResult(rows=rows, columns=columns, statement_id=statement_id)


def canonicalize_table_frame(
    canonical_table: str,
    df: pd.DataFrame,
    requested_columns: Sequence[str],
    brand_aliases: Optional[Mapping[str, str]] = None,
    alias_map: Optional[Mapping[str, str]] = None,
) -> pd.DataFrame:
    out = df.copy()
    aliases = {str(k): str(v) for k, v in (alias_map or {}).items()}
    for source_col, canonical_col in aliases.items():
        if source_col in out.columns and canonical_col not in out.columns:
            out = out.rename(columns={source_col: canonical_col})

    required_cols = list(dict.fromkeys([*(str(c) for c in requested_columns), "app_id"]))
    for col in required_cols:
        if col not in out.columns:
            out[col] = pd.NA

    app_values = out["app_id"].astype("string").fillna(pd.NA)
    alias_lookup = {str(k): str(v) for k, v in (brand_aliases or {}).items()}
    out["brand_id"] = app_values.map(lambda x: alias_lookup.get(str(x), str(x)) if pd.notna(x) else pd.NA)

    keep_cols = list(dict.fromkeys(required_cols + ["brand_id"]))
    return out[keep_cols]


def _fetch_remote_columns(
    config: DatabricksSQLConfig,
    source_table: str,
    http_session: Optional[Any] = None,
) -> List[str]:
    sql = f"SELECT * FROM {config.catalog}.{config.database}.{source_table} LIMIT 0"
    result = execute_statement(config=config, statement=sql, parameters=None, http_session=http_session)
    return result.columns


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


def _poll_to_terminal(
    config: DatabricksSQLConfig,
    statement_doc: Mapping[str, Any],
    headers: Mapping[str, str],
    http_session: Optional[Any] = None,
) -> Dict[str, Any]:
    session = http_session or requests.Session()
    current = dict(statement_doc)
    statement_id = str(current.get("statement_id") or "")

    while True:
        status = dict(current.get("status") or {})
        state = str(status.get("state") or "")
        if state in {"SUCCEEDED", "FAILED", "CANCELED"}:
            return current
        if not statement_id:
            raise DatabricksStatementError(statement_id="", state="UNKNOWN", message="Missing statement_id in response")

        time.sleep(max(0, int(config.poll_interval_seconds)))
        status_url = f"{config.base_url}/api/2.0/sql/statements/{statement_id}"
        response = session.get(status_url, headers=headers, timeout=60)
        response.raise_for_status()
        current = response.json()


def _manifest_columns(doc: Mapping[str, Any]) -> List[str]:
    manifest = dict(doc.get("manifest") or {})
    schema = dict(manifest.get("schema") or {})
    columns = schema.get("columns") or []
    out: List[str] = []
    for col in columns:
        if isinstance(col, Mapping):
            name = str(col.get("name") or "").strip()
            if name:
                out.append(name)
    return out


def _collect_all_rows(
    doc: Mapping[str, Any],
    config: DatabricksSQLConfig,
    headers: Mapping[str, str],
    http_session: Optional[Any],
    columns: Sequence[str],
) -> List[List[Any]]:
    session = http_session or requests.Session()
    rows: List[List[Any]] = []
    payload = _result_payload(doc)
    rows.extend(_rows_from_payload(payload, columns=columns, http_session=session))
    next_link = str(payload.get("next_chunk_internal_link") or "").strip()

    while next_link:
        chunk_url = _absolute_url(config.base_url, next_link)
        response = session.get(chunk_url, headers=headers, timeout=60)
        response.raise_for_status()
        chunk_doc = response.json()
        chunk_payload = _result_payload(chunk_doc)
        rows.extend(_rows_from_payload(chunk_payload, columns=columns, http_session=session))
        next_link = str(chunk_payload.get("next_chunk_internal_link") or "").strip()

    return rows


def _result_payload(doc: Mapping[str, Any]) -> Dict[str, Any]:
    result = doc.get("result")
    if isinstance(result, Mapping):
        return dict(result)
    return dict(doc)


def _rows_from_payload(
    payload: Mapping[str, Any],
    columns: Sequence[str],
    http_session: Optional[Any] = None,
) -> List[List[Any]]:
    rows: List[List[Any]] = []
    data_array = payload.get("data_array")
    if isinstance(data_array, list):
        rows.extend(_normalize_row_block(data_array, columns=columns))

    external_links = payload.get("external_links")
    if isinstance(external_links, list):
        for item in external_links:
            if not isinstance(item, Mapping):
                continue
            link = str(item.get("external_link") or item.get("url") or "").strip()
            if not link:
                continue
            rows.extend(_fetch_external_rows(link, columns=columns, http_session=http_session))
    return rows


def _fetch_external_rows(
    url: str,
    columns: Sequence[str],
    http_session: Optional[Any] = None,
) -> List[List[Any]]:
    session = http_session or requests.Session()
    response = session.get(url, timeout=60)
    response.raise_for_status()

    try:
        payload = response.json()
        return _rows_from_external_payload(payload, columns=columns)
    except ValueError:
        text = response.text.strip()
        if not text:
            return []
        rows: List[List[Any]] = []
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            parsed = json.loads(line)
            rows.extend(_rows_from_external_payload(parsed, columns=columns))
        return rows


def _rows_from_external_payload(payload: Any, columns: Sequence[str]) -> List[List[Any]]:
    if isinstance(payload, Mapping):
        result = payload.get("result")
        if isinstance(result, Mapping):
            return _rows_from_external_payload(result, columns=columns)
        data = payload.get("data_array")
        if isinstance(data, list):
            return _normalize_row_block(data, columns=columns)
        if isinstance(payload.get("data"), list):
            return _normalize_row_block(payload["data"], columns=columns)
        return []
    if isinstance(payload, list):
        return _normalize_row_block(payload, columns=columns)
    return []


def _normalize_row_block(payload: Sequence[Any], columns: Sequence[str]) -> List[List[Any]]:
    rows: List[List[Any]] = []
    for item in payload:
        if isinstance(item, list):
            rows.append(list(item))
            continue
        if isinstance(item, Mapping):
            rows.append([item.get(col) for col in columns])
    return rows


def _absolute_url(base_url: str, link: str) -> str:
    if link.startswith("http://") or link.startswith("https://"):
        return link
    return urljoin(f"{base_url}/", link.lstrip("/"))
