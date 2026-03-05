from __future__ import annotations

import unittest
from datetime import date

import pandas as pd

import run_pipeline
from src.databricks_sql import (
    DatabricksSQLConfig,
    DatabricksStatementError,
    QuerySelection,
    build_table_statement,
    canonicalize_table_frame,
    execute_statement,
    load_tables_from_databricks_sql,
)


class FakeResponse:
    def __init__(self, payload=None, status_code: int = 200, text: str = "") -> None:
        self._payload = payload
        self.status_code = int(status_code)
        self.text = text

    def json(self):
        if self._payload is None:
            raise ValueError("No JSON payload")
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class FakeSession:
    def __init__(self, post_responses=None, get_responses=None) -> None:
        self.post_responses = list(post_responses or [])
        self.get_responses = list(get_responses or [])
        self.post_calls = []
        self.get_calls = []

    def post(self, url, headers=None, json=None, timeout=None):
        self.post_calls.append(
            {
                "url": url,
                "headers": headers,
                "json": json,
                "timeout": timeout,
            }
        )
        if not self.post_responses:
            raise AssertionError("Unexpected POST call")
        return self.post_responses.pop(0)

    def get(self, url, headers=None, timeout=None):
        self.get_calls.append(
            {
                "url": url,
                "headers": headers,
                "timeout": timeout,
            }
        )
        if not self.get_responses:
            raise AssertionError("Unexpected GET call")
        return self.get_responses.pop(0)


class BuildTableStatementTests(unittest.TestCase):
    def test_app_id_only_statement_has_no_time_predicate(self) -> None:
        selection = QuerySelection(app_ids=("123", "124"), brand_aliases={}, start_date=None, end_date=None)
        sql, params = build_table_statement(
            canonical_table="purchase",
            source_table="purchase_transaction",
            catalog="projects_prd",
            database="datacleansing",
            selected_columns=("app_id", "transaction_id", "paid_datetime"),
            selection=selection,
        )

        self.assertIn("coalesce(cast(try_cast(app_id AS BIGINT) AS STRING)", sql)
        self.assertIn("IN (:app_id_0, :app_id_1)", sql)
        self.assertNotIn("start_ts", sql)
        self.assertNotIn("end_ts_exclusive", sql)
        self.assertEqual(
            params,
            [
                {"name": "app_id_0", "value": "123", "type": "STRING"},
                {"name": "app_id_1", "value": "124", "type": "STRING"},
            ],
        )

    def test_date_range_uses_primary_timestamp_expression(self) -> None:
        selection = QuerySelection(
            app_ids=("123",),
            brand_aliases={},
            start_date=date(2021, 1, 1),
            end_date=date(2021, 12, 31),
        )
        sql, params = build_table_statement(
            canonical_table="purchase",
            source_table="purchase_transaction",
            catalog="projects_prd",
            database="datacleansing",
            selected_columns=("app_id", "transaction_id", "paid_datetime"),
            selection=selection,
        )

        self.assertIn("coalesce(to_timestamp(paid_datetime), to_timestamp(create_datetime)) >= to_timestamp(:start_ts)", sql)
        self.assertIn("coalesce(to_timestamp(paid_datetime), to_timestamp(create_datetime)) < to_timestamp(:end_ts_exclusive)", sql)
        param_map = {row["name"]: row["value"] for row in params}
        self.assertEqual(param_map["start_ts"], "2021-01-01 00:00:00")
        self.assertEqual(param_map["end_ts_exclusive"], "2022-01-01 00:00:00")

    def test_identity_tables_ignore_date_range(self) -> None:
        selection = QuerySelection(
            app_ids=("123",),
            brand_aliases={},
            start_date=date(2021, 1, 1),
            end_date=date(2021, 12, 31),
        )
        sql, params = build_table_statement(
            canonical_table="user_identity",
            source_table="user_identity",
            catalog="projects_prd",
            database="datacleansing",
            selected_columns=("app_id", "user_id"),
            selection=selection,
        )

        self.assertIn("coalesce(cast(try_cast(app_id AS BIGINT) AS STRING)", sql)
        self.assertIn("IN (:app_id_0)", sql)
        self.assertNotIn("start_ts", sql)
        self.assertNotIn("end_ts_exclusive", sql)
        self.assertEqual(len(params), 1)


class CanonicalizationTests(unittest.TestCase):
    def test_aliases_are_normalized_and_missing_columns_are_added(self) -> None:
        purchase_df = pd.DataFrame(
            [
                {
                    "app_id": "123",
                    "transaction_id": "tx-1",
                    "created_datetime": "2021-01-02 00:00:00",
                    "payment_datetime": "2021-01-03 00:00:00",
                    "status": "paid",
                }
            ]
        )
        out = canonicalize_table_frame(
            canonical_table="purchase",
            df=purchase_df,
            requested_columns=run_pipeline.COLUMNS_MAP["purchase"],
            brand_aliases={},
            alias_map={"created_datetime": "create_datetime", "payment_datetime": "paid_datetime", "status": "transaction_status"},
        )

        self.assertEqual(out.loc[0, "create_datetime"], "2021-01-02 00:00:00")
        self.assertEqual(out.loc[0, "paid_datetime"], "2021-01-03 00:00:00")
        self.assertEqual(out.loc[0, "transaction_status"], "paid")
        self.assertTrue(pd.isna(out.loc[0, "itemsold"]))

        user_info_df = pd.DataFrame([{"app_id": "123", "user_id": "u1", "dob": "1990-01-01"}])
        user_info_out = canonicalize_table_frame(
            canonical_table="user_info",
            df=user_info_df,
            requested_columns=run_pipeline.COLUMNS_MAP["user_info"],
            brand_aliases={},
            alias_map={"dob": "dateofbirth"},
        )
        self.assertEqual(user_info_out.loc[0, "dateofbirth"], "1990-01-01")
        self.assertTrue(pd.isna(user_info_out.loc[0, "gender"]))

    def test_brand_defaults_to_app_id_when_alias_missing(self) -> None:
        df = pd.DataFrame([{"app_id": "123", "user_id": "u1"}])
        out = canonicalize_table_frame(
            canonical_table="user_identity",
            df=df,
            requested_columns=run_pipeline.COLUMNS_MAP["user_identity"],
            brand_aliases={},
            alias_map={},
        )
        self.assertEqual(out.loc[0, "brand_id"], "123")

    def test_brand_alias_overrides_when_present(self) -> None:
        df = pd.DataFrame([{"app_id": "123", "user_id": "u1"}, {"app_id": "999", "user_id": "u2"}])
        out = canonicalize_table_frame(
            canonical_table="user_identity",
            df=df,
            requested_columns=run_pipeline.COLUMNS_MAP["user_identity"],
            brand_aliases={"123": "c-vit"},
            alias_map={},
        )
        self.assertEqual(out.loc[0, "brand_id"], "c-vit")
        self.assertEqual(out.loc[1, "brand_id"], "999")

    def test_brand_alias_handles_numeric_like_string_app_id(self) -> None:
        df = pd.DataFrame([{"app_id": "123.0", "user_id": "u1"}])
        out = canonicalize_table_frame(
            canonical_table="user_identity",
            df=df,
            requested_columns=run_pipeline.COLUMNS_MAP["user_identity"],
            brand_aliases={"123": "c-vit"},
            alias_map={},
        )
        self.assertEqual(out.loc[0, "app_id"], "123")
        self.assertEqual(out.loc[0, "brand_id"], "c-vit")


class ExecuteStatementTests(unittest.TestCase):
    def setUp(self) -> None:
        self.config = DatabricksSQLConfig(
            host="https://example.databricks.com",
            token="token",
            warehouse_id="warehouse",
            poll_interval_seconds=0,
        )

    def test_execute_statement_polls_until_success(self) -> None:
        session = FakeSession(
            post_responses=[
                FakeResponse(
                    {
                        "statement_id": "stmt-1",
                        "status": {"state": "PENDING"},
                    }
                )
            ],
            get_responses=[
                FakeResponse({"statement_id": "stmt-1", "status": {"state": "RUNNING"}}),
                FakeResponse(
                    {
                        "statement_id": "stmt-1",
                        "status": {"state": "SUCCEEDED"},
                        "manifest": {
                            "schema": {
                                "columns": [
                                    {"name": "app_id"},
                                    {"name": "user_id"},
                                ]
                            }
                        },
                        "result": {
                            "data_array": [
                                ["123", "u1"],
                                ["124", "u2"],
                            ]
                        },
                    }
                ),
            ],
        )

        result = execute_statement(
            config=self.config,
            statement="SELECT 1",
            http_session=session,
        )

        self.assertEqual(result.columns, ["app_id", "user_id"])
        self.assertEqual(result.rows, [["123", "u1"], ["124", "u2"]])
        self.assertEqual(len(session.get_calls), 2)
        submitted_payload = session.post_calls[0]["json"]
        self.assertEqual(submitted_payload["format"], "JSON_ARRAY")
        self.assertEqual(submitted_payload["disposition"], "EXTERNAL_LINKS")

    def test_execute_statement_raises_on_failed_state(self) -> None:
        session = FakeSession(
            post_responses=[
                FakeResponse(
                    {
                        "statement_id": "stmt-fail",
                        "status": {"state": "FAILED", "error_message": "bad query"},
                    }
                )
            ]
        )

        with self.assertRaises(DatabricksStatementError) as ctx:
            execute_statement(
                config=self.config,
                statement="SELECT broken",
                http_session=session,
            )

        self.assertIn("stmt-fail", str(ctx.exception))
        self.assertIn("FAILED", str(ctx.exception))

    def test_execute_statement_collects_all_rows_with_chunk_index_pagination(self) -> None:
        session = FakeSession(
            post_responses=[
                FakeResponse(
                    {
                        "statement_id": "stmt-idx",
                        "status": {"state": "SUCCEEDED"},
                        "manifest": {
                            "schema": {
                                "columns": [
                                    {"name": "app_id"},
                                    {"name": "user_id"},
                                ]
                            }
                        },
                        "result": {
                            "data_array": [["123", "u1"]],
                            "next_chunk_index": 1,
                        },
                    }
                )
            ],
            get_responses=[
                FakeResponse(
                    {
                        "statement_id": "stmt-idx",
                        "result": {
                            "data_array": [["124", "u2"]],
                            "next_chunk_index": 2,
                        },
                    }
                ),
                FakeResponse(
                    {
                        "statement_id": "stmt-idx",
                        "result": {
                            "data_array": [["125", "u3"]],
                        },
                    }
                ),
            ],
        )

        result = execute_statement(
            config=self.config,
            statement="SELECT 1",
            http_session=session,
        )

        self.assertEqual(result.columns, ["app_id", "user_id"])
        self.assertEqual(result.rows, [["123", "u1"], ["124", "u2"], ["125", "u3"]])
        self.assertEqual(len(session.get_calls), 2)
        self.assertTrue(session.get_calls[0]["url"].endswith("/api/2.0/sql/statements/stmt-idx/result/chunks/1"))
        self.assertTrue(session.get_calls[1]["url"].endswith("/api/2.0/sql/statements/stmt-idx/result/chunks/2"))


class LoadTablesFromDatabricksSQLTests(unittest.TestCase):
    def test_loader_fetches_schema_then_data_and_applies_brand_alias(self) -> None:
        config = DatabricksSQLConfig(
            host="https://example.databricks.com",
            token="token",
            warehouse_id="warehouse",
            poll_interval_seconds=0,
        )
        selection = QuerySelection(
            app_ids=("123",),
            brand_aliases={"123": "c-vit"},
            start_date=None,
            end_date=None,
        )
        session = FakeSession(
            post_responses=[
                FakeResponse(
                    {
                        "statement_id": "schema-1",
                        "status": {"state": "SUCCEEDED"},
                        "manifest": {
                            "schema": {
                                "columns": [
                                    {"name": "app_id"},
                                    {"name": "user_id"},
                                    {"name": "line_id"},
                                    {"name": "external_id"},
                                ]
                            }
                        },
                        "result": {"data_array": []},
                    }
                ),
                FakeResponse(
                    {
                        "statement_id": "data-1",
                        "status": {"state": "SUCCEEDED"},
                        "manifest": {
                            "schema": {
                                "columns": [
                                    {"name": "app_id"},
                                    {"name": "user_id"},
                                    {"name": "line_id"},
                                    {"name": "external_id"},
                                ]
                            }
                        },
                        "result": {
                            "data_array": [
                                ["123", "u1", "line-1", "ext-1"],
                            ]
                        },
                    }
                ),
            ]
        )

        tables = load_tables_from_databricks_sql(
            config=config,
            selection=selection,
            columns_map={"user_identity": run_pipeline.COLUMNS_MAP["user_identity"]},
            http_session=session,
        )

        self.assertIn("user_identity", tables)
        self.assertEqual(
            tables["user_identity"].to_dict(orient="records"),
            [
                {
                    "app_id": "123",
                    "user_id": "u1",
                    "line_id": "line-1",
                    "external_id": "ext-1",
                    "brand_id": "c-vit",
                }
            ],
        )
        self.assertEqual(len(session.post_calls), 2)


class RuntimeValidationTests(unittest.TestCase):
    def test_databricks_mode_requires_connection_settings_and_app_ids(self) -> None:
        parser = run_pipeline.build_arg_parser()
        args = parser.parse_args(["--source-mode", "databricks_sql"])

        with self.assertRaises(ValueError) as ctx:
            run_pipeline._resolve_source_runtime(args)

        message = str(ctx.exception)
        self.assertIn("query_app_ids", message)
        self.assertIn("databricks_host", message)
        self.assertIn("databricks_token", message)
        self.assertIn("databricks_warehouse_id", message)

    def test_parquet_mode_stays_backward_compatible(self) -> None:
        parser = run_pipeline.build_arg_parser()
        args = parser.parse_args([])

        runtime_filters, config, selection = run_pipeline._resolve_source_runtime(args)

        self.assertIsNone(config)
        self.assertIsNone(selection)
        self.assertEqual(
            runtime_filters,
            {
                "c-vit": ["1993744540760190"],
                "see-chan": ["838315041537793"],
            },
        )

    def test_databricks_pyspark_mode_uses_default_brand_mapping(self) -> None:
        parser = run_pipeline.build_arg_parser()
        args = parser.parse_args(["--source-mode", "databricks_pyspark"])

        runtime_filters, config, selection = run_pipeline._resolve_source_runtime(args)

        self.assertIsNone(config)
        self.assertIsNotNone(selection)
        assert selection is not None
        self.assertEqual(
            runtime_filters,
            {
                "c-vit": ["1993744540760190"],
                "see-chan": ["838315041537793"],
            },
        )
        self.assertEqual(set(selection.app_ids), {"1993744540760190", "838315041537793"})
        self.assertEqual(
            selection.brand_aliases,
            {
                "1993744540760190": "c-vit",
                "838315041537793": "see-chan",
            },
        )

    def test_databricks_pyspark_mode_respects_query_ids_and_aliases(self) -> None:
        parser = run_pipeline.build_arg_parser()
        args = parser.parse_args(
            [
                "--source-mode",
                "databricks_pyspark",
                "--query-app-ids",
                "101,102",
                "--brand-aliases",
                "101=brand-a",
            ]
        )

        runtime_filters, config, selection = run_pipeline._resolve_source_runtime(args)

        self.assertIsNone(config)
        self.assertIsNotNone(selection)
        assert selection is not None
        self.assertEqual(
            runtime_filters,
            {
                "brand-a": ["101"],
                "102": ["102"],
            },
        )
        self.assertEqual(selection.app_ids, ("101", "102"))
        self.assertEqual(
            selection.brand_aliases,
            {
                "101": "brand-a",
                "102": "102",
            },
        )

    def test_publish_defaults_are_set(self) -> None:
        parser = run_pipeline.build_arg_parser()
        args = parser.parse_args([])

        cfg = run_pipeline._resolve_publish_runtime(args, source_mode="parquet")

        self.assertEqual(
            cfg,
            {
                "enabled": False,
                "table_name": "projects_prd.marketingautomation.kpis_predicted",
                "write_mode": "overwrite",
                "fail_on_cast_error": True,
            },
        )

    def test_publish_requires_databricks_pyspark_source_mode(self) -> None:
        parser = run_pipeline.build_arg_parser()
        args = parser.parse_args(["--publish-kpis-predicted", "true"])

        with self.assertRaises(ValueError) as ctx:
            run_pipeline._resolve_publish_runtime(args, source_mode="parquet")

        self.assertIn("--source-mode databricks_pyspark", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
