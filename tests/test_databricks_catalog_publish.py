from __future__ import annotations

import unittest

import pandas as pd

from src.databricks_catalog_publish import (
    _align_pred_df_to_table_schema,
    publish_kpis_predicted_to_catalog,
)


class _FakeDataType:
    def __init__(self, name: str) -> None:
        self._name = str(name)

    def typeName(self) -> str:
        return self._name


class _FakeField:
    def __init__(self, name: str, dtype: str) -> None:
        self.name = str(name)
        self.dataType = _FakeDataType(dtype)


class _FakeTable:
    def __init__(self, schema, row_count: int) -> None:
        self.schema = schema
        self._row_count = int(row_count)

    def count(self) -> int:
        return self._row_count


class _FakeWriter:
    def __init__(self, spark) -> None:
        self.spark = spark

    def mode(self, mode_name: str):
        self.spark.write_mode = str(mode_name)
        return self

    def saveAsTable(self, table_name: str) -> None:
        self.spark.saved_table = str(table_name)


class _FakeSparkDataFrame:
    def __init__(self, spark) -> None:
        self.spark = spark
        self.write = _FakeWriter(spark)

    def createOrReplaceTempView(self, view_name: str) -> None:
        self.spark.temp_view_name = str(view_name)


class _FakeSparkSession:
    def __init__(self, schema, table_rows_after_write: int = 0) -> None:
        self.schema = schema
        self.table_rows_after_write = int(table_rows_after_write)
        self.created_pdf = None
        self.created_schema = None
        self.write_mode = None
        self.saved_table = None
        self.temp_view_name = None
        self.executed_sql = None
        self.catalog = self

    def table(self, table_name: str):
        self.last_table = str(table_name)
        return _FakeTable(schema=self.schema, row_count=self.table_rows_after_write)

    def createDataFrame(self, pdf, schema=None):
        self.created_pdf = pdf.copy()
        self.created_schema = schema
        return _FakeSparkDataFrame(self)

    def sql(self, sql_text: str):
        self.executed_sql = str(sql_text)
        return None

    def dropTempView(self, view_name: str) -> None:
        self.dropped_temp_view = str(view_name)


class AlignPredDfTests(unittest.TestCase):
    def test_align_adds_missing_columns_drops_extra_and_reorders(self) -> None:
        pred_df = pd.DataFrame(
            [
                {
                    "b": "1.5",
                    "a": 100,
                    "extra": "ignored",
                }
            ]
        )
        schema = [
            _FakeField("a", "string"),
            _FakeField("b", "double"),
            _FakeField("c", "bigint"),
        ]

        out = _align_pred_df_to_table_schema(pred_df=pred_df, table_schema=schema, fail_on_cast_error=True)

        self.assertEqual(list(out.columns), ["a", "b", "c"])
        self.assertEqual(out.loc[0, "a"], "100")
        self.assertAlmostEqual(float(out.loc[0, "b"]), 1.5)
        self.assertTrue(pd.isna(out.loc[0, "c"]))

    def test_align_serializes_nested_to_string(self) -> None:
        pred_df = pd.DataFrame(
            [
                {
                    "drivers": [{"key": "gmv_net"}],
                    "payload": {"a": 1},
                }
            ]
        )
        schema = [
            _FakeField("drivers", "string"),
            _FakeField("payload", "string"),
        ]

        out = _align_pred_df_to_table_schema(pred_df=pred_df, table_schema=schema, fail_on_cast_error=True)

        self.assertEqual(out.loc[0, "drivers"], '[{"key": "gmv_net"}]')
        self.assertEqual(out.loc[0, "payload"], '{"a": 1}')

    def test_align_casts_timestamp_and_bool_and_numeric(self) -> None:
        pred_df = pd.DataFrame(
            [
                {
                    "window_end_date": "2026-01-01T00:00:00Z",
                    "score": "95.5",
                    "count": "2",
                    "is_good": "true",
                },
                {
                    "window_end_date": "bad-ts",
                    "score": "bad-num",
                    "count": "2.5",
                    "is_good": "unknown",
                },
            ]
        )
        schema = [
            _FakeField("window_end_date", "timestamp"),
            _FakeField("score", "double"),
            _FakeField("count", "bigint"),
            _FakeField("is_good", "boolean"),
        ]

        out = _align_pred_df_to_table_schema(pred_df=pred_df, table_schema=schema, fail_on_cast_error=False)

        self.assertEqual(str(out.loc[0, "window_end_date"]), "2026-01-01 00:00:00")
        self.assertAlmostEqual(float(out.loc[0, "score"]), 95.5)
        self.assertEqual(int(out.loc[0, "count"]), 2)
        self.assertTrue(bool(out.loc[0, "is_good"]))
        self.assertTrue(pd.isna(out.loc[1, "window_end_date"]))
        self.assertTrue(pd.isna(out.loc[1, "score"]))
        self.assertTrue(pd.isna(out.loc[1, "count"]))
        self.assertTrue(pd.isna(out.loc[1, "is_good"]))

    def test_align_fails_on_cast_error_when_enabled(self) -> None:
        pred_df = pd.DataFrame([{"score": "bad-num"}])
        schema = [_FakeField("score", "double")]

        with self.assertRaises(ValueError) as ctx:
            _align_pred_df_to_table_schema(pred_df=pred_df, table_schema=schema, fail_on_cast_error=True)

        self.assertIn("score", str(ctx.exception))


class PublishToCatalogTests(unittest.TestCase):
    def test_publish_overwrite_uses_table_schema_and_returns_summary(self) -> None:
        schema = [
            _FakeField("brand_id", "string"),
            _FakeField("predicted_health_score", "double"),
            _FakeField("drivers", "string"),
        ]
        spark = _FakeSparkSession(schema=schema, table_rows_after_write=12)
        pred_df = pd.DataFrame(
            [
                {
                    "brand_id": "c-vit",
                    "predicted_health_score": "77.5",
                    "drivers": [{"key": "gmv_net"}],
                    "extra_col": "should_drop",
                }
            ]
        )

        summary = publish_kpis_predicted_to_catalog(
            pred_df=pred_df,
            table_name="projects_prd.marketingautomation.kpis_predicted",
            write_mode="overwrite",
            fail_on_cast_error=True,
            spark_session=spark,
        )

        self.assertEqual(summary["rows_written"], 1)
        self.assertEqual(summary["columns_written"], 3)
        self.assertEqual(summary["table_rows_after_write"], 12)
        self.assertEqual(spark.write_mode, "overwrite")
        self.assertEqual(spark.saved_table, "projects_prd.marketingautomation.kpis_predicted")
        self.assertEqual(list(spark.created_pdf.columns), ["brand_id", "predicted_health_score", "drivers"])

    def test_publish_merge_uses_row_level_upsert_sql(self) -> None:
        schema = [
            _FakeField("brand_id", "string"),
            _FakeField("window_end_date", "timestamp"),
            _FakeField("window_size", "string"),
            _FakeField("predicted_health_score", "double"),
        ]
        spark = _FakeSparkSession(schema=schema, table_rows_after_write=25)
        pred_df = pd.DataFrame(
            [
                {
                    "brand_id": "c-vit",
                    "window_end_date": "2026-03-07T00:00:00Z",
                    "window_size": "30d",
                    "predicted_health_score": "77.5",
                }
            ]
        )

        summary = publish_kpis_predicted_to_catalog(
            pred_df=pred_df,
            table_name="projects_prd.marketingautomation.kpis_predicted",
            write_mode="merge",
            fail_on_cast_error=True,
            spark_session=spark,
        )

        self.assertEqual(summary["rows_written"], 1)
        self.assertEqual(summary["table_rows_after_write"], 25)
        self.assertIsNone(spark.write_mode)
        self.assertIsNotNone(spark.temp_view_name)
        self.assertEqual(spark.temp_view_name, spark.dropped_temp_view)
        self.assertIn("MERGE INTO projects_prd.marketingautomation.kpis_predicted AS target", str(spark.executed_sql))
        self.assertIn("WHEN MATCHED THEN UPDATE SET", str(spark.executed_sql))
        self.assertIn("target.`brand_id` <=> source.`brand_id`", str(spark.executed_sql))
        self.assertIn("target.`window_end_date` <=> source.`window_end_date`", str(spark.executed_sql))
        self.assertIn("target.`window_size` <=> source.`window_size`", str(spark.executed_sql))

    def test_publish_merge_rejects_duplicate_source_keys(self) -> None:
        schema = [
            _FakeField("brand_id", "string"),
            _FakeField("window_end_date", "timestamp"),
            _FakeField("window_size", "string"),
            _FakeField("predicted_health_score", "double"),
        ]
        spark = _FakeSparkSession(schema=schema, table_rows_after_write=0)
        pred_df = pd.DataFrame(
            [
                {
                    "brand_id": "c-vit",
                    "window_end_date": "2026-03-07T00:00:00Z",
                    "window_size": "30d",
                    "predicted_health_score": "77.5",
                },
                {
                    "brand_id": "c-vit",
                    "window_end_date": "2026-03-07T00:00:00Z",
                    "window_size": "30d",
                    "predicted_health_score": "80.0",
                },
            ]
        )

        with self.assertRaises(ValueError) as ctx:
            publish_kpis_predicted_to_catalog(
                pred_df=pred_df,
                table_name="projects_prd.marketingautomation.kpis_predicted",
                write_mode="merge",
                fail_on_cast_error=True,
                spark_session=spark,
            )

        self.assertIn("unique source rows per merge key", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
