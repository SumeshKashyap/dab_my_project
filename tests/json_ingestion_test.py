from unittest.mock import MagicMock
import pytest
from pyspark.sql.types import StructType, StructField, StringType, LongType

from dab_my_project_ingestion.json_ingestion import infer_json_schema, run_ingestion


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_spark(schema: StructType, batch_row_count: int = 2):
    """Return (spark_mock, batch_df_mock, write_stream_writer_mock).

    Wires up the Spark fluent-API chain so that:
    - spark.read.option(...).json(...).schema  → the supplied schema
    - spark.readStream...foreachBatch(fn)      → calls fn(batch_df, 0) immediately
                                                 then returns a write-stream writer mock
    """
    spark = MagicMock()

    # Schema inference path
    read_df = MagicMock()
    read_df.schema = schema
    spark.read.option.return_value.json.return_value = read_df

    # Streaming path
    batch_df = MagicMock()
    batch_df.count.return_value = batch_row_count

    write_stream_writer = MagicMock()

    def invoke_foreach_batch(fn):
        fn(batch_df, 0)          # simulate one micro-batch
        return write_stream_writer

    stream_df = MagicMock()
    stream_df.writeStream.foreachBatch.side_effect = invoke_foreach_batch
    spark.readStream.option.return_value.schema.return_value.json.return_value = stream_df

    return spark, batch_df, write_stream_writer


VALID_SCHEMA = StructType([StructField("id", LongType()), StructField("name", StringType())])
CORRUPT_ONLY_SCHEMA = StructType([StructField("_corrupt_record", StringType())])
MIXED_SCHEMA = StructType([StructField("id", LongType()), StructField("_corrupt_record", StringType())])


# ---------------------------------------------------------------------------
# infer_json_schema tests
# ---------------------------------------------------------------------------

def test_infer_schema_strips_corrupt_record_field():
    spark = MagicMock()
    spark.read.option.return_value.json.return_value.schema = MIXED_SCHEMA

    result = infer_json_schema(spark, "/fake/path")

    field_names = [f.name for f in result.fields]
    assert "_corrupt_record" not in field_names
    assert "id" in field_names


def test_infer_schema_raises_when_only_corrupt_record_present():
    spark = MagicMock()
    spark.read.option.return_value.json.return_value.schema = CORRUPT_ONLY_SCHEMA

    with pytest.raises(ValueError, match="Could not infer schema"):
        infer_json_schema(spark, "/fake/empty/path")


def test_infer_schema_raises_on_empty_schema():
    spark = MagicMock()
    spark.read.option.return_value.json.return_value.schema = StructType([])

    with pytest.raises(ValueError, match="Could not infer schema"):
        infer_json_schema(spark, "/fake/empty/path")


# ---------------------------------------------------------------------------
# run_ingestion tests
# ---------------------------------------------------------------------------

def test_run_ingestion_returns_total_record_count():
    spark, _, _ = _make_spark(VALID_SCHEMA, batch_row_count=7)

    total = run_ingestion(spark, "/src", "/chk", "cat.sch.tbl")

    assert total == 7


def test_run_ingestion_writes_in_delta_append_mode():
    spark, batch_df, _ = _make_spark(VALID_SCHEMA)

    run_ingestion(spark, "/src", "/chk", "cat.sch.tbl")

    batch_df.write.format.assert_called_once_with("delta")
    batch_df.write.format.return_value.mode.assert_called_once_with("append")


def test_run_ingestion_enables_merge_schema():
    spark, batch_df, _ = _make_spark(VALID_SCHEMA)

    run_ingestion(spark, "/src", "/chk", "cat.sch.tbl")

    batch_df.write.format.return_value.mode.return_value.option.assert_called_once_with("mergeSchema", "true")


def test_run_ingestion_saves_to_correct_table():
    spark, batch_df, _ = _make_spark(VALID_SCHEMA)
    target = "my_catalog.my_schema.my_table"

    run_ingestion(spark, "/src", "/chk", target)

    batch_df.write.format.return_value.mode.return_value.option.return_value.saveAsTable.assert_called_once_with(target)


def test_run_ingestion_sets_checkpoint_location():
    spark, _, write_stream_writer = _make_spark(VALID_SCHEMA)

    run_ingestion(spark, "/src", "/my/checkpoint", "cat.sch.tbl")

    write_stream_writer.option.assert_called_once_with("checkpointLocation", "/my/checkpoint")
