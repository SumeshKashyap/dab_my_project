import argparse
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def infer_json_schema(spark: SparkSession, source_path: str) -> StructType:
    """Infers schema from JSON files at source_path, stripping the _corrupt_record sentinel field.

    Raises ValueError if no valid JSON fields are found (empty folder or all files malformed).
    """
    logger.info("Inferring schema from JSON files at %s", source_path)
    raw_schema = spark.read.option("multiLine", "true").json(source_path).schema
    # Spark adds _corrupt_record when it finds no valid JSON records (empty folder or all files malformed)
    clean_fields = [f for f in raw_schema.fields if f.name != "_corrupt_record"]
    if not clean_fields:
        raise ValueError(
            f"Could not infer schema from JSON files at {source_path}. "
            "Ensure the source folder exists and contains at least one valid JSON file."
        )
    inferred_schema = StructType(clean_fields)
    logger.info("Schema inferred with %d field(s): %s", len(clean_fields), [f.name for f in clean_fields])
    return inferred_schema


def run_ingestion(spark: SparkSession, source_path: str, checkpoint_path: str, target_table: str) -> int:
    """Runs batch JSON ingestion from source_path into target_table via Spark Structured Streaming.

    Returns the total number of records written.
    """
    inferred_schema = infer_json_schema(spark, source_path)

    stream_df = spark.readStream.option("multiLine", "true").schema(inferred_schema).json(source_path)
    logger.info("Streaming read initialised with checkpoint at %s", checkpoint_path)

    total_records = 0

    def write_batch(batch_df, batch_id):
        nonlocal total_records
        record_count = batch_df.count()
        total_records += record_count
        logger.info("Batch %d: %d record(s) read from source", batch_id, record_count)
        batch_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(target_table)
        logger.info("Batch %d: write to %s completed successfully", batch_id, target_table)

    query = (
        stream_df.writeStream.foreachBatch(write_batch)
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .start()
    )

    query.awaitTermination()
    logger.info("Ingestion complete: %d total record(s) written to %s", total_records, target_table)
    return total_records


def main():
    parser = argparse.ArgumentParser(description="Batch JSON ingestion via Spark Structured Streaming")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--table_name", required=True)
    parser.add_argument("--source_folder", required=True)
    args = parser.parse_args()

    from databricks.sdk.runtime import spark  # noqa: PLC0415

    source_path = f"/Volumes/{args.catalog}/test_schema/source_json_volume/{args.source_folder}/"
    checkpoint_path = f"/Volumes/{args.catalog}/test_schema/source_json_volume/checkpoints/{args.table_name}"
    target_table = f"{args.catalog}.{args.schema}.{args.table_name}"

    logger.info("Ingestion started: source=%s, target=%s", source_path, target_table)
    run_ingestion(spark, source_path, checkpoint_path, target_table)


if __name__ == "__main__":
    main()
