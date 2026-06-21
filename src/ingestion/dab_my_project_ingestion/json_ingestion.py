import argparse
from pyspark.sql.types import StructType
from databricks.sdk.runtime import spark


def main():
    parser = argparse.ArgumentParser(description="Batch JSON ingestion via Spark Structured Streaming")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--table_name", required=True)
    parser.add_argument("--source_folder", required=True)
    args = parser.parse_args()

    source_path = f"/Volumes/{args.catalog}/test_schema/source_json_volume/{args.source_folder}/"
    checkpoint_path = f"/Volumes/{args.catalog}/test_schema/source_json_volume/checkpoints/{args.table_name}"
    target_table = f"{args.catalog}.{args.schema}.{args.table_name}"

    raw_schema = spark.read.option("multiLine", "true").json(source_path).schema
    # Spark adds _corrupt_record when it finds no valid JSON records (empty folder or all files malformed)
    clean_fields = [f for f in raw_schema.fields if f.name != "_corrupt_record"]
    if not clean_fields:
        raise ValueError(
            f"Could not infer schema from JSON files at {source_path}. "
            "Ensure the source folder exists and contains at least one valid JSON file."
        )
    inferred_schema = StructType(clean_fields)

    stream_df = spark.readStream.option("multiLine", "true").schema(inferred_schema).json(source_path)

    def write_batch(batch_df, batch_id):
        batch_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(target_table)

    query = (
        stream_df.writeStream.foreachBatch(write_batch)
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
