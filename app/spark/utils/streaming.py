from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType
from pyspark.sql.streaming import StreamingQuery

import logging
from typing import Callable


def create_spark_session(
    app_name: str,
    extra_config: dict[str, str] | None = None,
) -> SparkSession:
    """
    Create (or reuse) a SparkSession with the defaults shared by all streaming jobs.
    extra_config lets a specific job add options on top (e.g. spark.ui.showConsoleProgress)
    without repeating the builder chain.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        force=True,
    )
    
    builder = SparkSession.builder.appName(app_name)
    for key, value in (extra_config or {}).items():
        builder = builder.config(key, value)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_kafka_json_stream(
    spark: SparkSession,
    topic: str,
    schema: StructType,
    bootstrap_servers: str,
    starting_offsets: str = "latest",
    include_kafka_metadata: bool = True,
) -> DataFrame:
    """
    Subscribe to a Kafka topic and parse its JSON payload according to `schema`.
    When include_kafka_metadata is True, also exposes kafka_offset/kafka_partition/kafka_topic
    alongside the parsed fields.
    """
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .load()
    )

    if include_kafka_metadata:
        return (
            raw.selectExpr("CAST(value AS STRING)", "offset", "partition", "topic")
            .select(
                from_json(col("value"), schema).alias("data"),
                col("offset").alias("kafka_offset"),
                col("partition").alias("kafka_partition"),
                col("topic").alias("kafka_topic"),
            )
            .select("data.*", "kafka_offset", "kafka_partition", "kafka_topic")
        )

    return (
        raw.selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), schema).alias("data"))
        .select("data.*")
    )


def run_streaming_query(
    df: DataFrame,
    foreach_batch_fn: Callable[[DataFrame, int], None],
    checkpoint_location: str,
    output_mode: str = "append",
    trigger_seconds: int = 10,
) -> StreamingQuery:
    """
    Start a foreachBatch streaming query with the checkpoint/trigger conventions
    shared across jobs, and block until it terminates.
    """
    query = (
        df.writeStream
        .foreachBatch(foreach_batch_fn)
        .outputMode(output_mode)
        .trigger(processingTime=f"{trigger_seconds} seconds")
        .option("checkpointLocation", checkpoint_location)
        .start()
    )
    query.awaitTermination()
    return query
