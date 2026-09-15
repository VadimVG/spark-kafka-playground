from pyspark.sql import DataFrame
from pyspark.sql.functions import col, window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, TimestampType
)

from app.common_utils.connections.config import kafka_config
from app.spark.utils.logging import log_spark_batch
from app.spark.utils.streaming import create_spark_session, read_kafka_json_stream, run_streaming_query
from app.spark.utils.partition_upserter import make_partition_upserter


LOGS_METRICS_UPSERT_SQL = """
    INSERT INTO logs_metrics (
        window_start, window_end, action, page, count
    ) VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (window_start, window_end, action, page)
    DO UPDATE SET count = EXCLUDED.count
"""

# names of Row fields, in the same order as %s placeholders above.
# "cnt" is used instead of "count" because Row.count is a built-in method,
# so a column named "count" gets renamed to "cnt" earlier in the pipeline
LOGS_METRICS_ROW_FIELDS = ["window_start", "window_end", "action", "page", "cnt"]

upsert_partition = make_partition_upserter(LOGS_METRICS_UPSERT_SQL, LOGS_METRICS_ROW_FIELDS)


def main():
    """
    Streaming pipeline: Kafka logs topic -> Spark -> PostgreSQL (aggregated metrics).
    
    Calculates count of user actions per page per minute.
    Uses window aggregation + watermark for late data handling.
    Upserts results into logs_metrics table.
    """
    spark = create_spark_session(
        "logs-metrics-to-postgres",
        extra_config={"spark.ui.showConsoleProgress": "false"},
    )

    logs_schema = StructType([
        StructField("log_id", StringType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("action", StringType(), True),
        StructField("page", StringType(), True),
        StructField("timestamp", TimestampType(), True),
    ])

    parsed = read_kafka_json_stream(
        spark,
        topic="logs",
        schema=logs_schema,
        bootstrap_servers=kafka_config.bootstrap_servers,
        include_kafka_metadata=False,
    )

    # Aggregation: count actions per page per 1-minute window
    # Watermark: wait up to 5 minutes for late events
    aggregated = (
        parsed
        .withWatermark("timestamp", "5 minutes")
        .groupBy(
            window("timestamp", "1 minute"),
            col("action"),
            col("page"),
        )
        .count()
    )

    run_streaming_query(
        aggregated,
        foreach_batch_fn=write_to_postgres,
        checkpoint_location="/opt/spark/checkpoints/logs_metrics",
        output_mode="update",
    )


@log_spark_batch("logs_metrics")
def write_to_postgres(batch_df: DataFrame, batch_id: int) -> None:
    flattened = batch_df.select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("action"),
        col("page"),
        col("count").alias("cnt"),
    )

    flattened.foreachPartition(upsert_partition)


if __name__ == "__main__":
    main()