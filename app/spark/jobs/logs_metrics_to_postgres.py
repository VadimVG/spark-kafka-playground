from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, TimestampType
)

import os


def main():
    """
    Streaming pipeline: Kafka logs topic -> Spark -> PostgreSQL (aggregated metrics).
    
    Calculates count of user actions per page per minute.
    Uses window aggregation + watermark for late data handling.
    Upserts results into logs_metrics table.
    """
    spark = (
        SparkSession.builder
        .appName("logs-metrics-to-postgres")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    logs_schema = StructType([
        StructField("log_id", StringType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("action", StringType(), True),
        StructField("page", StringType(), True),
        StructField("timestamp", TimestampType(), True),
    ])

    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "logs")
        .option("startingOffsets", "latest")
        .load()
    )

    parsed = (
        df.selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), logs_schema).alias("data"))
        .select("data.*")
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

    # Write to PostgreSQL with upsert
    query = (
        aggregated.writeStream
        .foreachBatch(write_to_postgres)
        .outputMode("update")  # только изменённые агрегаты
        .trigger(processingTime="10 seconds")
        .option("checkpointLocation", "/opt/spark/checkpoints/logs_metrics")
        .start()
    )

    query.awaitTermination()


def write_to_postgres(batch_df: DataFrame, batch_id: int) -> None:
    """
    Upsert aggregated metrics to PostgreSQL.
    If metric already exists for window+action+page - update count.
    If not - insert new row.
    """
    count = batch_df.count()
    print(f"Batch {batch_id}: processing {count} metric rows")

    # Раскрываем window-структуру
    flattened = batch_df.select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("action"),
        col("page"),
        col("count").alias("cnt"),
    )

    # Upsert по партициям
    flattened.foreachPartition(upsert_partition)

    print(f"Batch {batch_id}: metrics upserted")


def upsert_partition(iterator):
    """Upsert metric rows from a single partition."""
    import psycopg2

    db_host = os.getenv("POSTGRES_HOST")
    db_name = os.getenv("POSTGRES_DB")
    db_user = os.getenv("POSTGRES_USER")
    db_password = os.getenv("POSTGRES_PASSWORD")

    conn = psycopg2.connect(
        host=db_host,
        database=db_name,
        user=db_user,
        password=db_password
    )
    cursor = conn.cursor()

    try:
        for row in iterator:
            cursor.execute("""
                INSERT INTO logs_metrics (
                    window_start, window_end, action, page, count
                ) VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (window_start, window_end, action, page) 
                DO UPDATE SET count = EXCLUDED.count
            """, (
                row.window_start, row.window_end, row.action, row.page, row.cnt
            ))

        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Upsert partition failed: {e}")
        raise
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()