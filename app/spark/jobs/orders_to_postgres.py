from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col, row_number, desc
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DecimalType, TimestampType
)

import os


def main():
    spark = (
        SparkSession.builder
        .appName("ordres-to-postgres")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    orders_schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("order_number", StringType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price", DecimalType(10, 2), True),
        StructField("status", StringType(), True),
        StructField("total", DecimalType(10, 2), True),
        StructField("timestamp", TimestampType(), True),
    ])

    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "orders")
        .option("startingOffsets", "latest")
        .load()
    )

    parsed = (
        df.selectExpr("CAST(value AS STRING)", "offset", "partition", "topic")
        .select(
            from_json(col("value"), orders_schema).alias("data"),
            col("offset").alias("kafka_offset"),
            col("partition").alias("kafka_partition"),
            col("topic").alias("kafka_topic"),
        )
        .select("data.*", "kafka_offset", "kafka_partition", "kafka_topic")
    )

    repartitioned = parsed.repartition(4, "order_number")

    # Write stream to PostgreSQL
    query = (
        repartitioned.writeStream
        .foreachBatch(write_to_postgres)
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .option("checkpointLocation", "/opt/spark/checkpoints/orders")
        .start()
    )

    query.awaitTermination()


def write_to_postgres(batch_df: DataFrame, batch_id: int):
    window_spec = Window.partitionBy("order_id").orderBy(desc("timestamp"))
    deduplicated = (
        batch_df
        .withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )

    count = deduplicated.count()
    print(f"Batch {batch_id}: processing {count} rows")

    deduplicated.foreachPartition(upsert_partition)

    print(f"Batch {batch_id}: upsert completed")


def upsert_partition(iterator):
    """Upsert rows from a single partition to PostgreSQL."""
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
                INSERT INTO orders (
                    order_id, order_number, user_id, product_id, quantity,
                    price, status, total, timestamp,
                    kafka_offset, kafka_partition, kafka_topic
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (order_id) 
                DO UPDATE SET
                    status = EXCLUDED.status,
                    timestamp = EXCLUDED.timestamp,
                    kafka_offset = EXCLUDED.kafka_offset,
                    kafka_partition = EXCLUDED.kafka_partition,
                    kafka_topic = EXCLUDED.kafka_topic
            """, (
                row.order_id, row.order_number, row.user_id, row.product_id,
                row.quantity, row.price, row.status, row.total, row.timestamp,
                row.kafka_offset, row.kafka_partition, row.kafka_topic
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