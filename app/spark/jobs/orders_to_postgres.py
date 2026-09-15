from pyspark.sql import DataFrame
from pyspark.sql.functions import col, row_number, desc
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DecimalType, TimestampType
)

from app.common_utils.connections.config import kafka_config
from app.spark.utils.streaming import create_spark_session, read_kafka_json_stream, run_streaming_query
from app.spark.utils.partition_upserter import make_partition_upserter


ORDERS_UPSERT_SQL = """
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
"""

ORDERS_COLUMNS = [
    "order_id", "order_number", "user_id", "product_id", "quantity",
    "price", "status", "total", "timestamp",
    "kafka_offset", "kafka_partition", "kafka_topic",
]

upsert_partition = make_partition_upserter(ORDERS_UPSERT_SQL, ORDERS_COLUMNS)


def main():
    spark = create_spark_session(
        app_name="ordres-to-postgres",
        extra_config={"spark.ui.showConsoleProgress": "false"},
    )

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

    parsed = read_kafka_json_stream(
        spark,
        topic="orders",
        schema=orders_schema,
        bootstrap_servers=kafka_config.bootstrap_servers,
    )

    repartitioned = parsed.repartition(4, "order_number")

    run_streaming_query(
        repartitioned,
        foreach_batch_fn=write_to_postgres,
        checkpoint_location="/opt/spark/checkpoints/orders",
    )


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


if __name__ == "__main__":
    main()