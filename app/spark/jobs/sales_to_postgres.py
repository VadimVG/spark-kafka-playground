from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType

from app.common_utils.connections.config import pg_config, kafka_config
from app.spark.utils.logging import log_spark_batch
from app.spark.utils.streaming import create_spark_session, read_kafka_json_stream, run_streaming_query


def main():
    spark = create_spark_session("sales-to-postgres")

    sales_schema = StructType([
        StructField("sale_id", StringType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("amount", DecimalType(10, 2), True),
        StructField("timestamp", TimestampType(), True),
    ])

    parsed = read_kafka_json_stream(
        spark,
        topic="sales",
        schema=sales_schema,
        bootstrap_servers=kafka_config.bootstrap_servers,
    )

    run_streaming_query(
        parsed,
        foreach_batch_fn=write_to_postgres,
        checkpoint_location="/opt/spark/checkpoints/sales",
    )


@log_spark_batch("sales")
def write_to_postgres(batch_df: DataFrame, batch_id: int) -> None:
    batch_df.write \
        .format("jdbc") \
        .option("url", pg_config.pg_url) \
        .option("dbtable", "sales") \
        .option("user", pg_config.user) \
        .option("password", pg_config.password) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()


if __name__ == "__main__":
    main()