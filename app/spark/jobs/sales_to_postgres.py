from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType

import os

def main():
    # Create SparkSession - the entry point to Spark cluster
    # getOrCreate() returns existing session if already exists, otherwise creates new
    # Must be only one SparkSession in Python process
    spark = (
        SparkSession.builder
        .appName("sales-to-postgres")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN") 

    # Define schema for sales events JSON
    # Schema helps Spark parse JSON faster and more reliably than inferring types
    sales_schema = StructType([
        StructField("sale_id", StringType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("amount", DecimalType(10, 2), True),
        StructField("timestamp", TimestampType(), True),
    ])

    # Read streaming data from Kafka topic "sales"
    # This creates a lazy DataFrame, no actual reading happens until start()
    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "sales")
        .option("startingOffsets", "latest")
        .load()
    )

    # Parse JSON from Kafka messages
    # Kafka sends value as binary , so we use CAST to string
    # from_json parses string into structure according to schema
    # select("data.*") expands structure into separate columns
    parsed = (
        df.selectExpr("CAST(value AS STRING)", "offset", "partition", "topic")
        .select(
            from_json(col("value"), sales_schema).alias("data"),
            col("offset").alias("kafka_offset"),
            col("partition").alias("kafka_partition"),
            col("topic").alias("kafka_topic"),
        )
        .select("data.*", "kafka_offset", "kafka_partition", "kafka_topic")
    )

    # Write stream to PostgreSQL
    # foreachBatch - for each micro-batch call write_to_postgres function
    # outputMode("append") - only add new rows, don't update existing
    # trigger("10 seconds") - process accumulated data every 10 seconds
    # If job fails, restart continues from last checkpoint, no data loss
    query = (
        parsed.writeStream
        .foreachBatch(write_to_postgres)
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .option("checkpointLocation", "/opt/spark/checkpoints/sales")
        .start()
    )

    #  Keep the streaming job running until stopped
    query.awaitTermination()


def write_to_postgres(batch_df: DataFrame, batch_id: int):
    # batch_df - DataFrame with data accumulated during trigger interval
    # batch_id - sequential number of the micro-batch

    db_url = os.getenv("POSTGRES_URL")
    db_table = "sales"
    db_user = os.getenv("POSTGRES_USER")
    db_password = os.getenv("POSTGRES_PASSWORD")

    count = batch_df.count()
    print(f"Batch {batch_id}: writing {count} rows to PostgreSQL table 'sales'")

    batch_df.write \
        .format("jdbc") \
        .option("url", db_url) \
        .option("dbtable", db_table) \
        .option("user", db_user) \
        .option("password", db_password) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()


if __name__ == "__main__":
    main()