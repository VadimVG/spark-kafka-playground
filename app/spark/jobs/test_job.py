from pyspark.sql import SparkSession


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("test-job")
        .getOrCreate()
    )

    df = spark.range(100)

    print("!!! DATA !!!")
    df.show()

    print("!!! COUNT !!!")
    print(df.count())

    spark.stop()


if __name__ == "__main__":
    main()