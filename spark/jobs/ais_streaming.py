from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, to_timestamp
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    DoubleType
)

spark = SparkSession.builder \
    .appName("MaritimeAISStreaming") \
    .getOrCreate()

schema = StructType([
    StructField("mmsi", LongType(), True),
    StructField("base_date_time", StringType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("sog", DoubleType(), True),
    StructField("cog", DoubleType(), True)
])

raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "ais-events") \
    .option("startingOffsets", "latest") \
    .load()

parsed = raw.select(
    col("value").cast("string").alias("raw_value"),
    col("topic"),
    col("partition"),
    col("offset"),
    from_json(
        col("value").cast("string"),
        schema
    ).alias("data")
)

valid = parsed \
    .filter(col("data").isNotNull()) \
    .select(
        "data.*",
        "topic",
        "partition",
        "offset"
    ) \
    .withColumn(
        "base_date_time",
        to_timestamp(col("base_date_time"), "yyyy-MM-dd HH:mm:ss")
    ) \
    .withColumn(
        "processed_at",
        current_timestamp()
    )

rejected = parsed \
    .filter(col("data").isNull()) \
    .select(
        "raw_value",
        "topic",
        "partition",
        "offset"
    ) \
    .withColumn(
        "rejected_at",
        current_timestamp()
    )

valid_query = valid.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option(
        "path",
        "hdfs://namenode:9000/maritime/streaming"
    ) \
    .option(
        "checkpointLocation",
        "hdfs://namenode:9000/maritime/checkpoints/ais-streaming"
    ) \
    .start()

rejected_query = rejected.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option(
        "path",
        "hdfs://namenode:9000/maritime/rejected"
    ) \
    .option(
        "checkpointLocation",
        "hdfs://namenode:9000/maritime/checkpoints/ais-rejected"
    ) \
    .start()

try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("Stopping streaming queries...")
    valid_query.stop()
    rejected_query.stop()
finally:
    spark.stop()
    print("Spark session stopped.")
