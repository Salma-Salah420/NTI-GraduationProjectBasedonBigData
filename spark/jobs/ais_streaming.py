from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
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

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "ais-events") \
    .option("startingOffsets", "latest") \
    .load()

parsed = df.select(
    from_json(
        col("value").cast("string"),
        schema
    ).alias("data")
).select("data.*")

query = parsed.writeStream \
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

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("Stopping streaming query...")
    query.stop()
finally:
    spark.stop()
    print("Spark session stopped.")