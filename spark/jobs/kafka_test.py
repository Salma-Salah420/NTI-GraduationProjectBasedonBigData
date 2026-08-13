from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col

spark = SparkSession.builder \
    .appName("KafkaAISProcessor") \
    .getOrCreate()

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "ais-live") \
    .option("startingOffsets", "latest") \
    .load()

parsed = df.select(
    split(col("value").cast("string"), ",").getItem(0).alias("mmsi"),
    split(col("value").cast("string"), ",").getItem(1).alias("message"),
    col("topic"),
    col("partition"),
    col("offset")
)

query = parsed.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", "false") \
    .option("checkpointLocation", "/tmp/kafka-ais-processor-checkpoint") \
    .start()

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("Stopping streaming query...")
    query.stop()
finally:
    spark.stop()
    print("Spark session stopped.")