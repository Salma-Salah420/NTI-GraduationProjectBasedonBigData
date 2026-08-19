from pyspark.sql import SparkSession
from pyspark.sql.functions import col


INPUT_PATH = "hdfs://namenode:9000/maritime/raw/2024/ais-2024-01-01.csv"
OUTPUT_PATH = "hdfs://namenode:9000/maritime/processed/2024"


spark = (
    SparkSession.builder
    .appName("Maritime AIS Processing")
    .getOrCreate()
)

print("Reading NOAA AIS data...")

df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(INPUT_PATH)
)

input_count = df.count()

print(f"Input records: {input_count}")

clean_df = (
    df
    .withColumn("mmsi", col("mmsi").cast("long"))
    .withColumn("longitude", col("longitude").cast("double"))
    .withColumn("latitude", col("latitude").cast("double"))
    .withColumn("sog", col("sog").cast("double"))
    .withColumn("cog", col("cog").cast("double"))
    .withColumn("heading", col("heading").cast("double"))
    .filter(col("mmsi").isNotNull())
    .filter(col("latitude").isNotNull())
    .filter(col("longitude").isNotNull())
)

output_count = clean_df.count()

print(f"Clean records: {output_count}")
print(f"Removed records: {input_count - output_count}")

print("Writing Parquet...")

(
    clean_df
    .write
    .mode("overwrite")
    .parquet(OUTPUT_PATH)
)

print(f"Processed data written to: {OUTPUT_PATH}")

spark.stop()
