from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


spark = (
    SparkSession.builder
    .appName("Maritime AIS Feature Engineering")
    .getOrCreate()
)

# =========================
# Paths
# =========================

INPUT_PATH =  "hdfs://namenode:9000/maritime/raw/2024/*.csv" 
OUTPUT_PATH = "hdfs://namenode:9000/maritime/features/2024"

print("Reading NOAA AIS data...")

# =========================
# Read raw data
# =========================

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(INPUT_PATH)
)

print(f"Input records: {df.count()}")

# =========================
# Basic type conversion
# =========================

df = (
    df
    .withColumn("mmsi", F.col("mmsi").cast("long"))
    .withColumn("longitude", F.col("longitude").cast("double"))
    .withColumn("latitude", F.col("latitude").cast("double"))
    .withColumn("sog", F.col("sog").cast("double"))
    .withColumn("cog", F.col("cog").cast("double"))
    .withColumn("heading", F.col("heading").cast("double"))
    .withColumn("base_date_time", F.to_timestamp("base_date_time"))
)

# =========================
# Remove invalid/null records
# =========================

df = df.filter(
    F.col("mmsi").isNotNull()
    & F.col("latitude").isNotNull()
    & F.col("longitude").isNotNull()
    & F.col("base_date_time").isNotNull()
)

# =========================
# Coordinate validation
# =========================

df = df.filter(
    (F.col("latitude") >= -90)
    & (F.col("latitude") <= 90)
    & (F.col("longitude") >= -180)
    & (F.col("longitude") <= 180)
)

# =========================
# Deduplication
# =========================

before_dedup = df.count()

df = df.dropDuplicates(
    [
        "mmsi",
        "base_date_time",
        "longitude",
        "latitude",
        "sog",
        "cog",
        "heading"
    ]
)

after_dedup = df.count()

print(f"Records before deduplication: {before_dedup}")
print(f"Records after deduplication: {after_dedup}")
print(f"Duplicates removed: {before_dedup - after_dedup}")

# =========================
# Timestamp features
# =========================

df = (
    df
    .withColumn("date", F.to_date("base_date_time"))
    .withColumn("hour", F.hour("base_date_time"))
)

# =========================
# Window per vessel
# =========================

window_spec = (
    Window
    .partitionBy("mmsi")
    .orderBy("base_date_time")
)

# Previous values
df = (
    df
    .withColumn("prev_latitude", F.lag("latitude").over(window_spec))
    .withColumn("prev_longitude", F.lag("longitude").over(window_spec))
    .withColumn("prev_sog", F.lag("sog").over(window_spec))
    .withColumn("prev_cog", F.lag("cog").over(window_spec))
    .withColumn("prev_timestamp", F.lag("base_date_time").over(window_spec))
)

# =========================
# Distance - Haversine
# =========================

R = 6371.0

lat1 = F.radians(F.col("prev_latitude"))
lat2 = F.radians(F.col("latitude"))

delta_lat = F.radians(
    F.col("latitude") - F.col("prev_latitude")
)

delta_lon = F.radians(
    F.col("longitude") - F.col("prev_longitude")
)

a = (
    F.pow(F.sin(delta_lat / 2), 2)
    +
    F.cos(lat1)
    * F.cos(lat2)
    * F.pow(F.sin(delta_lon / 2), 2)
)

distance = (
    2 * F.lit(R)
    * F.asin(F.sqrt(a))
)

df = df.withColumn(
    "distance_km",
    F.when(
        F.col("prev_latitude").isNotNull()
        & F.col("prev_longitude").isNotNull(),
        distance
    ).otherwise(0.0)
)

# =========================
# Speed Change
# =========================

df = df.withColumn(
    "speed_change",
    F.when(
        F.col("prev_sog").isNotNull(),
        F.col("sog") - F.col("prev_sog")
    ).otherwise(0.0)
)

# =========================
# Course Change
# =========================

raw_course_change = (
    F.abs(F.col("cog") - F.col("prev_cog"))
)

df = df.withColumn(
    "course_change",
    F.when(
        F.col("prev_cog").isNotNull(),
        F.least(
            raw_course_change,
            F.lit(360) - raw_course_change
        )
    ).otherwise(0.0)
)

# =========================
# Travel / Stop Duration
# =========================

df = df.withColumn(
    "time_diff_seconds",
    F.when(
        F.col("prev_timestamp").isNotNull(),
        F.col("base_date_time").cast("long")
        - F.col("prev_timestamp").cast("long")
    ).otherwise(0)
)

df = df.withColumn(
    "stop_duration",
    F.when(
        F.col("sog") <= 0.5,
        F.col("time_diff_seconds")
    ).otherwise(0)
)

df = df.withColumn(
    "travel_duration",
    F.when(
        F.col("sog") > 0.5,
        F.col("time_diff_seconds")
    ).otherwise(0)
)

# =========================
# Cleanup helper columns
# =========================

df = df.drop(
    "prev_latitude",
    "prev_longitude",
    "prev_sog",
    "prev_cog",
    "prev_timestamp"
)

# =========================
# Aggregations
# =========================

vessel_analytics = (
    df.groupBy("mmsi")
    .agg(
        F.count("*").alias("record_count"),
        F.avg("sog").alias("average_speed"),
        F.sum("distance_km").alias("total_distance_km"),
        F.sum("stop_duration").alias("total_stop_duration"),
        F.sum("travel_duration").alias("total_travel_duration")
    )
)

# =========================
# Write Feature Dataset
# =========================

print("Writing feature dataset...")

(
    df
    .write
    .mode("overwrite")
    .parquet(OUTPUT_PATH)
)

print("Writing vessel analytics...")

(
    vessel_analytics
    .write
    .mode("overwrite")
    .parquet(
        "hdfs://namenode:9000/maritime/analytics/vessel_2024"
    )
)

print("Feature engineering completed successfully!")

spark.stop()