from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MariTimeTest") \
    .getOrCreate()

print("Spark version:", spark.version)

df = spark.range(10)
df.show()

spark.stop()