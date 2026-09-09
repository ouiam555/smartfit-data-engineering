from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    when,
    sum as spark_sum
)


spark = (
    SparkSession
    .builder
    .appName("SmartFitSilver")
    .getOrCreate()
)


hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()

hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
hadoop_conf.set("fs.s3a.access.key", "minioadmin")
hadoop_conf.set("fs.s3a.secret.key", "minioadmin123")
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")


bronze_path = (
    "s3a://bronze/"
    "smartfit/"
    "year=2026/"
    "month=09/"
    "day=05/"
    "*.jsonl"
)


df = spark.read.json(bronze_path)


print("ROWS:", df.count())
print("COLUMNS:", len(df.columns))

df.printSchema()
print("\n===== BASIC PROFILE =====")

print("ROWS:", df.count())
print("COLUMNS:", len(df.columns))


print("\n===== DUPLICATE LOCATION IDs =====")

duplicate_ids = (
    df
    .groupBy("location_id")
    .count()
    .filter(col("count") > 1)
)

duplicate_ids.show(20, truncate=False)


print("\n===== NULL COUNTS =====")

null_counts = df.select([
    spark_sum(
        when(col(c).isNull(), 1).otherwise(0)
    ).alias(c)

    for c in df.columns
])

null_counts.show(vertical=True)