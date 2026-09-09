from pyspark.sql import SparkSession


spark = (
    SparkSession
    .builder
    .appName("SmartFitBronzeReader")
    .getOrCreate()
)


# Hadoop configuration used by Spark
hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()

# MinIO connection
hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
hadoop_conf.set("fs.s3a.access.key", "minioadmin")
hadoop_conf.set("fs.s3a.secret.key", "minioadmin123")
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")


# Bronze location
bronze_path = (
    "s3a://bronze/"
    "smartfit/"
    "year=2026/"
    "month=09/"
    "day=05/"
    "*.jsonl"
)


# Read JSONL files into a Spark DataFrame
df = spark.read.json(bronze_path)


print("\n===== SCHEMA =====")
df.printSchema()

print("\n===== SAMPLE =====")
df.show(5, truncate=False)

print("\n===== ROW COUNT =====")
print(df.count())


spark.stop()