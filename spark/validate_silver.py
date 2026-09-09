from pyspark.sql import SparkSession
from pyspark.sql.functions import col


# ==========================================
# 1. CREATE SPARK SESSION
# ==========================================

spark = (
    SparkSession.builder
    .appName("SmartFitSilverValidation")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ==========================================
# 2. CONFIGURE MINIO / S3A
# ==========================================

hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()

hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
hadoop_conf.set("fs.s3a.access.key", "minioadmin")
hadoop_conf.set("fs.s3a.secret.key", "minioadmin123")
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")


# ==========================================
# 3. SILVER PATH
# ==========================================

silver_path = (
    "s3a://silver/"
    "smartfit/"
    "year=2026/"
    "month=09/"
    "day=05/"
)


# ==========================================
# 4. READ SILVER PARQUET
# ==========================================

df = spark.read.parquet(silver_path)


# ==========================================
# 5. BASIC VALIDATION
# ==========================================

print("\n===== SILVER ROW COUNT =====")
print(df.count())

print("\n===== SILVER COLUMN COUNT =====")
print(len(df.columns))

print("\n===== SILVER SCHEMA =====")
df.printSchema()

print("\n===== SILVER SAMPLE =====")
df.show(5, truncate=False)


# ==========================================
# 6. CHECK DUPLICATE LOCATION_ID
# ==========================================

print("\n===== DUPLICATE LOCATION_ID =====")

duplicates = (
    df
    .groupBy("location_id")
    .count()
    .filter(col("count") > 1)
)

duplicates.show(truncate=False)

print("Duplicate location_ids:", duplicates.count())


# ==========================================
# 7. STOP SPARK
# ==========================================

spark.stop()