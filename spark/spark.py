from pyspark.sql import SparkSession

spark = (
    SparkSession
    .builder
    .appName("SmartFitBronzeReader")
    .getOrCreate()
)

print("SparkSession created successfully")
print("Spark version:", spark.version)

spark.stop()