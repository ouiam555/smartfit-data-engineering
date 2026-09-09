from pyspark.sql import SparkSession
from datetime import datetime
from zoneinfo import ZoneInfo
from pyspark.sql.functions import (
    col,
    row_number,
    to_timestamp,
    when,
    sum as spark_sum,
    trim,
    explode,
    lit
)
from pyspark.sql.window import Window

run_date = datetime.now(
    ZoneInfo("Africa/Casablanca")
)

year = run_date.year
month = run_date.month
day = run_date.day


# =========================================================
# 1. CREATE SPARK SESSION
# =========================================================

spark = (
    SparkSession
    .builder
    .appName("SmartFitSilver")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# =========================================================
# 2. CONFIGURE MINIO / S3A
# =========================================================

hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()

hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
hadoop_conf.set("fs.s3a.access.key", "minioadmin")
hadoop_conf.set("fs.s3a.secret.key", "minioadmin123")
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")


# =========================================================
# 3. PATHS
# =========================================================

bbronze_path = (
    f"s3a://bronze/smartfit/"
    f"year={year}/"
    f"month={month:02d}/"
    f"day={day:02d}/"
)

silver_base = "s3a://silver/smartfit"

clubs_path = (
    f"{silver_base}/clubs/"
    f"year={year}/month={month:02d}/day={day:02d}/"
)

facilities_path = (
    f"{silver_base}/facilities/"
    f"year={year}/month={month:02d}/day={day:02d}/"
)

features_path = (
    f"{silver_base}/features/"
    f"year={year}/month={month:02d}/day={day:02d}/"
)

activities_path = (
    f"{silver_base}/activities/"
    f"year={year}/month={month:02d}/day={day:02d}/"
)

club_plans_path = (
    f"{silver_base}/club_plans/"
    f"year={year}/month={month:02d}/day={day:02d}/"
)

schedules_path = (
    f"{silver_base}/schedules/"
    f"year={year}/month={month:02d}/day={day:02d}/"
)


# =========================================================
# 4. READ BRONZE
# =========================================================

df = spark.read.json(bronze_path)


# =========================================================
# 5. BASIC PROFILING
# =========================================================

print("\n===== BASIC PROFILE =====")

print("TOTAL ROWS:", df.count())
print("TOTAL COLUMNS:", len(df.columns))

print(
    "UNIQUE LOCATIONS:",
    df.select("location_id").distinct().count()
)


# =========================================================
# 6. CONVERT scraped_at TO TIMESTAMP
# =========================================================

df = df.withColumn(
    "scraped_at",
    to_timestamp(col("scraped_at"))
)


# =========================================================
# 7. DEDUPLICATION
# KEEP LATEST RECORD PER LOCATION
# =========================================================

window_latest = (
    Window
    .partitionBy("location_id")
    .orderBy(col("scraped_at").desc())
)

df = (
    df
    .withColumn(
        "row_num",
        row_number().over(window_latest)
    )
    .filter(col("row_num") == 1)
    .drop("row_num")
)


# =========================================================
# 8. VALIDATE DEDUPLICATION
# =========================================================

print("\n===== AFTER DEDUPLICATION =====")

print("ROWS:", df.count())

remaining_duplicates = (
    df
    .groupBy("location_id")
    .count()
    .filter(col("count") > 1)
)

print(
    "DUPLICATE LOCATION IDs:",
    remaining_duplicates.count()
)


# =========================================================
# 9. NULL PROFILE
# =========================================================

print("\n===== NULL VALUES =====")

null_counts = df.select([
    spark_sum(
        when(
            col(c).isNull(),
            1
        ).otherwise(0)
    ).alias(c)

    for c in df.columns
])

null_counts.show(
    truncate=False,
    vertical=True
)


# =========================================================
# 10. CLEAN STRINGS
# =========================================================

string_columns = [
    "club_name",
    "address_first_line",
    "address_second_line",
    "city_state",
    "distance",
    "permalink",
    "picture_url",
    "source_url"
]

for c in string_columns:

    df = df.withColumn(
        c,
        trim(col(c))
    )


# =========================================================
# 11. CLEAN DATA TYPES
# =========================================================

df = (
    df
    .withColumn(
        "location_id",
        col("location_id").cast("long")
    )
    .withColumn(
        "latitude",
        col("latitude").cast("double")
    )
    .withColumn(
        "longitude",
        col("longitude").cast("double")
    )
    .withColumn(
        "opened",
        col("opened").cast("boolean")
    )
    .withColumn(
        "franchise",
        col("franchise").cast("boolean")
    )
    .withColumn(
        "sales_available",
        col("sales_available").cast("boolean")
    )
)


# =========================================================
# 12. FLATTEN PRICE FIELDS
# =========================================================

df = (
    df
    .withColumn(
        "price_smart",
        col("prices.smart.value").cast("double")
    )
    .withColumn(
        "price_black",
        col("prices.black.value").cast("double")
    )
    .withColumn(
        "price_fit",
        col("prices.fit.value").cast("double")
    )
    .withColumn(
        "price_black_no_commitment",
        col(
            "prices.black_sin_permanencia.value"
        ).cast("double")
    )
)


# =========================================================
# 13. FLATTEN PROMOTION
# =========================================================

df = (
    df
    .withColumn(
        "promotion_id",
        col("promotion.id").cast("long")
    )
    .withColumn(
        "promotion_name",
        col("promotion.name")
    )
    .withColumn(
        "promotion_title",
        col("promotion.title")
    )
    .withColumn(
        "promotion_kind",
        col("promotion.kind")
    )
)


# =========================================================
# 14. CLUBS DATASET
# Grain:
# 1 row = 1 Smart Fit club
# =========================================================

clubs_df = df.select(
    "location_id",
    "club_name",
    "address_first_line",
    "address_second_line",
    "city_state",
    "latitude",
    "longitude",
    "distance",
    "opened",
    "franchise",
    "sales_available",
    "permalink",
    "picture_url",
    "price_smart",
    "price_black",
    "price_fit",
    "price_black_no_commitment",
    "promotion_id",
    "promotion_name",
    "promotion_title",
    "promotion_kind",
    "scraped_at",
    "source_url"
)


# =========================================================
# 15. FACILITIES DATASET
# Grain:
# 1 row = 1 facility available in 1 club
# =========================================================

facilities_df = (
    df
    .select(
        col("location_id"),
        explode(
            col("facilities")
        ).alias("facility")
    )
    .select(
        col("location_id"),

        col("facility.id").alias(
            "facility_id"
        ),

        col("facility.name").alias(
            "facility_name"
        ),

        col("facility.description").alias(
            "facility_description"
        ),

        col("facility.icon_svg_slug").alias(
            "facility_icon_slug"
        )
    )
    .dropDuplicates([
        "location_id",
        "facility_id"
    ])
)


# =========================================================
# 16. FEATURES DATASET
# Grain:
# 1 row = 1 feature available in 1 club
# =========================================================

features_df = (
    df
    .select(
        col("location_id"),
        explode(
            col("features")
        ).alias("feature")
    )
    .select(
        col("location_id"),

        col("feature.id").alias(
            "feature_id"
        ),

        col("feature.name").alias(
            "feature_name"
        ),

        col("feature.description").alias(
            "feature_description"
        ),

        col("feature.icon_svg_slug").alias(
            "feature_icon_slug"
        )
    )
    .dropDuplicates([
        "location_id",
        "feature_id"
    ])
)


# =========================================================
# 17. ACTIVITIES DATASET
# Grain:
# 1 row = 1 activity available in 1 club
# =========================================================

activities_df = (
    df
    .select(
        col("location_id"),
        explode(
            col("activities")
        ).alias("activity")
    )
    .select(
        col("location_id"),

        col("activity.id").alias(
            "activity_id"
        ),

        col("activity.name").alias(
            "activity_name"
        ),

        col("activity.description").alias(
            "activity_description"
        ),

        col("activity.icon_svg_slug").alias(
            "activity_icon_slug"
        )
    )
    .dropDuplicates([
        "location_id",
        "activity_id"
    ])
)


# =========================================================
# 18. CLUB PLANS DATASET
# Grain:
# 1 row = 1 plan available in 1 club
# =========================================================

club_plans_df = (
    df
    .select(
        col("location_id"),

        explode(
            col("plan_names")
        ).alias("plan_name")
    )
    .dropDuplicates([
        "location_id",
        "plan_name"
    ])
)


# =========================================================
# 19. SCHEDULE DATASET
# Grain:
# 1 row = 1 club + 1 day + opening hours
# =========================================================

schedule_days = [
    ("Lun", "Monday"),
    ("Mar", "Tuesday"),
    ("Mie", "Wednesday"),
    ("Jue", "Thursday"),
    ("Vie", "Friday"),
    ("Sáb", "Saturday"),
    ("Dom", "Sunday")
]


schedule_dfs = []


for source_day, day_name in schedule_days:

    day_df = (
        df
        .select(
            col("location_id"),

            explode(
                col(
                    f"schedules.`{source_day}`"
                )
            ).alias("schedule")
        )
        .select(
            col("location_id"),

            lit(day_name).alias(
                "day_name"
            ),

            col(
                "schedule.table.weekday"
            ).alias(
                "weekday_original"
            ),

            col(
                "schedule.table.time"
            ).alias(
                "opening_hours"
            )
        )
    )

    schedule_dfs.append(day_df)


schedules_df = schedule_dfs[0]


for day_df in schedule_dfs[1:]:

    schedules_df = (
        schedules_df
        .unionByName(day_df)
    )


schedules_df = schedules_df.dropDuplicates([
    "location_id",
    "day_name",
    "opening_hours"
])


# =========================================================
# 20. VALIDATION
# =========================================================

print("\n===== SILVER DATASET COUNTS =====")

print(
    "CLUBS:",
    clubs_df.count()
)

print(
    "FACILITIES:",
    facilities_df.count()
)

print(
    "FEATURES:",
    features_df.count()
)

print(
    "ACTIVITIES:",
    activities_df.count()
)

print(
    "CLUB PLANS:",
    club_plans_df.count()
)

print(
    "SCHEDULES:",
    schedules_df.count()
)


# =========================================================
# 21. DUPLICATE VALIDATION
# =========================================================

print("\n===== DUPLICATE VALIDATION =====")


club_duplicates = (
    clubs_df
    .groupBy("location_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

facility_duplicates = (
    facilities_df
    .groupBy(
        "location_id",
        "facility_id"
    )
    .count()
    .filter(col("count") > 1)
    .count()
)

feature_duplicates = (
    features_df
    .groupBy(
        "location_id",
        "feature_id"
    )
    .count()
    .filter(col("count") > 1)
    .count()
)

activity_duplicates = (
    activities_df
    .groupBy(
        "location_id",
        "activity_id"
    )
    .count()
    .filter(col("count") > 1)
    .count()
)

plan_duplicates = (
    club_plans_df
    .groupBy(
        "location_id",
        "plan_name"
    )
    .count()
    .filter(col("count") > 1)
    .count()
)

schedule_duplicates = (
    schedules_df
    .groupBy(
        "location_id",
        "day_name",
        "opening_hours"
    )
    .count()
    .filter(col("count") > 1)
    .count()
)


print(
    "CLUB DUPLICATES:",
    club_duplicates
)

print(
    "FACILITY DUPLICATES:",
    facility_duplicates
)

print(
    "FEATURE DUPLICATES:",
    feature_duplicates
)

print(
    "ACTIVITY DUPLICATES:",
    activity_duplicates
)

print(
    "PLAN DUPLICATES:",
    plan_duplicates
)

print(
    "SCHEDULE DUPLICATES:",
    schedule_duplicates
)


# =========================================================
# 22. SAMPLE OUTPUTS
# =========================================================

print("\n===== CLUB SAMPLE =====")
clubs_df.show(
    5,
    truncate=False
)

print("\n===== FEATURE SAMPLE =====")
features_df.show(
    5,
    truncate=False
)

print("\n===== ACTIVITY SAMPLE =====")
activities_df.show(
    5,
    truncate=False
)

print("\n===== PLAN SAMPLE =====")
club_plans_df.show(
    10,
    truncate=False
)

print("\n===== SCHEDULE SAMPLE =====")
schedules_df.show(
    10,
    truncate=False
)


# =========================================================
# 23. WRITE CLUBS
# =========================================================

(
    clubs_df
    .write
    .mode("overwrite")
    .parquet(clubs_path)
)


# =========================================================
# 24. WRITE FACILITIES
# =========================================================

(
    facilities_df
    .write
    .mode("overwrite")
    .parquet(facilities_path)
)


# =========================================================
# 25. WRITE FEATURES
# =========================================================

(
    features_df
    .write
    .mode("overwrite")
    .parquet(features_path)
)


# =========================================================
# 26. WRITE ACTIVITIES
# =========================================================

(
    activities_df
    .write
    .mode("overwrite")
    .parquet(activities_path)
)


# =========================================================
# 27. WRITE CLUB PLANS
# =========================================================

(
    club_plans_df
    .write
    .mode("overwrite")
    .parquet(club_plans_path)
)


# =========================================================
# 28. WRITE SCHEDULES
# =========================================================

(
    schedules_df
    .write
    .mode("overwrite")
    .parquet(schedules_path)
)


# =========================================================
# 29. FINAL MESSAGE
# =========================================================

print("\n======================================")
print("SMART FIT SILVER PIPELINE COMPLETE")
print("======================================")

print("CLUBS SAVED TO:")
print(clubs_path)

print("FACILITIES SAVED TO:")
print(facilities_path)

print("FEATURES SAVED TO:")
print(features_path)

print("ACTIVITIES SAVED TO:")
print(activities_path)

print("CLUB PLANS SAVED TO:")
print(club_plans_path)

print("SCHEDULES SAVED TO:")
print(schedules_path)


# =========================================================
# 30. STOP SPARK
# =========================================================

spark.stop()