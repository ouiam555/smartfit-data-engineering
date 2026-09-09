import io
import os

import pandas as pd
import snowflake.connector

from dotenv import load_dotenv
from minio import Minio
from snowflake.connector.pandas_tools import write_pandas
from datetime import datetime
from zoneinfo import ZoneInfo

run_date = datetime.now(
    ZoneInfo("Africa/Casablanca")
)

year = run_date.year
month = run_date.month
day = run_date.day

# =========================================================
# 1. LOAD ENVIRONMENT VARIABLES
# =========================================================

load_dotenv()


# =========================================================
# 2. CONFIG
# =========================================================

SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")


# =========================================================
# 3. VALIDATE ENVIRONMENT VARIABLES
# =========================================================

required_variables = {
    "SNOWFLAKE_ACCOUNT": SNOWFLAKE_ACCOUNT,
    "SNOWFLAKE_USER": SNOWFLAKE_USER,
    "SNOWFLAKE_PASSWORD": SNOWFLAKE_PASSWORD,
    "SNOWFLAKE_WAREHOUSE": SNOWFLAKE_WAREHOUSE,
    "SNOWFLAKE_DATABASE": SNOWFLAKE_DATABASE,
    "SNOWFLAKE_SCHEMA": SNOWFLAKE_SCHEMA,
    "SNOWFLAKE_ROLE": SNOWFLAKE_ROLE,
    "MINIO_ENDPOINT": MINIO_ENDPOINT,
    "MINIO_ACCESS_KEY": MINIO_ACCESS_KEY,
    "MINIO_SECRET_KEY": MINIO_SECRET_KEY,
    "MINIO_BUCKET": MINIO_BUCKET,
}

missing = [
    name
    for name, value in required_variables.items()
    if not value
]

if missing:
    raise ValueError(
        f"Missing environment variables: {missing}"
    )


# =========================================================
# 4. CONNECT TO MINIO
# =========================================================

minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

print("MinIO connection ready")


# =========================================================
# 5. CONNECT TO SNOWFLAKE
# =========================================================

snowflake_connection = snowflake.connector.connect(
    account=SNOWFLAKE_ACCOUNT,
    user=SNOWFLAKE_USER,
    password=SNOWFLAKE_PASSWORD,
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    schema=SNOWFLAKE_SCHEMA,
    role=SNOWFLAKE_ROLE
)

print("Snowflake connection successful")


# =========================================================
# 6. SILVER DATASETS
# =========================================================

datasets = {
    "CLUBS":
        f"smartfit/clubs/year={year}/month={month:02d}/day={day:02d}/",

    "FACILITIES":
        f"smartfit/facilities/year={year}/month={month:02d}/day={day:02d}/",

    "FEATURES":
        f"smartfit/features/year={year}/month={month:02d}/day={day:02d}/",

    "ACTIVITIES":
        f"smartfit/activities/year={year}/month={month:02d}/day={day:02d}/",

    "CLUB_PLANS":
        f"smartfit/club_plans/year={year}/month={month:02d}/day={day:02d}/",

    "SCHEDULES":
        f"smartfit/schedules/year={year}/month={month:02d}/day={day:02d}/",
}


# =========================================================
# 7. FUNCTION: READ PARQUET FROM MINIO
# =========================================================

def read_parquet_dataset(prefix):

    parts = []

    objects = minio_client.list_objects(
        MINIO_BUCKET,
        prefix=prefix,
        recursive=True
    )

    for obj in objects:

        if not obj.object_name.endswith(".parquet"):
            continue

        print(
            f"Reading {MINIO_BUCKET}/{obj.object_name}"
        )

        response = minio_client.get_object(
            MINIO_BUCKET,
            obj.object_name
        )

        try:
            content = response.read()

            part_df = pd.read_parquet(
                io.BytesIO(content)
            )

            parts.append(part_df)

        finally:
            response.close()
            response.release_conn()

    if not parts:
        raise FileNotFoundError(
            f"No parquet files found under: {prefix}"
        )

    return pd.concat(
        parts,
        ignore_index=True
    )


# =========================================================
# 8. LOAD DATASETS TO SNOWFLAKE
# =========================================================

for table_name, prefix in datasets.items():

    print("\n===================================")
    print(f"LOADING {table_name}")
    print("===================================")

    df = read_parquet_dataset(prefix)

    print(
        f"{table_name} rows from MinIO:",
        len(df)
    )

    df.columns = [
        column.upper()
        for column in df.columns
    ]

    success, nchunks, nrows, output = write_pandas(
        conn=snowflake_connection,
        df=df,
        table_name=table_name,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        auto_create_table=True,
        overwrite=True,
        quote_identifiers=False
    )

    if not success:
        raise RuntimeError(
            f"Failed loading {table_name}"
        )

    print(
        f"{table_name} loaded successfully"
    )

    print(
        "Rows loaded:",
        nrows
    )

    print(
        "Chunks:",
        nchunks
    )


# =========================================================
# 9. VALIDATE ROW COUNTS IN SNOWFLAKE
# =========================================================

print("\n===================================")
print("SNOWFLAKE VALIDATION")
print("===================================")

cursor = snowflake_connection.cursor()

try:

    for table_name in datasets:

        query = f"""
        SELECT COUNT(*)
        FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{table_name}
        """

        cursor.execute(query)

        count = cursor.fetchone()[0]

        print(
            f"{table_name}: {count} rows"
        )

finally:
    cursor.close()


# =========================================================
# 10. CLOSE CONNECTION
# =========================================================

snowflake_connection.close()


print("\n===================================")
print("SILVER TO SNOWFLAKE LOAD COMPLETE")
print("===================================")