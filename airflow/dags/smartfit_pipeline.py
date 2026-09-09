from datetime import timedelta
import os

import docker
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email


LOCAL_TZ = pendulum.timezone(
    "Africa/Casablanca"
)


# ==========================================================
# EMAIL
# ==========================================================

ALERT_EMAIL = os.environ.get(
    "AIRFLOW__SMTP__SMTP_USER"
)


def send_failure_email(context):

    task_instance = context["task_instance"]

    dag_id = context["dag"].dag_id
    task_id = task_instance.task_id
    run_id = context["run_id"]

    exception = context.get(
        "exception",
        "Unknown error"
    )

    subject = (
        f" SmartFit Pipeline Failed - {task_id}"
    )

    html_content = f"""
    <h2>SmartFit Pipeline Failed </h2>

    <p><b>DAG:</b> {dag_id}</p>
    <p><b>Task:</b> {task_id}</p>
    <p><b>Run ID:</b> {run_id}</p>

    <p><b>Error:</b></p>
    <pre>{exception}</pre>

    <p>
        Please check the Airflow logs
        for more details.
    </p>
    """

    send_email(
        to=ALERT_EMAIL,
        subject=subject,
        html_content=html_content,
    )


def send_success_email():

    subject = (
        " SmartFit Pipeline Completed Successfully"
    )

    html_content = """
    <h2>SmartFit Daily Pipeline </h2>

    <p>
        The complete SmartFit data pipeline
        finished successfully.
    </p>

    <hr>

    <p> Scraping completed</p>
    <p> Kafka ingestion completed</p>
    <p> Bronze layer completed</p>
    <p> Spark Silver transformation completed</p>
    <p> Snowflake load completed</p>
    <p> dbt Staging completed</p>
    <p> dbt Gold completed</p>
    <p> dbt Data Quality tests passed</p>

    <hr>

    <p>
        SmartFit data is ready for analytics.
    </p>
    """

    send_email(
        to=ALERT_EMAIL,
        subject=subject,
        html_content=html_content,
    )


# ==========================================================
# DEFAULT AIRFLOW CONFIG
# ==========================================================

default_args = {

    "owner": "ouiam",

    "retries": 1,

    "retry_delay": timedelta(
        minutes=5
    ),

    "on_failure_callback":
        send_failure_email,
}


# ==========================================================
# DOCKER EXECUTION FUNCTION
# ==========================================================

def run_in_container(
    container_name,
    command
):

    client = docker.from_env()

    container = client.containers.get(
        container_name
    )

    print(
        f"Running inside {container_name}:"
    )

    print(command)

    result = container.exec_run(
        [
            "bash",
            "-lc",
            command
        ],
        stream=False,
        demux=True,
    )

    stdout, stderr = result.output

    if stdout:

        print(
            stdout.decode(
                "utf-8",
                errors="replace"
            )
        )

    if stderr:

        print(
            stderr.decode(
                "utf-8",
                errors="replace"
            )
        )

    if result.exit_code != 0:

        raise RuntimeError(
            f"{container_name} command failed "
            f"with exit code "
            f"{result.exit_code}"
        )


# ==========================================================
# PIPELINE FUNCTIONS
# ==========================================================

def scrape():

    run_in_container(
        "smartfit-runner",
        """
        cd /opt/project/scraping &&
        scrapy crawl smartfit
        """
    )


def kafka_to_bronze():

    run_in_container(
        "smartfit-runner",
        """
        cd /opt/project &&
        python kafka/bronze_consumer.py
        """
    )


def spark_to_silver():

    run_in_container(
        "smartfit-spark",
        """
        mkdir -p /tmp/.ivy2 &&

        /opt/spark/bin/spark-submit \
          --conf spark.jars.ivy=/tmp/.ivy2 \
          --packages org.apache.hadoop:hadoop-aws:3.4.2 \
          /opt/spark/work-dir/silver_transform.py
        """
    )


def silver_to_snowflake():

    run_in_container(
        "smartfit-runner",
        """
        cd /opt/project &&
        python snowflake/load_silver.py
        """
    )


def run_dbt_staging():

    run_in_container(
        "smartfit-runner",
        """
        cd /opt/project/dbt/smartfit_dbt &&
        dbt run --select staging
        """
    )


def run_dbt_gold():

    run_in_container(
        "smartfit-runner",
        """
        cd /opt/project/dbt/smartfit_dbt &&
        dbt run --select marts
        """
    )


def run_dbt_tests():

    run_in_container(
        "smartfit-runner",
        """
        cd /opt/project/dbt/smartfit_dbt &&
        dbt test \
        --select path:models/marts
        """
    )


# ==========================================================
# DAG
# ==========================================================

with DAG(

    dag_id="smartfit_daily_pipeline",

    description=(
        "Smart Fit end-to-end "
        "data engineering pipeline"
    ),

    start_date=pendulum.datetime(
        2026,
        9,
        7,
        tz=LOCAL_TZ
    ),

    schedule="0 0 * * *",

    catchup=False,

    default_args=default_args,

    tags=[
        "smartfit",
        "data-engineering",
    ],

) as dag:


    # ======================================================
    # 1. SCRAPING
    # ======================================================

    scrape_smartfit = PythonOperator(
        task_id="scrape_smartfit",
        python_callable=scrape,
    )


    # ======================================================
    # 2. KAFKA -> BRONZE
    # ======================================================

    consume_bronze = PythonOperator(
        task_id="kafka_to_bronze",
        python_callable=kafka_to_bronze,
    )


    # ======================================================
    # 3. SPARK -> SILVER
    # ======================================================

    transform_silver = PythonOperator(
        task_id="spark_silver",
        python_callable=spark_to_silver,
    )


    # ======================================================
    # 4. SNOWFLAKE
    # ======================================================

    load_snowflake = PythonOperator(
        task_id="load_snowflake",
        python_callable=silver_to_snowflake,
    )


    # ======================================================
    # 5. DBT STAGING
    # ======================================================

    dbt_staging = PythonOperator(
        task_id="dbt_staging",
        python_callable=run_dbt_staging,
    )


    # ======================================================
    # 6. DBT GOLD
    # ======================================================

    dbt_gold = PythonOperator(
        task_id="dbt_gold",
        python_callable=run_dbt_gold,
    )


    # ======================================================
    # 7. DBT TESTS
    # ======================================================

    dbt_tests = PythonOperator(
        task_id="dbt_tests",
        python_callable=run_dbt_tests,
    )


    # ======================================================
    # 8. SUCCESS EMAIL
    # ======================================================

    success_email = PythonOperator(
        task_id="success_email",
        python_callable=send_success_email,
    )


    # ======================================================
    # DEPENDENCIES
    # ======================================================

    (
        scrape_smartfit
        >> consume_bronze
        >> transform_silver
        >> load_snowflake
        >> dbt_staging
        >> dbt_gold
        >> dbt_tests
        >> success_email
    )