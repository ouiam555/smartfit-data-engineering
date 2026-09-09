FROM python:3.12-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       build-essential \
       curl \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    scrapy \
    confluent-kafka \
    minio \
    snowflake-connector-python \
    python-dotenv \
    pandas \
    "pyarrow>=14,<24" \
    dbt-core==1.12.3 \
    dbt-snowflake==1.12.0

WORKDIR /opt/project

CMD ["sleep", "infinity"]