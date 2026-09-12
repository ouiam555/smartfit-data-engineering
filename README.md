# 🏋️ Smart Fit End-to-End Data Engineering Pipeline

![Python](https://img.shields.io/badge/Python-Data%20Engineering-blue)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-Streaming-black)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-Processing-orange)
![MinIO](https://img.shields.io/badge/MinIO-Data%20Lake-red)
![Snowflake](https://img.shields.io/badge/Snowflake-Data%20Warehouse-lightblue)
![dbt](https://img.shields.io/badge/dbt-Transformation-orange)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-Orchestration-blue)
![Power BI](https://img.shields.io/badge/Power%20BI-Analytics-yellow)
![Docker](https://img.shields.io/badge/Docker-Containerization-blue)

An end-to-end Data Engineering project that collects Smart Fit club data, streams and stores raw records, processes and cleans them, builds an analytical data warehouse, and delivers business insights through Power BI.

The entire data pipeline is orchestrated automatically with Apache Airflow.

---

## 📌 Project Overview

Smart Fit operates a large network of fitness clubs with information distributed across locations, membership plans, pricing, activities, facilities, features, and opening schedules.

The goal of this project is to transform this operational data into a structured and analytics-ready platform.

The project covers the complete data lifecycle:

**Data Collection → Streaming → Data Lake → Processing → Data Warehouse → Transformation → Data Quality → BI Analytics → Orchestration**

---

## 🎯 Project Objectives

The main objectives are to:

- Collect Smart Fit club data automatically
- Build a streaming ingestion layer using Apache Kafka
- Preserve raw data in a Bronze layer
- Process and clean data using Apache Spark
- Store cleaned datasets in a Silver layer
- Load analytical data into Snowflake
- Transform warehouse data using dbt
- Build an analytical dimensional model
- Validate data quality using dbt tests
- Create an interactive Power BI dashboard
- Automate the complete workflow with Apache Airflow
- Send email notifications after pipeline execution

---

# 🏗️ Architecture

```text
                    SMART FIT DATA SOURCE
                             │
                             ▼
                         🕷️ Scrapy
                             │
                             ▼
                      Apache Kafka
                       smartfit_raw
                             │
                             ▼
                    ┌────────────────┐
                    │     MinIO      │
                    │  Bronze Layer  │
                    │    RAW DATA    │
                    └────────────────┘
                             │
                             ▼
                       Apache Spark
                    Cleaning / Processing
                             │
                             ▼
                    ┌────────────────┐
                    │     MinIO      │
                    │  Silver Layer  │
                    │  CLEAN DATA    │
                    └────────────────┘
                             │
                             ▼
                         Snowflake
                      Data Warehouse
                             │
                             ▼
                            dbt
                  Staging / Gold / Tests
                             │
                             ▼
                   Analytical Data Model
                             │
                             ▼
                         Power BI
                      BI & Analytics


              Apache Airflow orchestrates
                 the complete pipeline
```

---

# 🛠️ Technology Stack

| Layer | Technology | Purpose |
|---|---|---|
| Data Collection | Scrapy | Extract Smart Fit club data |
| Streaming | Apache Kafka | Stream raw records |
| Data Lake | MinIO | Store Bronze and Silver datasets |
| Data Processing | Apache Spark / PySpark | Clean and transform raw data |
| Data Warehouse | Snowflake | Central analytical warehouse |
| Transformation | dbt | Build staging and analytical models |
| Data Quality | dbt Tests | Validate analytical datasets |
| Orchestration | Apache Airflow | Automate the pipeline |
| Visualization | Power BI | Business intelligence dashboard |
| Infrastructure | Docker | Containerize pipeline services |
| Development | Python | Pipeline development |
| Version Control | Git / GitHub | Source-code management |

---

# 🔄 Data Pipeline

## 1️⃣ Data Extraction — Scrapy

Smart Fit data is collected from the source using **Scrapy**.

The extracted information includes data related to:

- Clubs
- Cities
- Addresses
- Geographic coordinates
- Membership plans
- Prices
- Activities
- Facilities
- Features
- Opening schedules

Scrapy provides the ingestion entry point of the pipeline.

```text
Smart Fit
    │
    ▼
 Scrapy
```

---

## 2️⃣ Streaming Ingestion — Apache Kafka

Instead of directly loading scraped records into the Data Lake, the records are first published to Apache Kafka.

Kafka acts as the streaming ingestion layer and decouples data extraction from downstream storage and processing.

Kafka topic:

```text
smartfit_raw
```

Flow:

```text
Scrapy
   │
   ▼
Kafka Producer
   │
   ▼
smartfit_raw
```

---

## 3️⃣ Bronze Layer — MinIO

Raw Kafka messages are consumed and persisted into **MinIO**.

The Bronze layer preserves the raw version of the collected data before business transformations are applied.

```text
Kafka
   │
   ▼
MinIO
BRONZE
```

### Bronze responsibilities

- Preserve raw data
- Separate ingestion from transformation
- Maintain a reproducible source layer
- Provide input for Spark processing

---

## 4️⃣ Silver Layer — Apache Spark

Apache Spark processes data coming from the Bronze layer.

The transformation process prepares reliable datasets for analytical workloads.

Processing includes:

- Schema handling
- Data type standardization
- Null handling
- Duplicate handling
- Data cleaning
- Field normalization
- Structural transformations

The resulting datasets are written to the **Silver layer in MinIO**.

```text
MinIO Bronze
      │
      ▼
 Apache Spark
      │
      ▼
MinIO Silver
```

---

# ❄️ 5️⃣ Snowflake Data Warehouse

Cleaned Silver data is loaded into **Snowflake**.

Snowflake acts as the cloud analytical Data Warehouse of the project.

```text
MinIO Silver
      │
      ▼
  Snowflake
```

Main database:

```text
SMARTFIT_DB
```

The warehouse provides the foundation for dbt transformations and Power BI analytics.

---

# 🔧 6️⃣ dbt Transformation Layer

dbt is used to transform warehouse data into analytics-ready models.

The dbt workflow separates transformations into logical layers.

```text
Snowflake
    │
    ▼
  STAGING
    │
    ▼
   MARTS
    │
    ▼
Gold Analytical Model
```

dbt is also responsible for validating the final models using automated tests.

---

# ⭐ Analytical Data Model

The Gold layer uses a dimensional analytical model composed of dimensions, a fact table, and bridge tables.

## Dimension Tables

```text
DIM_CLUB
DIM_ACTIVITY
DIM_FACILITY
DIM_FEATURE
DIM_PLAN
```

## Fact Table

```text
FACT_SCHEDULE
```

## Bridge Tables

```text
BRIDGE_CLUB_ACTIVITY
BRIDGE_CLUB_FACILITY
BRIDGE_CLUB_FEATURE
BRIDGE_CLUB_PLAN
```

The bridge tables handle many-to-many relationships between clubs and their associated services or plans.

### Conceptual Model

```text
                    DIM_ACTIVITY
                         │
                         │
                BRIDGE_CLUB_ACTIVITY
                         │
                         ▼
DIM_FACILITY ────►   DIM_CLUB   ◄──── DIM_FEATURE
     │                   │                  │
     │                   │                  │
BRIDGE_CLUB_         FACT_SCHEDULE     BRIDGE_CLUB_
 FACILITY                                   FEATURE
                         │
                         │
                  BRIDGE_CLUB_PLAN
                         │
                         ▼
                     DIM_PLAN
```

---

# 🧪 Data Quality

Data quality is validated throughout the pipeline.

Checks include:

- Missing-value validation
- Duplicate detection
- Data-type validation
- Schema consistency
- Relationship validation
- dbt model tests
- Pipeline execution monitoring

dbt tests are executed before the pipeline is considered successfully completed.

---

# 🌬️ Apache Airflow Orchestration

Apache Airflow orchestrates the complete workflow.

The pipeline runs automatically every day.

The DAG executes the different components in the required dependency order:

```text
scrape_smartfit
       │
       ▼
kafka_to_bronze
       │
       ▼
spark_silver
       │
       ▼
load_snowflake
       │
       ▼
dbt_staging
       │
       ▼
dbt_gold
       │
       ▼
dbt_tests
       │
       ▼
success_email
```

Airflow provides:

- Task dependency management
- Daily scheduling
- Pipeline monitoring
- Execution logs
- Failure visibility
- Email notifications

This allows the pipeline to run with minimal manual intervention.

---

# 📊 Power BI Dashboard

The final Gold models are consumed by **Power BI** to provide an analytical view of the Smart Fit network.

The dashboard is divided into four analytical pages.

---

## 📈 1. Overview

Provides a global view of the Smart Fit network.

### Main KPIs

- Total Clubs
- Cities Covered
- Total Activities
- Total Facilities
- Total Features

The page also provides an overview of club distribution and service coverage.

---

## 🌍 2. Club Network & Geography

Analyzes the geographical distribution of clubs.

### Main KPIs

- Total Clubs
- Cities Covered
- Average Clubs per City
- Top City Share
- Sales Availability Rate

The page provides geographical analysis through maps, city-level distributions, and club-level information.

---

## 💳 3. Plans & Pricing

Analyzes membership-plan coverage and available pricing information.

### Main KPIs

- Total Plans
- Clubs with Smart Price
- Clubs with Fit Price
- Clubs with Black Price
- Clubs without Plans

The page allows pricing and plan availability to be explored across clubs.

---

## 🏋️ 4. Services & Availability

Analyzes the services available across the Smart Fit network.

### Main KPIs

- Average Activities per Club
- Average Facilities per Club
- Average Features per Club
- Average Services per Club
- Schedule Coverage

This page provides insight into how activities, facilities and features are distributed across clubs.

---

# 📂 Project Structure

```text
smart-fit/
│
├── airflow/
│   └── smartfit.py
│
├── kafka/
│
├── spark/
│
├── scraping/
│
├── dbt/
│
├── docker-compose.yaml
│
├── requirements.txt
│
├── .env.example
│
├── .gitignore
│
└── README.md
```

> The exact internal structure of each component may evolve as the project is maintained.

---

# 🐳 Docker Infrastructure

Docker is used to provide reproducible infrastructure for the different pipeline services.

Start the infrastructure with:

```bash
docker compose up -d
```

Check the containers:

```bash
docker compose ps
```

The containerized architecture makes it possible to run the different components in isolated environments while keeping the pipeline reproducible.

---

# ⚙️ Installation

## 1. Clone the repository

```bash
git clone <YOUR_REPOSITORY_URL>
cd smart-fit
```

## 2. Create a virtual environment

```bash
python3 -m venv .venv
```

Activate it:

```bash
source .venv/bin/activate
```

## 3. Install dependencies

```bash
pip install -r requirements.txt
```

## 4. Configure environment variables

Create your local environment file from the template:

```bash
cp .env.example .env
```

Then configure the required credentials locally.

> Never commit the `.env` file or credentials to GitHub.

## 5. Start the infrastructure

```bash
docker compose up -d
```

---

# 🔐 Security

Credentials and sensitive configuration are managed through environment variables.

The real `.env` file is excluded from version control through `.gitignore`.

The repository contains only:

```text
.env.example
```

with empty/example variables.

Sensitive information such as the following must never be committed:

- Snowflake passwords
- MinIO credentials
- SMTP credentials
- API keys
- Tokens

---

# 📦 Main Python Dependencies

The project uses packages including:

```text
Scrapy
confluent-kafka
minio
PySpark
snowflake-connector-python
dbt-core
dbt-snowflake
python-dotenv
requests
pandas
polars
pyarrow
```

Exact versions are available in:

```text
requirements.txt
```

---

# 🚀 Key Engineering Concepts Demonstrated

This project demonstrates practical implementation of:

- End-to-end Data Engineering
- Streaming ingestion
- Data Lake architecture
- Bronze / Silver / Gold architecture
- ETL / ELT workflows
- Distributed data processing
- Cloud Data Warehousing
- Dimensional modeling
- Many-to-many modeling with bridge tables
- Data quality testing
- Workflow orchestration
- Containerization
- BI modeling
- Automated pipelines
- Environment-variable management
- Git version control

---

# 📌 Current Project Scope

The current implementation focuses on an automated snapshot of Smart Fit club and service data.

Historical snapshot storage and time-series analysis are potential future extensions.

This distinction is important because the current dashboard analyzes the latest available network state rather than claiming historical or year-over-year trends.

---

# 🔮 Possible Future Improvements

Potential extensions include:

- Historical snapshot storage
- Incremental data loading
- Data lineage and observability
- Advanced Airflow failure alerting
- CI/CD for dbt and pipeline code
- Additional dbt tests
- Power BI Service deployment
- Automated BI dataset refresh
- Cloud deployment of the pipeline

---

# 👩‍💻 Author

**Ouiam**

Data Analyst | Data Engineering

This project was developed as an end-to-end portfolio project combining Data Engineering, Data Modeling, Data Quality, Orchestration and Business Intelligence.

---

⭐ If you found this project useful, feel free to star the repository.