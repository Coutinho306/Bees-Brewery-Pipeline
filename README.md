# BEES Brewery Data Pipeline

Data engineering pipeline that ingests brewery data from the Open Brewery DB API and processes it through a medallion architecture (Bronze → Silver → Gold) using Apache Airflow, PySpark, and Soda Core.

**Source API**: `https://api.openbrewerydb.org/v1/breweries`  
**Technology stack**: Apache Airflow 2.8.0 · Python 3.11 · PySpark 3.5.0 · PyArrow 14.0.2 · Soda Core Spark 3.3.2 · Docker

---

## Quick Start

```bash
# 1. Generate Fernet key and copy environment config
cp .env.example .env
sed -i "s/AIRFLOW__CORE__FERNET_KEY=.*/AIRFLOW__CORE__FERNET_KEY=$(python3 -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())')/" .env

# 2. Start all services
docker compose up -d

# 3. Open Airflow UI — http://localhost:8080  (admin / admin)

# 4. Trigger the Bronze DAG manually; Silver and Gold auto-trigger via Dataset dependencies

# 5. Run the test suite
./run_tests.sh
```

---

## Architecture

```
Open Brewery DB API
        │
        ▼
┌───────────────────────────────────────────────────────┐
│  Bronze  │  Raw JSON  │  data/bronze/YYYY-MM-DD/      │
│          │            │  breweries.json                │
└───────────────────────────────────────────────────────┘
        │  Dataset trigger
        ▼
┌───────────────────────────────────────────────────────┐
│  Silver  │  Clean Parquet partitioned by state/country │
│          │  data/silver/YYYY-MM-DD/state=*/country=*/  │
└───────────────────────────────────────────────────────┘
        │  Dataset trigger
        ▼
┌───────────────────────────────────────────────────────┐
│  Gold    │  Aggregated Parquet tables                  │
│          │  data/gold/YYYY-MM-DD/                      │
│          │  ├── brewery_type_summary.parquet            │
│          │  └── breweries_by_state.parquet              │
└───────────────────────────────────────────────────────┘
```

The three layers are wired as an Airflow Dataset chain: each DAG publishes an outlet that automatically triggers the next DAG without manual intervention.

---

## DAGs

| DAG ID | Schedule | Trigger mode |
|---|---|---|
| `bees_ingestion_api_bronze_brewery_data` | `@daily` | Time-based |
| `bees_ingestion_api_silver_brewery_data` | — | Dataset: `bronze_dataset` |
| `bees_ingestion_api_gold_brewery_data`   | — | Dataset: `silver_dataset` |

### Bronze DAG — `bees_ingestion_api_bronze_brewery_data`

Tasks (in order):

1. **`ingest_from_api`** — Paginates through the Open Brewery DB API (`page_size=200`, `retries=3`, retry codes: 429 500 502 503 504, `backoff_factor=1`, `timeout=30s`). Saves the full response to `data/bronze/YYYY-MM-DD/breweries.json`. Passes `record_count` and `file_path` via XCom.
2. **`quality_check_bronze`** — Runs Soda Core against `utils/soda_checks/bronze_checks.yml`. Fails the task if any check does not pass.

Outlet: publishes `bronze_dataset`.

### Silver DAG — `bees_ingestion_api_silver_brewery_data`

Tasks (in order):

1. **`transform_to_parquet`** — Reads the bronze JSON with an enforced PySpark schema, applies silver cleansing (see transformations below), writes Snappy-compressed Parquet partitioned by `state` and `country`. Passes `silver_count`, `bronze_count`, and `output_path` via XCom.
2. **`quality_check_silver`** — Runs Soda Core against `utils/soda_checks/silver_checks.yml`.

Outlet: publishes `silver_dataset`.

### Gold DAG — `bees_ingestion_api_gold_brewery_data`

Tasks (in order):

1. **`create_aggregations`** — Reads silver Parquet and writes two aggregation tables:
   - `brewery_type_summary.parquet`: group by `brewery_type`, count
   - `breweries_by_state.parquet`: group by `state` + `country`, count
2. **`quality_check_gold`** — Runs Soda Core against `utils/soda_checks/gold_type_checks.yml` and `utils/soda_checks/gold_state_checks.yml`.

---

## Data Schema

All fields come directly from the Open Brewery DB API. The PySpark schema is defined in `airflow/utils/spark_utils.py::get_brewery_schema()`.

| Field | Type | Required | Notes |
|---|---|---|---|
| `id` | String | **Yes** | Primary key — unique brewery UUID |
| `name` | String | No | Brewery display name |
| `brewery_type` | String | No | See brewery type enum below |
| `address_1` | String | No | Primary street address |
| `address_2` | String | No | Secondary address line |
| `address_3` | String | No | Tertiary address line |
| `city` | String | No | City name |
| `state_province` | String | No | State or province as returned by API |
| `postal_code` | String | No | Postal / ZIP code |
| `country` | String | No | Full country name |
| `longitude` | Double | No | Geographic longitude; nullable |
| `latitude` | Double | No | Geographic latitude; nullable |
| `phone` | String | No | Contact phone number |
| `website_url` | String | No | Brewery website |
| `state` | String | No | Partition key — standardised state/region |
| `street` | String | No | Computed street address |

### Brewery Type Enum (`brewery_type`)

Values returned by the Open Brewery DB API: `micro`, `nano`, `regional`, `brewpub`, `large`, `planning`, `bar`, `contract`, `proprietor`, `taproom`.

Null or empty values are filled with empty string during silver cleansing; unrecognised values are kept as-is.

---

## Silver Transformations

Applied in `airflow/utils/spark_transformations.py::apply_silver_cleansing()`:

1. **Null fill** — All `StringType` columns (14 fields) filled with `""` where null.
2. **Unknown fill** — `state` and `country` filled with `"unknown"` if still empty after step 1.
3. **Deduplication** — `dropDuplicates(["id"])` removes duplicate brewery records.
4. **Partitioning** — Output partitioned by `["state", "country"]` using Snappy compression.

---

## Data Quality Checks (Soda Core)

Checks are defined as YAML files under `airflow/utils/soda_checks/` and executed on Spark DataFrames without collecting data to the driver.

### Bronze — `bronze_checks.yml`

```yaml
checks for breweries:
  - row_count >= 100
  - missing_count(id) = 0
  - duplicate_count(id) = 0
  - schema:
      fail:
        when required column missing: [id, name, brewery_type]
```

### Silver — `silver_checks.yml`

```yaml
checks for breweries:
  - missing_count(id) = 0
  - missing_count(state) = 0
  - missing_count(country) = 0
  - duplicate_count(id) = 0
  - schema:
      fail:
        when required column missing: [id, state, country]
```

### Gold — `gold_type_checks.yml`

```yaml
checks for brewery_type_summary:
  - row_count >= 3
  - missing_count(brewery_type) = 0
  - duplicate_count(brewery_type) = 0
  - schema:
      fail:
        when required column missing: [brewery_type, count]
```

### Gold — `gold_state_checks.yml`

```yaml
checks for breweries_by_state:
  - row_count >= 10
  - missing_count(state) = 0
  - missing_count(country) = 0
  - schema:
      fail:
        when required column missing: [state, country, count]
```

A failed Soda check raises `DataQualityError` and marks the Airflow task as failed, preventing downstream DAGs from triggering.

---

## Project Structure

```
Bees-Brewery-Pipeline/
├── airflow/
│   ├── dags/
│   │   ├── bees_ingestion_api_bronze_brewery_data.py
│   │   ├── bees_ingestion_api_silver_brewery_data.py
│   │   └── bees_ingestion_api_gold_brewery_data.py
│   └── utils/
│       ├── api_client.py            # BreweryAPIClient — pagination, retries
│       ├── spark_utils.py           # SparkSession factory, schema definition
│       ├── spark_transformations.py # Bronze→Silver and Silver→Gold logic
│       ├── data_quality.py          # Soda Core runner, DataQualityError
│       └── soda_checks/
│           ├── bronze_checks.yml
│           ├── silver_checks.yml
│           ├── gold_type_checks.yml
│           └── gold_state_checks.yml
├── tests/
│   └── test_pipeline.py             # 6 test classes, ~255 lines
├── data/                            # Auto-generated by pipeline runs
│   ├── bronze/YYYY-MM-DD/breweries.json
│   ├── silver/YYYY-MM-DD/state=*/country=*/part-*.snappy.parquet
│   └── gold/YYYY-MM-DD/
│       ├── brewery_type_summary.parquet
│       └── breweries_by_state.parquet
├── docker-compose.yml               # postgres + airflow-webserver + scheduler + init
├── Dockerfile                       # Airflow 2.8.0 + Java 17 + Spark 3.5.0
├── requirements.txt
├── pytest.ini
├── .env.example
├── MONITORING.md
└── run_tests.sh
```

---

## Spark Configuration

Defined in `airflow/utils/spark_utils.py::get_spark_session()`:

| Config key | Value | Reason |
|---|---|---|
| `spark.master` | `local[*]` | All available cores in the container |
| `spark.driver.memory` | `2g` | Sized for container environment |
| `spark.executor.memory` | `2g` | Same as driver in local mode |
| `spark.sql.shuffle.partitions` | `4` | Reduces shuffle overhead for small dataset |
| `spark.sql.adaptive.enabled` | `true` | Adaptive query execution |
| `spark.sql.adaptive.coalescePartitions.enabled` | `true` | Coalesce output partitions automatically |
| `spark.ui.enabled` | `false` | Disabled to reduce container resource usage |

In production (AWS EMR, Databricks, GCP Dataproc) only the `spark.master` and memory settings change; all transformation logic remains identical.

---

## Testing

```bash
# Inside Docker (recommended)
./run_tests.sh

# Or directly inside the scheduler container
docker-compose exec -u airflow airflow-scheduler python -m pytest tests/ -v
```

Test classes in `tests/test_pipeline.py`:

| Class | What it covers |
|---|---|
| `TestDAGsLoad` | All 3 DAGs import without errors |
| `TestDAGStructure` | Task names present in each DAG |
| `TestBronzeToSilverTransformation` | Null fill, deduplication, schema enforcement |
| `TestSilverToGoldAggregation` | Aggregation logic, row count preservation |
| `TestAPIClientErrorScenarios` | Timeout, 500, connection errors, pagination, empty/malformed responses |
| `TestDataQualityModule` | `DataQualityError` class, Soda check file existence |

---

## Environment Variables

Full reference in `.env.example`. Key variables:

| Variable | Default | Description |
|---|---|---|
| `AIRFLOW__CORE__FERNET_KEY` | — | Required. Encrypt secrets in Airflow metadata DB |
| `AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION` | `true` | New DAGs start paused |
| `AIRFLOW__CORE__LOAD_EXAMPLES` | `false` | Skip bundled example DAGs |
| `POSTGRES_USER` | `airflow` | Airflow metadata DB user |
| `POSTGRES_PASSWORD` | `airflow` | Airflow metadata DB password |
| `BRONZE_PATH` | `/opt/airflow/data/bronze` | Bronze data root inside container |
| `SILVER_PATH` | `/opt/airflow/data/silver` | Silver data root inside container |
| `GOLD_PATH` | `/opt/airflow/data/gold` | Gold data root inside container |
| `BREWERY_API_BASE_URL` | `https://api.openbrewerydb.org/v1` | API base URL |
| `BREWERY_API_PAGE_SIZE` | `200` | Records per API page (max 200) |
| `BREWERY_API_MAX_RETRIES` | `3` | HTTP retry attempts |

---

## Design Decisions

### Why PySpark for ~8k Records?

PySpark runs in `local[*]` mode inside Docker and processes the brewery dataset (approximately 8,000 records per run) in seconds. The intentional trade-off is portability: the same DAG code scales to billions of records on AWS EMR or Databricks with only configuration changes, no code rewrite.

### Separation of Concerns

- **DAGs** (`airflow/dags/`): thin orchestration — Airflow context, XCom, Dataset outlets
- **Utils** (`airflow/utils/`): all business logic — testable independently, no Airflow imports required

### Data Quality as Code

Soda Core checks are declared in YAML files (`soda_checks/`) versioned alongside the pipeline. Each layer has its own contract. Checks run on the Spark DataFrame in-process — no data is collected to the driver, no separate DQ cluster needed.

---

## Monitoring & Alerting

See [MONITORING.md](./MONITORING.md) for the full observability strategy including Soda Core check details, CloudWatch metrics, Grafana dashboards, and Slack alerting.
