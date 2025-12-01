# BEES Brewery Data Pipeline

Data engineering pipeline that ingests brewery data from Open Brewery DB API and processes it through a medallion architecture (Bronze → Silver → Gold) using Apache Airflow.

## Quick Start

**Security Note:** The project uses a `.env` file for configuration. A proper Fernet key is required for Airflow encryption.

```bash
1. Setup environment (update passwords in .env.example and copy to .env)

2. Start services
docker-compose up -d

3. Open Airflow UI
http://localhost:8080

4. Trigger DAGs in order:
    - bees_ingestion_api_bronze_brewery_data
    - bees_ingestion_api_silver_brewery_data (auto-triggers from bronze dataset)
    - bees_ingestion_api_gold_brewery_data (auto-triggers from silver dataset)

5. Run tests
./run_tests.sh
```

## Architecture

**Medallion Architecture:**
```
API → Bronze (Raw JSON) → Silver (Clean Parquet) → Gold (Aggregations)
```

- **Bronze**: Raw data from API stored as JSON
- **Silver**: Cleansed data in Parquet, partitioned by state/country
- **Gold**: Aggregated analytics (by type, by state)

## Project Structure

```
case_bees/
├── airflow/
│   ├── dags/           # 3 DAGs (bronze, silver, gold)
│   └── utils/          # API client, data quality checks
├── tests/              # Pipeline tests (24 tests total)
├── data/               # Data lake (auto-generated)
├── .env.example        # Environment configuration template
└── docker-compose.yml  # Docker setup
```

## Data Flow

### Bronze Layer
- **Input**: Open Brewery DB API
- **Process**: Fetch all breweries via pagination
- **Output**: `data/bronze/YYYY-MM-DD/breweries.json`
- **Quality**: Min records, required fields, unique IDs

### Silver Layer
- **Input**: Bronze JSON
- **Process**:
  - **Data Cleansing**: Fill nulls in string columns with empty strings
  - **Standardization**: Fill null state/country with "unknown"
  - **Deduplication**: Remove duplicate records based on brewery ID
  - **Format Conversion**: JSON → Parquet with Snappy compression
  - **Partitioning**: Partition by state and country for query optimization
- **Output**: `data/silver/YYYY-MM-DD/state=*/country=*/` (Parquet partitioned by location)
- **Quality**: Row count matches bronze, no nulls in key fields

### Gold Layer
- **Input**: Silver Parquet
- **Process**: Aggregate by type/location and by state
- **Output**: `data/gold/YYYY-MM-DD/` (Aggregated Parquet)
- **Quality**: Aggregation counts match silver totals

## Testing

Comprehensive test suite with **24 tests** covering all pipeline components.

**To run tests:**
```bash
./run_tests.sh

# Or run manually
docker-compose exec -u airflow airflow-scheduler python -m pytest tests/ -v
```

**Test Coverage:**
- ✅ DAG structure and integrity (5 tests)
- ✅ Data transformations - Bronze → Silver → Gold (5 tests)
- ✅ API client error scenarios (6 tests)
- ✅ Data quality validation failures (8 tests)

See `tests/test_pipeline.py` for all test implementations.

## Monitoring & Alerting

Detailed monitoring and alerting strategy available in [MONITORING.md](./MONITORING.md).

## Technology Stack

- Apache Airflow 2.8.0
- Python 3.11
- Pandas, PyArrow
- Great Expectations
- Docker, Docker Compose
- pytest
