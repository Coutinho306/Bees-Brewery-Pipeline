# BEES Brewery Data Pipeline

Data engineering pipeline that ingests brewery data from Open Brewery DB API and processes it through a medallion architecture (Bronze → Silver → Gold) using Apache Airflow.

## Quick Start

```bash
1. Setup environment and generate Fernet key
  cp .env.example .env && sed -i "s/AIRFLOW__CORE__FERNET_KEY=.*/AIRFLOW__CORE__FERNET_KEY=$(python3 -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())')/" .env

2. Start services
  docker compose up -d

3. Open Airflow UI at http://localhost:8080 (login: admin/admin)

4. Trigger the Bronze DAG (Silver and Gold auto-trigger via Dataset dependencies)

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
├── tests/              # Pipeline tests
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

Comprehensive test suite covering all pipeline components.

**To run tests:**
```bash
./run_tests.sh

# Or run manually
docker-compose exec -u airflow airflow-scheduler python -m pytest tests/ -v
```

**Test Coverage:**
- ✅ DAG structure and integrity
- ✅ Data transformations (Bronze → Silver → Gold logic)
- ✅ API client error handling and retries
- ✅ Data quality module integration

See `tests/test_pipeline.py` for all test implementations.

## Monitoring & Alerting

Detailed monitoring and alerting strategy available in [MONITORING.md](./MONITORING.md).

## Design Decisions

### Why PySpark for a Small Dataset?

This project uses **PySpark** for data processing despite the brewery dataset being relatively small (~8k records). This is an **intentional architectural decision** to demonstrate production-ready patterns:

#### Key Reasons:

1. **Production-Ready Patterns**: The same code structure scales to billions of records without refactoring
2. **Distributed Processing Concepts**: Demonstrates proper partitioning, schema management, and data optimization techniques used in real-world big data pipelines
3. **Industry-Standard Tooling**: PySpark is the de facto standard for large-scale data processing in modern data platforms (Databricks, EMR, Dataproc)
4. **Scalability Mindset**: Shows understanding that even small projects should follow patterns that work at scale
5. **Portfolio Demonstration**: Aligns with big data engineering best practices from companies processing massive datasets

#### Local Development Setup:

- **Spark Local Mode**: Configured to run in `local[*]` mode within Docker containers
- **Resource Optimization**: Tuned for containerized environments (2GB driver memory, 4 shuffle partitions)
- **No Cluster Required**: Runs efficiently on a single machine for development and testing

#### Production Deployment:

In a production scenario with larger datasets, this architecture would seamlessly scale to distributed cluster processing (AWS EMR, Databricks, Google Dataproc) with **minimal code changes** - just configuration updates.

### Architecture Pattern: Separation of Concerns

The codebase follows a clean separation between **orchestration** and **business logic**:

- **DAGs** (`airflow/dags/`): Thin orchestration layer handling Airflow context, XCom, and task dependencies
- **Utils** (`airflow/utils/`): Business logic for transformations, data quality, and Spark operations
- **Benefits**: Reusable code, easier testing, clearer responsibilities

### Data Quality with Soda Core

**Soda Core** handles data quality checks directly on Spark DataFrames without converting to Pandas. This approach provides:

- **Native Spark Integration**: Runs quality checks on distributed data without collecting to driver
- **YAML-based Checks**: Declarative data contracts stored as code in `utils/soda_checks/`
- **Lightweight**: Minimal overhead compared to alternatives
- **Production Pattern**: Same tooling used in modern data platforms

Quality checks are defined per layer (Bronze, Silver, Gold) and automatically executed after each transformation.

## Technology Stack

- Apache Airflow 2.8.0
- Python 3.11
- **PySpark 3.5.0** (distributed data processing)
- PyArrow (Parquet I/O)
- **Soda Core Spark 3.3.2** (data quality)
- Docker, Docker Compose
- pytest
