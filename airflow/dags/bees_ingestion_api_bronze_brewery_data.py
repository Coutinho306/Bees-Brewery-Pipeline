"""DAG Bronze - Ingestão da API (Dataset Producer)."""
import json
import logging
from datetime import datetime
from pathlib import Path
from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator
from utils.api_client import BreweryAPIClient
from utils.data_quality import validate_bronze_data

logger = logging.getLogger(__name__)


bronze_dataset = Dataset("file:///opt/airflow/data/bronze")


def on_failure_callback(context):
    """Log failure details."""
    task_instance = context.get("task_instance")
    exception = context.get("exception")
    logger.error(f"Task {task_instance.task_id} failed: {exception}")


def ingest_brewery_data(**context):
    """Fetch data from API and save as JSON."""
    execution_date = context["data_interval_end"].strftime('%Y-%m-%d')
    logger.info(f"Bronze ingestion: {execution_date}")

    client = BreweryAPIClient()
    breweries = client.fetch_all_breweries()
    client.close()

    bronze_path = Path("/opt/airflow/data/bronze")
    output_dir = bronze_path / execution_date
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / "breweries.json"
    with open(output_file, "w") as f:
        json.dump(breweries, f)

    logger.info(f"Saved {len(breweries)} records to {output_file}")
    return {"count": len(breweries), "file": str(output_file), "date": execution_date}


def quality_check_bronze(**context):
    """Run data quality checks on bronze data."""
    ti = context["ti"]
    ingest_result = ti.xcom_pull(task_ids="ingest_from_api")
    api_count = ingest_result.get("count") if ingest_result else None
    current_date = ingest_result.get("date") if ingest_result else None

    logger.info(f"Bronze quality check: {current_date}")

    bronze_path = Path("/opt/airflow/data/bronze")
    bronze_file = bronze_path / current_date / "breweries.json"

    with open(bronze_file, "r") as f:
        breweries = json.load(f)

    expected_count = api_count if api_count else 100
    quality_result = validate_bronze_data(breweries, min_records=expected_count)

    logger.info(f"Quality check passed: {quality_result}")
    return quality_result


# DAG Bronze - Runs daily automatically
with DAG(
    dag_id="bees_ingestion_api_bronze_brewery_data",
    description="Ingestão de dados da API (Bronze Layer) - Produz dataset bronze",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["brewery", "bronze", "dataset-producer"],
) as dag:

    ingest_task = PythonOperator(
        task_id="ingest_from_api",
        python_callable=ingest_brewery_data,
        retries=3,
        on_failure_callback=on_failure_callback,
    )

    quality_check_task = PythonOperator(
        task_id="quality_check_bronze",
        python_callable=quality_check_bronze,
        outlets=[bronze_dataset],
        on_failure_callback=on_failure_callback,
    )

    ingest_task >> quality_check_task
