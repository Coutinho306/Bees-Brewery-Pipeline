"""DAG Silver - Transformação (Dataset Consumer & Producer)."""
import logging
from datetime import datetime
from pathlib import Path
from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator
from utils.spark_transformations import transform_bronze_to_silver, read_silver_data
from utils.data_quality import validate_silver_data

logger = logging.getLogger(__name__)

bronze_dataset = Dataset("file:///opt/airflow/data/bronze")
silver_dataset = Dataset("file:///opt/airflow/data/silver")


def transform_to_parquet(**context):
    """Orchestrate Bronze → Silver transformation."""
    execution_date = context["data_interval_end"].strftime('%Y-%m-%d')
    logger.info(f"Silver transformation triggered: {execution_date}")

    # Construct paths based on execution date
    bronze_file = Path(f"/opt/airflow/data/bronze/{execution_date}/breweries.json")
    output_path = Path("/opt/airflow/data/silver") / execution_date

    # Execute transformation (business logic in utils)
    result = transform_bronze_to_silver(bronze_file, output_path)

    logger.info(f"Transformation complete: {result['count']} records")
    return result


def quality_check_silver(**context):
    """Orchestrate Silver data quality checks."""
    execution_date = context["ds"]
    logger.info(f"Silver quality check: {execution_date}")

    # Get transformation metadata from previous task via XCom
    ti = context["ti"]
    transform_result = ti.xcom_pull(task_ids="transform_to_parquet")
    silver_path = transform_result.get("path") if transform_result else None
    bronze_count = transform_result.get("bronze_count") if transform_result else None

    if not silver_path:
        raise ValueError("Could not get silver path from transform task")

    # Read silver data and run quality checks (business logic in utils)
    df = read_silver_data(Path(silver_path))
    quality_result = validate_silver_data(df, bronze_count=bronze_count)

    logger.info(f"Quality check passed: {quality_result}")
    return quality_result


# DAG Silver - Auto-triggers when Bronze completes
with DAG(
    dag_id="bees_ingestion_api_silver_brewery_data",
    description="Transformação Bronze → Silver - Trigger automático via Dataset",
    schedule=[bronze_dataset],
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["brewery", "silver", "dataset-consumer", "dataset-producer"],
) as dag:

    transform_task = PythonOperator(
        task_id="transform_to_parquet",
        python_callable=transform_to_parquet,
    )

    quality_check_task = PythonOperator(
        task_id="quality_check_silver",
        python_callable=quality_check_silver,
        outlets=[silver_dataset],
    )

    # Task dependencies: transform -> quality_check (triggers silver dataset)
    transform_task >> quality_check_task
