"""DAG Silver - Transformação (Dataset Consumer & Producer)."""
import logging
import pandas as pd
from datetime import datetime
from pathlib import Path
from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator
from utils.data_quality import validate_silver_data

logger = logging.getLogger(__name__)

bronze_dataset = Dataset("file:///opt/airflow/data/bronze")
silver_dataset = Dataset("file:///opt/airflow/data/silver")


def transform_to_parquet(**context):
    """Transform Bronze → Silver (JSON → Parquet)."""
    execution_date = context["ds"]
    logger.info(f"Silver transformation triggered: {execution_date}")

    bronze_file = Path(f"/opt/airflow/data/bronze/{execution_date}/breweries.json")

    if not bronze_file.exists():
        raise FileNotFoundError(f"Bronze data not found for {execution_date}: {bronze_file}")

    logger.info(f"Reading bronze data from: {bronze_file}")
    df = pd.read_json(bronze_file)
    bronze_count = len(df)
    logger.info(f"Read {bronze_count} records from bronze")

    # Fill only string columns with empty strings, leave numeric columns as NaN
    string_columns = df.select_dtypes(include=['object']).columns
    df[string_columns] = df[string_columns].fillna("")

    df["state"] = df["state"].fillna("unknown")
    df["country"] = df["country"].fillna("unknown")
    df = df.drop_duplicates(subset=["id"])

    silver_path = Path("/opt/airflow/data/silver")
    output_path = silver_path / execution_date
    df.to_parquet(
        output_path,
        engine="pyarrow",
        partition_cols=["state", "country"],
        compression="snappy",
        index=False
    )

    logger.info(f"Saved {len(df)} records to {output_path}")
    return {"count": len(df), "bronze_count": bronze_count, "path": str(output_path)}


def quality_check_silver(**context):
    """Run data quality checks on silver data."""
    execution_date = context["ds"]
    logger.info(f"Silver quality check: {execution_date}")

    ti = context["ti"]
    transform_result = ti.xcom_pull(task_ids="transform_to_parquet")
    silver_path = transform_result.get("path") if transform_result else None
    bronze_count = transform_result.get("bronze_count") if transform_result else None

    if not silver_path:
        raise ValueError("Could not get silver path from transform task")

    logger.info(f"Reading silver data from: {silver_path}")
    df = pd.read_parquet(silver_path)

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
