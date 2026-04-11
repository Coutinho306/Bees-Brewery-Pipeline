"""DAG Gold - Agregações (Dataset Consumer)."""
import logging
from datetime import datetime, timedelta
from pathlib import Path
from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator
from utils.spark_transformations import create_gold_aggregations, read_gold_aggregations
from utils.data_quality import validate_gold_aggregations

logger = logging.getLogger(__name__)

silver_dataset = Dataset("file:///opt/airflow/data/silver")


def create_aggregations(**context):
    """Orchestrate Silver → Gold aggregation."""
    execution_date = context["data_interval_end"].strftime('%Y-%m-%d')
    logger.info(f"Gold aggregation triggered: {execution_date}")

    # Construct paths based on execution date
    silver_dir = Path(f"/opt/airflow/data/silver/{execution_date}")
    output_dir = Path("/opt/airflow/data/gold") / execution_date

    # Execute aggregations (business logic in utils)
    result = create_gold_aggregations(silver_dir, output_dir)

    logger.info(f"Aggregation complete: {result['types']} types, {result['states']} states")
    return result


def quality_check_gold(**context):
    """Orchestrate Gold data quality checks."""
    execution_date = context["ds"]
    logger.info(f"Gold quality check: {execution_date}")

    # Get aggregation metadata from previous task via XCom
    ti = context["ti"]
    agg_result = ti.xcom_pull(task_ids="create_aggregations")
    gold_dir_path = agg_result.get("path") if agg_result else None
    silver_count = agg_result.get("silver_count") if agg_result else None

    if not gold_dir_path:
        raise ValueError("Could not get gold path from aggregation task")

    # Read gold aggregations and run quality checks (business logic in utils)
    gold_dir = Path(gold_dir_path)
    by_type_df, by_state_df = read_gold_aggregations(gold_dir)

    quality_result = validate_gold_aggregations(
        by_type_df=by_type_df,
        by_state_df=by_state_df,
        silver_count=silver_count
    )

    logger.info(f"Quality check passed: {quality_result}")
    return quality_result


# DAG Gold - Auto-triggers when Silver completes
with DAG(
    dag_id="bees_ingestion_api_gold_brewery_data",
    description="Agregações Silver → Gold - Trigger automático via Dataset",
    schedule=[silver_dataset],
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["brewery", "gold", "dataset-consumer"],
) as dag:

    aggregate_task = PythonOperator(
        task_id="create_aggregations",
        python_callable=create_aggregations,
    )

    quality_check_task = PythonOperator(
        task_id="quality_check_gold",
        python_callable=quality_check_gold,
    )

    aggregate_task >> quality_check_task
