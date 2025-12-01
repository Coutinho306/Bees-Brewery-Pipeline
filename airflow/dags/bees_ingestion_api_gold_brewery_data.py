"""DAG Gold - Agregações (Dataset Consumer)."""
import logging
import pandas as pd
from datetime import datetime
from pathlib import Path
from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator
from utils.data_quality import validate_gold_aggregations

logger = logging.getLogger(__name__)

silver_dataset = Dataset("file:///opt/airflow/data/silver")


def create_aggregations(**context):
    """Create aggregations Silver → Gold."""
    execution_date = context["ds"]
    logger.info(f"Gold aggregation triggered: {execution_date}")

    silver_dir = Path(f"/opt/airflow/data/silver/{execution_date}")

    if not silver_dir.exists():
        raise FileNotFoundError(f"Silver data not found for {execution_date}: {silver_dir}")

    logger.info(f"Reading silver data from: {silver_dir}")
    df = pd.read_parquet(silver_dir, engine="pyarrow")
    silver_count = len(df)
    logger.info(f"Read {silver_count} records from silver")

    gold_path = Path("/opt/airflow/data/gold")
    output_dir = gold_path / execution_date
    output_dir.mkdir(parents=True, exist_ok=True)

    by_type = df.groupby("brewery_type").size().reset_index(name="count")
    by_type.to_parquet(output_dir / "brewery_type_summary.parquet", index=False)
    logger.info(f"Created brewery_type_summary: {len(by_type)} types")

    by_state = df.groupby(["state", "country"]).size().reset_index(name="count")
    by_state = by_state.sort_values("count", ascending=False)
    by_state.to_parquet(output_dir / "breweries_by_state.parquet", index=False)
    logger.info(f"Created breweries_by_state: {len(by_state)} locations")

    logger.info(f"Created aggregations in {output_dir}")
    return {
        "types": len(by_type),
        "states": len(by_state),
        "silver_count": silver_count,
        "path": str(output_dir)
    }


def quality_check_gold(**context):
    """Run data quality checks on gold aggregations."""
    execution_date = context["ds"]
    logger.info(f"Gold quality check: {execution_date}")

    ti = context["ti"]
    agg_result = ti.xcom_pull(task_ids="create_aggregations")
    gold_dir_path = agg_result.get("path") if agg_result else None
    silver_count = agg_result.get("silver_count") if agg_result else None

    if not gold_dir_path:
        raise ValueError("Could not get gold path from aggregation task")

    gold_dir = Path(gold_dir_path)
    logger.info(f"Reading gold data from: {gold_dir}")

    by_type_df = pd.read_parquet(gold_dir / "brewery_type_summary.parquet")
    by_state_df = pd.read_parquet(gold_dir / "breweries_by_state.parquet")

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
