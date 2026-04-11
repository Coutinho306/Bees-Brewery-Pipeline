"""Data Quality Checks using Soda Core for Brewery Pipeline."""
import logging
from typing import Dict, Any, Optional
from pathlib import Path
from soda.scan import Scan
from pyspark.sql import DataFrame as SparkDataFrame, SparkSession
import pandas as pd

logger = logging.getLogger(__name__)


class DataQualityError(Exception):
    """Raised when data quality checks fail."""
    pass


def run_soda_scan(
    spark: SparkSession,
    df: SparkDataFrame,
    table_name: str,
    checks_file: str
) -> Dict[str, Any]:
    """
    Run Soda data quality checks on Spark DataFrame.

    Args:
        spark: Spark session
        df: Spark DataFrame to validate
        table_name: Name to register the temp table
        checks_file: Path to Soda checks YAML file

    Returns:
        Dictionary with validation results

    Raises:
        DataQualityError: If checks fail
    """
    df.createOrReplaceTempView(table_name)

    scan = Scan()
    scan.set_scan_definition_name(f"{table_name}_scan")
    scan.add_spark_session(spark, data_source_name="spark_df")

    checks_path = Path(__file__).parent / "soda_checks" / checks_file
    scan.add_sodacl_yaml_file(str(checks_path))

    scan.execute()

    # Get check results using Soda Core API
    scan_results = scan.get_scan_results()
    checks = scan_results.get('checks', [])

    passed = 0
    failed = 0
    warned = 0

    for check in checks:
        outcome = check.get('outcome')
        if outcome == 'pass':
            passed += 1
        elif outcome == 'fail':
            failed += 1
        elif outcome == 'warn':
            warned += 1

    total = passed + failed + warned

    result = {
        "data_source": table_name,
        "total_checks": total,
        "passed": passed,
        "failed": failed,
        "warned": warned,
        "success": failed == 0
    }

    if failed > 0:
        logger.error(f"❌ {table_name} quality checks FAILED: {failed} failures")
        for check in checks:
            if check.get('outcome') == 'fail':
                logger.error(f"  - {check.get('name', 'unknown')}")
        raise DataQualityError(
            f"{table_name}: {failed} quality checks failed"
        )

    logger.info(f"✓ {table_name} quality checks passed: {passed}/{total}")
    return result


def validate_bronze_data(data: list, min_records: int = 100) -> Dict[str, Any]:
    """Validate bronze layer data quality."""
    from utils.spark_utils import get_spark_session

    logger.info(f"Running Bronze quality checks on {len(data)} records")

    spark = get_spark_session("BEES-Bronze-QualityCheck")

    try:
        df = spark.createDataFrame(pd.DataFrame(data))
        return run_soda_scan(spark, df, "breweries", "bronze_checks.yml")
    finally:
        spark.stop()


def validate_silver_data(
    df: SparkDataFrame,
    bronze_count: Optional[int] = None
) -> Dict[str, Any]:
    """Validate silver layer data quality."""
    record_count = df.count()
    logger.info(f"Running Silver quality checks on {record_count} records")

    # Use the Spark session from the DataFrame
    spark = df.sparkSession

    result = run_soda_scan(spark, df, "breweries", "silver_checks.yml")

    if bronze_count is not None:
        if record_count != bronze_count:
            raise DataQualityError(
                f"Silver row count mismatch - expected {bronze_count}, got {record_count}"
            )
        result["bronze_count"] = bronze_count

    return result


def validate_gold_aggregations(
    by_type_df: SparkDataFrame,
    by_state_df: SparkDataFrame,
    silver_count: Optional[int] = None
) -> Dict[str, Any]:
    """Validate gold layer aggregations."""
    logger.info(f"Running Gold quality checks on aggregations")

    # Use the Spark session from the DataFrames
    spark = by_type_df.sparkSession

    type_result = run_soda_scan(
        spark, by_type_df, "brewery_type_summary", "gold_type_checks.yml"
    )

    state_result = run_soda_scan(
        spark, by_state_df, "breweries_by_state", "gold_state_checks.yml"
    )

    if silver_count is not None:
        type_total = by_type_df.agg({"count": "sum"}).collect()[0][0]
        state_total = by_state_df.agg({"count": "sum"}).collect()[0][0]

        if type_total != silver_count:
            raise DataQualityError(
                f"Gold: Type aggregation mismatch - expected {silver_count}, got {type_total}"
            )

        if state_total != silver_count:
            raise DataQualityError(
                f"Gold: State aggregation mismatch - expected {silver_count}, got {state_total}"
            )

    result = {
        "type_checks": type_result,
        "state_checks": state_result,
        "total_passed": type_result["passed"] + state_result["passed"],
        "total_failed": type_result["failed"] + state_result["failed"],
    }

    logger.info(f"✓ Gold quality checks passed")
    return result
