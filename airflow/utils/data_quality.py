"""Data Quality Checks using Great Expectations for Brewery Pipeline."""
import logging
from typing import Dict, Any, Optional
import pandas as pd
import great_expectations as gx

logger = logging.getLogger(__name__)


class DataQualityError(Exception):
    """Raised when data quality checks fail."""
    pass


def run_ge_validation(
    data_source: str,
    df: pd.DataFrame,
    expectations: list
) -> Dict[str, Any]:
    """
    Run Great Expectations validation on DataFrame.

    Args:
        data_source: Name of the data source
        df: DataFrame to validate
        expectations: List of expectation configurations

    Returns:
        Dictionary with validation results

    Raises:
        DataQualityError: If critical checks fail
    """
    context = gx.get_context()

    datasource = context.sources.add_or_update_pandas(name=f"{data_source}_source")
    data_asset = datasource.add_dataframe_asset(name=f"{data_source}_asset")
    batch_request = data_asset.build_batch_request(dataframe=df)

    suite_name = f"{data_source}_suite"
    context.add_or_update_expectation_suite(expectation_suite_name=suite_name)

    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name
    )

    for exp in expectations:
        exp_type = exp.pop("type")
        getattr(validator, exp_type)(**exp)

    results = validator.validate()

    failed_expectations = [
        r for r in results.results
        if not r.success
    ]

    passed_count = len([r for r in results.results if r.success])
    failed_count = len(failed_expectations)

    result = {
        "data_source": data_source,
        "total_checks": len(results.results),
        "passed": passed_count,
        "failed": failed_count,
        "success": results.success
    }

    if not results.success:
        logger.error(f"❌ {data_source} quality checks FAILED: {failed_count} failures")
        for fail in failed_expectations:
            logger.error(f"  - {fail.expectation_config.expectation_type}")
        raise DataQualityError(
            f"{data_source}: {failed_count} quality checks failed"
        )

    logger.info(f"✓ {data_source} quality checks passed: {passed_count}/{len(results.results)}")
    return result


def validate_bronze_data(data: list, min_records: int = 100) -> Dict[str, Any]:
    """Validate bronze layer data quality."""
    logger.info(f"Running Bronze quality checks on {len(data)} records")

    df = pd.DataFrame(data)

    expectations = [
        {"type": "expect_table_row_count_to_be_between", "min_value": min_records},
        {"type": "expect_column_to_exist", "column": "id"},
        {"type": "expect_column_to_exist", "column": "name"},
        {"type": "expect_column_to_exist", "column": "brewery_type"},
        {"type": "expect_column_values_to_not_be_null", "column": "id"},
        {"type": "expect_column_values_to_be_unique", "column": "id"},
    ]

    return run_ge_validation("bronze", df, expectations)


def validate_silver_data(
    df: pd.DataFrame,
    bronze_count: Optional[int] = None
) -> Dict[str, Any]:
    """Validate silver layer data quality."""
    logger.info(f"Running Silver quality checks on {len(df)} records")

    if bronze_count:
        expectations = [
            {"type": "expect_table_row_count_to_equal", "value": bronze_count},
            {"type": "expect_column_to_exist", "column": "id"},
            {"type": "expect_column_to_exist", "column": "state"},
            {"type": "expect_column_to_exist", "column": "country"},
            {"type": "expect_column_values_to_not_be_null", "column": "id"},
            {"type": "expect_column_values_to_be_unique", "column": "id"},
            {"type": "expect_column_values_to_not_be_null", "column": "state"},
            {"type": "expect_column_values_to_not_be_null", "column": "country"},
        ]
    else:
        expectations = [
            {"type": "expect_table_row_count_to_be_between", "min_value": 100},
            {"type": "expect_column_to_exist", "column": "id"},
            {"type": "expect_column_to_exist", "column": "state"},
            {"type": "expect_column_to_exist", "column": "country"},
            {"type": "expect_column_values_to_not_be_null", "column": "id"},
            {"type": "expect_column_values_to_be_unique", "column": "id"},
            {"type": "expect_column_values_to_not_be_null", "column": "state"},
            {"type": "expect_column_values_to_not_be_null", "column": "country"},
        ]

    result = run_ge_validation("silver", df, expectations)

    if bronze_count is not None:
        result["bronze_count"] = bronze_count

    return result


def validate_gold_aggregations(
    by_type_df: pd.DataFrame,
    by_state_df: pd.DataFrame,
    silver_count: Optional[int] = None
) -> Dict[str, Any]:
    """Validate gold layer aggregations."""
    logger.info(f"Running Gold quality checks on aggregations")

    type_max_value = silver_count - 1 if silver_count else None
    type_expectations = [
        {"type": "expect_table_row_count_to_be_between", "min_value": 3, "max_value": type_max_value} if type_max_value else {"type": "expect_table_row_count_to_be_between", "min_value": 3},
        {"type": "expect_column_to_exist", "column": "brewery_type"},
        {"type": "expect_column_to_exist", "column": "count"},
        {"type": "expect_column_values_to_not_be_null", "column": "brewery_type"},
        {"type": "expect_column_values_to_be_unique", "column": "brewery_type"},
    ]

    type_result = run_ge_validation("gold_by_type", by_type_df, type_expectations)

    state_max_value = silver_count - 1 if silver_count else None
    state_expectations = [
        {"type": "expect_table_row_count_to_be_between", "min_value": 10, "max_value": state_max_value} if state_max_value else {"type": "expect_table_row_count_to_be_between", "min_value": 10},
        {"type": "expect_column_to_exist", "column": "state"},
        {"type": "expect_column_to_exist", "column": "country"},
        {"type": "expect_column_to_exist", "column": "count"},
        {"type": "expect_column_values_to_not_be_null", "column": "state"},
        {"type": "expect_column_values_to_not_be_null", "column": "country"},
        {"type": "expect_compound_columns_to_be_unique", "column_list": ["state", "country"]},
    ]

    state_result = run_ge_validation("gold_by_state", by_state_df, state_expectations)

    if silver_count is not None:
        type_total = by_type_df["count"].sum()
        state_total = by_state_df["count"].sum()

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
