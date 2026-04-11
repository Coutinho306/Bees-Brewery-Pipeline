"""Spark transformation logic for BEES Brewery Pipeline.

This module contains all data transformation business logic for the
Silver and Gold layers, separated from DAG orchestration code.
"""
import logging
from pathlib import Path
from typing import Dict, Any
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit, size
from utils.spark_utils import get_spark_session, get_brewery_schema, get_string_columns

logger = logging.getLogger(__name__)


def transform_bronze_to_silver(bronze_file: Path, output_path: Path) -> Dict[str, Any]:
    """
    Transform Bronze → Silver (JSON → Parquet) using PySpark.

    Transformations applied:
    - Schema enforcement for data quality
    - Null handling for string columns (fill with empty string)
    - Standardization (fill empty state/country with "unknown")
    - Deduplication based on brewery ID
    - Partitioning by state and country for query optimization

    Args:
        bronze_file: Path to bronze JSON file
        output_path: Path to write silver parquet files

    Returns:
        Dictionary with transformation metadata (count, bronze_count, path)

    Raises:
        FileNotFoundError: If bronze file doesn't exist
    """
    if not bronze_file.exists():
        raise FileNotFoundError(f"Bronze data not found: {bronze_file}")

    spark = get_spark_session("BEES-Silver-Layer")

    try:
        logger.info(f"Reading bronze data from: {bronze_file}")
        schema = get_brewery_schema()
        df = spark.read.schema(schema).json(str(bronze_file))
        bronze_count = df.count()
        logger.info(f"Read {bronze_count} records from bronze")

        # Apply Silver transformations
        df = apply_silver_cleansing(df, schema)

        # Write partitioned Parquet with Snappy compression
        logger.info(f"Writing silver data to: {output_path}")
        df.write \
            .mode("overwrite") \
            .partitionBy("state", "country") \
            .option("compression", "snappy") \
            .parquet(str(output_path))

        record_count = df.count()
        logger.info(f"Saved {record_count} records to {output_path}")

        return {
            "count": record_count,
            "bronze_count": bronze_count,
            "path": str(output_path)
        }

    finally:
        spark.stop()
        logger.info("Spark session stopped")


def apply_silver_cleansing(df: DataFrame, schema) -> DataFrame:
    """
    Apply Silver layer data cleansing and standardization.

    - Fill null string columns with empty strings
    - Replace empty state/country with "unknown"
    - Remove duplicate records by brewery ID

    Args:
        df: Input Spark DataFrame
        schema: Brewery schema for extracting string columns

    Returns:
        Cleansed Spark DataFrame
    """
    # Data cleansing: Fill null string columns with empty strings
    string_columns = get_string_columns(schema)
    for column in string_columns:
        df = df.withColumn(
            column,
            when(col(column).isNull(), lit("")).otherwise(col(column))
        )

    # Standardization: Replace empty state/country with "unknown"
    df = df.withColumn(
        "state",
        when(col("state") == "", lit("unknown")).otherwise(col("state"))
    )
    df = df.withColumn(
        "country",
        when(col("country") == "", lit("unknown")).otherwise(col("country"))
    )

    # Deduplication based on brewery ID
    df = df.dropDuplicates(["id"])

    return df


def read_silver_data(silver_path: Path) -> DataFrame:
    """
    Read Silver layer data using PySpark.

    Args:
        silver_path: Path to silver parquet directory

    Returns:
        Spark DataFrame with silver data

    Raises:
        FileNotFoundError: If silver path doesn't exist
    """
    if not silver_path.exists():
        raise FileNotFoundError(f"Silver data not found: {silver_path}")

    spark = get_spark_session("BEES-Silver-Reader")

    try:
        logger.info(f"Reading silver data from: {silver_path}")
        df = spark.read.parquet(str(silver_path))
        return df
    finally:
        # Note: Don't stop spark here, as caller needs to use the DataFrame
        pass


def create_gold_aggregations(silver_dir: Path, output_dir: Path) -> Dict[str, Any]:
    """
    Create Gold layer aggregations from Silver data.

    Creates two aggregations:
    1. Breweries by type (brewery_type_summary.parquet)
    2. Breweries by state and country (breweries_by_state.parquet)

    Args:
        silver_dir: Path to silver parquet directory
        output_dir: Path to write gold aggregations

    Returns:
        Dictionary with aggregation metadata

    Raises:
        FileNotFoundError: If silver directory doesn't exist
    """
    if not silver_dir.exists():
        raise FileNotFoundError(f"Silver data not found: {silver_dir}")

    spark = get_spark_session("BEES-Gold-Layer")

    try:
        logger.info(f"Reading silver data from: {silver_dir}")
        df = spark.read.parquet(str(silver_dir))
        silver_count = df.count()
        logger.info(f"Read {silver_count} records from silver")

        # Ensure output directory exists
        output_dir.mkdir(parents=True, exist_ok=True)

        # Aggregation 1: By brewery type
        by_type = df.groupBy("brewery_type") \
            .count() \
            .withColumnRenamed("count", "count") \
            .orderBy("brewery_type")

        by_type_path = output_dir / "brewery_type_summary.parquet"
        by_type.write.mode("overwrite").parquet(str(by_type_path))
        logger.info(f"Created brewery_type_summary: {by_type.count()} types")

        # Aggregation 2: By state and country
        by_state = df.groupBy("state", "country") \
            .count() \
            .withColumnRenamed("count", "count") \
            .orderBy(col("count").desc())

        by_state_path = output_dir / "breweries_by_state.parquet"
        by_state.write.mode("overwrite").parquet(str(by_state_path))
        logger.info(f"Created breweries_by_state: {by_state.count()} locations")

        logger.info(f"Created aggregations in {output_dir}")
        return {
            "types": by_type.count(),
            "states": by_state.count(),
            "silver_count": silver_count,
            "path": str(output_dir)
        }

    finally:
        spark.stop()
        logger.info("Spark session stopped")


def read_gold_aggregations(gold_dir: Path) -> tuple:
    """
    Read Gold layer aggregations.

    Args:
        gold_dir: Path to gold aggregations directory

    Returns:
        Tuple of (by_type_df, by_state_df) Spark DataFrames

    Raises:
        FileNotFoundError: If gold files don't exist
    """
    spark = get_spark_session("BEES-Gold-Reader")

    try:
        by_type_path = gold_dir / "brewery_type_summary.parquet"
        by_state_path = gold_dir / "breweries_by_state.parquet"

        if not by_type_path.exists() or not by_state_path.exists():
            raise FileNotFoundError(f"Gold aggregations not found in {gold_dir}")

        logger.info(f"Reading gold data from: {gold_dir}")
        by_type_df = spark.read.parquet(str(by_type_path))
        by_state_df = spark.read.parquet(str(by_state_path))

        return by_type_df, by_state_df

    finally:
        # Note: Don't stop spark here, as caller needs to use the DataFrames
        pass
