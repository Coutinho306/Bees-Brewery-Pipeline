"""Spark utilities for BEES Brewery Pipeline.

This module contains Spark session management, schema definitions, and
common transformations used across the Silver and Gold layers.
"""
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

logger = logging.getLogger(__name__)


def get_spark_session(app_name: str = "BEES-Brewery-Pipeline") -> SparkSession:
    """
    Create or get Spark session configured for local mode.

    Configured with optimizations suitable for small-to-medium datasets
    running in containerized environments.

    Args:
        app_name: Name of the Spark application

    Returns:
        SparkSession configured for local processing
    """
    logger.info(f"Initializing Spark session: {app_name}")

    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.ui.enabled", "false") \
        .getOrCreate()

    # Set log level to reduce noise
    spark.sparkContext.setLogLevel("WARN")

    logger.info(f"Spark session initialized: {spark.version}")
    return spark


def get_brewery_schema() -> StructType:
    """
    Define explicit schema for brewery data from Open Brewery DB API.

    Using explicit schemas improves performance and ensures data quality
    by enforcing types during read operations.

    Returns:
        StructType with brewery data schema
    """
    return StructType([
        StructField("id", StringType(), False),  # Primary key - required
        StructField("name", StringType(), True),
        StructField("brewery_type", StringType(), True),
        StructField("address_1", StringType(), True),
        StructField("address_2", StringType(), True),
        StructField("address_3", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state_province", StringType(), True),
        StructField("postal_code", StringType(), True),
        StructField("country", StringType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("phone", StringType(), True),
        StructField("website_url", StringType(), True),
        StructField("state", StringType(), True),
        StructField("street", StringType(), True),
    ])


def get_string_columns(schema: StructType) -> list:
    """
    Extract string column names from a Spark schema.

    Useful for applying string-specific transformations like
    null filling or trimming.

    Args:
        schema: Spark StructType schema

    Returns:
        List of column names with StringType
    """
    return [
        field.name
        for field in schema.fields
        if isinstance(field.dataType, StringType)
    ]
