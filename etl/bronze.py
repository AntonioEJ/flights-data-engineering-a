#!/usr/bin/env python3
"""
Bronze Layer ETL Script
=======================

Ingest raw CSV data from a local directory, upload to Amazon S3 as Parquet,
and register tables in AWS Glue Data Catalog.

This module implements the Bronze layer of the Medallion architecture
for the U.S. Domestic Flights 2015 dataset (~5.8M records).

Usage:
    .. code-block:: bash

        python etl/bronze.py --bucket <tu-bucket> --data-dir data/

Module Attributes:
    CSV_FILES (dict): Mapping of table names to their CSV filenames.
    GLUE_DATABASE (str): Name of the Glue database for the Bronze layer.
    S3_BRONZE_PATH_TEMPLATE (str): Template for S3 paths in the Bronze layer.

Author:
    José Antonio Esparza, Gustavo Pardo

Date:
    2026-04
"""

import sys
import os
import logging
import argparse
from typing import Dict
import pandas as pd
import awswrangler as wr

# ---------------------------------------------------------------------------
# Module Constants
# ---------------------------------------------------------------------------

CSV_FILES: Dict[str, str] = {
    "flights": "flights.csv",
    "airlines": "airlines.csv",
    "airports": "airports.csv",
}
"""dict: Mapping of logical table names to their source CSV filenames."""

GLUE_DATABASE: str = "flights_bronze"
"""str: AWS Glue Data Catalog database name for the Bronze layer."""

S3_BRONZE_PATH_TEMPLATE: str = "s3://{bucket}/flights/bronze/{table}/"
"""str: S3 URI template. Placeholders: ``{bucket}`` and ``{table}``."""

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------


def setup_logging() -> logging.Logger:
    """Configure and return the module-level logger.

    Creates a ``StreamHandler`` with a timestamped format so every log
    line includes date-time, level, and message.

    Returns:
        logging.Logger: Configured logger instance for the module.

    Example:
        >>> logger = setup_logging()
        >>> logger.info("Pipeline started")
        [2026-04-15 10:00:00] [INFO] Pipeline started
    """
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        fmt="[%(asctime)s] [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    if not logger.handlers:
        logger.addHandler(handler)
    return logger


logger: logging.Logger = setup_logging()
"""logging.Logger: Module-level logger used across all functions."""

# ---------------------------------------------------------------------------
# Validation Helpers
# ---------------------------------------------------------------------------


def validate_dataframe(df: pd.DataFrame, table_name: str) -> None:
    """Validate that a DataFrame is not empty and contains data.

    Args:
        df (pd.DataFrame): The DataFrame to validate.
        table_name (str): Logical name of the table (used in error messages).

    Raises:
        AssertionError: If the DataFrame is empty or has zero rows/columns.

    Example:
        >>> import pandas as pd
        >>> df = pd.DataFrame({"col": [1, 2]})
        >>> validate_dataframe(df, "test")  # passes silently
    """
    assert not df.empty, f"DataFrame for {table_name} is empty"
    assert df.shape[0] > 0, f"Table {table_name} has no rows"
    assert df.shape[1] > 0, f"Table {table_name} has no columns"


def validate_file_path(file_path: str) -> None:
    """Validate that a file exists on disk.

    Args:
        file_path (str): Absolute or relative path to the file.

    Raises:
        FileNotFoundError: If the file does not exist.

    Example:
        >>> validate_file_path("data/flights.csv")  # raises if missing
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")


# ---------------------------------------------------------------------------
# Extract Phase
# ---------------------------------------------------------------------------


def extract(data_dir: str) -> Dict[str, pd.DataFrame]:
    """Read all source CSV files from a local directory.

    Iterates over :pydata:`CSV_FILES`, reads each CSV into a
    ``pandas.DataFrame``, and returns them keyed by table name.

    Args:
        data_dir (str): Path to the directory that contains the CSV files.

    Returns:
        Dict[str, pd.DataFrame]: Dictionary mapping each table name
        (``flights``, ``airlines``, ``airports``) to its DataFrame.

    Raises:
        FileNotFoundError: If any expected CSV file is missing.
        pd.errors.ParserError: If a CSV file cannot be parsed.

    Example:
        >>> data = extract("data/")
        >>> data.keys()
        dict_keys(['flights', 'airlines', 'airports'])
    """
    logger.info("=" * 80)
    logger.info("STARTING EXTRACT PHASE")
    logger.info("=" * 80)

    data: Dict[str, pd.DataFrame] = {}
    for table_name, filename in CSV_FILES.items():
        file_path = os.path.join(data_dir, filename)
        try:
            logger.info(f"Reading file: {file_path}")
            validate_file_path(file_path)
            df = pd.read_csv(file_path)
            data[table_name] = df
            logger.info(
                f"✓ Read {table_name}: {df.shape[0]:,} rows, {df.shape[1]} columns"
            )
        except Exception:
            logger.exception(f"Failed to read {filename}")
            raise

    logger.info("=" * 80)
    logger.info("EXTRACT PHASE COMPLETED")
    logger.info("=" * 80)
    return data


# ---------------------------------------------------------------------------
# Transform Phase
# ---------------------------------------------------------------------------


def transform(data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    """Apply Bronze-level validations and column standardization.

    For each DataFrame the function:

    * Validates it is non-empty via :func:`validate_dataframe`.
    * Uppercases all column names for consistency downstream.

    Args:
        data (Dict[str, pd.DataFrame]): Raw DataFrames from
            :func:`extract`.

    Returns:
        Dict[str, pd.DataFrame]: Validated DataFrames with uppercase
        column names, keyed by table name.

    Raises:
        AssertionError: If any DataFrame fails validation.

    Example:
        >>> transformed = transform({"airlines": df_airlines})
        >>> list(transformed["airlines"].columns)
        ['IATA_CODE', 'AIRLINE']
    """
    logger.info("=" * 80)
    logger.info("STARTING TRANSFORM PHASE")
    logger.info("=" * 80)

    transformed_data: Dict[str, pd.DataFrame] = {}
    for table_name, df in data.items():
        try:
            logger.info(f"Validating {table_name}...")
            validate_dataframe(df, table_name)
            df.columns = df.columns.str.upper()
            logger.info(f"✓ {table_name} passed validation")
            transformed_data[table_name] = df
        except Exception:
            logger.exception(f"Transformation failed for {table_name}")
            raise

    logger.info("=" * 80)
    logger.info("TRANSFORM PHASE COMPLETED")
    logger.info("=" * 80)
    return transformed_data


# ---------------------------------------------------------------------------
# Load Phase
# ---------------------------------------------------------------------------


def create_glue_database(database_name: str) -> None:
    """Create an AWS Glue database if it does not already exist.

    Uses ``awswrangler.catalog.create_database`` with ``exist_ok=True``
    to guarantee idempotency.

    Args:
        database_name (str): Name of the Glue database to create.

    Raises:
        botocore.exceptions.ClientError: If the AWS API call fails.

    Example:
        >>> create_glue_database("flights_bronze")
    """
    try:
        logger.info(f"Creating Glue database: {database_name}")
        wr.catalog.create_database(
            name=database_name,
            description="Bronze layer for flights data",
            exist_ok=True,
        )
        logger.info(f"✓ Glue database '{database_name}' ready")
    except Exception:
        logger.exception("Failed to create Glue database")
        raise


def upload_to_s3(
    df: pd.DataFrame, table_name: str, bucket: str, database: str
) -> None:
    """Upload a DataFrame to S3 as Parquet and register it in Glue.

    Writes the data using Snappy compression and ``mode='overwrite'``
    to ensure idempotent execution.

    Args:
        df (pd.DataFrame): The DataFrame to upload.
        table_name (str): Logical table name (used for S3 path and
            Glue table registration).
        bucket (str): Target S3 bucket name.
        database (str): Glue database where the table is registered.

    Raises:
        botocore.exceptions.ClientError: If the S3/Glue API call fails.

    Example:
        >>> upload_to_s3(df_flights, "flights", "my-bucket", "flights_bronze")
    """
    try:
        s3_path = S3_BRONZE_PATH_TEMPLATE.format(bucket=bucket, table=table_name)
        logger.info(f"Uploading {table_name} to S3: {s3_path}")

        wr.s3.to_parquet(
            df=df,
            path=s3_path,
            dataset=True,
            database=database,
            table=table_name,
            mode="overwrite",
            compression="snappy",
        )

        row_count = df.shape[0]
        logger.info(f"✓ Table '{table_name}' loaded with {row_count:,} rows")
    except Exception:
        logger.exception(f"Failed to upload {table_name} to S3")
        raise


def load(
    data: Dict[str, pd.DataFrame],
    bucket: str,
    database: str = GLUE_DATABASE,
) -> None:
    """Load all DataFrames to S3 and register them in Glue Data Catalog.

    Orchestrates :func:`create_glue_database` followed by
    :func:`upload_to_s3` for every table in *data*.

    Args:
        data (Dict[str, pd.DataFrame]): Validated DataFrames to load.
        bucket (str): Target S3 bucket name.
        database (str, optional): Glue database name.
            Defaults to :pydata:`GLUE_DATABASE`.

    Raises:
        Exception: Re-raises any exception from sub-functions after
            logging.

    Example:
        >>> load({"flights": df, "airlines": df2}, "my-bucket")
    """
    logger.info("=" * 80)
    logger.info("STARTING LOAD PHASE")
    logger.info("=" * 80)

    try:
        create_glue_database(database)
        for table_name, df in data.items():
            upload_to_s3(df, table_name, bucket, database)
        logger.info("=" * 80)
        logger.info("LOAD PHASE COMPLETED")
        logger.info("=" * 80)
    except Exception:
        logger.exception("Load phase failed")
        raise


# ---------------------------------------------------------------------------
# Pipeline Orchestrator
# ---------------------------------------------------------------------------


def main(bucket: str, data_dir: str) -> None:
    """Execute the full Bronze-layer ETL pipeline.

    Sequence: **Extract → Transform → Load**.

    Args:
        bucket (str): Target S3 bucket name.
        data_dir (str): Local directory containing the source CSV files.

    Raises:
        ValueError: If *bucket* or *data_dir* are empty.
        FileNotFoundError: If *data_dir* does not exist.
        SystemExit: Exits with code ``1`` on any unhandled error.

    Example:
        >>> main(bucket="my-bucket", data_dir="data/")
    """
    try:
        logger.info("\n" + "╔" + "=" * 78 + "╗")
        logger.info(
            "║" + " " * 20 + "FLIGHTS BRONZE LAYER ETL PIPELINE" + " " * 24 + "║"
        )
        logger.info("╚" + "=" * 78 + "╝")
        logger.info(f"Bucket: {bucket}")
        logger.info(f"Data Directory: {data_dir}\n")

        if not bucket:
            raise ValueError("Bucket name is required")
        if not data_dir:
            raise ValueError("Data directory is required")

        logger.info(f"Validating data directory: {data_dir}")
        if not os.path.isdir(data_dir):
            raise FileNotFoundError(f"Data directory not found: {data_dir}")
        logger.info("✓ Data directory exists\n")

        raw_data = extract(data_dir)
        transformed_data = transform(raw_data)
        load(transformed_data, bucket)

        logger.info("\n" + "╔" + "=" * 78 + "╗")
        logger.info(
            "║" + " " * 25 + "✓ BRONZE LAYER COMPLETED" + " " * 29 + "║"
        )
        logger.info("╚" + "=" * 78 + "╝\n")

    except Exception:
        logger.exception("Bronze layer ETL pipeline failed")
        sys.exit(1)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments for the Bronze ETL script.

    Returns:
        argparse.Namespace: Parsed arguments with ``bucket`` and
        ``data_dir`` attributes.

    Example:
        >>> args = parse_arguments()
        >>> args.bucket
        'my-bucket'
    """
    parser = argparse.ArgumentParser(
        description="Bronze Layer ETL - Load raw data to S3 and register in Glue",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python etl/bronze.py --bucket my-data-bucket --data-dir data/
  python etl/bronze.py --bucket flights-prod --data-dir /path/to/csv/files/
        """,
    )

    parser.add_argument(
        "--bucket",
        type=str,
        required=True,
        help="AWS S3 bucket name (required)",
    )

    parser.add_argument(
        "--data-dir",
        type=str,
        required=True,
        help="Local directory containing CSV files (required)",
    )

    return parser.parse_args()


# ---------------------------------------------------------------------------
# Entry Point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    try:
        args = parse_arguments()
        main(bucket=args.bucket, data_dir=args.data_dir)
    except KeyboardInterrupt:
        logger.warning("Script interrupted by user")
        sys.exit(1)
    except Exception:
        logger.exception("Unexpected error")
        sys.exit(1)