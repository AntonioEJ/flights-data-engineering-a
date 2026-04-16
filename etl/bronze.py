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
from concurrent.futures import ThreadPoolExecutor, as_completed  # fix C0411
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

CHUNK_SIZE: int = 500_000
"""int: Number of rows per chunk when reading large CSV files."""

FLIGHTS_DTYPES: Dict[str, str] = {
    "YEAR": "int16",
    "MONTH": "int8", 
    "DAY": "int8",
    "DAY_OF_WEEK": "int8",
    "AIRLINE": "category",
    "FLIGHT_NUMBER": "int32",
    "TAIL_NUMBER": "str",
    "ORIGIN_AIRPORT": "category", 
    "DESTINATION_AIRPORT": "category",
    "SCHEDULED_DEPARTURE": "int32",
    "DEPARTURE_TIME": "float32",
    "DEPARTURE_DELAY": "float32",
    "TAXI_OUT": "float32",
    "WHEELS_OFF": "float32",
    "SCHEDULED_TIME": "float32",
    "ELAPSED_TIME": "float32",
    "AIR_TIME": "float32",
    "DISTANCE": "float32",
    "WHEELS_ON": "float32",
    "TAXI_IN": "float32",
    "SCHEDULED_ARRIVAL": "int32",
    "ARRIVAL_TIME": "float32",
    "ARRIVAL_DELAY": "float32",
    "DIVERTED": "int8",
    "CANCELLED": "int8",
    "CANCELLATION_REASON": "category",
    "AIR_SYSTEM_DELAY": "float32",
    "SECURITY_DELAY": "float32",
    "AIRLINE_DELAY": "float32",
    "LATE_AIRCRAFT_DELAY": "float32",
    "WEATHER_DELAY": "float32",
}
"""dict: Data types for the flights table."""

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------


def setup_logging() -> logging.Logger:
    """Configure and return the module-level logger with file and console output."""
    _logger = logging.getLogger(__name__)  # fix W0621: renamed from logger to _logger
    _logger.setLevel(logging.INFO)

    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    formatter = logging.Formatter(
        fmt="[%(asctime)s] [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    log_file = os.path.join(log_dir, "bronze_etl.log")
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)

    if not _logger.handlers:
        _logger.addHandler(console_handler)
        _logger.addHandler(file_handler)

    return _logger


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
    """Read small CSV files from a local directory (excludes flights).

    Reads ``airlines.csv`` and ``airports.csv`` into memory.
    The large ``flights.csv`` is handled separately by
    :func:`extract_and_load_flights` using chunked processing.

    Args:
        data_dir (str): Path to the directory that contains the CSV files.

    Returns:
        Dict[str, pd.DataFrame]: Dictionary mapping each small table name
        (``airlines``, ``airports``) to its DataFrame.

    Raises:
        FileNotFoundError: If any expected CSV file is missing.
        pd.errors.ParserError: If a CSV file cannot be parsed.

    Example:
        >>> data = extract("data/")
        >>> data.keys()
        dict_keys(['airlines', 'airports'])
    """
    logger.info("=" * 80)
    logger.info("STARTING EXTRACT PHASE (small tables)")
    logger.info("=" * 80)

    data: Dict[str, pd.DataFrame] = {}
    for table_name, filename in CSV_FILES.items():
        if table_name == "flights":
            continue  # handled by extract_and_load_flights
        file_path = os.path.join(data_dir, filename)
        try:
            logger.info("Reading file: %s", file_path)  # fix W1203
            validate_file_path(file_path)
            df = pd.read_csv(file_path, engine="c")
            data[table_name] = df
            logger.info(
                "✓ Read %s: %d rows, %d columns",  # fix W1203
                table_name, df.shape[0], df.shape[1]
            )
        except Exception:
            logger.exception("Failed to read %s", filename)  # fix W1203
            raise

    logger.info("=" * 80)
    logger.info("EXTRACT PHASE COMPLETED (small tables)")
    logger.info("=" * 80)
    return data


def extract_and_load_flights(
    data_dir: str, bucket: str, database: str = GLUE_DATABASE
) -> int:
    """Read flights.csv in chunks and upload each chunk to S3 as Parquet.

    Processes the large flights file in batches of :pydata:`CHUNK_SIZE`
    rows to avoid exceeding available memory.  The first chunk uses
    ``mode='overwrite'`` to replace any previous data; subsequent chunks
    use ``mode='append'``.

    Args:
        data_dir (str): Path to the directory that contains ``flights.csv``.
        bucket (str): Target S3 bucket name.
        database (str, optional): Glue database name.
            Defaults to :pydata:`GLUE_DATABASE`.

    Returns:
        int: Total number of rows processed.

    Raises:
        FileNotFoundError: If ``flights.csv`` is missing.
        botocore.exceptions.ClientError: If the S3/Glue API call fails.

    Example:
        >>> total = extract_and_load_flights("data/", "my-bucket")
        >>> total
        5819811
    """
    file_path = os.path.join(data_dir, CSV_FILES["flights"])
    validate_file_path(file_path)

    s3_path = S3_BRONZE_PATH_TEMPLATE.format(bucket=bucket, table="flights")
    logger.info("Processing flights.csv in chunks of %d rows", CHUNK_SIZE)
    logger.info("Target S3 path: %s", s3_path)

    total_rows = 0
    chunks = pd.read_csv(file_path, engine="c", chunksize=CHUNK_SIZE)

    for i, chunk in enumerate(chunks):
        chunk.columns = chunk.columns.str.upper()
        mode = "overwrite" if i == 0 else "append"
        wr.s3.to_parquet(
            df=chunk,
            path=s3_path,
            dataset=True,
            database=database,
            table="flights",
            mode=mode,
            compression="snappy",
        )
        total_rows += len(chunk)
        logger.info(
            "✓ Chunk %d uploaded: %d rows (total: %d)",
            i + 1, len(chunk), total_rows
        )
        del chunk  # free memory immediately

    logger.info("✓ flights table loaded with %d total rows", total_rows)
    return total_rows


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
            logger.info("Validating %s...", table_name)  # fix W1203
            validate_dataframe(df, table_name)
            df.columns = df.columns.str.upper()
            logger.info("✓ %s passed validation", table_name)  # fix W1203
            transformed_data[table_name] = df
        except Exception:
            logger.exception("Transformation failed for %s", table_name)  # fix W1203
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
        logger.info("Creating Glue database: %s", database_name)  # fix W1203
        wr.catalog.create_database(
            name=database_name,
            description="Bronze layer for flights data",
            exist_ok=True,
        )
        logger.info("✓ Glue database '%s' ready", database_name)  # fix W1203
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
        logger.info("Uploading %s to S3: %s", table_name, s3_path)  # fix W1203

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
        logger.info("✓ Table '%s' loaded with %d rows", table_name, row_count)  # fix W1203
    except Exception:
        logger.exception("Failed to upload %s to S3", table_name)  # fix W1203
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

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = {
                executor.submit(upload_to_s3, df, name, bucket, database): name
                for name, df in data.items()
            }
            for future in as_completed(futures):
                future.result()

        logger.info("=" * 80)
        logger.info("LOAD PHASE COMPLETED")
        logger.info("=" * 80)
    except Exception:  # pylint: disable=broad-exception-caught
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
        logger.info("\n╔%s╗", "=" * 78)
        logger.info("║%sFLIGHTS BRONZE LAYER ETL PIPELINE%s║", " " * 20, " " * 24)
        logger.info("╚%s╝", "=" * 78)
        logger.info("Bucket: %s", bucket)          # fix W1203
        logger.info("Data Directory: %s\n", data_dir)  # fix W1203

        if not bucket:
            raise ValueError("Bucket name is required")
        if not data_dir:
            raise ValueError("Data directory is required")

        logger.info("Validating data directory: %s", data_dir)  # fix W1203
        if not os.path.isdir(data_dir):
            raise FileNotFoundError(f"Data directory not found: {data_dir}")
        logger.info("✓ Data directory exists\n")

        # --- Small tables (airlines, airports): full in-memory pipeline ---
        raw_data = extract(data_dir)
        transformed_data = transform(raw_data)
        load(transformed_data, bucket)

        # --- Flights: chunked read → upload to avoid OOM ---
        logger.info("=" * 80)
        logger.info("STARTING CHUNKED FLIGHTS PROCESSING")
        logger.info("=" * 80)
        extract_and_load_flights(data_dir, bucket)

        logger.info("\n╔%s╗", "=" * 78)
        logger.info("║%s✓ BRONZE LAYER COMPLETED%s║", " " * 25, " " * 29)
        logger.info("╚%s╝\n", "=" * 78)

    except Exception:  # pylint: disable=broad-exception-caught
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
    except Exception:  # pylint: disable=broad-exception-caught  # fix W0718
        logger.exception("Unexpected error")
        sys.exit(1)
