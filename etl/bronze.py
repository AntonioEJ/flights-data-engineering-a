#!/usr/bin/env python3
"""
Bronze Layer ETL Script
=======================

Ingest raw CSV data from a local directory, upload to Amazon S3 as
Parquet, and register tables in AWS Glue Data Catalog.

This module implements the **Bronze layer** of the Medallion
architecture for the U.S. Domestic Flights 2015 dataset (~5.8 M rows).
Bronze stores data as-is from the source with minimal transformation
(column name uppercasing only).

Pipeline flow::

    CSV (local) ──▶ Validate ──▶ Parquet (S3) ──▶ Glue Catalog

Usage::

    python etl/bronze.py --bucket <tu-bucket> --data-dir data/flights/

Design decisions:
    * **Idempotent**: Glue DB created with ``exist_ok=True``; small
      tables use ``mode='overwrite'``; flights first chunk overwrites,
      subsequent chunks append.
    * **Memory-safe**: ``flights.csv`` (~5.8 M rows) is read in chunks
      of ``CHUNK_SIZE`` rows to avoid OOM on constrained environments.
    * **Fail-loud**: every critical step is wrapped in ``try/except``
      with ``logger.exception`` and ``sys.exit(1)``.

Authors:
    José Antonio Esparza, Gustavo Pardo

Date:
    2026-04
"""

import sys
import os
import logging
import argparse
from typing import Dict, List

import pandas as pd
import boto3
from botocore.exceptions import ClientError
import awswrangler as wr

# ── Constants ────────────────────────────────────────────────────────────────

CSV_FILES: Dict[str, str] = {
    "flights":  "flights.csv",
    "airlines": "airlines.csv",
    "airports": "airports.csv",
}
"""Mapping of logical table names to their source CSV filenames."""

GLUE_DATABASE: str = "flights_bronze"
"""AWS Glue Data Catalog database name for the Bronze layer."""

S3_PREFIX_TEMPLATE: str = "s3://{bucket}/flights/bronze/{table}/"
"""S3 URI template.  Placeholders: ``{bucket}`` and ``{table}``."""

CHUNK_SIZE: int = 500_000
"""Number of rows per chunk when reading ``flights.csv``."""

# Expected key columns per table — used for pre-write validation.
REQUIRED_COLUMNS: Dict[str, List[str]] = {
    "flights":  ["AIRLINE", "ORIGIN_AIRPORT", "DESTINATION_AIRPORT",
                 "FLIGHT_NUMBER"],
    "airlines": ["IATA_CODE", "AIRLINE"],
    "airports": ["IATA_CODE", "AIRPORT", "CITY", "STATE"],
}
"""Columns that *must* exist in each table before writing to S3."""

NON_NULL_COLUMNS: Dict[str, List[str]] = {
    "airlines": ["IATA_CODE"],
    "airports": ["IATA_CODE"],
}
"""Columns that must have zero NULLs for a table to pass validation."""


# ── Logging ──────────────────────────────────────────────────────────────────

def setup_logging() -> logging.Logger:
    """Create the module logger with console **and** file handlers.

    Log file is written to ``logs/bronze_etl.log``.  The directory is
    created automatically if it does not exist.

    Returns:
        logging.Logger: Configured logger instance.
    """
    _logger = logging.getLogger("bronze_etl")
    _logger.setLevel(logging.INFO)

    os.makedirs("logs", exist_ok=True)

    fmt = logging.Formatter(
        fmt="[%(asctime)s] [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    if not _logger.handlers:
        console = logging.StreamHandler()
        console.setFormatter(fmt)
        _logger.addHandler(console)

        fh = logging.FileHandler("logs/bronze_etl.log")
        fh.setFormatter(fmt)
        _logger.addHandler(fh)

    return _logger


logger: logging.Logger = setup_logging()


# ── Validation helpers ───────────────────────────────────────────────────────

def validate_file(file_path: str) -> None:
    """Raise ``FileNotFoundError`` if *file_path* does not exist.

    Args:
        file_path: Path to verify.

    Raises:
        FileNotFoundError: When the file is absent.
    """
    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")


def validate_dataframe(df: pd.DataFrame, table_name: str) -> None:
    """Run Bronze-level quality checks on *df*.

    Checks performed:
        1. DataFrame is not empty.
        2. Required columns exist (per ``REQUIRED_COLUMNS``).
        3. Non-null columns have zero NULLs (per ``NON_NULL_COLUMNS``).

    Args:
        df: The DataFrame to validate.
        table_name: Logical table name (for error messages and lookups).

    Raises:
        AssertionError: If any check fails.
    """
    # 1. Non-empty
    assert not df.empty, f"DataFrame for '{table_name}' is empty"
    assert df.shape[0] > 0, f"'{table_name}' has 0 rows"
    assert df.shape[1] > 0, f"'{table_name}' has 0 columns"

    # 2. Required columns
    required = REQUIRED_COLUMNS.get(table_name, [])
    for col in required:
        assert col in df.columns, (
            f"'{table_name}' is missing required column '{col}'"
        )

    # 3. Non-null constraints
    for col in NON_NULL_COLUMNS.get(table_name, []):
        if col in df.columns:
            null_count = int(df[col].isna().sum())
            assert null_count == 0, (
                f"'{table_name}.{col}' has {null_count} NULLs"
            )

    logger.info("  ✓ %s passed validation (%d rows, %d cols)",
                table_name, df.shape[0], df.shape[1])


# ── Extract ──────────────────────────────────────────────────────────────────

def extract_small_tables(data_dir: str) -> Dict[str, pd.DataFrame]:
    """Read ``airlines.csv`` and ``airports.csv`` fully into memory.

    ``flights.csv`` is excluded because it is too large; it is handled
    by :func:`process_flights_chunked`.

    Args:
        data_dir: Directory containing the CSV files.

    Returns:
        Dict mapping ``"airlines"`` and ``"airports"`` to DataFrames.

    Raises:
        FileNotFoundError: If a CSV file is missing.
    """
    logger.info("=" * 72)
    logger.info("EXTRACT — small tables (airlines, airports)")
    logger.info("=" * 72)

    data: Dict[str, pd.DataFrame] = {}
    for table, filename in CSV_FILES.items():
        if table == "flights":
            continue

        path = os.path.join(data_dir, filename)
        try:
            validate_file(path)
            df = pd.read_csv(path, engine="c")
            df.columns = df.columns.str.upper()
            data[table] = df
            logger.info("  ✓ %s: %d rows, %d cols",
                        table, df.shape[0], df.shape[1])
        except Exception:
            logger.exception("Failed to read %s", path)
            raise

    return data


# ── Transform ────────────────────────────────────────────────────────────────

def transform(
    data: Dict[str, pd.DataFrame],
) -> Dict[str, pd.DataFrame]:
    """Validate every DataFrame and uppercase column names.

    Args:
        data: Raw DataFrames keyed by table name.

    Returns:
        The same dict after validation (columns already uppercased in
        :func:`extract_small_tables`).

    Raises:
        AssertionError: If any DataFrame fails validation.
    """
    logger.info("=" * 72)
    logger.info("TRANSFORM — validate schemas")
    logger.info("=" * 72)

    for table, df in data.items():
        try:
            validate_dataframe(df, table)
        except Exception:
            logger.exception("Validation failed for %s", table)
            raise

    return data


# ── Load ─────────────────────────────────────────────────────────────────────

def create_glue_database(database: str) -> None:
    """Create an AWS Glue database if it does not already exist.

    Uses ``boto3`` directly with ``CreateDatabase`` to avoid the
    ``glue:UpdateDatabase`` permission required by awswrangler's
    ``exist_ok=True``.

    Args:
        database: Glue database name.

    Raises:
        botocore.exceptions.ClientError: On unexpected AWS API failure.
    """
    try:
        glue = boto3.client("glue")
        glue.create_database(
            DatabaseInput={
                "Name": database,
                "Description": "Bronze layer — raw flights data",
            }
        )
        logger.info("  ✓ Glue database '%s' created", database)
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "AlreadyExistsException":
            logger.info("  ✓ Glue database '%s' already exists", database)
        else:
            logger.exception("Failed to create Glue database '%s'", database)
            raise
    except Exception:
        logger.exception("Failed to create Glue database '%s'", database)
        raise


def upload_to_s3(
    df: pd.DataFrame,
    table_name: str,
    bucket: str,
    database: str,
) -> None:
    """Write *df* to S3 as Parquet and register it in Glue.

    Uses ``mode='overwrite'`` so the operation is **idempotent**.

    Args:
        df: DataFrame to upload.
        table_name: Logical table name (S3 prefix + Glue table).
        bucket: S3 bucket name.
        database: Glue database name.

    Raises:
        botocore.exceptions.ClientError: On AWS API failure.
    """
    s3_path = S3_PREFIX_TEMPLATE.format(bucket=bucket, table=table_name)
    try:
        wr.s3.to_parquet(
            df=df,
            path=s3_path,
            dataset=True,
            database=database,
            table=table_name,
            mode="overwrite",
            compression="snappy",
        )
        logger.info("  ✓ Tabla %s cargada con %d filas en %s",
                     table_name, df.shape[0], s3_path)
    except Exception:
        logger.exception("Failed to upload '%s' to %s", table_name, s3_path)
        raise


def load_small_tables(
    data: Dict[str, pd.DataFrame],
    bucket: str,
    database: str,
) -> None:
    """Upload airlines and airports to S3 and register in Glue.

    Args:
        data: Validated DataFrames (no flights).
        bucket: S3 bucket.
        database: Glue database.
    """
    logger.info("=" * 72)
    logger.info("LOAD — small tables to S3")
    logger.info("=" * 72)

    for table, df in data.items():
        upload_to_s3(df, table, bucket, database)


def process_flights_chunked(
    data_dir: str,
    bucket: str,
    database: str,
) -> int:
    """Read ``flights.csv`` in chunks, validate, and upload to S3.

    Each chunk is uploaded immediately and then freed from memory.
    The first chunk uses ``mode='overwrite'``; subsequent chunks use
    ``mode='append'``.  This guarantees idempotency (re-running
    replaces all previous data).

    Args:
        data_dir: Directory with ``flights.csv``.
        bucket: S3 bucket.
        database: Glue database.

    Returns:
        Total number of rows processed.

    Raises:
        FileNotFoundError: If the CSV is missing.
        AssertionError: If a chunk fails validation.
        botocore.exceptions.ClientError: On AWS failure.
    """
    logger.info("=" * 72)
    logger.info("LOAD — flights (chunked, %d rows/chunk)", CHUNK_SIZE)
    logger.info("=" * 72)

    file_path = os.path.join(data_dir, CSV_FILES["flights"])
    validate_file(file_path)

    s3_path = S3_PREFIX_TEMPLATE.format(bucket=bucket, table="flights")
    logger.info("  Source : %s", file_path)
    logger.info("  Target : %s", s3_path)

    total_rows = 0
    reader = pd.read_csv(file_path, engine="c", chunksize=CHUNK_SIZE)

    for i, chunk in enumerate(reader):
        chunk.columns = chunk.columns.str.upper()

        # Validate every chunk
        assert not chunk.empty, f"Chunk {i} of flights is empty"
        for col in REQUIRED_COLUMNS["flights"]:
            assert col in chunk.columns, (
                f"Chunk {i}: missing required column '{col}'"
            )

        mode = "overwrite" if i == 0 else "append"
        try:
            wr.s3.to_parquet(
                df=chunk,
                path=s3_path,
                dataset=True,
                database=database,
                table="flights",
                mode=mode,
                compression="snappy",
            )
        except Exception:
            logger.exception("Failed uploading chunk %d of flights", i)
            raise

        total_rows += len(chunk)
        logger.info("  ✓ Chunk %d: %d rows (acumulado: %d)",
                     i + 1, len(chunk), total_rows)
        del chunk

    logger.info("  ✓ Tabla flights cargada con %d filas en %s",
                total_rows, s3_path)
    return total_rows


# ── Orchestrator ─────────────────────────────────────────────────────────────

def main(bucket: str, data_dir: str) -> None:
    """Execute the full Bronze ETL pipeline.

    Sequence::

        1. Validate inputs
        2. Create Glue database
        3. Extract + Transform + Load  (airlines, airports)
        4. Chunked Extract + Load      (flights)

    Args:
        bucket: Target S3 bucket name.
        data_dir: Local directory with the source CSV files.

    Raises:
        SystemExit: Exits with code 1 on any unhandled error.
    """
    try:
        logger.info("\n%s", "=" * 72)
        logger.info("  FLIGHTS BRONZE LAYER ETL PIPELINE")
        logger.info("  Bucket     : %s", bucket)
        logger.info("  Data dir   : %s", data_dir)
        logger.info("%s\n", "=" * 72)

        # ── Input validation ─────────────────────────────────────────────
        if not bucket:
            raise ValueError("--bucket is required")
        if not data_dir:
            raise ValueError("--data-dir is required")
        if not os.path.isdir(data_dir):
            raise FileNotFoundError(
                f"Data directory not found: {data_dir}"
            )
        logger.info("✓ Input validation passed\n")

        # ── Glue database ────────────────────────────────────────────────
        create_glue_database(GLUE_DATABASE)

        # ── Small tables: airlines, airports ─────────────────────────────
        raw_data = extract_small_tables(data_dir)
        validated_data = transform(raw_data)
        load_small_tables(validated_data, bucket, GLUE_DATABASE)

        # ── Large table: flights (chunked) ───────────────────────────────
        process_flights_chunked(data_dir, bucket, GLUE_DATABASE)

        # ── Done ─────────────────────────────────────────────────────────
        logger.info("\n%s", "=" * 72)
        logger.info("  ✓ BRONZE LAYER COMPLETED SUCCESSFULLY")
        logger.info("%s\n", "=" * 72)

    except Exception:  # pylint: disable=broad-exception-caught
        logger.exception("Bronze ETL pipeline failed")
        sys.exit(1)


# ── CLI ──────────────────────────────────────────────────────────────────────

def parse_arguments() -> argparse.Namespace:
    """Parse CLI arguments.

    Returns:
        Namespace with ``bucket`` and ``data_dir``.
    """
    parser = argparse.ArgumentParser(
        description="Bronze Layer ETL — raw CSV → S3 Parquet + Glue",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python etl/bronze.py --bucket my-bucket "
            "--data-dir data/flights/\n"
        ),
    )
    parser.add_argument(
        "--bucket", required=True, help="AWS S3 bucket name",
    )
    parser.add_argument(
        "--data-dir", required=True,
        help="Local directory containing CSV files",
    )
    return parser.parse_args()


# ── Entry point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    try:
        args = parse_arguments()
        main(bucket=args.bucket, data_dir=args.data_dir)
    except KeyboardInterrupt:
        logger.warning("Interrupted by user")
        sys.exit(130)
    except SystemExit:
        raise  # noqa: W0706 — must re-raise to avoid catching sys.exit()
    except Exception:  # pylint: disable=broad-exception-caught
        logger.exception("Unexpected error")
        sys.exit(1)
