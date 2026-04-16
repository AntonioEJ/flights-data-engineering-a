#!/usr/bin/env python3
"""
Gold Layer ETL Script
=====================

Denormalize Bronze-layer tables via an Athena CTAS query and register
the resulting analytical table in AWS Glue Data Catalog.

This module implements the **Gold layer** of the Medallion
architecture for the U.S. Domestic Flights 2015 dataset.

Pipeline flow::

    Bronze (Glue tables)  ─CTAS─▶  Gold analytical table (S3 + Glue)

Table produced:
    * ``flights_gold.vuelos_analitica`` — fully denormalized flight
      records with airline names, origin/destination airport details,
      delays, and cancellation data.

Usage::

    python etl/gold.py --bucket <tu-bucket>

Design decisions:
    * **Idempotent**: existing table is deleted before each CTAS run.
    * **Athena-native**: the heavy lifting (joins, projections) happens
      inside Athena — the script only orchestrates and validates.
    * **Fail-loud**: every critical step is wrapped in ``try/except``
      with ``logger.exception`` and ``sys.exit(1)``.

Authors:
    José Antonio Esparza, Gustavo Pardo

Date:
    2026-04
"""

import sys
import os
import time
import logging
import argparse
from typing import List

import pandas as pd
import boto3
from botocore.exceptions import ClientError
import awswrangler as wr

# ── Constants ────────────────────────────────────────────────────────────────

GLUE_DATABASE_GOLD: str = "flights_gold"
"""Target Glue database (Gold layer)."""

GOLD_TABLE: str = "vuelos_analitica"
"""Analytical table produced by the CTAS query."""

S3_GOLD_PREFIX: str = "s3://{bucket}/flights/gold/"
"""S3 path template for Gold output (Athena results + table data)."""

VALIDATION_COLUMNS: List[str] = [
    "airline_name",
    "origin_airport_name",
    "destination_airport_name",
]
"""Columns that must be populated (non-null) in the validation sample."""

CTAS_QUERY: str = """
CREATE TABLE flights_gold.vuelos_analitica AS (
    SELECT
        f.year,
        f.month,
        f.day,
        f.origin_airport,
        ap_orig.airport   AS origin_airport_name,
        ap_orig.city      AS origin_city,
        ap_orig.state     AS origin_state,
        f.destination_airport,
        ap_dest.airport   AS destination_airport_name,
        al.airline        AS airline_name,
        f.departure_delay,
        f.arrival_delay,
        f.cancelled,
        f.cancellation_reason,
        f.distance,
        f.air_system_delay,
        f.airline_delay,
        f.weather_delay,
        f.late_aircraft_delay,
        f.security_delay
    FROM flights_bronze.flights f
    LEFT JOIN flights_bronze.airlines al
        ON f.airline = al.iata_code
    LEFT JOIN flights_bronze.airports ap_orig
        ON f.origin_airport = ap_orig.iata_code
    LEFT JOIN flights_bronze.airports ap_dest
        ON f.destination_airport = ap_dest.iata_code
)
"""
"""Athena CTAS query that denormalizes Bronze into a Gold table."""


# ── Logging ──────────────────────────────────────────────────────────────────

def setup_logging() -> logging.Logger:
    """Create the module logger with console **and** file handlers.

    Log file is written to ``logs/gold_etl.log``.  The directory is
    created automatically if it does not exist.

    Returns:
        logging.Logger: Configured logger instance.
    """
    _logger = logging.getLogger("gold_etl")
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

        fh = logging.FileHandler("logs/gold_etl.log")
        fh.setFormatter(fmt)
        _logger.addHandler(fh)

    return _logger


logger: logging.Logger = setup_logging()


# ── Glue helpers ─────────────────────────────────────────────────────────────

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
                "Description": "Gold layer — denormalized analytical data",
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


def drop_table_if_exists(database: str, table: str) -> None:
    """Delete a Glue table **and its S3 data** if it already exists.

    Athena CTAS stores data in S3.  Dropping only the Glue catalog
    entry leaves orphan files that cause ``HIVE_PATH_ALREADY_EXISTS``
    on the next CTAS run.  This function cleans both.

    Args:
        database: Glue database name.
        table: Table name to delete.
    """
    try:
        # Retrieve S3 location before dropping the catalog entry.
        s3_location = None
        try:
            s3_location = wr.catalog.get_table_location(
                database=database, table=table,
            )
        except (ClientError, Exception):  # pylint: disable=broad-exception-caught
            pass  # Table does not exist — nothing to clean.

        wr.catalog.delete_table_if_exists(database=database, table=table)
        logger.info("  ✓ Dropped existing table '%s.%s' (if any)",
                     database, table)

        # Delete orphan S3 data from previous CTAS run.
        if s3_location:
            wr.s3.delete_objects(path=s3_location)
            logger.info("  ✓ Cleaned S3 data at %s", s3_location)

    except Exception:
        logger.exception("Failed to drop table '%s.%s'", database, table)
        raise


# ── Extract ──────────────────────────────────────────────────────────────────

def extract(database: str) -> None:
    """Validate that the required Bronze source tables exist in Glue.

    The Gold layer reads from Bronze via Athena, so there is no
    DataFrame extraction.  This step verifies that the three Bronze
    tables referenced by the CTAS query are registered in Glue.

    Args:
        database: Bronze Glue database name (``flights_bronze``).

    Raises:
        AssertionError: If any required table is missing.
    """
    logger.info("=" * 72)
    logger.info("EXTRACT — validate Bronze source tables")
    logger.info("=" * 72)

    required_tables = ["flights", "airlines", "airports"]

    try:
        existing = wr.catalog.tables(database=database)
        existing_names = set(existing["Table"].tolist())
    except Exception:
        logger.exception("Failed to list tables in '%s'", database)
        raise

    try:
        for table in required_tables:
            assert table in existing_names, (
                f"Bronze table '{database}.{table}' not found in Glue. "
                "Run bronze.py first."
            )
            logger.info("  ✓ %s.%s exists", database, table)
    except AssertionError:
        logger.exception("Bronze source validation failed")
        raise


# ── Transform ────────────────────────────────────────────────────────────────

def transform() -> str:
    """Return the CTAS query that builds the Gold analytical table.

    In the Gold layer the transformation happens inside Athena, so
    this function simply returns the SQL string for traceability.

    Returns:
        The CTAS SQL query as a string.
    """
    logger.info("=" * 72)
    logger.info("TRANSFORM — prepare CTAS query")
    logger.info("=" * 72)
    logger.info("  Query:\n%s", CTAS_QUERY.strip())
    return CTAS_QUERY


# ── Load ─────────────────────────────────────────────────────────────────────

def load(query: str, bucket: str) -> None:
    """Execute the CTAS query in Athena to create the Gold table.

    Steps:
        1. Drop existing table (idempotency).
        2. Execute CTAS via ``wr.athena.read_sql_query`` with
           ``ctas_approach=False``.

    Args:
        query: The CTAS SQL query.
        bucket: S3 bucket for Athena query results.

    Raises:
        Exception: Re-raises any exception after logging.
    """
    logger.info("=" * 72)
    logger.info("LOAD — execute CTAS in Athena")
    logger.info("=" * 72)

    # ── Idempotency: drop existing table ─────────────────────────────────
    drop_table_if_exists(GLUE_DATABASE_GOLD, GOLD_TABLE)

    # ── Execute CTAS ─────────────────────────────────────────────────────
    output_location = S3_GOLD_PREFIX.format(bucket=bucket)
    logger.info("  Athena output: %s", output_location)

    try:
        wr.athena.read_sql_query(
            sql=query,
            database=GLUE_DATABASE_GOLD,
            ctas_approach=False,
            s3_output=output_location,
        )
        logger.info("  ✓ CTAS completed successfully")

    except Exception:
        logger.exception("CTAS execution failed")
        raise


# ── Validation ───────────────────────────────────────────────────────────────

def get_row_count(bucket: str) -> int:
    """Return the total row count of the Gold table via Athena.

    Args:
        bucket: S3 bucket for Athena query results.

    Returns:
        Total number of rows in ``vuelos_analitica``.
    """
    count_sql = (
        f"SELECT COUNT(*) AS total FROM {GLUE_DATABASE_GOLD}.{GOLD_TABLE}"
    )
    output_location = S3_GOLD_PREFIX.format(bucket=bucket)

    try:
        result = wr.athena.read_sql_query(
            sql=count_sql,
            database=GLUE_DATABASE_GOLD,
            ctas_approach=False,
            s3_output=output_location,
        )
        return int(result["total"].iloc[0])
    except Exception:
        logger.exception("Row count query failed")
        raise


def validate(bucket: str) -> pd.DataFrame:
    """Run a validation query against the newly created Gold table.

    Checks:
        1. The query returns at least 1 row.
        2. Key denormalized columns (``airline_name``,
           ``origin_airport_name``, ``destination_airport_name``)
           contain non-null values in the sample.

    Args:
        bucket: S3 bucket for Athena query results.

    Returns:
        pd.DataFrame: The 5-row validation sample.

    Raises:
        AssertionError: If any validation check fails.
    """
    logger.info("=" * 72)
    logger.info("VALIDATION — verify Gold table")
    logger.info("=" * 72)

    validation_sql = (
        f"SELECT * FROM {GLUE_DATABASE_GOLD}.{GOLD_TABLE} LIMIT 5"
    )
    output_location = S3_GOLD_PREFIX.format(bucket=bucket)

    try:
        sample = wr.athena.read_sql_query(
            sql=validation_sql,
            database=GLUE_DATABASE_GOLD,
            ctas_approach=False,
            s3_output=output_location,
        )
    except Exception:
        logger.exception("Validation query failed")
        raise

    assert not sample.empty, "Validation query returned 0 rows"
    logger.info("  ✓ Validation query returned %d rows", sample.shape[0])

    for col in VALIDATION_COLUMNS:
        non_null = sample[col].notna().sum()
        assert non_null > 0, (
            f"Column '{col}' is entirely NULL in validation sample"
        )
        logger.info("  ✓ %s — %d/%d non-null values",
                     col, non_null, sample.shape[0])

    return sample


# ── Orchestrator ─────────────────────────────────────────────────────────────

def main(bucket: str) -> None:
    """Execute the full Gold ETL pipeline.

    Sequence::

        1. Validate inputs
        2. Create Glue database
        3. Extract — verify Bronze tables exist
        4. Transform — prepare CTAS query
        5. Load — execute CTAS in Athena
        6. Validate — check Gold table

    Args:
        bucket: Target S3 bucket name.

    Raises:
        SystemExit: Exits with code 1 on any unhandled error.
    """
    try:
        logger.info("\n%s", "=" * 72)
        logger.info("  FLIGHTS GOLD LAYER ETL PIPELINE")
        logger.info("  Bucket : %s", bucket)
        logger.info("%s\n", "=" * 72)

        # ── Input validation ─────────────────────────────────────────────
        if not bucket:
            raise ValueError("--bucket is required")
        logger.info("✓ Input validation passed\n")

        # ── Glue database ────────────────────────────────────────────────
        create_glue_database(GLUE_DATABASE_GOLD)

        # ── Extract ──────────────────────────────────────────────────────
        t0 = time.time()
        extract(database="flights_bronze")

        # ── Transform ────────────────────────────────────────────────────
        query = transform()

        # ── Load (CTAS) ──────────────────────────────────────────────────
        load(query, bucket)
        elapsed_ctas = time.time() - t0

        # ── Validate ─────────────────────────────────────────────────────
        t1 = time.time()
        sample = validate(bucket)
        total_rows = get_row_count(bucket)
        elapsed_val = time.time() - t1

        # ── Cifras de control ─────────────────────────────────────────────
        elapsed_total = time.time() - t0
        logger.info("\n%s", "=" * 72)
        logger.info("  CIFRAS DE CONTROL")
        logger.info("%s", "-" * 72)
        logger.info("  Tabla creada              : %s.%s",
                     GLUE_DATABASE_GOLD, GOLD_TABLE)
        logger.info("  Filas totales             : %s", f"{total_rows:,}")
        logger.info("  Columnas                  : %d", sample.shape[1])
        logger.info("%s", "-" * 72)
        for col in VALIDATION_COLUMNS:
            non_null = sample[col].notna().sum()
            logger.info("  %-27s : %d/%d non-null",
                        col, non_null, sample.shape[0])
        logger.info("%s", "-" * 72)
        logger.info("  CTAS (extract+load)       : %.1f s", elapsed_ctas)
        logger.info("  Validación                : %.1f s", elapsed_val)
        logger.info("  Total                     : %.1f s", elapsed_total)
        logger.info("%s", "=" * 72)
        logger.info("  ✓ GOLD LAYER COMPLETED SUCCESSFULLY")
        logger.info("%s\n", "=" * 72)

    except Exception:  # pylint: disable=broad-exception-caught
        logger.exception("Gold ETL pipeline failed")
        sys.exit(1)


# ── CLI ──────────────────────────────────────────────────────────────────────

def parse_arguments() -> argparse.Namespace:
    """Parse CLI arguments.

    Returns:
        Namespace with ``bucket``.
    """
    parser = argparse.ArgumentParser(
        description="Gold Layer ETL — Athena CTAS denormalization → Glue",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python etl/gold.py --bucket my-bucket\n"
        ),
    )
    parser.add_argument(
        "--bucket", required=True, help="AWS S3 bucket name",
    )
    return parser.parse_args()


# ── Entry point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    try:
        args = parse_arguments()
        main(bucket=args.bucket)
    except KeyboardInterrupt:
        logger.warning("Interrupted by user")
        sys.exit(130)
    except SystemExit:
        raise  # noqa: W0706 — must re-raise to avoid catching sys.exit()
    except Exception:  # pylint: disable=broad-exception-caught
        logger.exception("Unexpected error")
        sys.exit(1)
