#!/usr/bin/env python3
"""
Silver Layer ETL Script
=======================

Read raw Parquet data from the Bronze layer in S3, apply business-level
aggregations, and write the resulting tables back to S3 as Parquet
registered in AWS Glue Data Catalog.

This module implements the **Silver layer** of the Medallion
architecture for the U.S. Domestic Flights 2015 dataset.

Pipeline flow::

    Bronze (S3 Parquet) ──▶ Aggregate ──▶ Silver (S3 Parquet) ──▶ Glue

Tables produced:
    * ``flights_daily``      — daily flight statistics.
    * ``flights_monthly``    — monthly statistics by airline.
    * ``flights_by_airport`` — departure statistics by origin airport.

Usage::

    python etl/silver.py --bucket <tu-bucket>

Design decisions:
    * **Idempotent**: Glue DB created idempotently; ``flights_daily``
      uses ``overwrite_partitions`` (partitioned by MONTH);
      remaining tables use ``mode='overwrite'``.
    * **Memory-safe**: reads Bronze data from S3 (already Parquet),
      aggregations reduce row count dramatically.
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

GLUE_DATABASE_SILVER: str = "flights_silver"
"""Target Glue database (Silver layer)."""

S3_BRONZE_PREFIX: str = "s3://{bucket}/flights/bronze/flights/"
"""S3 path template for the Bronze flights table."""

S3_SILVER_PREFIX: str = "s3://{bucket}/flights/silver/{table}/"
"""S3 path template for Silver output tables."""

# Pre-write validation: columns that must exist in each Silver table.
REQUIRED_COLUMNS: Dict[str, List[str]] = {
    "flights_daily": [
        "YEAR", "MONTH", "DAY", "TOTAL_FLIGHTS", "TOTAL_DELAYED",
        "TOTAL_CANCELLED", "AVG_DEPARTURE_DELAY", "AVG_ARRIVAL_DELAY",
    ],
    "flights_monthly": [
        "MONTH", "AIRLINE", "TOTAL_FLIGHTS", "TOTAL_DELAYED",
        "TOTAL_CANCELLED", "AVG_ARRIVAL_DELAY", "ON_TIME_PCT",
    ],
    "flights_by_airport": [
        "ORIGIN_AIRPORT", "TOTAL_DEPARTURES", "TOTAL_DELAYED",
        "TOTAL_CANCELLED", "AVG_DEPARTURE_DELAY", "PCT_WEATHER_DELAY",
    ],
}
"""Columns that *must* exist in each Silver table before writing."""


# ── Logging ──────────────────────────────────────────────────────────────────

def setup_logging() -> logging.Logger:
    """Create the module logger with console **and** file handlers.

    Log file is written to ``logs/silver_etl.log``.  The directory is
    created automatically if it does not exist.

    Returns:
        logging.Logger: Configured logger instance.
    """
    _logger = logging.getLogger("silver_etl")
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

        fh = logging.FileHandler("logs/silver_etl.log")
        fh.setFormatter(fmt)
        _logger.addHandler(fh)

    return _logger


logger: logging.Logger = setup_logging()


# ── Validation helpers ───────────────────────────────────────────────────────

def validate_dataframe(df: pd.DataFrame, table_name: str) -> None:
    """Run Silver-level quality checks on *df*.

    Checks performed:
        1. DataFrame is not empty.
        2. Required columns exist (per ``REQUIRED_COLUMNS``).

    Args:
        df: The DataFrame to validate.
        table_name: Logical table name (for error messages and lookups).

    Raises:
        AssertionError: If any check fails.
    """
    assert not df.empty, f"DataFrame for '{table_name}' is empty"
    assert df.shape[0] > 0, f"'{table_name}' has 0 rows"
    assert df.shape[1] > 0, f"'{table_name}' has 0 columns"

    required = REQUIRED_COLUMNS.get(table_name, [])
    for col in required:
        assert col in df.columns, (
            f"'{table_name}' is missing required column '{col}'"
        )

    logger.info("  ✓ %s passed validation (%d rows, %d cols)",
                table_name, df.shape[0], df.shape[1])


# ── Extract ──────────────────────────────────────────────────────────────────

def extract(bucket: str) -> pd.DataFrame:
    """Read the ``flights`` table from the Bronze layer in S3.

    Args:
        bucket: S3 bucket name where Bronze data resides.

    Returns:
        pd.DataFrame: Raw flights data from Bronze.

    Raises:
        Exception: If the S3 read fails (logged before re-raise).
    """
    logger.info("=" * 72)
    logger.info("EXTRACT — reading flights from Bronze layer")
    logger.info("=" * 72)

    s3_path = S3_BRONZE_PREFIX.format(bucket=bucket)
    logger.info("  Source: %s", s3_path)

    try:
        df = wr.s3.read_parquet(path=s3_path)
        df.columns = df.columns.str.upper()
        logger.info("  ✓ flights: %d rows, %d cols",
                     df.shape[0], df.shape[1])
    except Exception:
        logger.exception("Failed to read Bronze flights from %s", s3_path)
        raise

    # Validate source has essential columns
    try:
        for col in ["YEAR", "MONTH", "DAY", "AIRLINE", "ORIGIN_AIRPORT",
                    "DESTINATION_AIRPORT", "DEPARTURE_DELAY", "ARRIVAL_DELAY",
                    "CANCELLED", "WEATHER_DELAY"]:
            assert col in df.columns, (
                f"Bronze flights missing required column '{col}'"
            )
    except AssertionError:
        logger.exception("Bronze data validation failed")
        raise

    return df


# ── Transform ────────────────────────────────────────────────────────────────

def build_flights_daily(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate flights to daily level.

    Metrics:
        * ``TOTAL_FLIGHTS`` — count of flights per day.
        * ``TOTAL_DELAYED`` — flights with DEPARTURE_DELAY > 0.
        * ``TOTAL_CANCELLED`` — flights with CANCELLED == 1.
        * ``AVG_DEPARTURE_DELAY`` — mean delay excluding cancelled.
        * ``AVG_ARRIVAL_DELAY`` — mean delay excluding cancelled.

    Args:
        df: Raw flights DataFrame from Bronze.

    Returns:
        pd.DataFrame: Daily aggregation with YEAR, MONTH, DAY as keys.
    """
    logger.info("  Building flights_daily ...")

    not_cancelled = df[df["CANCELLED"] != 1]

    agg = df.groupby(["YEAR", "MONTH", "DAY"], as_index=False).agg(
        TOTAL_FLIGHTS=("AIRLINE", "count"),
        TOTAL_DELAYED=("DEPARTURE_DELAY",
                       lambda x: (x > 0).sum()),
        TOTAL_CANCELLED=("CANCELLED",
                         lambda x: (x == 1).sum()),
    )

    delays = (
        not_cancelled
        .groupby(["YEAR", "MONTH", "DAY"], as_index=False)
        .agg(
            AVG_DEPARTURE_DELAY=("DEPARTURE_DELAY", "mean"),
            AVG_ARRIVAL_DELAY=("ARRIVAL_DELAY", "mean"),
        )
    )

    result = agg.merge(delays, on=["YEAR", "MONTH", "DAY"], how="left")

    result["AVG_DEPARTURE_DELAY"] = result["AVG_DEPARTURE_DELAY"].round(2)
    result["AVG_ARRIVAL_DELAY"] = result["AVG_ARRIVAL_DELAY"].round(2)

    # Cast MONTH to int for clean partition keys (avoid MONTH=1.0)
    result["MONTH"] = result["MONTH"].astype(int)

    logger.info("  ✓ flights_daily: %d rows", result.shape[0])
    return result


def build_flights_monthly(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate flights to monthly level by airline.

    Metrics:
        * ``TOTAL_FLIGHTS`` — count per month/airline.
        * ``TOTAL_DELAYED`` — DEPARTURE_DELAY > 0.
        * ``TOTAL_CANCELLED`` — CANCELLED == 1.
        * ``AVG_ARRIVAL_DELAY`` — mean (excluding cancelled).
        * ``ON_TIME_PCT`` — % of flights with ARRIVAL_DELAY <= 15.

    Args:
        df: Raw flights DataFrame from Bronze.

    Returns:
        pd.DataFrame: Monthly aggregation with MONTH, AIRLINE as keys.
    """
    logger.info("  Building flights_monthly ...")

    not_cancelled = df[df["CANCELLED"] != 1]

    agg = df.groupby(["MONTH", "AIRLINE"], as_index=False).agg(
        TOTAL_FLIGHTS=("AIRLINE", "count"),
        TOTAL_DELAYED=("DEPARTURE_DELAY",
                       lambda x: (x > 0).sum()),
        TOTAL_CANCELLED=("CANCELLED",
                         lambda x: (x == 1).sum()),
    )

    delays = (
        not_cancelled
        .groupby(["MONTH", "AIRLINE"], as_index=False)
        .agg(
            AVG_ARRIVAL_DELAY=("ARRIVAL_DELAY", "mean"),
        )
    )

    on_time = (
        not_cancelled
        .groupby(["MONTH", "AIRLINE"], as_index=False)
        .agg(
            ON_TIME_COUNT=("ARRIVAL_DELAY",
                           lambda x: (x <= 15).sum()),
            NON_CANCELLED=("ARRIVAL_DELAY", "count"),
        )
    )
    on_time["ON_TIME_PCT"] = (
        (on_time["ON_TIME_COUNT"] / on_time["NON_CANCELLED"] * 100)
        .round(2)
    )
    on_time = on_time.drop(columns=["ON_TIME_COUNT", "NON_CANCELLED"])

    result = (
        agg
        .merge(delays, on=["MONTH", "AIRLINE"], how="left")
        .merge(on_time, on=["MONTH", "AIRLINE"], how="left")
    )

    result["AVG_ARRIVAL_DELAY"] = result["AVG_ARRIVAL_DELAY"].round(2)

    logger.info("  ✓ flights_monthly: %d rows", result.shape[0])
    return result


def build_flights_by_airport(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate departure statistics by origin airport.

    Metrics:
        * ``TOTAL_DEPARTURES`` — count per airport.
        * ``TOTAL_DELAYED`` — DEPARTURE_DELAY > 0.
        * ``TOTAL_CANCELLED`` — CANCELLED == 1.
        * ``AVG_DEPARTURE_DELAY`` — mean (excluding cancelled).
        * ``PCT_WEATHER_DELAY`` — weather delay as % of total delays.

    Args:
        df: Raw flights DataFrame from Bronze.

    Returns:
        pd.DataFrame: Per-airport aggregation.
    """
    logger.info("  Building flights_by_airport ...")

    not_cancelled = df[df["CANCELLED"] != 1]

    delay_cols = [
        "AIR_SYSTEM_DELAY", "SECURITY_DELAY", "AIRLINE_DELAY",
        "LATE_AIRCRAFT_DELAY", "WEATHER_DELAY",
    ]

    agg = df.groupby("ORIGIN_AIRPORT", as_index=False).agg(
        TOTAL_DEPARTURES=("AIRLINE", "count"),
        TOTAL_DELAYED=("DEPARTURE_DELAY",
                       lambda x: (x > 0).sum()),
        TOTAL_CANCELLED=("CANCELLED",
                         lambda x: (x == 1).sum()),
    )

    avg_delay = (
        not_cancelled
        .groupby("ORIGIN_AIRPORT", as_index=False)
        .agg(AVG_DEPARTURE_DELAY=("DEPARTURE_DELAY", "mean"))
    )

    # Weather delay as percentage of all delay causes
    delay_sums = not_cancelled.groupby("ORIGIN_AIRPORT", as_index=False).agg(
        WEATHER_SUM=("WEATHER_DELAY", "sum"),
    )
    total_delay = (
        not_cancelled
        .groupby("ORIGIN_AIRPORT", as_index=False)[delay_cols]
        .sum()
    )
    total_delay["TOTAL_DELAY_SUM"] = total_delay[delay_cols].sum(axis=1)
    total_delay = total_delay[["ORIGIN_AIRPORT", "TOTAL_DELAY_SUM"]]

    weather = delay_sums.merge(total_delay, on="ORIGIN_AIRPORT", how="left")

    # Avoid division by zero
    weather["PCT_WEATHER_DELAY"] = (
        weather["WEATHER_SUM"]
        .div(weather["TOTAL_DELAY_SUM"].replace(0, float("nan")))
        .mul(100)
        .round(2)
        .fillna(0.0)
    )
    weather = weather.drop(columns=["WEATHER_SUM", "TOTAL_DELAY_SUM"])

    result = (
        agg
        .merge(avg_delay, on="ORIGIN_AIRPORT", how="left")
        .merge(weather, on="ORIGIN_AIRPORT", how="left")
    )

    result["AVG_DEPARTURE_DELAY"] = result["AVG_DEPARTURE_DELAY"].round(2)

    logger.info("  ✓ flights_by_airport: %d rows", result.shape[0])
    return result


def transform(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """Build all Silver aggregation tables and validate them.

    Args:
        df: Raw flights DataFrame from Bronze.

    Returns:
        Dict mapping table names to their aggregated DataFrames.

    Raises:
        AssertionError: If any output table fails validation.
    """
    logger.info("=" * 72)
    logger.info("TRANSFORM — build Silver aggregations")
    logger.info("=" * 72)

    try:
        tables: Dict[str, pd.DataFrame] = {
            "flights_daily": build_flights_daily(df),
            "flights_monthly": build_flights_monthly(df),
            "flights_by_airport": build_flights_by_airport(df),
        }
    except Exception:
        logger.exception("Transform phase failed")
        raise

    # Validate every output table
    logger.info("  Validating Silver tables ...")
    for table_name, table_df in tables.items():
        validate_dataframe(table_df, table_name)

    return tables


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
                "Description": "Silver layer — aggregated flights data",
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


def upload_to_s3(  # pylint: disable=too-many-arguments,too-many-positional-arguments
    df: pd.DataFrame,
    table_name: str,
    bucket: str,
    database: str,
    mode: str = "overwrite",
    partition_cols: List[str] = None,
) -> None:
    """Write *df* to S3 as Parquet and register it in Glue.

    Args:
        df: DataFrame to upload.
        table_name: Logical table name (S3 prefix + Glue table).
        bucket: S3 bucket name.
        database: Glue database name.
        mode: Write mode (``overwrite`` or ``overwrite_partitions``).
        partition_cols: Optional list of partition columns.

    Raises:
        botocore.exceptions.ClientError: On AWS API failure.
    """
    s3_path = S3_SILVER_PREFIX.format(bucket=bucket, table=table_name)
    try:
        kwargs = {
            "df": df,
            "path": s3_path,
            "dataset": True,
            "database": database,
            "table": table_name,
            "mode": mode,
            "compression": "snappy",
        }
        if partition_cols:
            kwargs["partition_cols"] = partition_cols

        wr.s3.to_parquet(**kwargs)
        logger.info("  ✓ Tabla %s cargada con %d filas en %s",
                     table_name, df.shape[0], s3_path)
    except Exception:
        logger.exception("Failed to upload '%s' to %s", table_name, s3_path)
        raise


def load(
    tables: Dict[str, pd.DataFrame],
    bucket: str,
    database: str,
) -> None:
    """Upload all Silver tables to S3 and register in Glue.

    Write modes per table:
        * ``flights_daily``: ``overwrite_partitions`` (partitioned by MONTH).
        * ``flights_monthly``: ``overwrite``.
        * ``flights_by_airport``: ``overwrite``.

    Args:
        tables: Validated Silver DataFrames keyed by table name.
        bucket: S3 bucket.
        database: Glue database.

    Raises:
        Exception: Re-raises any exception after logging.
    """
    logger.info("=" * 72)
    logger.info("LOAD — Silver tables to S3")
    logger.info("=" * 72)

    try:
        # flights_daily — partitioned by MONTH
        upload_to_s3(
            df=tables["flights_daily"],
            table_name="flights_daily",
            bucket=bucket,
            database=database,
            mode="overwrite_partitions",
            partition_cols=["MONTH"],
        )

        # flights_monthly — full overwrite
        upload_to_s3(
            df=tables["flights_monthly"],
            table_name="flights_monthly",
            bucket=bucket,
            database=database,
            mode="overwrite",
        )

        # flights_by_airport — full overwrite
        upload_to_s3(
            df=tables["flights_by_airport"],
            table_name="flights_by_airport",
            bucket=bucket,
            database=database,
            mode="overwrite",
        )
    except Exception:
        logger.exception("Load phase failed")
        raise


# ── Orchestrator ─────────────────────────────────────────────────────────────

def main(bucket: str) -> None:
    """Execute the full Silver ETL pipeline.

    Sequence::

        1. Validate inputs
        2. Create Glue database
        3. Extract flights from Bronze
        4. Transform — build 3 aggregation tables
        5. Load — write to S3 + register in Glue

    Args:
        bucket: Target S3 bucket name.

    Raises:
        SystemExit: Exits with code 1 on any unhandled error.
    """
    try:
        logger.info("\n%s", "=" * 72)
        logger.info("  FLIGHTS SILVER LAYER ETL PIPELINE")
        logger.info("  Bucket : %s", bucket)
        logger.info("%s\n", "=" * 72)

        # ── Input validation ─────────────────────────────────────────────
        if not bucket:
            raise ValueError("--bucket is required")
        logger.info("✓ Input validation passed\n")

        # ── Glue database ────────────────────────────────────────────────
        create_glue_database(GLUE_DATABASE_SILVER)

        # ── Extract ──────────────────────────────────────────────────────
        flights_df = extract(bucket)

        # ── Transform ────────────────────────────────────────────────────
        silver_tables = transform(flights_df)

        # Free source DataFrame after transform
        del flights_df

        # ── Load ─────────────────────────────────────────────────────────
        load(silver_tables, bucket, GLUE_DATABASE_SILVER)

        # ── Done ─────────────────────────────────────────────────────────
        logger.info("\n%s", "=" * 72)
        logger.info("  ✓ SILVER LAYER COMPLETED SUCCESSFULLY")
        logger.info("%s\n", "=" * 72)

    except Exception:  # pylint: disable=broad-exception-caught
        logger.exception("Silver ETL pipeline failed")
        sys.exit(1)


# ── CLI ──────────────────────────────────────────────────────────────────────

def parse_arguments() -> argparse.Namespace:
    """Parse CLI arguments.

    Returns:
        Namespace with ``bucket``.
    """
    parser = argparse.ArgumentParser(
        description="Silver Layer ETL — Bronze aggregations → S3 Parquet + Glue",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python etl/silver.py --bucket my-bucket\n"
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
