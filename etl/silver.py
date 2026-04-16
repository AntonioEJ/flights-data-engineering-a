#!/usr/bin/env python3
"""
Silver Layer ETL Script
=======================

Read raw Parquet data from the Bronze layer in S3 **in chunks**,
compute business-level aggregations incrementally, and write the
resulting tables back to S3 as Parquet registered in AWS Glue.

This module implements the **Silver layer** of the Medallion
architecture for the U.S. Domestic Flights 2015 dataset.

Pipeline flow::

    Bronze (S3 Parquet)  ─chunk─▶  partial aggs  ─combine─▶  Silver (S3)

Tables produced:
    * ``flights_daily``      — daily flight statistics.
    * ``flights_monthly``    — monthly statistics by airline.
    * ``flights_by_airport`` — departure statistics by origin airport.

Usage::

    python etl/silver.py --bucket <tu-bucket>

Design decisions:
    * **Memory-safe**: Bronze data is read in chunks of ``CHUNK_SIZE``
      rows.  Each chunk produces small partial aggregations that are
      combined after all chunks have been processed.
    * **Idempotent**: Glue DB created idempotently; ``flights_daily``
      uses ``overwrite_partitions`` (partitioned by MONTH);
      remaining tables use ``mode='overwrite'``.
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

CHUNK_SIZE: int = 500_000
"""Number of rows per chunk when reading Bronze flights from S3."""

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


# ── Extract + Transform (chunked) ────────────────────────────────────────────

BRONZE_REQUIRED_COLS: List[str] = [
    "YEAR", "MONTH", "DAY", "AIRLINE", "ORIGIN_AIRPORT",
    "DESTINATION_AIRPORT", "DEPARTURE_DELAY", "ARRIVAL_DELAY",
    "CANCELLED", "WEATHER_DELAY", "AIR_SYSTEM_DELAY",
    "SECURITY_DELAY", "AIRLINE_DELAY", "LATE_AIRCRAFT_DELAY",
    "FLIGHT_NUMBER",
]
"""Columns required in Bronze flights for Silver aggregations."""

DELAY_COLS: List[str] = [
    "AIR_SYSTEM_DELAY", "SECURITY_DELAY", "AIRLINE_DELAY",
    "LATE_AIRCRAFT_DELAY", "WEATHER_DELAY",
]
"""All delay-cause columns used for PCT_WEATHER_DELAY computation."""


def _partial_daily(chunk: pd.DataFrame) -> pd.DataFrame:
    """Compute partial daily aggregation for a single chunk.

    Stores raw counts and sums so that means can be computed after
    all chunks are combined.

    Args:
        chunk: A chunk of Bronze flights data.

    Returns:
        DataFrame with partial sums/counts grouped by YEAR, MONTH, DAY.
    """
    nc = chunk[chunk["CANCELLED"] != 1]

    counts = chunk.groupby(["YEAR", "MONTH", "DAY"], as_index=False).agg(
        TOTAL_FLIGHTS=("FLIGHT_NUMBER", "count"),
        TOTAL_DELAYED=("DEPARTURE_DELAY", lambda x: (x > 0).sum()),
        TOTAL_CANCELLED=("CANCELLED", lambda x: (x == 1).sum()),
    )

    delay_sums = nc.groupby(["YEAR", "MONTH", "DAY"], as_index=False).agg(
        SUM_DEP_DELAY=("DEPARTURE_DELAY", "sum"),
        SUM_ARR_DELAY=("ARRIVAL_DELAY", "sum"),
        COUNT_NC=("FLIGHT_NUMBER", "count"),
    )

    return counts.merge(delay_sums, on=["YEAR", "MONTH", "DAY"], how="left").fillna(0)


def _finalize_daily(partials: pd.DataFrame) -> pd.DataFrame:
    """Combine partial daily aggregations into final metrics.

    Args:
        partials: Concatenated partial DataFrames from all chunks.

    Returns:
        Final flights_daily DataFrame with computed averages.
    """
    grp = partials.groupby(
        ["YEAR", "MONTH", "DAY"], as_index=False,
    ).sum(numeric_only=True)

    grp["AVG_DEPARTURE_DELAY"] = (
        grp["SUM_DEP_DELAY"].div(grp["COUNT_NC"].replace(0, float("nan")))
        .round(2).fillna(0.0)
    )
    grp["AVG_ARRIVAL_DELAY"] = (
        grp["SUM_ARR_DELAY"].div(grp["COUNT_NC"].replace(0, float("nan")))
        .round(2).fillna(0.0)
    )
    grp["MONTH"] = grp["MONTH"].astype(int)

    return grp[["YEAR", "MONTH", "DAY", "TOTAL_FLIGHTS", "TOTAL_DELAYED",
                "TOTAL_CANCELLED", "AVG_DEPARTURE_DELAY", "AVG_ARRIVAL_DELAY"]]


def _partial_monthly(chunk: pd.DataFrame) -> pd.DataFrame:
    """Compute partial monthly-by-airline aggregation for a single chunk.

    Args:
        chunk: A chunk of Bronze flights data.

    Returns:
        DataFrame with partial sums/counts grouped by MONTH, AIRLINE.
    """
    nc = chunk[chunk["CANCELLED"] != 1]

    counts = chunk.groupby(["MONTH", "AIRLINE"], as_index=False).agg(
        TOTAL_FLIGHTS=("FLIGHT_NUMBER", "count"),
        TOTAL_DELAYED=("DEPARTURE_DELAY", lambda x: (x > 0).sum()),
        TOTAL_CANCELLED=("CANCELLED", lambda x: (x == 1).sum()),
    )

    delay_sums = nc.groupby(["MONTH", "AIRLINE"], as_index=False).agg(
        SUM_ARR_DELAY=("ARRIVAL_DELAY", "sum"),
        COUNT_NC=("FLIGHT_NUMBER", "count"),
        ON_TIME_COUNT=("ARRIVAL_DELAY", lambda x: (x <= 15).sum()),
    )

    return counts.merge(delay_sums, on=["MONTH", "AIRLINE"], how="left").fillna(0)


def _finalize_monthly(partials: pd.DataFrame) -> pd.DataFrame:
    """Combine partial monthly aggregations into final metrics.

    Args:
        partials: Concatenated partial DataFrames from all chunks.

    Returns:
        Final flights_monthly DataFrame.
    """
    grp = partials.groupby(
        ["MONTH", "AIRLINE"], as_index=False,
    ).sum(numeric_only=True)

    grp["AVG_ARRIVAL_DELAY"] = (
        grp["SUM_ARR_DELAY"].div(grp["COUNT_NC"].replace(0, float("nan")))
        .round(2).fillna(0.0)
    )
    grp["ON_TIME_PCT"] = (
        grp["ON_TIME_COUNT"].div(grp["COUNT_NC"].replace(0, float("nan")))
        .mul(100).round(2).fillna(0.0)
    )

    return grp[["MONTH", "AIRLINE", "TOTAL_FLIGHTS", "TOTAL_DELAYED",
                "TOTAL_CANCELLED", "AVG_ARRIVAL_DELAY", "ON_TIME_PCT"]]


def _partial_airport(chunk: pd.DataFrame) -> pd.DataFrame:
    """Compute partial per-airport aggregation for a single chunk.

    Args:
        chunk: A chunk of Bronze flights data.

    Returns:
        DataFrame with partial sums/counts grouped by ORIGIN_AIRPORT.
    """
    nc = chunk[chunk["CANCELLED"] != 1]

    counts = chunk.groupby("ORIGIN_AIRPORT", as_index=False).agg(
        TOTAL_DEPARTURES=("FLIGHT_NUMBER", "count"),
        TOTAL_DELAYED=("DEPARTURE_DELAY", lambda x: (x > 0).sum()),
        TOTAL_CANCELLED=("CANCELLED", lambda x: (x == 1).sum()),
    )

    delay_sums = nc.groupby("ORIGIN_AIRPORT", as_index=False).agg(
        SUM_DEP_DELAY=("DEPARTURE_DELAY", "sum"),
        COUNT_NC=("FLIGHT_NUMBER", "count"),
        WEATHER_SUM=("WEATHER_DELAY", "sum"),
    )

    # Sum of all delay-cause columns per airport
    total_delay = nc.groupby("ORIGIN_AIRPORT", as_index=False)[DELAY_COLS].sum()
    total_delay["ALL_DELAY_SUM"] = total_delay[DELAY_COLS].sum(axis=1)
    total_delay = total_delay[["ORIGIN_AIRPORT", "ALL_DELAY_SUM"]]

    result = (
        counts
        .merge(delay_sums, on="ORIGIN_AIRPORT", how="left")
        .merge(total_delay, on="ORIGIN_AIRPORT", how="left")
        .fillna(0)
    )
    return result


def _finalize_airport(partials: pd.DataFrame) -> pd.DataFrame:
    """Combine partial airport aggregations into final metrics.

    Args:
        partials: Concatenated partial DataFrames from all chunks.

    Returns:
        Final flights_by_airport DataFrame.
    """
    grp = partials.groupby(
        "ORIGIN_AIRPORT", as_index=False,
    ).sum(numeric_only=True)

    grp["AVG_DEPARTURE_DELAY"] = (
        grp["SUM_DEP_DELAY"].div(grp["COUNT_NC"].replace(0, float("nan")))
        .round(2).fillna(0.0)
    )
    grp["PCT_WEATHER_DELAY"] = (
        grp["WEATHER_SUM"]
        .div(grp["ALL_DELAY_SUM"].replace(0, float("nan")))
        .mul(100).round(2).fillna(0.0)
    )

    return grp[["ORIGIN_AIRPORT", "TOTAL_DEPARTURES", "TOTAL_DELAYED",
                "TOTAL_CANCELLED", "AVG_DEPARTURE_DELAY", "PCT_WEATHER_DELAY"]]


def extract_and_transform(
    bucket: str,
) -> tuple:  # (Dict[str, pd.DataFrame], int, int)
    """Read Bronze flights in chunks and build Silver tables incrementally.

    For each chunk:
        1. Validate required columns exist.
        2. Compute partial aggregations (sums and counts).
        3. Free the chunk from memory.

    After all chunks, combine partials and compute final metrics
    (means, percentages).

    Args:
        bucket: S3 bucket name.

    Returns:
        Tuple of (tables_dict, total_rows, num_chunks).

    Raises:
        FileNotFoundError: If Bronze path has no data.
        AssertionError: If required columns are missing.
    """
    logger.info("=" * 72)
    logger.info("EXTRACT + TRANSFORM — chunked (%d rows/chunk)", CHUNK_SIZE)
    logger.info("=" * 72)

    s3_path = S3_BRONZE_PREFIX.format(bucket=bucket)
    logger.info("  Source: %s", s3_path)

    try:
        reader = wr.s3.read_parquet(path=s3_path, chunked=CHUNK_SIZE)
    except Exception:
        logger.exception("Failed to open Bronze flights from %s", s3_path)
        raise

    daily_parts: List[pd.DataFrame] = []
    monthly_parts: List[pd.DataFrame] = []
    airport_parts: List[pd.DataFrame] = []
    total_rows = 0
    num_chunks = 0

    for chunk in reader:
        chunk.columns = chunk.columns.str.upper()

        # Validate required columns on first chunk
        if num_chunks == 0:
            try:
                for col in BRONZE_REQUIRED_COLS:
                    assert col in chunk.columns, (
                        f"Bronze flights missing required column '{col}'"
                    )
            except AssertionError:
                logger.exception("Bronze data validation failed")
                raise

        daily_parts.append(_partial_daily(chunk))
        monthly_parts.append(_partial_monthly(chunk))
        airport_parts.append(_partial_airport(chunk))

        total_rows += len(chunk)
        num_chunks += 1
        logger.info("  ✓ Chunk %d: %d rows (acumulado: %d)",
                     num_chunks, len(chunk), total_rows)
        del chunk

    assert total_rows > 0, "No rows read from Bronze flights"

    logger.info("  Combining partials and computing final metrics ...")
    tables = {
        "flights_daily": _finalize_daily(pd.concat(daily_parts)),
        "flights_monthly": _finalize_monthly(pd.concat(monthly_parts)),
        "flights_by_airport": _finalize_airport(pd.concat(airport_parts)),
    }

    # Validate outputs
    for name, df in tables.items():
        validate_dataframe(df, name)

    logger.info("  ✓ All Silver tables built from %d total rows", total_rows)
    return tables, total_rows, num_chunks


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

        # ── Extract + Transform (chunked) ────────────────────────────────
        t0 = time.time()
        silver_tables, total_rows, num_chunks = extract_and_transform(bucket)
        elapsed_et = time.time() - t0

        # ── Load ─────────────────────────────────────────────────────────
        t1 = time.time()
        load(silver_tables, bucket, GLUE_DATABASE_SILVER)
        elapsed_load = time.time() - t1

        # ── Cifras de control ─────────────────────────────────────────────
        elapsed_total = time.time() - t0
        logger.info("\n%s", "=" * 72)
        logger.info("  CIFRAS DE CONTROL")
        logger.info("%s", "-" * 72)
        logger.info("  Bronze filas leídas       : %s", f"{total_rows:,}")
        logger.info("  Chunks procesados         : %d (× %s filas)",
                     num_chunks, f"{CHUNK_SIZE:,}")
        logger.info("%s", "-" * 72)
        for tbl_name, tbl_df in silver_tables.items():
            logger.info("  %-25s : %6s filas, %2d cols",
                        tbl_name, f"{tbl_df.shape[0]:,}", tbl_df.shape[1])
        logger.info("%s", "-" * 72)
        logger.info("  Extract + Transform       : %.1f s", elapsed_et)
        logger.info("  Load                      : %.1f s", elapsed_load)
        logger.info("  Total                     : %.1f s", elapsed_total)
        logger.info("%s", "=" * 72)
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
