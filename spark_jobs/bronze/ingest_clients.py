"""Reference Bronze ingestion job for the `clients` full-load dataset.

This module serves as the canonical reference pattern for Bronze ingestion:
- Explicit raw schema (no silent lossy casting)
- Standardized technical metadata (`_batch_date`, `_ingested_at`, `_source_file`)
- Idempotent Delta write

Note: `clients` is a full-load table so overwriting the Delta table is
idempotent. For daily partitioned tables (`employees`, `timesheets`,
`fx_rates`, `invoices`, `expenses`, `crm_events`, `employee_surveys`),
partition by `_batch_date` and replace only the target `_batch_date`
(e.g., via `option("replaceWhere", f"_batch_date = '{batch_date}'")` or
`DeltaTable.delete(...)` followed by `append`).
"""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from spark_jobs.common.io import add_bronze_metadata, write_delta_overwrite
from spark_jobs.common.spark_session import (
    get_lakehouse_root,
    get_landing_root,
    get_spark_session,
    join_path,
)

CLIENTS_RAW_SCHEMA = StructType(
    [
        StructField("client_id", StringType(), nullable=True),
        StructField("client_name", StringType(), nullable=True),
        StructField("tier", StringType(), nullable=True),
    ]
)


def ingest_clients(
    spark: SparkSession,
    batch_date: str,
    landing_root: str | None = None,
    lakehouse_root: str | None = None,
) -> DataFrame:
    """Read raw `clients.csv` and write idempotently to `bronze/clients` Delta."""
    resolved_landing = get_landing_root(landing_root)
    resolved_lakehouse = get_lakehouse_root(lakehouse_root)

    source_path = join_path(resolved_landing, "clients", "clients.csv")
    target_path = join_path(resolved_lakehouse, "bronze", "clients")

    raw_df = spark.read.option("header", "true").schema(CLIENTS_RAW_SCHEMA).csv(source_path)
    bronze_df = add_bronze_metadata(raw_df, batch_date)

    write_delta_overwrite(bronze_df, target_path)
    return bronze_df


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest raw clients.csv into Bronze Delta.")
    parser.add_argument("--batch-date", required=True, help="Logical batch date (YYYY-MM-DD).")
    parser.add_argument("--landing-root", default=None, help="Override Landing Zone path.")
    parser.add_argument("--lakehouse-root", default=None, help="Override Lakehouse root path.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = get_spark_session("bronze-ingest-clients")
    try:
        df = ingest_clients(
            spark,
            batch_date=args.batch_date,
            landing_root=args.landing_root,
            lakehouse_root=args.lakehouse_root,
        )
        print(f"Ingested {df.count()} rows into bronze/clients for batch_date={args.batch_date}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
