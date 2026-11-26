"""Bronze ingestion job for Finance domain (`fx_rates`, `invoices`, `expenses`)."""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from spark_jobs.common.io import (
    add_bronze_metadata,
    parse_batch_date,
    read_landing_partition,
    write_delta_partition,
)
from spark_jobs.common.spark_session import (
    get_lakehouse_root,
    get_landing_root,
    get_spark_session,
    join_path,
)

FX_RATES_RAW_SCHEMA = StructType(
    [
        StructField("rate_date", StringType(), nullable=True),
        StructField("currency_from", StringType(), nullable=True),
        StructField("currency_to", StringType(), nullable=True),
        StructField("rate", DoubleType(), nullable=True),
    ]
)

INVOICES_RAW_SCHEMA = StructType(
    [
        StructField("inv_id", StringType(), nullable=True),
        StructField("client_id", StringType(), nullable=True),
        StructField("project_id", StringType(), nullable=True),
        StructField("amount", DoubleType(), nullable=True),
        StructField("currency", StringType(), nullable=True),
        StructField("issued_at", StringType(), nullable=True),
        StructField("paid_at", StringType(), nullable=True),
    ]
)

EXPENSES_RAW_SCHEMA = StructType(
    [
        StructField("exp_id", StringType(), nullable=True),
        StructField("department", StringType(), nullable=True),
        StructField("category", StringType(), nullable=True),
        StructField("amount", DoubleType(), nullable=True),
        StructField("currency", StringType(), nullable=True),
        StructField("expense_date", StringType(), nullable=True),
    ]
)


def ingest_fx_rates(
    spark: SparkSession,
    batch_date: str,
    landing_root: str | None = None,
    lakehouse_root: str | None = None,
) -> DataFrame:
    """Ingest weekday-only `fx_rates` JSON idempotently into `bronze/fx_rates`.

    Missing files on Saturdays/Sundays are skipped gracefully without failing;
    missing files on weekdays raise `FileNotFoundError`.
    """
    resolved_landing = get_landing_root(landing_root)
    resolved_lakehouse = get_lakehouse_root(lakehouse_root)
    is_weekend = parse_batch_date(batch_date).weekday() >= 5

    raw_df = read_landing_partition(
        spark,
        resolved_landing,
        "fx_rates",
        batch_date,
        "part-000.json",
        "json",
        FX_RATES_RAW_SCHEMA,
        options={"multiLine": "true"},
        allow_missing=is_weekend,
    )
    if raw_df is None:
        raw_df = spark.createDataFrame([], schema=FX_RATES_RAW_SCHEMA)

    bronze_df = add_bronze_metadata(raw_df, batch_date)
    target_path = join_path(resolved_lakehouse, "bronze", "fx_rates")
    write_delta_partition(spark, bronze_df, target_path, batch_date)
    return bronze_df


def ingest_invoices(
    spark: SparkSession,
    batch_date: str,
    landing_root: str | None = None,
    lakehouse_root: str | None = None,
) -> DataFrame:
    """Ingest daily `invoices` JSONL preserving raw date strings into `bronze/invoices`."""
    resolved_landing = get_landing_root(landing_root)
    resolved_lakehouse = get_lakehouse_root(lakehouse_root)

    raw_df = read_landing_partition(
        spark,
        resolved_landing,
        "invoices",
        batch_date,
        "part-000.jsonl",
        "json",
        INVOICES_RAW_SCHEMA,
    )
    assert raw_df is not None
    bronze_df = add_bronze_metadata(raw_df, batch_date)

    target_path = join_path(resolved_lakehouse, "bronze", "invoices")
    write_delta_partition(spark, bronze_df, target_path, batch_date)
    return bronze_df


def ingest_expenses(
    spark: SparkSession,
    batch_date: str,
    landing_root: str | None = None,
    lakehouse_root: str | None = None,
) -> DataFrame:
    """Ingest daily `expenses` CSV idempotently into `bronze/expenses`."""
    resolved_landing = get_landing_root(landing_root)
    resolved_lakehouse = get_lakehouse_root(lakehouse_root)

    raw_df = read_landing_partition(
        spark,
        resolved_landing,
        "expenses",
        batch_date,
        "part-000.csv",
        "csv",
        EXPENSES_RAW_SCHEMA,
        options={"header": "true"},
    )
    assert raw_df is not None
    bronze_df = add_bronze_metadata(raw_df, batch_date)

    target_path = join_path(resolved_lakehouse, "bronze", "expenses")
    write_delta_partition(spark, bronze_df, target_path, batch_date)
    return bronze_df


def run(
    spark: SparkSession,
    batch_date: str,
    landing_root: str | None = None,
    lakehouse_root: str | None = None,
) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Ingest Finance datasets (`fx_rates`, `invoices`, `expenses`) into Bronze Delta."""
    fx_df = ingest_fx_rates(
        spark,
        batch_date=batch_date,
        landing_root=landing_root,
        lakehouse_root=lakehouse_root,
    )
    invoices_df = ingest_invoices(
        spark,
        batch_date=batch_date,
        landing_root=landing_root,
        lakehouse_root=lakehouse_root,
    )
    expenses_df = ingest_expenses(
        spark,
        batch_date=batch_date,
        landing_root=landing_root,
        lakehouse_root=lakehouse_root,
    )
    print(
        f"Ingested bronze/fx_rates={fx_df.count()}, "
        f"bronze/invoices={invoices_df.count()}, "
        f"bronze/expenses={expenses_df.count()} for batch_date={batch_date}"
    )
    return fx_df, invoices_df, expenses_df


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest Finance datasets into Bronze Delta.")
    parser.add_argument("--batch-date", required=True, help="Logical batch date (YYYY-MM-DD).")
    parser.add_argument("--landing-root", default=None, help="Override Landing Zone path.")
    parser.add_argument("--lakehouse-root", default=None, help="Override Lakehouse root path.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    spark = get_spark_session("bronze-ingest-finance")
    try:
        run(spark, args.batch_date, args.landing_root, args.lakehouse_root)
    finally:
        spark.stop()
