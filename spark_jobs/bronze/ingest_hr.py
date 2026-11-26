"""Bronze ingestion job for HR domain (`employees` CSV and `timesheets` Parquet)."""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DateType, DoubleType, StringType, StructField, StructType

from spark_jobs.common.io import (
    add_bronze_metadata,
    read_landing_partition,
    write_delta_partition,
)
from spark_jobs.common.spark_session import (
    get_lakehouse_root,
    get_landing_root,
    get_spark_session,
    join_path,
)

# All columns preserved as raw strings in Bronze to prevent lossy casting.
EMPLOYEES_RAW_SCHEMA = StructType(
    [
        StructField("emp_id", StringType(), nullable=True),
        StructField("full_name", StringType(), nullable=True),
        StructField("department", StringType(), nullable=True),
        StructField("role", StringType(), nullable=True),
        StructField("seniority_level", StringType(), nullable=True),
        StructField("hourly_rate_eur", StringType(), nullable=True),
        StructField("hire_date", StringType(), nullable=True),
        StructField("last_promotion_date", StringType(), nullable=True),
        StructField("work_mode", StringType(), nullable=True),
        StructField("distance_km", StringType(), nullable=True),
        StructField("manager_id", StringType(), nullable=True),
        StructField("is_active", StringType(), nullable=True),
        StructField("termination_date", StringType(), nullable=True),
        StructField("churn_type", StringType(), nullable=True),
        StructField("snapshot_date", StringType(), nullable=True),
    ]
)

TIMESHEETS_RAW_SCHEMA = StructType(
    [
        StructField("ts_id", StringType(), nullable=True),
        StructField("emp_id", StringType(), nullable=True),
        StructField("project_id", StringType(), nullable=True),
        StructField("work_date", DateType(), nullable=True),
        StructField("hours", DoubleType(), nullable=True),
    ]
)


def ingest_employees(
    spark: SparkSession,
    batch_date: str,
    landing_root: str | None = None,
    lakehouse_root: str | None = None,
) -> DataFrame:
    """Ingest daily `employees` CSV snapshot idempotently into `bronze/employees`."""
    resolved_landing = get_landing_root(landing_root)
    resolved_lakehouse = get_lakehouse_root(lakehouse_root)

    raw_df = read_landing_partition(
        spark,
        resolved_landing,
        "employees",
        batch_date,
        "part-000.csv",
        "csv",
        EMPLOYEES_RAW_SCHEMA,
        options={"header": "true"},
    )
    assert raw_df is not None
    bronze_df = add_bronze_metadata(raw_df, batch_date)

    target_path = join_path(resolved_lakehouse, "bronze", "employees")
    write_delta_partition(spark, bronze_df, target_path, batch_date)
    return bronze_df


def ingest_timesheets(
    spark: SparkSession,
    batch_date: str,
    landing_root: str | None = None,
    lakehouse_root: str | None = None,
) -> DataFrame:
    """Ingest daily `timesheets` Parquet batch idempotently into `bronze/timesheets`."""
    resolved_landing = get_landing_root(landing_root)
    resolved_lakehouse = get_lakehouse_root(lakehouse_root)

    raw_df = read_landing_partition(
        spark,
        resolved_landing,
        "timesheets",
        batch_date,
        "part-000.parquet",
        "parquet",
        TIMESHEETS_RAW_SCHEMA,
    )
    assert raw_df is not None
    bronze_df = add_bronze_metadata(raw_df, batch_date)

    target_path = join_path(resolved_lakehouse, "bronze", "timesheets")
    write_delta_partition(spark, bronze_df, target_path, batch_date)
    return bronze_df


def run(
    spark: SparkSession,
    batch_date: str,
    landing_root: str | None = None,
    lakehouse_root: str | None = None,
) -> tuple[DataFrame, DataFrame]:
    """Ingest HR datasets (`employees` and `timesheets`) into Bronze Delta."""
    employees_df = ingest_employees(
        spark,
        batch_date=batch_date,
        landing_root=landing_root,
        lakehouse_root=lakehouse_root,
    )
    timesheets_df = ingest_timesheets(
        spark,
        batch_date=batch_date,
        landing_root=landing_root,
        lakehouse_root=lakehouse_root,
    )
    print(
        f"Ingested bronze/employees={employees_df.count()}, "
        f"bronze/timesheets={timesheets_df.count()} for batch_date={batch_date}"
    )
    return employees_df, timesheets_df


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest HR datasets into Bronze Delta.")
    parser.add_argument("--batch-date", required=True, help="Logical batch date (YYYY-MM-DD).")
    parser.add_argument("--landing-root", default=None, help="Override Landing Zone path.")
    parser.add_argument("--lakehouse-root", default=None, help="Override Lakehouse root path.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    spark = get_spark_session("bronze-ingest-hr")
    try:
        run(spark, args.batch_date, args.landing_root, args.lakehouse_root)
    finally:
        spark.stop()
