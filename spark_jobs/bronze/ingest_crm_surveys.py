"""Bronze ingestion job for `crm_events` and `employee_surveys` (JSONL batches)."""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import ArrayType, IntegerType, StringType, StructField, StructType

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

# `payload` is explicitly typed as StringType so PySpark preserves the raw polymorphic JSON object.
CRM_EVENTS_RAW_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), nullable=True),
        StructField("client_id", StringType(), nullable=True),
        StructField("event_type", StringType(), nullable=True),
        StructField("timestamp", StringType(), nullable=True),
        StructField("payload", StringType(), nullable=True),
    ]
)

SURVEY_ANSWER_SCHEMA = StructType(
    [
        StructField("question_id", StringType(), nullable=True),
        StructField("dimension", StringType(), nullable=True),
        StructField("numeric_score", IntegerType(), nullable=True),
        StructField("written_answer", StringType(), nullable=True),
    ]
)

EMPLOYEE_SURVEYS_RAW_SCHEMA = StructType(
    [
        StructField("survey_id", StringType(), nullable=True),
        StructField("emp_id", StringType(), nullable=True),
        StructField("submitted_at", StringType(), nullable=True),
        StructField("enps_score", IntegerType(), nullable=True),
        StructField("overall_comment", StringType(), nullable=True),
        StructField("answers", ArrayType(SURVEY_ANSWER_SCHEMA), nullable=True),
    ]
)


def ingest_crm_events(
    spark: SparkSession,
    batch_date: str,
    landing_root: str | None = None,
    lakehouse_root: str | None = None,
) -> DataFrame:
    """Ingest daily `crm_events` JSONL preserving raw nested JSON `payload`."""
    resolved_landing = get_landing_root(landing_root)
    resolved_lakehouse = get_lakehouse_root(lakehouse_root)

    raw_df = read_landing_partition(
        spark,
        resolved_landing,
        "crm_events",
        batch_date,
        "part-000.jsonl",
        "json",
        CRM_EVENTS_RAW_SCHEMA,
    )
    assert raw_df is not None
    bronze_df = add_bronze_metadata(raw_df, batch_date)

    target_path = join_path(resolved_lakehouse, "bronze", "crm_events")
    write_delta_partition(spark, bronze_df, target_path, batch_date)
    return bronze_df


def ingest_employee_surveys(
    spark: SparkSession,
    batch_date: str,
    landing_root: str | None = None,
    lakehouse_root: str | None = None,
) -> DataFrame:
    """Ingest pulse `employee_surveys` JSONL idempotently into `bronze/employee_surveys`.

    Dates outside survey waves have no landing file and are handled gracefully.
    """
    resolved_landing = get_landing_root(landing_root)
    resolved_lakehouse = get_lakehouse_root(lakehouse_root)

    raw_df = read_landing_partition(
        spark,
        resolved_landing,
        "employee_surveys",
        batch_date,
        "part-000.jsonl",
        "json",
        EMPLOYEE_SURVEYS_RAW_SCHEMA,
        allow_missing=True,
    )
    if raw_df is None:
        raw_df = spark.createDataFrame([], schema=EMPLOYEE_SURVEYS_RAW_SCHEMA)

    bronze_df = add_bronze_metadata(raw_df, batch_date)
    target_path = join_path(resolved_lakehouse, "bronze", "employee_surveys")
    write_delta_partition(spark, bronze_df, target_path, batch_date)
    return bronze_df


def run(
    spark: SparkSession,
    batch_date: str,
    landing_root: str | None = None,
    lakehouse_root: str | None = None,
) -> tuple[DataFrame, DataFrame]:
    """Ingest `crm_events` and `employee_surveys` into Bronze Delta."""
    crm_df = ingest_crm_events(
        spark,
        batch_date=batch_date,
        landing_root=landing_root,
        lakehouse_root=lakehouse_root,
    )
    surveys_df = ingest_employee_surveys(
        spark,
        batch_date=batch_date,
        landing_root=landing_root,
        lakehouse_root=lakehouse_root,
    )
    print(
        f"Ingested bronze/crm_events={crm_df.count()}, "
        f"bronze/employee_surveys={surveys_df.count()} for batch_date={batch_date}"
    )
    return crm_df, surveys_df


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest CRM & Survey datasets into Bronze Delta.")
    parser.add_argument("--batch-date", required=True, help="Logical batch date (YYYY-MM-DD).")
    parser.add_argument("--landing-root", default=None, help="Override Landing Zone path.")
    parser.add_argument("--lakehouse-root", default=None, help="Override Lakehouse root path.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    spark = get_spark_session("bronze-ingest-crm-surveys")
    try:
        run(spark, args.batch_date, args.landing_root, args.lakehouse_root)
    finally:
        spark.stop()
