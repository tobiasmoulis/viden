"""Bronze ingestion job for full-load reference datasets (`projects` and `survey_questions`)."""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from spark_jobs.common.io import add_bronze_metadata, write_delta_overwrite
from spark_jobs.common.spark_session import (
    get_lakehouse_root,
    get_landing_root,
    get_spark_session,
    join_path,
)

PROJECTS_RAW_SCHEMA = StructType(
    [
        StructField("project_id", StringType(), nullable=True),
        StructField("client_id", StringType(), nullable=True),
        StructField("budget_eur", DoubleType(), nullable=True),
        StructField("status", StringType(), nullable=True),
    ]
)

SURVEY_QUESTIONS_RAW_SCHEMA = StructType(
    [
        StructField("question_id", StringType(), nullable=True),
        StructField("dimension", StringType(), nullable=True),
        StructField("scale_min", IntegerType(), nullable=True),
        StructField("scale_max", IntegerType(), nullable=True),
        StructField("prompt_text", StringType(), nullable=True),
    ]
)


def ingest_projects(
    spark: SparkSession,
    batch_date: str,
    landing_root: str | None = None,
    lakehouse_root: str | None = None,
) -> DataFrame:
    """Read raw `projects.csv` and write idempotently to `bronze/projects` Delta."""
    resolved_landing = get_landing_root(landing_root)
    resolved_lakehouse = get_lakehouse_root(lakehouse_root)

    source_path = join_path(resolved_landing, "projects", "projects.csv")
    target_path = join_path(resolved_lakehouse, "bronze", "projects")

    raw_df = spark.read.option("header", "true").schema(PROJECTS_RAW_SCHEMA).csv(source_path)
    bronze_df = add_bronze_metadata(raw_df, batch_date)

    write_delta_overwrite(bronze_df, target_path)
    return bronze_df


def ingest_survey_questions(
    spark: SparkSession,
    batch_date: str,
    landing_root: str | None = None,
    lakehouse_root: str | None = None,
) -> DataFrame:
    """Read raw `survey_questions.json` and write idempotently to `bronze/survey_questions`."""
    resolved_landing = get_landing_root(landing_root)
    resolved_lakehouse = get_lakehouse_root(lakehouse_root)

    source_path = join_path(resolved_landing, "survey_questions", "survey_questions.json")
    target_path = join_path(resolved_lakehouse, "bronze", "survey_questions")

    raw_df = (
        spark.read.option("multiLine", "true").schema(SURVEY_QUESTIONS_RAW_SCHEMA).json(source_path)
    )
    bronze_df = add_bronze_metadata(raw_df, batch_date)

    write_delta_overwrite(bronze_df, target_path)
    return bronze_df


def run(
    spark: SparkSession,
    batch_date: str,
    landing_root: str | None = None,
    lakehouse_root: str | None = None,
) -> tuple[DataFrame, DataFrame]:
    """Ingest `projects` and `survey_questions` full-load reference tables into Bronze."""
    projects_df = ingest_projects(
        spark,
        batch_date=batch_date,
        landing_root=landing_root,
        lakehouse_root=lakehouse_root,
    )
    questions_df = ingest_survey_questions(
        spark,
        batch_date=batch_date,
        landing_root=landing_root,
        lakehouse_root=lakehouse_root,
    )
    print(
        f"Ingested bronze/projects={projects_df.count()}, "
        f"bronze/survey_questions={questions_df.count()} for batch_date={batch_date}"
    )
    return projects_df, questions_df


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest reference datasets into Bronze Delta.")
    parser.add_argument("--batch-date", required=True, help="Logical batch date (YYYY-MM-DD).")
    parser.add_argument("--landing-root", default=None, help="Override Landing Zone path.")
    parser.add_argument("--lakehouse-root", default=None, help="Override Lakehouse root path.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    spark = get_spark_session("bronze-ingest-reference")
    try:
        run(spark, args.batch_date, args.landing_root, args.lakehouse_root)
    finally:
        spark.stop()
