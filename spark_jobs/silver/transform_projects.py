"""Silver transformation & quarantine job for `silver_projects`."""

from __future__ import annotations

import argparse
import uuid

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from dq.checks import check_quarantine_ratio, check_unique_key
from dq.framework import (
    check_expected_columns,
    check_not_null,
    enforce_dq_gate,
    write_audit_results,
)
from spark_jobs.common.io import (
    clean_str,
    parse_batch_date,
    write_delta_overwrite,
    write_quarantine,
)
from spark_jobs.common.spark_session import (
    get_lakehouse_root,
    get_spark_session,
    join_path,
)

VALID_PROJECT_STATUSES = ("Planned", "Active", "Completed", "OnHold")

SILVER_PROJECTS_COLUMNS = (
    "project_id",
    "client_id",
    "budget_eur",
    "status",
    "_batch_date",
    "_silver_updated_at",
)


def transform_projects(
    spark: SparkSession,
    batch_date: str,
    lakehouse_root: str | None = None,
    run_id: str | None = None,
) -> tuple[DataFrame, DataFrame]:
    """Transform `bronze/projects` into `silver_projects` and `silver_quarantine/projects`."""
    iso_date = parse_batch_date(batch_date).isoformat()
    resolved_lakehouse = get_lakehouse_root(lakehouse_root)
    resolved_run_id = run_id or f"manual-{iso_date}-{uuid.uuid4().hex[:8]}"

    bronze_path = join_path(resolved_lakehouse, "bronze", "projects")
    silver_path = join_path(resolved_lakehouse, "silver", "silver_projects")
    quarantine_path = join_path(resolved_lakehouse, "silver_quarantine", "projects")
    audit_path = join_path(resolved_lakehouse, "dq", "dq_audit_results")

    bronze_df = spark.read.format("delta").load(bronze_path)

    normalized_df = (
        bronze_df.withColumn("project_id", clean_str("project_id"))
        .withColumn("client_id", clean_str("client_id"))
        .withColumn("budget_eur", F.col("budget_eur").cast("double"))
        .withColumn("status", clean_str("status"))
        .withColumn("_batch_date", F.to_date(F.lit(iso_date)))
    )

    reason_exprs = F.array(
        F.when(F.col("project_id").isNull(), F.lit("missing_project_id")),
        F.when(F.col("client_id").isNull(), F.lit("missing_client_id")),
        F.when(
            F.col("budget_eur").isNull() | (F.col("budget_eur") <= 0),
            F.lit("invalid_budget_eur"),
        ),
        F.when(
            F.col("status").isNull() | (~F.col("status").isin(*VALID_PROJECT_STATUSES)),
            F.lit("invalid_project_status"),
        ),
    )
    evaluated_df = normalized_df.withColumn(
        "dq_reasons", F.filter(reason_exprs, lambda reason: reason.isNotNull())
    ).persist()

    try:
        quarantine_df = evaluated_df.filter(F.size("dq_reasons") > 0)

        dedup_window = Window.partitionBy("project_id").orderBy(
            F.col("_ingested_at").desc(),
            F.col("budget_eur").desc(),
        )
        valid_df = (
            evaluated_df.filter(F.size("dq_reasons") == 0)
            .drop("dq_reasons")
            .withColumn("_rn", F.row_number().over(dedup_window))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
            .withColumn("_silver_updated_at", F.current_timestamp())
            .select(*SILVER_PROJECTS_COLUMNS)
        )

        write_delta_overwrite(valid_df, silver_path)
        write_quarantine(spark, quarantine_df, quarantine_path, iso_date)
    finally:
        evaluated_df.unpersist()

    persisted_df = spark.read.format("delta").load(silver_path)
    persisted_quarantine_df = (
        spark.read.format("delta")
        .load(quarantine_path)
        .filter(F.col("_batch_date") == F.to_date(F.lit(iso_date)))
    )
    valid_count = persisted_df.count()
    quarantined_count = persisted_quarantine_df.count()

    dq_results = [
        check_expected_columns(
            persisted_df,
            SILVER_PROJECTS_COLUMNS,
            run_id=resolved_run_id,
            batch_date=iso_date,
            layer="silver",
            table_name="silver_projects",
            severity="ERROR",
        ),
        check_not_null(
            persisted_df,
            "project_id",
            run_id=resolved_run_id,
            batch_date=iso_date,
            layer="silver",
            table_name="silver_projects",
            severity="ERROR",
        ),
        check_unique_key(
            persisted_df,
            ["project_id"],
            run_id=resolved_run_id,
            batch_date=iso_date,
            layer="silver",
            table_name="silver_projects",
            severity="ERROR",
        ),
        check_quarantine_ratio(
            valid_count=valid_count,
            quarantined_count=quarantined_count,
            run_id=resolved_run_id,
            batch_date=iso_date,
            layer="silver",
            table_name="silver_projects",
            severity="WARN",
        ),
    ]
    write_audit_results(spark, dq_results, audit_path)
    enforce_dq_gate(dq_results)

    return persisted_df, persisted_quarantine_df


def run(
    spark: SparkSession,
    batch_date: str,
    lakehouse_root: str | None = None,
    run_id: str | None = None,
) -> tuple[DataFrame, DataFrame]:
    """Build `silver/silver_projects` from `bronze/projects`."""
    valid_df, quarantine_df = transform_projects(
        spark,
        batch_date=batch_date,
        lakehouse_root=lakehouse_root,
        run_id=run_id,
    )
    print(
        f"Silver projects complete for batch_date={batch_date}: "
        f"valid={valid_df.count()}, quarantined={quarantine_df.count()}"
    )
    return valid_df, quarantine_df


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build silver_projects.")
    parser.add_argument("--batch-date", required=True, help="Logical batch date (YYYY-MM-DD).")
    parser.add_argument("--lakehouse-root", default=None, help="Override Lakehouse root path.")
    parser.add_argument("--run-id", default=None, help="Orchestrator run identifier.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    spark = get_spark_session("silver-transform-projects")
    try:
        run(spark, args.batch_date, args.lakehouse_root, args.run_id)
    finally:
        spark.stop()
