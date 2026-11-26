"""Silver transformation & quarantine job for `silver_timesheets`.

Implements:
- Deterministic deduplication by `ts_id` (per `docs/architecture.md` Section 5)
- Quarantine routing (`silver_quarantine/timesheets`) for:
  - `project_id IS NULL`
  - `hours <= 0` or `hours > 24`
  - Post-termination ("zombie") entries (`work_date >= employee.termination_date`)
- Idempotent `_batch_date` partition replacement
- Hard & soft DQ checks persisted to `dq/dq_audit_results`
"""

from __future__ import annotations

import argparse
import uuid

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from dq.checks import check_quarantine_ratio, check_ratio_threshold, check_unique_key
from dq.framework import (
    check_expected_columns,
    check_not_null,
    enforce_dq_gate,
    write_audit_results,
)
from spark_jobs.common.io import (
    clean_str,
    parse_batch_date,
    write_delta_partition,
    write_quarantine,
)
from spark_jobs.common.spark_session import (
    get_lakehouse_root,
    get_spark_session,
    join_path,
)

SILVER_TIMESHEETS_COLUMNS = (
    "ts_id",
    "emp_id",
    "project_id",
    "work_date",
    "hours",
    "_batch_date",
    "_silver_updated_at",
)


def transform_timesheets(
    spark: SparkSession,
    batch_date: str,
    lakehouse_root: str | None = None,
    run_id: str | None = None,
) -> tuple[DataFrame, DataFrame]:
    """Transform `bronze/timesheets` for `batch_date` into Silver and quarantine tables."""
    iso_date = parse_batch_date(batch_date).isoformat()
    resolved_lakehouse = get_lakehouse_root(lakehouse_root)
    resolved_run_id = run_id or f"manual-{iso_date}-{uuid.uuid4().hex[:8]}"

    bronze_path = join_path(resolved_lakehouse, "bronze", "timesheets")
    employees_path = join_path(resolved_lakehouse, "silver", "silver_employees_scd2")
    silver_path = join_path(resolved_lakehouse, "silver", "silver_timesheets")
    quarantine_path = join_path(resolved_lakehouse, "silver_quarantine", "timesheets")
    audit_path = join_path(resolved_lakehouse, "dq", "dq_audit_results")

    bronze_df = (
        spark.read.format("delta")
        .load(bronze_path)
        .filter(F.col("_batch_date") == F.to_date(F.lit(iso_date)))
    )

    normalized_df = bronze_df.select(
        clean_str("ts_id").alias("ts_id"),
        clean_str("emp_id").alias("emp_id"),
        clean_str("project_id").alias("project_id"),
        F.col("work_date").cast("date").alias("work_date"),
        F.col("hours").cast("double").alias("hours"),
        F.col("_batch_date"),
        F.col("_ingested_at"),
        F.col("_source_file"),
    )

    raw_count = normalized_df.count()

    # Deterministic deduplication by `ts_id` (per docs/architecture.md Section 5):
    # Prefer valid row over corrupted near-duplicate, then lowest positive hours.
    is_valid_hours_proj = (
        F.col("project_id").isNotNull()
        & F.col("hours").isNotNull()
        & (F.col("hours") > 0)
        & (F.col("hours") <= 24)
    )
    dedup_window = Window.partitionBy("ts_id").orderBy(
        F.when(is_valid_hours_proj, F.lit(0)).otherwise(F.lit(1)).asc(),
        F.col("hours").asc_nulls_last(),
        F.col("project_id").asc_nulls_last(),
        F.col("_ingested_at").desc(),
    )

    non_null_key_df = (
        normalized_df.filter(F.col("ts_id").isNotNull())
        .withColumn("_rn", F.row_number().over(dedup_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
    null_key_df = normalized_df.filter(F.col("ts_id").isNull())
    dedup_df = non_null_key_df.unionByName(null_key_df)

    dedup_count = dedup_df.count()
    duplicate_count = max(0, raw_count - dedup_count)

    # Extract each employee's termination_date from silver_employees_scd2 for zombie checks.
    emp_term_df = (
        spark.read.format("delta")
        .load(employees_path)
        .groupBy("emp_id")
        .agg(F.min("termination_date").alias("_emp_termination_date"))
    )

    enriched_df = dedup_df.join(F.broadcast(emp_term_df), on="emp_id", how="left")

    is_zombie = F.col("_emp_termination_date").isNotNull() & (
        F.col("work_date") >= F.col("_emp_termination_date")
    )

    reason_exprs = F.array(
        F.when(F.col("ts_id").isNull(), F.lit("missing_ts_id")),
        F.when(F.col("emp_id").isNull(), F.lit("missing_emp_id")),
        F.when(F.col("work_date").isNull(), F.lit("missing_work_date")),
        F.when(F.col("project_id").isNull(), F.lit("missing_project_id")),
        F.when(F.col("hours").isNull() | (F.col("hours") <= 0), F.lit("non_positive_hours")),
        F.when(F.col("hours") > 24, F.lit("hours_exceeds_24")),
        F.when(is_zombie, F.lit("post_termination_work_date")),
    )
    evaluated_df = enriched_df.withColumn(
        "dq_reasons", F.filter(reason_exprs, lambda reason: reason.isNotNull())
    ).persist()

    try:
        quarantine_df = evaluated_df.filter(F.size("dq_reasons") > 0).drop("_emp_termination_date")
        zombie_count = evaluated_df.filter(is_zombie).count()

        valid_df = (
            evaluated_df.filter(F.size("dq_reasons") == 0)
            .drop("dq_reasons", "_emp_termination_date")
            .withColumn("_silver_updated_at", F.current_timestamp())
            .select(*SILVER_TIMESHEETS_COLUMNS)
        )

        write_delta_partition(spark, valid_df, silver_path, iso_date)
        write_quarantine(spark, quarantine_df, quarantine_path, iso_date)
    finally:
        evaluated_df.unpersist()

    persisted_batch_df = (
        spark.read.format("delta")
        .load(silver_path)
        .filter(F.col("_batch_date") == F.to_date(F.lit(iso_date)))
    )
    persisted_quarantine_df = (
        spark.read.format("delta")
        .load(quarantine_path)
        .filter(F.col("_batch_date") == F.to_date(F.lit(iso_date)))
    )
    valid_count = persisted_batch_df.count()
    quarantined_count = persisted_quarantine_df.count()

    dq_results = [
        check_expected_columns(
            persisted_batch_df,
            SILVER_TIMESHEETS_COLUMNS,
            run_id=resolved_run_id,
            batch_date=iso_date,
            layer="silver",
            table_name="silver_timesheets",
            severity="ERROR",
        ),
        check_not_null(
            persisted_batch_df,
            "ts_id",
            run_id=resolved_run_id,
            batch_date=iso_date,
            layer="silver",
            table_name="silver_timesheets",
            severity="ERROR",
        ),
        check_unique_key(
            persisted_batch_df,
            ["ts_id"],
            run_id=resolved_run_id,
            batch_date=iso_date,
            layer="silver",
            table_name="silver_timesheets",
            severity="ERROR",
        ),
        check_quarantine_ratio(
            valid_count=valid_count,
            quarantined_count=quarantined_count,
            max_ratio=0.08,
            run_id=resolved_run_id,
            batch_date=iso_date,
            layer="silver",
            table_name="silver_timesheets",
            severity="WARN",
        ),
        check_ratio_threshold(
            failed_count=duplicate_count,
            total_count=raw_count,
            check_name="duplicate_rate",
            max_ratio=0.10,
            run_id=resolved_run_id,
            batch_date=iso_date,
            layer="silver",
            table_name="silver_timesheets",
            severity="WARN",
        ),
        check_ratio_threshold(
            failed_count=zombie_count,
            total_count=dedup_count,
            check_name="zombie_timesheet_rate",
            max_ratio=0.01,
            run_id=resolved_run_id,
            batch_date=iso_date,
            layer="silver",
            table_name="silver_timesheets",
            severity="WARN",
        ),
    ]
    write_audit_results(spark, dq_results, audit_path)
    enforce_dq_gate(dq_results)

    return persisted_batch_df, persisted_quarantine_df


def run(
    spark: SparkSession,
    batch_date: str,
    lakehouse_root: str | None = None,
    run_id: str | None = None,
) -> tuple[DataFrame, DataFrame]:
    """Build `silver/silver_timesheets` and `silver_quarantine/timesheets`."""
    valid_df, quarantine_df = transform_timesheets(
        spark,
        batch_date=batch_date,
        lakehouse_root=lakehouse_root,
        run_id=run_id,
    )
    print(
        f"Silver timesheets complete for batch_date={batch_date}: "
        f"valid={valid_df.count()}, quarantined={quarantine_df.count()}"
    )
    return valid_df, quarantine_df


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Transform bronze/timesheets into silver_timesheets."
    )
    parser.add_argument("--batch-date", required=True, help="Logical batch date (YYYY-MM-DD).")
    parser.add_argument("--lakehouse-root", default=None, help="Override Lakehouse root path.")
    parser.add_argument("--run-id", default=None, help="Orchestrator run identifier.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    spark = get_spark_session("silver-transform-timesheets")
    try:
        run(spark, args.batch_date, args.lakehouse_root, args.run_id)
    finally:
        spark.stop()
