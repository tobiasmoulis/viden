"""Silver SCD Type 2 transformation job for `silver_employees_scd2`.

Builds Slowly Changing Dimension Type 2 history (`valid_from`, `valid_to`, `is_current`)
from `bronze/employees` daily snapshots up to `batch_date`:
- Interval convention: half-open `[valid_from, valid_to)` with sentinel `DATE '9999-12-31'`.
- Tracked attributes: `department`, `role`, `hourly_rate_eur`, `is_active`,
  `termination_date`, `churn_type`.
- Invalid rows are routed to `silver_quarantine/employees` with `dq_reasons`.
- Hard DQ gates enforce schema, non-null `emp_id`, unique `(emp_id, valid_from)`,
  and zero overlapping intervals (`check_scd2_no_overlaps`).
"""

from __future__ import annotations

import argparse
import uuid
from datetime import date

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from dq.checks import check_quarantine_ratio, check_scd2_no_overlaps, check_unique_key
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

OPEN_END_SENTINEL = date(9999, 12, 31)

TRACKED_SCD2_ATTRIBUTES = (
    "department",
    "role",
    "hourly_rate_eur",
    "is_active",
    "termination_date",
    "churn_type",
)

SILVER_EMPLOYEES_SCD2_COLUMNS = (
    "emp_id",
    "full_name",
    "department",
    "role",
    "seniority_level",
    "hourly_rate_eur",
    "hire_date",
    "last_promotion_date",
    "work_mode",
    "distance_km",
    "manager_id",
    "is_active",
    "termination_date",
    "churn_type",
    "valid_from",
    "valid_to",
    "is_current",
    "_silver_updated_at",
)


def _parse_bool_str(column_name: str):
    cleaned = F.lower(clean_str(column_name))
    return (
        F.when(cleaned == F.lit("true"), F.lit(True))
        .when(cleaned == F.lit("false"), F.lit(False))
        .otherwise(F.lit(None).cast("boolean"))
    )


def _parse_iso_date(column_name: str):
    cleaned = clean_str(column_name)
    return F.when(
        cleaned.rlike(r"^\d{4}-\d{2}-\d{2}$"), F.to_date(cleaned, "yyyy-MM-dd")
    ).otherwise(F.lit(None).cast("date"))


def transform_employees_scd2(
    spark: SparkSession,
    batch_date: str,
    lakehouse_root: str | None = None,
    run_id: str | None = None,
) -> tuple[DataFrame, DataFrame]:
    """Build `silver/silver_employees_scd2` preserving full Bronze history on earlier re-runs."""
    parsed_date = parse_batch_date(batch_date)
    iso_date = parsed_date.isoformat()
    resolved_lakehouse = get_lakehouse_root(lakehouse_root)
    resolved_run_id = run_id or f"manual-{iso_date}-{uuid.uuid4().hex[:8]}"

    bronze_path = join_path(resolved_lakehouse, "bronze", "employees")
    silver_path = join_path(resolved_lakehouse, "silver", "silver_employees_scd2")
    quarantine_path = join_path(resolved_lakehouse, "silver_quarantine", "employees")
    audit_path = join_path(resolved_lakehouse, "dq", "dq_audit_results")

    # Never truncate later Bronze history when re-running an earlier `batch_date`.
    bronze_all_df = spark.read.format("delta").load(bronze_path)
    max_bronze_row = bronze_all_df.agg(F.max("_batch_date").alias("max_bd")).first()
    max_bronze_date = max_bronze_row["max_bd"] if max_bronze_row is not None else None
    effective_end_date = max(parsed_date, max_bronze_date) if max_bronze_date else parsed_date

    bronze_df = bronze_all_df.filter(
        F.col("_batch_date") <= F.to_date(F.lit(effective_end_date.isoformat()))
    )

    typed_df = bronze_df.select(
        clean_str("emp_id").alias("emp_id"),
        clean_str("full_name").alias("full_name"),
        clean_str("department").alias("department"),
        clean_str("role").alias("role"),
        clean_str("seniority_level").alias("seniority_level"),
        F.col("hourly_rate_eur").cast("double").alias("hourly_rate_eur"),
        _parse_iso_date("hire_date").alias("hire_date"),
        _parse_iso_date("last_promotion_date").alias("last_promotion_date"),
        clean_str("work_mode").alias("work_mode"),
        F.col("distance_km").cast("double").alias("distance_km"),
        clean_str("manager_id").alias("manager_id"),
        _parse_bool_str("is_active").alias("is_active"),
        _parse_iso_date("termination_date").alias("termination_date"),
        clean_str("churn_type").alias("churn_type"),
        F.coalesce(_parse_iso_date("snapshot_date"), F.col("_batch_date")).alias("snapshot_date"),
        F.col("_batch_date"),
        F.col("_ingested_at"),
        F.col("_source_file"),
    )

    reason_exprs = F.array(
        F.when(F.col("emp_id").isNull(), F.lit("missing_emp_id")),
        F.when(F.col("snapshot_date").isNull(), F.lit("missing_snapshot_date")),
        F.when(
            F.col("hourly_rate_eur").isNull() | (F.col("hourly_rate_eur") <= 0),
            F.lit("invalid_hourly_rate_eur"),
        ),
        F.when(F.col("is_active").isNull(), F.lit("invalid_is_active")),
    )
    evaluated_df = typed_df.withColumn(
        "dq_reasons", F.filter(reason_exprs, lambda reason: reason.isNotNull())
    ).persist()

    try:
        current_batch_quarantine_df = evaluated_df.filter(
            (F.size("dq_reasons") > 0) & (F.col("_batch_date") == F.to_date(F.lit(iso_date)))
        )
        current_batch_valid_count = evaluated_df.filter(
            (F.size("dq_reasons") == 0) & (F.col("_batch_date") == F.to_date(F.lit(iso_date)))
        ).count()

        # Deduplicate snapshots per (emp_id, snapshot_date) deterministically.
        dedup_window = Window.partitionBy("emp_id", "snapshot_date").orderBy(
            F.col("_ingested_at").desc(),
            F.col("hourly_rate_eur").asc_nulls_last(),
        )
        clean_snapshots_df = (
            evaluated_df.filter(F.size("dq_reasons") == 0)
            .drop("dq_reasons")
            .withColumn("_rn", F.row_number().over(dedup_window))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )

        # Detect SCD Type 2 changes across consecutive snapshots per employee.
        tracked_hash = F.sha2(
            F.concat_ws(
                "||",
                *[
                    F.coalesce(F.col(c).cast("string"), F.lit("__NULL__"))
                    for c in TRACKED_SCD2_ATTRIBUTES
                ],
            ),
            256,
        )
        emp_timeline = Window.partitionBy("emp_id").orderBy(F.col("snapshot_date").asc())
        changes_df = (
            clean_snapshots_df.withColumn("_tracked_hash", tracked_hash)
            .withColumn("_prev_hash", F.lag("_tracked_hash").over(emp_timeline))
            .filter(F.col("_prev_hash").isNull() | (F.col("_tracked_hash") != F.col("_prev_hash")))
        )

        version_window = Window.partitionBy("emp_id").orderBy(F.col("snapshot_date").asc())
        scd2_df = (
            changes_df.withColumn("valid_from", F.col("snapshot_date"))
            .withColumn("_next_valid_from", F.lead("snapshot_date").over(version_window))
            .withColumn(
                "valid_to",
                F.coalesce(F.col("_next_valid_from"), F.lit(OPEN_END_SENTINEL)),
            )
            .withColumn("is_current", F.col("_next_valid_from").isNull())
            .withColumn("_silver_updated_at", F.current_timestamp())
            .select(*SILVER_EMPLOYEES_SCD2_COLUMNS)
        )

        write_delta_overwrite(scd2_df, silver_path)
        write_quarantine(spark, current_batch_quarantine_df, quarantine_path, iso_date)
    finally:
        evaluated_df.unpersist()

    persisted_df = spark.read.format("delta").load(silver_path)
    persisted_quarantine_df = (
        spark.read.format("delta")
        .load(quarantine_path)
        .filter(F.col("_batch_date") == F.to_date(F.lit(iso_date)))
    )
    quarantined_count = persisted_quarantine_df.count()
    dq_results = [
        check_expected_columns(
            persisted_df,
            SILVER_EMPLOYEES_SCD2_COLUMNS,
            run_id=resolved_run_id,
            batch_date=iso_date,
            layer="silver",
            table_name="silver_employees_scd2",
            severity="ERROR",
        ),
        check_not_null(
            persisted_df,
            "emp_id",
            run_id=resolved_run_id,
            batch_date=iso_date,
            layer="silver",
            table_name="silver_employees_scd2",
            severity="ERROR",
        ),
        check_unique_key(
            persisted_df,
            ["emp_id", "valid_from"],
            run_id=resolved_run_id,
            batch_date=iso_date,
            layer="silver",
            table_name="silver_employees_scd2",
            severity="ERROR",
        ),
        check_scd2_no_overlaps(
            persisted_df,
            run_id=resolved_run_id,
            batch_date=iso_date,
            layer="silver",
            table_name="silver_employees_scd2",
            half_open=True,
            severity="ERROR",
        ),
        check_quarantine_ratio(
            valid_count=current_batch_valid_count,
            quarantined_count=quarantined_count,
            run_id=resolved_run_id,
            batch_date=iso_date,
            layer="silver",
            table_name="silver_employees_scd2",
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
    """Build `silver/silver_employees_scd2` from `bronze/employees`."""
    scd2_df, quarantine_df = transform_employees_scd2(
        spark,
        batch_date=batch_date,
        lakehouse_root=lakehouse_root,
        run_id=run_id,
    )
    print(
        f"Silver employees SCD2 complete for batch_date={batch_date}: "
        f"versions={scd2_df.count()}, quarantined={quarantine_df.count()}"
    )
    return scd2_df, quarantine_df


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build silver_employees_scd2.")
    parser.add_argument("--batch-date", required=True, help="Logical batch date (YYYY-MM-DD).")
    parser.add_argument("--lakehouse-root", default=None, help="Override Lakehouse root path.")
    parser.add_argument("--run-id", default=None, help="Orchestrator run identifier.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    spark = get_spark_session("silver-transform-employees-scd2")
    try:
        run(spark, args.batch_date, args.lakehouse_root, args.run_id)
    finally:
        spark.stop()
