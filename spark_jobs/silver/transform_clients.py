"""Reference Silver transformation & quarantine job for `clients`.

Demonstrates:
- String normalization and domain validation
- Quarantine routing (`silver_quarantine/clients`) with `dq_reasons: array<string>`
  and idempotent `_batch_date` partition replacement
- Deterministic deduplication (`Window.partitionBy(...).orderBy(...)`)
- Emitting a structured `DQCheckResult` to `dq/dq_audit_results` and enforcing
  the hard DQ gate
"""

from __future__ import annotations

import argparse
import uuid

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from dq.framework import check_not_null, enforce_dq_gate, write_audit_results
from spark_jobs.common.io import clean_str, write_delta_overwrite, write_quarantine
from spark_jobs.common.spark_session import (
    get_lakehouse_root,
    get_spark_session,
    join_path,
)

VALID_CLIENT_TIERS = ("Standard", "Silver", "Gold", "Enterprise")


def transform_clients(
    spark: SparkSession,
    batch_date: str,
    lakehouse_root: str | None = None,
    run_id: str | None = None,
) -> tuple[DataFrame, DataFrame]:
    """Transform `bronze/clients` into `silver/silver_clients` and `silver_quarantine/clients`."""
    resolved_lakehouse = get_lakehouse_root(lakehouse_root)
    resolved_run_id = run_id or f"manual-{batch_date}-{uuid.uuid4().hex[:8]}"

    bronze_path = join_path(resolved_lakehouse, "bronze", "clients")
    silver_path = join_path(resolved_lakehouse, "silver", "silver_clients")
    quarantine_path = join_path(resolved_lakehouse, "silver_quarantine", "clients")
    audit_path = join_path(resolved_lakehouse, "dq", "dq_audit_results")

    bronze_df = spark.read.format("delta").load(bronze_path)

    normalized_df = (
        bronze_df.withColumn("client_id", clean_str("client_id"))
        .withColumn("client_name", clean_str("client_name"))
        .withColumn("tier", clean_str("tier"))
        .withColumn("_batch_date", F.to_date(F.lit(batch_date)))
    )

    reason_exprs = F.array(
        F.when(F.col("client_id").isNull(), F.lit("missing_client_id")),
        F.when(F.col("client_name").isNull(), F.lit("missing_client_name")),
        F.when(
            F.col("tier").isNull() | (~F.col("tier").isin(*VALID_CLIENT_TIERS)),
            F.lit("invalid_client_tier"),
        ),
    )
    evaluated_df = normalized_df.withColumn(
        "dq_reasons", F.filter(reason_exprs, lambda reason: reason.isNotNull())
    )

    quarantine_df = evaluated_df.filter(F.size("dq_reasons") > 0)

    dedup_window = Window.partitionBy("client_id").orderBy(
        F.col("_ingested_at").desc(),
        F.col("client_name").asc(),
    )
    valid_df = (
        evaluated_df.filter(F.size("dq_reasons") == 0)
        .drop("dq_reasons")
        .withColumn("_rn", F.row_number().over(dedup_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .withColumn("_silver_updated_at", F.current_timestamp())
    )

    # Write cleansed Silver table idempotently (full-load dimension overwrite).
    write_delta_overwrite(valid_df, silver_path)

    # Write quarantine table idempotently by `_batch_date`.
    write_quarantine(spark, quarantine_df, quarantine_path, batch_date)

    # Sample Data Quality check & audit persistence.
    dq_result = check_not_null(
        valid_df,
        "client_id",
        run_id=resolved_run_id,
        batch_date=batch_date,
        layer="silver",
        table_name="silver_clients",
        severity="ERROR",
    )
    write_audit_results(spark, [dq_result], audit_path)
    enforce_dq_gate([dq_result])

    return valid_df, quarantine_df


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Transform bronze/clients into silver_clients.")
    parser.add_argument("--batch-date", required=True, help="Logical batch date (YYYY-MM-DD).")
    parser.add_argument("--lakehouse-root", default=None, help="Override Lakehouse root path.")
    parser.add_argument("--run-id", default=None, help="Orchestrator run identifier.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = get_spark_session("silver-transform-clients")
    try:
        valid_df, quarantine_df = transform_clients(
            spark,
            batch_date=args.batch_date,
            lakehouse_root=args.lakehouse_root,
            run_id=args.run_id,
        )
        print(
            f"Silver clients complete for batch_date={args.batch_date}: "
            f"valid={valid_df.count()}, quarantined={quarantine_df.count()}"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
