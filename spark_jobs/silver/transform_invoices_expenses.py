"""Silver transformation & quarantine job for `silver_invoices` and `silver_expenses`.

Implements:
1. `silver_invoices`:
   - Parses ISO (`YYYY-MM-DD`) and European (`DD.MM.YYYY`) dates in `issued_at` & `paid_at`
   - Quarantines orphan `project_id` references via `left_anti` against `silver_projects`
   - Retains valid negative credit notes (`is_credit_note`, `net_amount_eur`) and open invoices
   - Converts to EUR via `silver_fx_rates_daily` (EUR rate = 1.0)
2. `silver_expenses`:
   - Deterministic deduplication by `exp_id` (per `docs/architecture.md` Section 5)
   - Converts to EUR via `silver_fx_rates_daily` (EUR rate = 1.0)
3. Hard DQ check: zero `NULL` `amount_eur` after FX conversion.
"""

from __future__ import annotations

import argparse
import uuid

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from dq.checks import (
    check_quarantine_ratio,
    check_ratio_threshold,
    check_unique_key,
    check_zero_null_eur,
)
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

SILVER_INVOICES_COLUMNS = (
    "inv_id",
    "client_id",
    "project_id",
    "amount",
    "currency",
    "issued_date",
    "paid_date",
    "is_credit_note",
    "amount_eur",
    "net_amount_eur",
    "_batch_date",
    "_silver_updated_at",
)

SILVER_EXPENSES_COLUMNS = (
    "exp_id",
    "department",
    "category",
    "amount",
    "currency",
    "expense_date",
    "amount_eur",
    "_batch_date",
    "_silver_updated_at",
)


def parse_flexible_date(column_name: str) -> Column:
    """Parse canonical ISO (`YYYY-MM-DD`) and European (`DD.MM.YYYY`) date strings safely."""
    cleaned = clean_str(column_name)
    return (
        F.when(cleaned.rlike(r"^\d{4}-\d{2}-\d{2}$"), F.to_date(cleaned, "yyyy-MM-dd"))
        .when(cleaned.rlike(r"^\d{2}\.\d{2}\.\d{4}$"), F.to_date(cleaned, "dd.MM.yyyy"))
        .otherwise(F.lit(None).cast("date"))
    )


def _load_fx_to_eur(spark: SparkSession, lakehouse_root: str) -> DataFrame:
    fx_path = join_path(lakehouse_root, "silver", "silver_fx_rates_daily")
    return (
        spark.read.format("delta")
        .load(fx_path)
        .filter(F.col("currency_to") == F.lit("EUR"))
        .select(
            F.col("rate_date").alias("_fx_date"),
            F.col("currency_from").alias("_fx_currency"),
            F.col("rate").alias("_fx_rate"),
        )
    )


def transform_invoices(
    spark: SparkSession,
    batch_date: str,
    lakehouse_root: str | None = None,
    run_id: str | None = None,
) -> tuple[DataFrame, DataFrame]:
    """Transform `bronze/invoices` for `batch_date` into `silver_invoices` & quarantine."""
    iso_date = parse_batch_date(batch_date).isoformat()
    resolved_lakehouse = get_lakehouse_root(lakehouse_root)
    resolved_run_id = run_id or f"manual-{iso_date}-{uuid.uuid4().hex[:8]}"

    bronze_path = join_path(resolved_lakehouse, "bronze", "invoices")
    projects_path = join_path(resolved_lakehouse, "silver", "silver_projects")
    silver_path = join_path(resolved_lakehouse, "silver", "silver_invoices")
    quarantine_path = join_path(resolved_lakehouse, "silver_quarantine", "invoices")
    audit_path = join_path(resolved_lakehouse, "dq", "dq_audit_results")

    bronze_df = (
        spark.read.format("delta")
        .load(bronze_path)
        .filter(F.col("_batch_date") == F.to_date(F.lit(iso_date)))
    )

    normalized_df = bronze_df.select(
        clean_str("inv_id").alias("inv_id"),
        clean_str("client_id").alias("client_id"),
        clean_str("project_id").alias("project_id"),
        F.col("amount").cast("double").alias("amount"),
        F.upper(clean_str("currency")).alias("currency"),
        clean_str("issued_at").alias("_raw_issued_at"),
        clean_str("paid_at").alias("_raw_paid_at"),
        parse_flexible_date("issued_at").alias("issued_date"),
        parse_flexible_date("paid_at").alias("paid_date"),
        F.col("_batch_date"),
        F.col("_ingested_at"),
        F.col("_source_file"),
    )

    # Identify orphan `project_id` references via `left_anti` against `silver_projects`.
    valid_projects_df = (
        spark.read.format("delta").load(projects_path).select("project_id").distinct()
    )
    orphan_projects_df = (
        normalized_df.filter(F.col("project_id").isNotNull())
        .select("project_id")
        .distinct()
        .join(F.broadcast(valid_projects_df), on="project_id", how="left_anti")
        .withColumn("_is_orphan_project", F.lit(True))
    )

    checked_df = normalized_df.join(F.broadcast(orphan_projects_df), on="project_id", how="left")
    is_orphan = F.coalesce(F.col("_is_orphan_project"), F.lit(False))

    reason_exprs = F.array(
        F.when(F.col("inv_id").isNull(), F.lit("missing_inv_id")),
        F.when(F.col("client_id").isNull(), F.lit("missing_client_id")),
        F.when(F.col("project_id").isNull(), F.lit("missing_project_id")),
        F.when(is_orphan, F.lit("orphan_project_id")),
        F.when(F.col("amount").isNull(), F.lit("missing_amount")),
        F.when(F.col("currency").isNull(), F.lit("missing_currency")),
        F.when(F.col("issued_date").isNull(), F.lit("invalid_issued_at")),
        F.when(
            F.col("_raw_paid_at").isNotNull() & F.col("paid_date").isNull(),
            F.lit("invalid_paid_at"),
        ),
    )
    evaluated_df = checked_df.withColumn(
        "dq_reasons", F.filter(reason_exprs, lambda reason: reason.isNotNull())
    ).persist()

    try:
        orphan_count = evaluated_df.filter(is_orphan).count()
        total_evaluated = evaluated_df.count()

        quarantine_df = evaluated_df.filter(F.size("dq_reasons") > 0).drop(
            "_is_orphan_project", "_raw_issued_at", "_raw_paid_at"
        )

        dedup_window = Window.partitionBy("inv_id").orderBy(
            F.col("_ingested_at").desc(),
            F.col("amount").asc(),
        )
        valid_candidates_df = (
            evaluated_df.filter(F.size("dq_reasons") == 0)
            .drop("dq_reasons", "_is_orphan_project", "_raw_issued_at", "_raw_paid_at")
            .withColumn("_rn", F.row_number().over(dedup_window))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )

        fx_to_eur_df = _load_fx_to_eur(spark, resolved_lakehouse)
        enriched_df = valid_candidates_df.join(
            F.broadcast(fx_to_eur_df),
            on=(
                (valid_candidates_df["issued_date"] == fx_to_eur_df["_fx_date"])
                & (valid_candidates_df["currency"] == fx_to_eur_df["_fx_currency"])
            ),
            how="left",
        )

        effective_rate = F.when(F.col("currency") == F.lit("EUR"), F.lit(1.0)).otherwise(
            F.col("_fx_rate")
        )
        amount_eur_col = F.col("amount") * effective_rate

        valid_df = (
            enriched_df.withColumn("is_credit_note", F.col("amount") < F.lit(0.0))
            .withColumn("amount_eur", amount_eur_col)
            .withColumn("net_amount_eur", amount_eur_col)
            .withColumn("_silver_updated_at", F.current_timestamp())
            .select(*SILVER_INVOICES_COLUMNS)
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
            SILVER_INVOICES_COLUMNS,
            run_id=resolved_run_id,
            batch_date=iso_date,
            layer="silver",
            table_name="silver_invoices",
            severity="ERROR",
        ),
        check_not_null(
            persisted_batch_df,
            "inv_id",
            run_id=resolved_run_id,
            batch_date=iso_date,
            layer="silver",
            table_name="silver_invoices",
            severity="ERROR",
        ),
        check_unique_key(
            persisted_batch_df,
            ["inv_id"],
            run_id=resolved_run_id,
            batch_date=iso_date,
            layer="silver",
            table_name="silver_invoices",
            severity="ERROR",
        ),
        check_zero_null_eur(
            persisted_batch_df,
            "amount_eur",
            run_id=resolved_run_id,
            batch_date=iso_date,
            layer="silver",
            table_name="silver_invoices",
            severity="ERROR",
        ),
        check_quarantine_ratio(
            valid_count=valid_count,
            quarantined_count=quarantined_count,
            max_ratio=0.08,
            run_id=resolved_run_id,
            batch_date=iso_date,
            layer="silver",
            table_name="silver_invoices",
            severity="WARN",
        ),
        check_ratio_threshold(
            failed_count=orphan_count,
            total_count=total_evaluated,
            check_name="orphan_invoice_rate",
            max_ratio=0.05,
            run_id=resolved_run_id,
            batch_date=iso_date,
            layer="silver",
            table_name="silver_invoices",
            severity="WARN",
        ),
    ]
    write_audit_results(spark, dq_results, audit_path)
    enforce_dq_gate(dq_results)

    return persisted_batch_df, persisted_quarantine_df


def transform_expenses(
    spark: SparkSession,
    batch_date: str,
    lakehouse_root: str | None = None,
    run_id: str | None = None,
) -> tuple[DataFrame, DataFrame]:
    """Transform `bronze/expenses` for `batch_date` into `silver_expenses` & quarantine."""
    iso_date = parse_batch_date(batch_date).isoformat()
    resolved_lakehouse = get_lakehouse_root(lakehouse_root)
    resolved_run_id = run_id or f"manual-{iso_date}-{uuid.uuid4().hex[:8]}"

    bronze_path = join_path(resolved_lakehouse, "bronze", "expenses")
    silver_path = join_path(resolved_lakehouse, "silver", "silver_expenses")
    quarantine_path = join_path(resolved_lakehouse, "silver_quarantine", "expenses")
    audit_path = join_path(resolved_lakehouse, "dq", "dq_audit_results")

    bronze_df = (
        spark.read.format("delta")
        .load(bronze_path)
        .filter(F.col("_batch_date") == F.to_date(F.lit(iso_date)))
    )

    normalized_df = bronze_df.select(
        clean_str("exp_id").alias("exp_id"),
        clean_str("department").alias("department"),
        clean_str("category").alias("category"),
        F.col("amount").cast("double").alias("amount"),
        F.upper(clean_str("currency")).alias("currency"),
        parse_flexible_date("expense_date").alias("expense_date"),
        F.col("_batch_date"),
        F.col("_ingested_at"),
        F.col("_source_file"),
    )

    raw_count = normalized_df.count()

    # Deterministic deduplication by `exp_id` (per docs/architecture.md Section 5):
    # Prefer valid positive-amount row over corrupted near-duplicate, then lowest amount.
    is_valid_expense = (
        F.col("department").isNotNull()
        & F.col("category").isNotNull()
        & F.col("amount").isNotNull()
        & (F.col("amount") > 0)
        & F.col("currency").isNotNull()
        & F.col("expense_date").isNotNull()
    )
    dedup_window = Window.partitionBy("exp_id").orderBy(
        F.when(is_valid_expense, F.lit(0)).otherwise(F.lit(1)).asc(),
        F.col("amount").asc_nulls_last(),
        F.col("currency").asc_nulls_last(),
        F.col("department").asc_nulls_last(),
        F.col("category").asc_nulls_last(),
        F.col("_ingested_at").desc(),
    )
    non_null_key_df = (
        normalized_df.filter(F.col("exp_id").isNotNull())
        .withColumn("_rn", F.row_number().over(dedup_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
    null_key_df = normalized_df.filter(F.col("exp_id").isNull())
    dedup_df = non_null_key_df.unionByName(null_key_df)

    dedup_count = dedup_df.count()
    duplicate_count = max(0, raw_count - dedup_count)

    reason_exprs = F.array(
        F.when(F.col("exp_id").isNull(), F.lit("missing_exp_id")),
        F.when(F.col("department").isNull(), F.lit("missing_department")),
        F.when(F.col("category").isNull(), F.lit("missing_category")),
        F.when(F.col("amount").isNull() | (F.col("amount") <= 0), F.lit("invalid_amount")),
        F.when(F.col("currency").isNull(), F.lit("missing_currency")),
        F.when(F.col("expense_date").isNull(), F.lit("invalid_expense_date")),
    )
    evaluated_df = dedup_df.withColumn(
        "dq_reasons", F.filter(reason_exprs, lambda reason: reason.isNotNull())
    ).persist()

    try:
        quarantine_df = evaluated_df.filter(F.size("dq_reasons") > 0)
        valid_candidates_df = evaluated_df.filter(F.size("dq_reasons") == 0).drop("dq_reasons")

        fx_to_eur_df = _load_fx_to_eur(spark, resolved_lakehouse)
        enriched_df = valid_candidates_df.join(
            F.broadcast(fx_to_eur_df),
            on=(
                (valid_candidates_df["expense_date"] == fx_to_eur_df["_fx_date"])
                & (valid_candidates_df["currency"] == fx_to_eur_df["_fx_currency"])
            ),
            how="left",
        )

        effective_rate = F.when(F.col("currency") == F.lit("EUR"), F.lit(1.0)).otherwise(
            F.col("_fx_rate")
        )
        valid_df = (
            enriched_df.withColumn("amount_eur", F.col("amount") * effective_rate)
            .withColumn("_silver_updated_at", F.current_timestamp())
            .select(*SILVER_EXPENSES_COLUMNS)
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
            SILVER_EXPENSES_COLUMNS,
            run_id=resolved_run_id,
            batch_date=iso_date,
            layer="silver",
            table_name="silver_expenses",
            severity="ERROR",
        ),
        check_not_null(
            persisted_batch_df,
            "exp_id",
            run_id=resolved_run_id,
            batch_date=iso_date,
            layer="silver",
            table_name="silver_expenses",
            severity="ERROR",
        ),
        check_unique_key(
            persisted_batch_df,
            ["exp_id"],
            run_id=resolved_run_id,
            batch_date=iso_date,
            layer="silver",
            table_name="silver_expenses",
            severity="ERROR",
        ),
        check_zero_null_eur(
            persisted_batch_df,
            "amount_eur",
            run_id=resolved_run_id,
            batch_date=iso_date,
            layer="silver",
            table_name="silver_expenses",
            severity="ERROR",
        ),
        check_quarantine_ratio(
            valid_count=valid_count,
            quarantined_count=quarantined_count,
            max_ratio=0.08,
            run_id=resolved_run_id,
            batch_date=iso_date,
            layer="silver",
            table_name="silver_expenses",
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
            table_name="silver_expenses",
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
    """Build `silver/silver_invoices` and `silver/silver_expenses`."""
    inv_df, q_inv = transform_invoices(
        spark,
        batch_date=batch_date,
        lakehouse_root=lakehouse_root,
        run_id=run_id,
    )
    exp_df, q_exp = transform_expenses(
        spark,
        batch_date=batch_date,
        lakehouse_root=lakehouse_root,
        run_id=run_id,
    )
    print(
        f"Silver Finance facts complete for batch_date={batch_date}: "
        f"invoices(valid={inv_df.count()}, q={q_inv.count()}), "
        f"expenses(valid={exp_df.count()}, q={q_exp.count()})"
    )
    return inv_df, exp_df


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build silver_invoices and silver_expenses.")
    parser.add_argument("--batch-date", required=True, help="Logical batch date (YYYY-MM-DD).")
    parser.add_argument("--lakehouse-root", default=None, help="Override Lakehouse root path.")
    parser.add_argument("--run-id", default=None, help="Orchestrator run identifier.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    spark = get_spark_session("silver-transform-invoices-expenses")
    try:
        run(spark, args.batch_date, args.lakehouse_root, args.run_id)
    finally:
        spark.stop()
