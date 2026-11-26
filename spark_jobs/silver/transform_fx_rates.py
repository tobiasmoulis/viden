"""Silver FX calendar spine & LOCF forward-fill job for `silver_fx_rates_daily`.

Generates a complete calendar spine (`min_rate_date`..`batch_date`) cross-joined
with all 6 canonical currency pairs (`EUR`, `USD`, `CZK`) and forward-fills missing
weekend/holiday rates via `F.last("raw_rate", ignorenulls=True)`.
"""

from __future__ import annotations

import argparse
import uuid

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from dq.checks import check_unique_key
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

CANONICAL_CURRENCY_PAIRS = (
    ("EUR", "USD"),
    ("USD", "EUR"),
    ("EUR", "CZK"),
    ("CZK", "EUR"),
    ("USD", "CZK"),
    ("CZK", "USD"),
)

CURRENCY_PAIR_SCHEMA = StructType(
    [
        StructField("currency_from", StringType(), nullable=False),
        StructField("currency_to", StringType(), nullable=False),
    ]
)

SILVER_FX_RATES_COLUMNS = (
    "rate_date",
    "currency_from",
    "currency_to",
    "rate",
    "_is_forward_filled",
    "_silver_updated_at",
)


def transform_fx_rates(
    spark: SparkSession,
    batch_date: str,
    lakehouse_root: str | None = None,
    run_id: str | None = None,
) -> DataFrame:
    """Build `silver/silver_fx_rates_daily` using calendar spine + LOCF forward-fill."""
    end_date = parse_batch_date(batch_date)
    iso_date = end_date.isoformat()
    resolved_lakehouse = get_lakehouse_root(lakehouse_root)
    resolved_run_id = run_id or f"manual-{iso_date}-{uuid.uuid4().hex[:8]}"

    bronze_path = join_path(resolved_lakehouse, "bronze", "fx_rates")
    silver_path = join_path(resolved_lakehouse, "silver", "silver_fx_rates_daily")
    quarantine_path = join_path(resolved_lakehouse, "silver_quarantine", "fx_rates")
    audit_path = join_path(resolved_lakehouse, "dq", "dq_audit_results")

    bronze_df = (
        spark.read.format("delta")
        .load(bronze_path)
        .filter(F.col("_batch_date") <= F.to_date(F.lit(iso_date)))
    )

    normalized_df = bronze_df.select(
        F.coalesce(
            F.to_date(clean_str("rate_date"), "yyyy-MM-dd"),
            F.col("_batch_date"),
        ).alias("rate_date"),
        F.upper(clean_str("currency_from")).alias("currency_from"),
        F.upper(clean_str("currency_to")).alias("currency_to"),
        F.col("rate").cast("double").alias("raw_rate"),
        F.col("_batch_date"),
        F.col("_ingested_at"),
    )

    reason_exprs = F.array(
        F.when(F.col("rate_date").isNull(), F.lit("missing_rate_date")),
        F.when(F.col("currency_from").isNull(), F.lit("missing_currency_from")),
        F.when(F.col("currency_to").isNull(), F.lit("missing_currency_to")),
        F.when(F.col("raw_rate").isNull() | (F.col("raw_rate") <= 0), F.lit("invalid_fx_rate")),
    )
    evaluated_df = normalized_df.withColumn(
        "dq_reasons", F.filter(reason_exprs, lambda reason: reason.isNotNull())
    )

    quarantine_df = evaluated_df.filter(
        (F.size("dq_reasons") > 0) & (F.col("_batch_date") == F.to_date(F.lit(iso_date)))
    )

    dedup_window = Window.partitionBy("rate_date", "currency_from", "currency_to").orderBy(
        F.col("_ingested_at").desc(),
        F.col("raw_rate").asc(),
    )
    valid_raw_df = (
        evaluated_df.filter(F.size("dq_reasons") == 0)
        .drop("dq_reasons")
        .withColumn("_rn", F.row_number().over(dedup_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .select("rate_date", "currency_from", "currency_to", "raw_rate")
    )

    min_date_row = valid_raw_df.agg(F.min("rate_date").alias("min_date")).first()
    start_date = min_date_row["min_date"] if min_date_row is not None else None
    if start_date is None:
        raise ValueError(f"No valid bronze/fx_rates records found on or before {iso_date}")

    calendar_spine_df = spark.sql(
        f"SELECT explode(sequence(DATE '{start_date.isoformat()}', "
        f"DATE '{iso_date}', INTERVAL 1 DAY)) AS rate_date"
    )
    canonical_pairs_df = spark.createDataFrame(
        list(CANONICAL_CURRENCY_PAIRS), schema=CURRENCY_PAIR_SCHEMA
    )
    pairs_df = canonical_pairs_df.unionByName(
        valid_raw_df.select("currency_from", "currency_to").distinct()
    ).distinct()

    spine_df = calendar_spine_df.crossJoin(pairs_df)

    locf_window = (
        Window.partitionBy("currency_from", "currency_to")
        .orderBy(F.col("rate_date").asc())
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    silver_df = (
        spine_df.join(
            valid_raw_df,
            on=["rate_date", "currency_from", "currency_to"],
            how="left",
        )
        .withColumn("rate", F.last("raw_rate", ignorenulls=True).over(locf_window))
        .withColumn("_is_forward_filled", F.col("raw_rate").isNull())
        .withColumn("_silver_updated_at", F.current_timestamp())
        .select(*SILVER_FX_RATES_COLUMNS)
    )

    write_delta_overwrite(silver_df, silver_path)
    write_quarantine(spark, quarantine_df, quarantine_path, iso_date)

    persisted_df = spark.read.format("delta").load(silver_path)
    dq_results = [
        check_expected_columns(
            persisted_df,
            SILVER_FX_RATES_COLUMNS,
            run_id=resolved_run_id,
            batch_date=iso_date,
            layer="silver",
            table_name="silver_fx_rates_daily",
            severity="ERROR",
        ),
        check_not_null(
            persisted_df,
            "rate",
            run_id=resolved_run_id,
            batch_date=iso_date,
            layer="silver",
            table_name="silver_fx_rates_daily",
            severity="ERROR",
        ),
        check_unique_key(
            persisted_df,
            ["rate_date", "currency_from", "currency_to"],
            run_id=resolved_run_id,
            batch_date=iso_date,
            layer="silver",
            table_name="silver_fx_rates_daily",
            severity="ERROR",
        ),
    ]
    write_audit_results(spark, dq_results, audit_path)
    enforce_dq_gate(dq_results)

    return persisted_df


def run(
    spark: SparkSession,
    batch_date: str,
    lakehouse_root: str | None = None,
    run_id: str | None = None,
) -> DataFrame:
    """Build `silver/silver_fx_rates_daily` using calendar spine + LOCF."""
    fx_df = transform_fx_rates(
        spark,
        batch_date=batch_date,
        lakehouse_root=lakehouse_root,
        run_id=run_id,
    )
    print(f"Silver FX rates complete for batch_date={batch_date}: rows={fx_df.count()}")
    return fx_df


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build silver_fx_rates_daily.")
    parser.add_argument("--batch-date", required=True, help="Logical batch date (YYYY-MM-DD).")
    parser.add_argument("--lakehouse-root", default=None, help="Override Lakehouse root path.")
    parser.add_argument("--run-id", default=None, help="Orchestrator run identifier.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    spark = get_spark_session("silver-transform-fx-rates")
    try:
        run(spark, args.batch_date, args.lakehouse_root, args.run_id)
    finally:
        spark.stop()
