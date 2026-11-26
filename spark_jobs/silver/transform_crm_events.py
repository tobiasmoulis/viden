"""Silver skew-optimized transformation job for `silver_crm_events`.

Implements:
- UTC `TIMESTAMP` parsing (`event_timestamp`, `event_date`)
- Polymorphic JSON `payload` validation (`invalid_payload_json`) and extraction across
  all 5 `event_type` variants (`page_view`, `support_ticket`, `contract_renewal`,
  `api_call`, `meeting_logged`) while preserving `payload_raw`
- Explicit skew-handling join with `silver_clients` (`F.broadcast` + Spark AQE) to
  mitigate `VIP_CORP_001` (~45% of all CRM events) and quarantine `orphan_client_id`;
  supports `--skew-strategy sort_merge` for baseline Spark UI benchmarks
- Quarantine routing (`silver_quarantine/crm_events`), idempotent `_batch_date` writes,
  and full job-level DQ audit persistence
"""

from __future__ import annotations

import argparse
import uuid

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    MapType,
    StringType,
    StructField,
    StructType,
)

from dq.checks import check_non_empty_partition, check_quarantine_ratio, check_unique_key
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

VALID_EVENT_TYPES = (
    "page_view",
    "support_ticket",
    "contract_renewal",
    "api_call",
    "meeting_logged",
)

# Superset JSON schema covering all 5 polymorphic payload variants.
CRM_PAYLOAD_SCHEMA = StructType(
    [
        # page_view
        StructField("path", StringType(), nullable=True),
        StructField("referrer", StringType(), nullable=True),
        StructField("duration_sec", IntegerType(), nullable=True),
        StructField("device", StringType(), nullable=True),
        # support_ticket
        StructField("ticket_id", StringType(), nullable=True),
        StructField("severity", StringType(), nullable=True),
        StructField("module", StringType(), nullable=True),
        StructField("sla_hours", IntegerType(), nullable=True),
        # contract_renewal
        StructField("opportunity_id", StringType(), nullable=True),
        StructField("stage", StringType(), nullable=True),
        StructField("arr_delta_eur", DoubleType(), nullable=True),
        StructField("probability", DoubleType(), nullable=True),
        # api_call
        StructField("endpoint", StringType(), nullable=True),
        StructField("status_code", IntegerType(), nullable=True),
        StructField("latency_ms", IntegerType(), nullable=True),
        StructField("region", StringType(), nullable=True),
        # meeting_logged
        StructField("meeting_type", StringType(), nullable=True),
        StructField("attendees_count", IntegerType(), nullable=True),
        StructField("outcome", StringType(), nullable=True),
        StructField("next_step_days", IntegerType(), nullable=True),
    ]
)

SILVER_CRM_EVENTS_COLUMNS = (
    "event_id",
    "client_id",
    "client_name",
    "client_tier",
    "event_type",
    "event_timestamp",
    "event_date",
    "payload_raw",
    "path",
    "referrer",
    "duration_sec",
    "device",
    "ticket_id",
    "severity",
    "module",
    "sla_hours",
    "opportunity_id",
    "stage",
    "arr_delta_eur",
    "probability",
    "endpoint",
    "status_code",
    "latency_ms",
    "region",
    "meeting_type",
    "attendees_count",
    "outcome",
    "next_step_days",
    "_batch_date",
    "_silver_updated_at",
)


def transform_crm_events(
    spark: SparkSession,
    batch_date: str,
    lakehouse_root: str | None = None,
    run_id: str | None = None,
    *,
    skew_strategy: str = "broadcast",
) -> tuple[DataFrame, DataFrame]:
    """Build `silver/silver_crm_events` for `batch_date` with explicit skew mitigation."""
    if skew_strategy not in ("broadcast", "sort_merge"):
        raise ValueError(f"Unsupported skew_strategy: {skew_strategy!r}")

    iso_date = parse_batch_date(batch_date).isoformat()
    resolved_lakehouse = get_lakehouse_root(lakehouse_root)
    resolved_run_id = run_id or f"manual-{iso_date}-{uuid.uuid4().hex[:8]}"

    bronze_path = join_path(resolved_lakehouse, "bronze", "crm_events")
    clients_path = join_path(resolved_lakehouse, "silver", "silver_clients")
    silver_path = join_path(resolved_lakehouse, "silver", "silver_crm_events")
    quarantine_path = join_path(resolved_lakehouse, "silver_quarantine", "crm_events")
    audit_path = join_path(resolved_lakehouse, "dq", "dq_audit_results")

    if skew_strategy == "broadcast":
        spark.conf.set("spark.sql.adaptive.enabled", "true")
        spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    else:
        spark.conf.set("spark.sql.adaptive.enabled", "false")
        spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "false")

    bronze_df = (
        spark.read.format("delta")
        .load(bronze_path)
        .filter(F.col("_batch_date") == F.to_date(F.lit(iso_date)))
    )

    normalized_df = bronze_df.select(
        clean_str("event_id").alias("event_id"),
        clean_str("client_id").alias("client_id"),
        clean_str("event_type").alias("event_type"),
        clean_str("timestamp").alias("_raw_timestamp"),
        F.to_timestamp(clean_str("timestamp")).alias("event_timestamp"),
        clean_str("payload").alias("payload_raw"),
        F.col("_batch_date"),
        F.col("_ingested_at"),
        F.col("_source_file"),
    )

    # Skew-mitigated join with `silver_clients` BEFORE quarantine split so orphan clients
    # are routed to `silver_quarantine/crm_events` instead of leaking NULL attributes.
    clients_dim_df = (
        spark.read.format("delta")
        .load(clients_path)
        .select(
            F.col("client_id"),
            F.col("client_name"),
            F.col("tier").alias("client_tier"),
            F.lit(True).alias("_client_matched"),
        )
    )
    rhs_clients = (
        F.broadcast(clients_dim_df)
        if skew_strategy == "broadcast"
        else clients_dim_df.hint("merge")
    )
    enriched_df = normalized_df.join(rhs_clients, on="client_id", how="left")

    payload_map_type = MapType(StringType(), StringType())
    invalid_payload = (
        F.col("payload_raw").isNull() | F.from_json(F.col("payload_raw"), payload_map_type).isNull()
    )

    reason_exprs = F.array(
        F.when(F.col("event_id").isNull(), F.lit("missing_event_id")),
        F.when(F.col("client_id").isNull(), F.lit("missing_client_id")),
        F.when(
            F.col("client_id").isNotNull() & F.col("_client_matched").isNull(),
            F.lit("orphan_client_id"),
        ),
        F.when(
            F.col("event_type").isNull() | (~F.col("event_type").isin(*VALID_EVENT_TYPES)),
            F.lit("invalid_event_type"),
        ),
        F.when(F.col("event_timestamp").isNull(), F.lit("invalid_timestamp")),
        F.when(invalid_payload, F.lit("invalid_payload_json")),
    )
    evaluated_df = enriched_df.withColumn(
        "dq_reasons", F.filter(reason_exprs, lambda reason: reason.isNotNull())
    ).persist()

    try:
        quarantine_df = evaluated_df.filter(F.size("dq_reasons") > 0).drop("_client_matched")

        dedup_window = Window.partitionBy("event_id").orderBy(
            F.col("event_timestamp").desc(),
            F.col("_ingested_at").desc(),
            F.col("payload_raw").asc_nulls_last(),
        )
        valid_events_df = (
            evaluated_df.filter(F.size("dq_reasons") == 0)
            .drop("dq_reasons", "_client_matched", "_raw_timestamp")
            .withColumn("_rn", F.row_number().over(dedup_window))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )

        parsed_df = valid_events_df.withColumn(
            "_parsed_payload", F.from_json(F.col("payload_raw"), CRM_PAYLOAD_SCHEMA)
        )

        silver_df = parsed_df.select(
            F.col("event_id"),
            F.col("client_id"),
            F.col("client_name"),
            F.col("client_tier"),
            F.col("event_type"),
            F.col("event_timestamp"),
            F.to_date(F.col("event_timestamp")).alias("event_date"),
            F.col("payload_raw"),
            F.col("_parsed_payload.path").alias("path"),
            F.col("_parsed_payload.referrer").alias("referrer"),
            F.col("_parsed_payload.duration_sec").alias("duration_sec"),
            F.col("_parsed_payload.device").alias("device"),
            F.col("_parsed_payload.ticket_id").alias("ticket_id"),
            F.col("_parsed_payload.severity").alias("severity"),
            F.col("_parsed_payload.module").alias("module"),
            F.col("_parsed_payload.sla_hours").alias("sla_hours"),
            F.col("_parsed_payload.opportunity_id").alias("opportunity_id"),
            F.col("_parsed_payload.stage").alias("stage"),
            F.col("_parsed_payload.arr_delta_eur").alias("arr_delta_eur"),
            F.col("_parsed_payload.probability").alias("probability"),
            F.col("_parsed_payload.endpoint").alias("endpoint"),
            F.col("_parsed_payload.status_code").alias("status_code"),
            F.col("_parsed_payload.latency_ms").alias("latency_ms"),
            F.col("_parsed_payload.region").alias("region"),
            F.col("_parsed_payload.meeting_type").alias("meeting_type"),
            F.col("_parsed_payload.attendees_count").alias("attendees_count"),
            F.col("_parsed_payload.outcome").alias("outcome"),
            F.col("_parsed_payload.next_step_days").alias("next_step_days"),
            F.col("_batch_date"),
            F.current_timestamp().alias("_silver_updated_at"),
        )

        write_delta_partition(spark, silver_df, silver_path, iso_date)
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
            SILVER_CRM_EVENTS_COLUMNS,
            run_id=resolved_run_id,
            batch_date=iso_date,
            layer="silver",
            table_name="silver_crm_events",
            severity="ERROR",
        ),
        check_non_empty_partition(
            persisted_batch_df,
            run_id=resolved_run_id,
            batch_date=iso_date,
            layer="silver",
            table_name="silver_crm_events",
            severity="ERROR",
        ),
        check_not_null(
            persisted_batch_df,
            "event_id",
            run_id=resolved_run_id,
            batch_date=iso_date,
            layer="silver",
            table_name="silver_crm_events",
            severity="ERROR",
        ),
        check_not_null(
            persisted_batch_df,
            "client_id",
            run_id=resolved_run_id,
            batch_date=iso_date,
            layer="silver",
            table_name="silver_crm_events",
            severity="ERROR",
        ),
        check_not_null(
            persisted_batch_df,
            "event_timestamp",
            run_id=resolved_run_id,
            batch_date=iso_date,
            layer="silver",
            table_name="silver_crm_events",
            severity="ERROR",
        ),
        check_unique_key(
            persisted_batch_df,
            ["event_id"],
            run_id=resolved_run_id,
            batch_date=iso_date,
            layer="silver",
            table_name="silver_crm_events",
            severity="ERROR",
        ),
        check_quarantine_ratio(
            valid_count=valid_count,
            quarantined_count=quarantined_count,
            max_ratio=0.08,
            run_id=resolved_run_id,
            batch_date=iso_date,
            layer="silver",
            table_name="silver_crm_events",
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
    *,
    skew_strategy: str = "broadcast",
) -> tuple[DataFrame, DataFrame]:
    """Build `silver/silver_crm_events` with documented skew mitigation."""
    valid_df, quarantine_df = transform_crm_events(
        spark,
        batch_date=batch_date,
        lakehouse_root=lakehouse_root,
        run_id=run_id,
        skew_strategy=skew_strategy,
    )
    print(
        f"Silver CRM events complete for batch_date={batch_date} "
        f"(strategy={skew_strategy}): valid={valid_df.count()}, "
        f"quarantined={quarantine_df.count()}"
    )
    return valid_df, quarantine_df


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build silver_crm_events.")
    parser.add_argument("--batch-date", required=True, help="Logical batch date (YYYY-MM-DD).")
    parser.add_argument("--lakehouse-root", default=None, help="Override Lakehouse root path.")
    parser.add_argument("--run-id", default=None, help="Orchestrator run identifier.")
    parser.add_argument(
        "--skew-strategy",
        default="broadcast",
        choices=["broadcast", "sort_merge"],
        help="Join strategy for enriching CRM events with silver_clients.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    extra_conf = (
        {
            "spark.sql.autoBroadcastJoinThreshold": "-1",
            "spark.sql.adaptive.enabled": "false",
            "spark.sql.adaptive.skewJoin.enabled": "false",
        }
        if args.skew_strategy == "sort_merge"
        else {
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true",
        }
    )
    spark = get_spark_session(
        f"silver-transform-crm-events-{args.skew_strategy}",
        extra_conf=extra_conf,
    )
    try:
        run(
            spark,
            args.batch_date,
            args.lakehouse_root,
            args.run_id,
            skew_strategy=args.skew_strategy,
        )
    finally:
        spark.stop()
