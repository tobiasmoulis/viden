"""Centralized Data Quality Gate CLI runner for Bronze, Silver, and Gold layers.

Invoked by Airflow (`dq_bronze_gate`, `dq_silver_gate`, `dq_gold_gate`) or manually:
    spark-submit /opt/workspace/dq/run_gate.py \
        --layer bronze --batch-date 2026-01-01 --run-id manual-1

Checks `DeltaTable.isDeltaTable(spark, path)` before loading each table so missing
tables produce structured `table_exists` audit entries instead of unhandled errors.
All results are persisted idempotently to `dq/dq_audit_results` and enforced via
`enforce_dq_gate`.
"""

from __future__ import annotations

import argparse
import uuid
from collections.abc import Sequence

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from dq.checks import (
    check_non_empty_partition,
    check_scd2_no_overlaps,
    check_unique_key,
    check_zero_null_eur,
)
from dq.framework import (
    DQCheckResult,
    build_dq_result,
    check_not_null,
    enforce_dq_gate,
    write_audit_results,
)
from spark_jobs.common.io import parse_batch_date
from spark_jobs.common.spark_session import (
    get_lakehouse_root,
    get_spark_session,
    join_path,
)

BRONZE_FULL_LOAD_TABLES = ("clients", "projects", "survey_questions")
BRONZE_DAILY_MANDATORY_TABLES = ("employees", "timesheets", "invoices", "expenses", "crm_events")

# All 11 Silver tables are implemented and enforced as mandatory by `dq_silver_gate`.
SILVER_MANDATORY_SPECS: tuple[dict, ...] = (
    {
        "table": "silver_clients",
        "pk": ("client_id",),
        "partitioned": False,
    },
    {
        "table": "silver_projects",
        "pk": ("project_id",),
        "partitioned": False,
    },
    {
        "table": "silver_survey_questions",
        "pk": ("question_id",),
        "partitioned": False,
    },
    {
        "table": "silver_employees_scd2",
        "pk": ("emp_id", "valid_from"),
        "partitioned": False,
        "scd2": True,
    },
    {
        "table": "silver_fx_rates_daily",
        "pk": ("rate_date", "currency_from", "currency_to"),
        "partitioned": False,
        "not_null_cols": ("rate",),
        "freshness_date_col": "rate_date",
    },
    {
        "table": "silver_timesheets",
        "pk": ("ts_id",),
        "partitioned": True,
    },
    {
        "table": "silver_invoices",
        "pk": ("inv_id",),
        "partitioned": True,
        "eur_col": "amount_eur",
    },
    {
        "table": "silver_expenses",
        "pk": ("exp_id",),
        "partitioned": True,
        "eur_col": "amount_eur",
    },
    {
        "table": "silver_crm_events",
        "pk": ("event_id",),
        "partitioned": True,
        "not_null_cols": ("client_id", "event_timestamp"),
    },
    {
        "table": "silver_survey_responses",
        "pk": ("survey_id",),
        "partitioned": True,
        "allow_empty": True,
        "not_null_cols": ("emp_id", "submitted_at"),
    },
    {
        "table": "silver_survey_answers",
        "pk": ("survey_id", "question_id"),
        "partitioned": True,
        "allow_empty": True,
        "not_null_cols": ("numeric_score",),
    },
)

SILVER_OPTIONAL_SPECS: tuple[dict, ...] = ()

GOLD_SPECS: tuple[dict, ...] = (
    {
        "table": "gold_project_profitability",
        "pk": ("project_id", "year_month"),
    },
    {
        "table": "gold_department_finance",
        "pk": ("department", "category", "year_month"),
    },
    {
        "table": "gold_client_engagement",
        "pk": ("client_id", "client_tier", "event_type", "event_date"),
    },
    {
        "table": "gold_hr_enps_analytics",
        "pk": ("department", "seniority_level", "work_mode", "survey_wave"),
    },
    {
        "table": "gold_employee_churn_features",
        "pk": ("emp_id",),
    },
)


def _check_table_exists(
    spark: SparkSession,
    table_path: str,
    *,
    run_id: str,
    batch_date: str,
    layer: str,
    table_name: str,
    severity: str = "ERROR",
) -> tuple[bool, DQCheckResult]:
    exists = DeltaTable.isDeltaTable(spark, table_path)
    result = build_dq_result(
        run_id=run_id,
        batch_date=batch_date,
        layer=layer,
        table_name=table_name,
        check_name="gate_table_exists",
        severity=severity,
        passed=exists,
        failed_count=0 if exists else 1,
        total_count=1,
    )
    return exists, result


def _load_batch_df(
    spark: SparkSession,
    table_path: str,
    batch_date: str,
    *,
    partitioned: bool,
) -> DataFrame:
    df = spark.read.format("delta").load(table_path)
    if partitioned and "_batch_date" in df.columns:
        return df.filter(F.col("_batch_date") == F.to_date(F.lit(batch_date)))
    return df


def run_bronze_gate(
    spark: SparkSession,
    batch_date: str,
    lakehouse_root: str | None = None,
    run_id: str | None = None,
) -> list[DQCheckResult]:
    """Validate all 10 Bronze tables for `batch_date` and enforce the Bronze DQ gate."""
    iso_date = parse_batch_date(batch_date).isoformat()
    resolved_lakehouse = get_lakehouse_root(lakehouse_root)
    resolved_run_id = run_id or f"gate-bronze-{iso_date}-{uuid.uuid4().hex[:8]}"
    audit_path = join_path(resolved_lakehouse, "dq", "dq_audit_results")

    results: list[DQCheckResult] = []

    for table in BRONZE_FULL_LOAD_TABLES:
        path = join_path(resolved_lakehouse, "bronze", table)
        exists, exists_res = _check_table_exists(
            spark,
            path,
            run_id=resolved_run_id,
            batch_date=iso_date,
            layer="bronze",
            table_name=table,
        )
        results.append(exists_res)
        if exists:
            df = spark.read.format("delta").load(path)
            results.append(
                check_non_empty_partition(
                    df,
                    run_id=resolved_run_id,
                    batch_date=iso_date,
                    layer="bronze",
                    table_name=table,
                    allow_weekends_empty=False,
                    severity="ERROR",
                )
            )

    for table in BRONZE_DAILY_MANDATORY_TABLES:
        path = join_path(resolved_lakehouse, "bronze", table)
        exists, exists_res = _check_table_exists(
            spark,
            path,
            run_id=resolved_run_id,
            batch_date=iso_date,
            layer="bronze",
            table_name=table,
        )
        results.append(exists_res)
        if exists:
            df = _load_batch_df(spark, path, iso_date, partitioned=True)
            results.append(
                check_non_empty_partition(
                    df,
                    run_id=resolved_run_id,
                    batch_date=iso_date,
                    layer="bronze",
                    table_name=table,
                    allow_weekends_empty=False,
                    severity="ERROR",
                )
            )

    # Weekday-only fx_rates (empty on Saturday/Sunday is valid).
    fx_path = join_path(resolved_lakehouse, "bronze", "fx_rates")
    fx_exists, fx_exists_res = _check_table_exists(
        spark,
        fx_path,
        run_id=resolved_run_id,
        batch_date=iso_date,
        layer="bronze",
        table_name="fx_rates",
    )
    results.append(fx_exists_res)
    if fx_exists:
        fx_df = _load_batch_df(spark, fx_path, iso_date, partitioned=True)
        results.append(
            check_non_empty_partition(
                fx_df,
                run_id=resolved_run_id,
                batch_date=iso_date,
                layer="bronze",
                table_name="fx_rates",
                allow_weekends_empty=True,
                severity="ERROR",
            )
        )

    # Pulse-only employee_surveys (table must exist; 0 rows outside survey waves is valid).
    srv_path = join_path(resolved_lakehouse, "bronze", "employee_surveys")
    _, srv_exists_res = _check_table_exists(
        spark,
        srv_path,
        run_id=resolved_run_id,
        batch_date=iso_date,
        layer="bronze",
        table_name="employee_surveys",
    )
    results.append(srv_exists_res)

    write_audit_results(spark, results, audit_path)
    enforce_dq_gate(results)
    return results


def _validate_silver_spec(
    spark: SparkSession,
    spec: dict,
    *,
    resolved_lakehouse: str,
    iso_date: str,
    resolved_run_id: str,
    mandatory: bool,
) -> list[DQCheckResult]:
    table = spec["table"]
    path = join_path(resolved_lakehouse, "silver", table)
    exists, exists_res = _check_table_exists(
        spark,
        path,
        run_id=resolved_run_id,
        batch_date=iso_date,
        layer="silver",
        table_name=table,
        severity="ERROR" if mandatory else "WARN",
    )
    table_results: list[DQCheckResult] = [exists_res]
    if not exists:
        return table_results

    df = _load_batch_df(spark, path, iso_date, partitioned=spec.get("partitioned", False))

    if not spec.get("allow_empty", False):
        table_results.append(
            check_non_empty_partition(
                df,
                run_id=resolved_run_id,
                batch_date=iso_date,
                layer="silver",
                table_name=table,
                severity="ERROR",
            )
        )

    pk_cols: Sequence[str] = spec["pk"]
    for pk_col in pk_cols:
        table_results.append(
            check_not_null(
                df,
                pk_col,
                run_id=resolved_run_id,
                batch_date=iso_date,
                layer="silver",
                table_name=table,
                severity="ERROR",
            )
        )

    table_results.append(
        check_unique_key(
            df,
            pk_cols,
            run_id=resolved_run_id,
            batch_date=iso_date,
            layer="silver",
            table_name=table,
            severity="ERROR",
        )
    )

    for col_name in spec.get("not_null_cols", ()):
        table_results.append(
            check_not_null(
                df,
                col_name,
                run_id=resolved_run_id,
                batch_date=iso_date,
                layer="silver",
                table_name=table,
                severity="ERROR",
            )
        )

    if spec.get("scd2"):
        table_results.append(
            check_scd2_no_overlaps(
                df,
                run_id=resolved_run_id,
                batch_date=iso_date,
                layer="silver",
                table_name=table,
                half_open=True,
                severity="ERROR",
            )
        )

    eur_col = spec.get("eur_col")
    if eur_col:
        table_results.append(
            check_zero_null_eur(
                df,
                eur_col,
                run_id=resolved_run_id,
                batch_date=iso_date,
                layer="silver",
                table_name=table,
                severity="ERROR",
            )
        )

    freshness_col = spec.get("freshness_date_col")
    if freshness_col:
        day_rows = df.filter(F.col(freshness_col) == F.to_date(F.lit(iso_date))).count()
        table_results.append(
            build_dq_result(
                run_id=resolved_run_id,
                batch_date=iso_date,
                layer="silver",
                table_name=table,
                check_name=f"gate_freshness_{freshness_col}",
                severity="ERROR",
                passed=day_rows > 0,
                failed_count=0 if day_rows > 0 else 1,
                total_count=day_rows,
            )
        )

    return table_results


def run_silver_gate(
    spark: SparkSession,
    batch_date: str,
    lakehouse_root: str | None = None,
    run_id: str | None = None,
    *,
    strict: bool = False,
) -> list[DQCheckResult]:
    """Validate Silver layer tables for `batch_date` and enforce the Silver DQ gate."""
    iso_date = parse_batch_date(batch_date).isoformat()
    resolved_lakehouse = get_lakehouse_root(lakehouse_root)
    resolved_run_id = run_id or f"gate-silver-{iso_date}-{uuid.uuid4().hex[:8]}"
    audit_path = join_path(resolved_lakehouse, "dq", "dq_audit_results")

    results: list[DQCheckResult] = []
    for spec in SILVER_MANDATORY_SPECS:
        results.extend(
            _validate_silver_spec(
                spark,
                spec,
                resolved_lakehouse=resolved_lakehouse,
                iso_date=iso_date,
                resolved_run_id=resolved_run_id,
                mandatory=True,
            )
        )

    for spec in SILVER_OPTIONAL_SPECS:
        results.extend(
            _validate_silver_spec(
                spark,
                spec,
                resolved_lakehouse=resolved_lakehouse,
                iso_date=iso_date,
                resolved_run_id=resolved_run_id,
                mandatory=strict,
            )
        )

    write_audit_results(spark, results, audit_path)
    enforce_dq_gate(results)
    return results


def run_gold_gate(
    spark: SparkSession,
    batch_date: str,
    lakehouse_root: str | None = None,
    run_id: str | None = None,
    *,
    strict: bool = False,
) -> list[DQCheckResult]:
    """Validate Gold data marts for `batch_date` and enforce the Gold DQ gate.

    When `strict=False` (default while Gold marts are being built incrementally),
    missing Gold tables emit `WARN` on `gate_table_exists` while any existing Gold
    mart is validated with hard `ERROR` checks on grain PKs and non-empty rows.
    """
    iso_date = parse_batch_date(batch_date).isoformat()
    resolved_lakehouse = get_lakehouse_root(lakehouse_root)
    resolved_run_id = run_id or f"gate-gold-{iso_date}-{uuid.uuid4().hex[:8]}"
    audit_path = join_path(resolved_lakehouse, "dq", "dq_audit_results")

    results: list[DQCheckResult] = []
    for spec in GOLD_SPECS:
        table = spec["table"]
        path = join_path(resolved_lakehouse, "gold", table)
        exists, exists_res = _check_table_exists(
            spark,
            path,
            run_id=resolved_run_id,
            batch_date=iso_date,
            layer="gold",
            table_name=table,
            severity="ERROR" if strict else "WARN",
        )
        results.append(exists_res)
        if not exists:
            continue

        df = spark.read.format("delta").load(path)
        results.append(
            check_non_empty_partition(
                df,
                run_id=resolved_run_id,
                batch_date=iso_date,
                layer="gold",
                table_name=table,
                severity="ERROR",
            )
        )
        pk_cols: Sequence[str] = spec["pk"]
        for pk_col in pk_cols:
            results.append(
                check_not_null(
                    df,
                    pk_col,
                    run_id=resolved_run_id,
                    batch_date=iso_date,
                    layer="gold",
                    table_name=table,
                    severity="ERROR",
                )
            )
        results.append(
            check_unique_key(
                df,
                pk_cols,
                run_id=resolved_run_id,
                batch_date=iso_date,
                layer="gold",
                table_name=table,
                severity="ERROR",
            )
        )

    write_audit_results(spark, results, audit_path)
    enforce_dq_gate(results)
    return results


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run centralized Medallion DQ gate.")
    parser.add_argument(
        "--layer",
        required=True,
        choices=["bronze", "silver", "gold"],
        help="Medallion layer to validate.",
    )
    parser.add_argument("--batch-date", required=True, help="Logical batch date (YYYY-MM-DD).")
    parser.add_argument("--run-id", default=None, help="Orchestrator run identifier.")
    parser.add_argument("--lakehouse-root", default=None, help="Override Lakehouse root path.")
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Require all pending/optional Silver and Gold tables to exist.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = get_spark_session(f"dq-gate-{args.layer}")
    try:
        if args.layer == "bronze":
            results = run_bronze_gate(
                spark,
                batch_date=args.batch_date,
                lakehouse_root=args.lakehouse_root,
                run_id=args.run_id,
            )
        elif args.layer == "silver":
            results = run_silver_gate(
                spark,
                batch_date=args.batch_date,
                lakehouse_root=args.lakehouse_root,
                run_id=args.run_id,
                strict=args.strict,
            )
        else:
            results = run_gold_gate(
                spark,
                batch_date=args.batch_date,
                lakehouse_root=args.lakehouse_root,
                run_id=args.run_id,
                strict=args.strict,
            )
        print(
            f"DQ {args.layer} gate complete for batch_date={args.batch_date}: "
            f"{len(results)} checks recorded."
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
