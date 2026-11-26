"""Reusable Data Quality checks for Bronze, Silver, and Gold layers.

Implements the hard (`severity="ERROR"`) and soft (`severity="WARN"`) checks
specified in `docs/STUDENT_ASSIGNMENT.md` (Section 4.3) and `docs/SANITY_CHECKS.md`.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from dq.framework import DQCheckResult, _parse_batch_date, build_dq_result


def check_unique_key(
    df: DataFrame,
    key_columns: Sequence[str],
    *,
    run_id: str,
    batch_date: str | date,
    layer: str,
    table_name: str,
    severity: str = "ERROR",
) -> DQCheckResult:
    """Verify uniqueness of primary or composite keys (e.g. `(emp_id, valid_from)`)."""
    keys = list(key_columns)
    if not keys:
        raise ValueError("key_columns must not be empty")

    total_count = df.count()
    duplicate_rows = (
        df.groupBy(*keys)
        .agg(F.count(F.lit(1)).alias("_cnt"))
        .filter(F.col("_cnt") > 1)
        .agg(F.coalesce(F.sum(F.col("_cnt") - F.lit(1)), F.lit(0)).alias("extra_rows"))
        .first()["extra_rows"]
    )
    failed_count = int(duplicate_rows)
    return build_dq_result(
        run_id=run_id,
        batch_date=batch_date,
        layer=layer,
        table_name=table_name,
        check_name=f"unique_key_{'_'.join(keys)}",
        severity=severity,
        passed=failed_count == 0,
        failed_count=failed_count,
        total_count=total_count,
    )


def check_non_empty_partition(
    df: DataFrame,
    *,
    run_id: str,
    batch_date: str | date,
    layer: str,
    table_name: str,
    allow_weekends_empty: bool = False,
    severity: str = "ERROR",
) -> DQCheckResult:
    """Verify mandatory partitions contain >0 rows (accounting for weekend `fx_rates`)."""
    parsed_date = _parse_batch_date(batch_date)
    is_weekend = parsed_date.weekday() >= 5
    total_count = df.count()
    passed = total_count > 0 or (allow_weekends_empty and is_weekend)
    return build_dq_result(
        run_id=run_id,
        batch_date=parsed_date,
        layer=layer,
        table_name=table_name,
        check_name="non_empty_partition",
        severity=severity,
        passed=passed,
        failed_count=0 if passed else 1,
        total_count=total_count,
    )


def check_scd2_no_overlaps(
    df: DataFrame,
    *,
    run_id: str,
    batch_date: str | date,
    layer: str = "silver",
    table_name: str = "silver_employees_scd2",
    half_open: bool = True,
    severity: str = "ERROR",
) -> DQCheckResult:
    """Verify no overlapping or inverted validity intervals per `emp_id`.

    Default convention (per `docs/architecture.md`): half-open `[valid_from, valid_to)`
    with sentinel `9999-12-31` for `is_current = true`.
    """
    total_count = df.count()
    order_window = Window.partitionBy("emp_id").orderBy(
        F.col("valid_from").asc(),
        F.col("valid_to").asc_nulls_last(),
    )
    with_next = df.withColumn("_next_valid_from", F.lead("valid_from").over(order_window))

    if half_open:
        invalid_bounds = F.col("valid_from").isNull() | (
            F.col("valid_to").isNotNull() & (F.col("valid_from") >= F.col("valid_to"))
        )
        overlap = F.col("_next_valid_from").isNotNull() & (
            F.col("valid_to").isNull() | (F.col("_next_valid_from") < F.col("valid_to"))
        )
    else:
        invalid_bounds = F.col("valid_from").isNull() | (
            F.col("valid_to").isNotNull() & (F.col("valid_from") > F.col("valid_to"))
        )
        overlap = F.col("_next_valid_from").isNotNull() & (
            F.col("valid_to").isNull() | (F.col("_next_valid_from") <= F.col("valid_to"))
        )

    failed_count = with_next.filter(invalid_bounds | overlap).count()
    return build_dq_result(
        run_id=run_id,
        batch_date=batch_date,
        layer=layer,
        table_name=table_name,
        check_name="scd2_no_overlaps",
        severity=severity,
        passed=failed_count == 0,
        failed_count=failed_count,
        total_count=total_count,
    )


def check_zero_null_eur(
    df: DataFrame,
    eur_column: str,
    *,
    run_id: str,
    batch_date: str | date,
    layer: str,
    table_name: str,
    severity: str = "ERROR",
) -> DQCheckResult:
    """Verify zero NULL EUR amounts after FX conversion (`silver_invoices`, `silver_expenses`)."""
    total_count = df.count()
    failed_count = df.filter(F.col(eur_column).isNull()).count()
    return build_dq_result(
        run_id=run_id,
        batch_date=batch_date,
        layer=layer,
        table_name=table_name,
        check_name=f"zero_null_eur_{eur_column}",
        severity=severity,
        passed=failed_count == 0,
        failed_count=failed_count,
        total_count=total_count,
    )


def check_quarantine_ratio(
    valid_count: int,
    quarantined_count: int,
    *,
    max_ratio: float = 0.08,
    run_id: str,
    batch_date: str | date,
    layer: str = "silver",
    table_name: str,
    severity: str = "WARN",
) -> DQCheckResult:
    """Emit WARN when `quarantined_count / (valid_count + quarantined_count) > max_ratio`.

    Records `failed_count = quarantined_count` for audit visibility even when below threshold.
    """
    total_count = int(valid_count) + int(quarantined_count)
    ratio = (quarantined_count / total_count) if total_count > 0 else 0.0
    passed = ratio <= max_ratio
    return build_dq_result(
        run_id=run_id,
        batch_date=batch_date,
        layer=layer,
        table_name=table_name,
        check_name="quarantine_ratio",
        severity=severity,
        passed=passed,
        failed_count=int(quarantined_count),
        total_count=total_count,
    )


def check_ratio_threshold(
    failed_count: int,
    total_count: int,
    *,
    check_name: str,
    max_ratio: float,
    run_id: str,
    batch_date: str | date,
    layer: str,
    table_name: str,
    severity: str = "WARN",
) -> DQCheckResult:
    """Generic soft/hard ratio threshold check (e.g. orphan rate, duplicate rate)."""
    ratio = (failed_count / total_count) if total_count > 0 else 0.0
    passed = ratio <= max_ratio
    return build_dq_result(
        run_id=run_id,
        batch_date=batch_date,
        layer=layer,
        table_name=table_name,
        check_name=check_name,
        severity=severity,
        passed=passed,
        failed_count=int(failed_count),
        total_count=int(total_count),
    )
