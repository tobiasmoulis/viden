"""Lightweight Data Quality framework and `dq_audit_results` Delta logger."""

from __future__ import annotations

import logging
import os
import time
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime, timezone

from delta.tables import DeltaTable
from py4j.protocol import Py4JJavaError
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

logger = logging.getLogger(__name__)

DQ_AUDIT_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), nullable=False),
        StructField("batch_date", DateType(), nullable=False),
        StructField("layer", StringType(), nullable=False),
        StructField("table_name", StringType(), nullable=False),
        StructField("check_name", StringType(), nullable=False),
        StructField("severity", StringType(), nullable=False),
        StructField("status", StringType(), nullable=False),
        StructField("failed_count", LongType(), nullable=False),
        StructField("total_count", LongType(), nullable=False),
        StructField("checked_at", TimestampType(), nullable=False),
    ]
)


class DQGateFailedError(RuntimeError):
    """Raised when one or more hard (ERROR severity) DQ checks fail."""


@dataclass(frozen=True)
class DQCheckResult:
    """Structured result emitted by a single Data Quality check."""

    run_id: str
    batch_date: date
    layer: str
    table_name: str
    check_name: str
    severity: str
    status: str
    failed_count: int
    total_count: int
    checked_at: datetime


def _parse_batch_date(value: str | date) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(value)


def build_dq_result(
    *,
    run_id: str,
    batch_date: str | date,
    layer: str,
    table_name: str,
    check_name: str,
    severity: str,
    passed: bool,
    failed_count: int,
    total_count: int,
) -> DQCheckResult:
    """Build a standardized DQCheckResult with consistent PASS / WARN / FAIL status."""
    sev = severity.upper()
    if passed:
        status = "PASS"
    elif sev == "WARN":
        status = "WARN"
    else:
        status = "FAIL"

    return DQCheckResult(
        run_id=run_id,
        batch_date=_parse_batch_date(batch_date),
        layer=layer,
        table_name=table_name,
        check_name=check_name,
        severity=sev,
        status=status,
        failed_count=int(failed_count),
        total_count=int(total_count),
        checked_at=datetime.now(timezone.utc),
    )


def check_not_null(
    df: DataFrame,
    column_name: str,
    *,
    run_id: str,
    batch_date: str | date,
    layer: str,
    table_name: str,
    severity: str = "ERROR",
) -> DQCheckResult:
    """Verify that `column_name` contains no NULL values."""
    total_count = df.count()
    failed_count = df.filter(F.col(column_name).isNull()).count()
    return build_dq_result(
        run_id=run_id,
        batch_date=batch_date,
        layer=layer,
        table_name=table_name,
        check_name=f"not_null_{column_name}",
        severity=severity,
        passed=failed_count == 0,
        failed_count=failed_count,
        total_count=total_count,
    )


def check_expected_columns(
    df: DataFrame,
    expected_columns: Sequence[str],
    *,
    run_id: str,
    batch_date: str | date,
    layer: str,
    table_name: str,
    severity: str = "ERROR",
) -> DQCheckResult:
    """Verify that all expected columns are present in the DataFrame schema."""
    actual = set(df.columns)
    missing = [col for col in expected_columns if col not in actual]
    return build_dq_result(
        run_id=run_id,
        batch_date=batch_date,
        layer=layer,
        table_name=table_name,
        check_name="expected_schema_columns",
        severity=severity,
        passed=len(missing) == 0,
        failed_count=len(missing),
        total_count=len(expected_columns),
    )


def _is_delta_concurrency_error(exc: Exception) -> bool:
    msg = str(exc)
    return any(
        token in msg
        for token in (
            "ConcurrentAppendException",
            "ConcurrentDeleteReadException",
            "ConcurrentDeleteDeleteException",
            "ConcurrentTransactionException",
            "MetadataChangedException",
            "ProtocolChangedException",
        )
    )


def write_audit_results(
    spark: SparkSession,
    results: Sequence[DQCheckResult],
    audit_table_path: str,
    *,
    max_retries: int = 5,
) -> DataFrame:
    """Persist DQCheckResult records idempotently and concurrency-safely to Delta.

    Uses atomic Delta `MERGE` keyed on `(batch_date, layer, table_name, check_name)`
    with retry/backoff on Delta optimistic concurrency conflicts so parallel Airflow
    tasks can write audit records safely at the same time without erasing each other.
    """
    if not results:
        return spark.createDataFrame([], schema=DQ_AUDIT_SCHEMA)

    # Deduplicate by audit primary key (keep latest) so MERGE never matches multiple source rows.
    deduped: dict[tuple[date, str, str, str], DQCheckResult] = {}
    for r in results:
        deduped[(r.batch_date, r.layer, r.table_name, r.check_name)] = r

    rows = [
        (
            r.run_id,
            r.batch_date,
            r.layer,
            r.table_name,
            r.check_name,
            r.severity,
            r.status,
            int(r.failed_count),
            int(r.total_count),
            r.checked_at,
        )
        for r in deduped.values()
    ]
    audit_df = spark.createDataFrame(rows, schema=DQ_AUDIT_SCHEMA)

    join_cond = (
        "target.batch_date = source.batch_date "
        "AND target.layer = source.layer "
        "AND target.table_name = source.table_name "
        "AND target.check_name = source.check_name"
    )

    for attempt in range(max_retries):
        try:
            if DeltaTable.isDeltaTable(spark, audit_table_path):
                (
                    DeltaTable.forPath(spark, audit_table_path)
                    .alias("target")
                    .merge(audit_df.alias("source"), join_cond)
                    .whenMatchedUpdateAll()
                    .whenNotMatchedInsertAll()
                    .execute()
                )
            else:
                (
                    audit_df.write.format("delta")
                    .mode("overwrite")
                    .partitionBy("batch_date")
                    .save(audit_table_path)
                )
            return audit_df
        except (Py4JJavaError, Exception) as exc:
            if attempt < max_retries - 1 and _is_delta_concurrency_error(exc):
                delay = 0.25 * (2**attempt) + (os.getpid() % 5) * 0.07
                logger.warning(
                    "Delta concurrency conflict writing dq_audit_results "
                    "(attempt %d/%d); retrying in %.2fs",
                    attempt + 1,
                    max_retries,
                    delay,
                )
                time.sleep(delay)
                continue
            raise

    return audit_df


def enforce_dq_gate(results: Sequence[DQCheckResult]) -> None:
    """Log all check outcomes and raise DQGateFailedError if any ERROR check failed."""
    hard_failures: list[DQCheckResult] = []
    for res in results:
        msg = (
            f"[DQ {res.status}] {res.layer}.{res.table_name}::{res.check_name} "
            f"(severity={res.severity}, failed={res.failed_count}/{res.total_count}, "
            f"batch_date={res.batch_date.isoformat()})"
        )
        if res.status == "FAIL":
            logger.error(msg)
            if res.severity == "ERROR":
                hard_failures.append(res)
        elif res.status == "WARN":
            logger.warning(msg)
        else:
            logger.info(msg)

    if hard_failures:
        summary = ", ".join(
            f"{r.table_name}.{r.check_name} ({r.failed_count})" for r in hard_failures
        )
        raise DQGateFailedError(f"Hard DQ gate failed: {summary}")
