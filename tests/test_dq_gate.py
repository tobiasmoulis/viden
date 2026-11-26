"""Unit tests for centralized DQ gates (`dq.run_gate`) and audit scoping."""

from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from dq.framework import DQCheckResult, DQGateFailedError, write_audit_results
from dq.run_gate import run_bronze_gate, run_gold_gate, run_silver_gate
from spark_jobs.common.io import add_bronze_metadata, write_delta_overwrite, write_delta_partition
from spark_jobs.common.spark_session import join_path


def test_write_audit_results_preserves_distinct_check_names(spark, tmp_path: Path) -> None:
    """Central `run_gate.py` must not wipe checks written earlier by individual jobs."""
    audit_path = str(tmp_path / "dq" / "dq_audit_results")
    now = datetime.now(timezone.utc)

    job_check = DQCheckResult(
        run_id="job-run-1",
        batch_date=date(2026, 1, 1),
        layer="silver",
        table_name="silver_invoices",
        check_name="orphan_invoice_rate",
        severity="WARN",
        status="PASS",
        failed_count=1,
        total_count=100,
        checked_at=now,
    )
    gate_check_1 = DQCheckResult(
        run_id="gate-run-1",
        batch_date=date(2026, 1, 1),
        layer="silver",
        table_name="silver_invoices",
        check_name="gate_table_exists",
        severity="ERROR",
        status="PASS",
        failed_count=0,
        total_count=1,
        checked_at=now,
    )
    gate_check_2 = DQCheckResult(
        run_id="gate-run-2",
        batch_date=date(2026, 1, 1),
        layer="silver",
        table_name="silver_invoices",
        check_name="gate_table_exists",
        severity="ERROR",
        status="PASS",
        failed_count=0,
        total_count=1,
        checked_at=now,
    )

    write_audit_results(spark, [job_check], audit_path)
    write_audit_results(spark, [gate_check_1], audit_path)
    write_audit_results(spark, [gate_check_2], audit_path)

    audit_df = spark.read.format("delta").load(audit_path)
    rows = {(r["check_name"], r["run_id"]) for r in audit_df.collect()}
    assert rows == {
        ("orphan_invoice_rate", "job-run-1"),
        ("gate_table_exists", "gate-run-2"),
    }


def test_run_bronze_gate_missing_table_and_weekend_fx(spark, tmp_path: Path) -> None:
    lh = str(tmp_path / "lakehouse")

    # Empty lakehouse must fail gracefully with DQGateFailedError (not AnalysisException).
    with pytest.raises(DQGateFailedError, match="gate_table_exists"):
        run_bronze_gate(spark, "2026-01-03", lakehouse_root=lh, run_id="b-fail")

    # Populate all 10 Bronze tables for Saturday 2026-01-03 (empty fx_rates & surveys allowed).
    sample_df = spark.createDataFrame([("1",)], ["id"])
    for tbl in ("clients", "projects", "survey_questions"):
        write_delta_overwrite(
            add_bronze_metadata(sample_df, "2026-01-03"),
            join_path(lh, "bronze", tbl),
        )
    for tbl in ("employees", "timesheets", "invoices", "expenses", "crm_events"):
        write_delta_partition(
            spark,
            add_bronze_metadata(sample_df, "2026-01-03"),
            join_path(lh, "bronze", tbl),
            "2026-01-03",
        )
    empty_df = spark.createDataFrame([], sample_df.schema)
    for tbl in ("fx_rates", "employee_surveys"):
        write_delta_partition(
            spark,
            add_bronze_metadata(empty_df, "2026-01-03"),
            join_path(lh, "bronze", tbl),
            "2026-01-03",
        )

    results = run_bronze_gate(spark, "2026-01-03", lakehouse_root=lh, run_id="b-pass")
    assert all(r.status == "PASS" for r in results)


def test_run_gold_gate_non_strict_vs_strict_and_valid_mart(spark, tmp_path: Path) -> None:
    lh = str(tmp_path / "lakehouse")

    # Non-strict mode warns on unbuilt Gold tables instead of failing.
    warn_results = run_gold_gate(spark, "2026-01-01", lakehouse_root=lh, run_id="g-warn")
    assert len(warn_results) == 5
    assert all(r.status == "WARN" for r in warn_results)

    # Strict mode fails when Gold tables are missing.
    with pytest.raises(DQGateFailedError, match="gate_table_exists"):
        run_gold_gate(spark, "2026-01-01", lakehouse_root=lh, run_id="g-strict", strict=True)

    # Populating one Gold mart triggers hard PK / uniqueness / non-empty checks on it.
    gold_df = spark.createDataFrame(
        [("PRJ_1", "2026-01", 1000.0)], ["project_id", "year_month", "net_revenue_eur"]
    )
    write_delta_overwrite(gold_df, join_path(lh, "gold", "gold_project_profitability"))
    mixed_results = run_gold_gate(spark, "2026-01-01", lakehouse_root=lh, run_id="g-mixed")
    prof_statuses = {
        r.check_name: r.status
        for r in mixed_results
        if r.table_name == "gold_project_profitability"
    }
    assert prof_statuses["gate_table_exists"] == "PASS"
    assert prof_statuses["unique_key_project_id_year_month"] == "PASS"
    assert prof_statuses["non_empty_partition"] == "PASS"


def test_run_silver_gate_fails_on_missing_mandatory_table(spark, tmp_path: Path) -> None:
    lh = str(tmp_path / "lakehouse")
    with pytest.raises(DQGateFailedError, match="silver_clients.gate_table_exists"):
        run_silver_gate(spark, "2026-01-01", lakehouse_root=lh, run_id="s-fail")
