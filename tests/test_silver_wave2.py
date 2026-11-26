"""Unit tests for Silver Wave 2 fact jobs: timesheets, invoices, and expenses."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest
from pyspark.sql.types import DateType, DoubleType, StringType, StructField, StructType

from spark_jobs.bronze.ingest_finance import ingest_expenses, ingest_fx_rates, ingest_invoices
from spark_jobs.bronze.ingest_hr import ingest_employees, ingest_timesheets
from spark_jobs.bronze.ingest_reference import ingest_projects
from spark_jobs.silver.transform_employees_scd2 import transform_employees_scd2
from spark_jobs.silver.transform_fx_rates import transform_fx_rates
from spark_jobs.silver.transform_invoices_expenses import (
    SILVER_EXPENSES_COLUMNS,
    SILVER_INVOICES_COLUMNS,
    transform_expenses,
    transform_invoices,
)
from spark_jobs.silver.transform_projects import transform_projects
from spark_jobs.silver.transform_timesheets import (
    SILVER_TIMESHEETS_COLUMNS,
    transform_timesheets,
)


def _write_parquet_partition(spark, landing: Path, ds: str, rows: list[tuple]) -> None:
    ts_schema = StructType(
        [
            StructField("ts_id", StringType(), True),
            StructField("emp_id", StringType(), True),
            StructField("project_id", StringType(), True),
            StructField("work_date", DateType(), True),
            StructField("hours", DoubleType(), True),
        ]
    )
    ts_dir = landing / "timesheets" / f"date={ds}"
    df = spark.createDataFrame(rows, schema=ts_schema)
    df.coalesce(1).write.mode("overwrite").parquet(str(ts_dir))
    parquet_file = next(ts_dir.glob("*.parquet"))
    parquet_file.rename(ts_dir / "part-000.parquet")


def test_silver_timesheets_dedup_quarantine_zombie_and_idempotency(spark, tmp_path: Path) -> None:
    landing = tmp_path / "landing"
    lakehouse = tmp_path / "lakehouse"

    # Seed employees SCD2: EMP_ACTIVE active, EMP_LEFT terminated on 2026-01-02
    emp_dir = landing / "employees" / "date=2026-01-02"
    emp_dir.mkdir(parents=True)
    header = (
        "emp_id,full_name,department,role,seniority_level,hourly_rate_eur,hire_date,"
        "last_promotion_date,work_mode,distance_km,manager_id,is_active,termination_date,"
        "churn_type,snapshot_date\n"
    )
    row_active = (
        "EMP_ACTIVE,Alice,Engineering,Engineer,Senior,70.0,2023-01-01,2024-01-01,"
        "Hybrid,5.0,EMP_9,true,,,2026-01-02\n"
    )
    row_left = (
        "EMP_LEFT,Bob,Sales,AE,Mid,50.0,2023-01-01,2024-01-01,"
        "Remote,0.0,EMP_9,false,2026-01-02,voluntary,2026-01-02\n"
    )
    (emp_dir / "part-000.csv").write_text(header + row_active + row_left, encoding="utf-8")
    ingest_employees(spark, "2026-01-02", str(landing), str(lakehouse))
    transform_employees_scd2(spark, "2026-01-02", str(lakehouse), run_id="emp-1")

    # Timesheets batch with near-duplicate, NULL project, hours<=0, hours>24, and zombie entry
    _write_parquet_partition(
        spark,
        landing,
        "2026-01-02",
        [
            ("TS-1", "EMP_ACTIVE", "PRJ_1", date(2026, 1, 2), 6.0),
            ("TS-1", "EMP_ACTIVE", "PRJ_1", date(2026, 1, 2), 4.0),  # near-dup -> keep 4.0
            ("TS-2", "EMP_ACTIVE", None, date(2026, 1, 2), 8.0),  # NULL project_id
            ("TS-3", "EMP_ACTIVE", "PRJ_1", date(2026, 1, 2), 0.0),  # hours <= 0
            ("TS-4", "EMP_ACTIVE", "PRJ_1", date(2026, 1, 2), 25.0),  # hours > 24
            ("TS-5", "EMP_LEFT", "PRJ_1", date(2026, 1, 2), 5.0),  # zombie (work_date >= term)
        ],
    )
    ingest_timesheets(spark, "2026-01-02", str(landing), str(lakehouse))

    # Run twice to verify idempotency
    transform_timesheets(spark, "2026-01-02", str(lakehouse), run_id="ts-1")
    valid_df, q_df = transform_timesheets(spark, "2026-01-02", str(lakehouse), run_id="ts-2")

    assert tuple(valid_df.columns) == SILVER_TIMESHEETS_COLUMNS
    valid_rows = [(r["ts_id"], r["hours"]) for r in valid_df.collect()]
    assert valid_rows == [("TS-1", 4.0)]

    q_by_id = {r["ts_id"]: list(r["dq_reasons"]) for r in q_df.collect()}
    assert q_by_id == {
        "TS-2": ["missing_project_id"],
        "TS-3": ["non_positive_hours"],
        "TS-4": ["hours_exceeds_24"],
        "TS-5": ["post_termination_work_date"],
    }


def test_silver_invoices_and_expenses_dates_orphans_credit_notes_and_fx(
    spark, tmp_path: Path
) -> None:
    landing = tmp_path / "landing"
    lakehouse = tmp_path / "lakehouse"

    # Seed silver_projects
    (landing / "projects").mkdir(parents=True)
    (landing / "projects" / "projects.csv").write_text(
        "project_id,client_id,budget_eur,status\nPRJ_0001,CLI_0002,50000.0,Active\n",
        encoding="utf-8",
    )
    ingest_projects(spark, "2026-01-02", str(landing), str(lakehouse))
    transform_projects(spark, "2026-01-02", str(lakehouse), run_id="p-1")

    # Seed silver_fx_rates_daily (Friday 2026-01-02 forward-filled to Saturday 2026-01-03)
    fx_dir = landing / "fx_rates" / "date=2026-01-02"
    fx_dir.mkdir(parents=True)
    (fx_dir / "part-000.json").write_text(
        json.dumps(
            [
                {
                    "rate_date": "2026-01-02",
                    "currency_from": "USD",
                    "currency_to": "EUR",
                    "rate": 0.90,
                },
                {
                    "rate_date": "2026-01-02",
                    "currency_from": "EUR",
                    "currency_to": "USD",
                    "rate": 1.11,
                },
                {
                    "rate_date": "2026-01-02",
                    "currency_from": "CZK",
                    "currency_to": "EUR",
                    "rate": 0.04,
                },
                {
                    "rate_date": "2026-01-02",
                    "currency_from": "EUR",
                    "currency_to": "CZK",
                    "rate": 25.0,
                },
                {
                    "rate_date": "2026-01-02",
                    "currency_from": "USD",
                    "currency_to": "CZK",
                    "rate": 22.5,
                },
                {
                    "rate_date": "2026-01-02",
                    "currency_from": "CZK",
                    "currency_to": "USD",
                    "rate": 0.044,
                },
            ]
        ),
        encoding="utf-8",
    )
    ingest_fx_rates(spark, "2026-01-02", str(landing), str(lakehouse))
    ingest_fx_rates(spark, "2026-01-03", str(landing), str(lakehouse))
    transform_fx_rates(spark, "2026-01-03", str(lakehouse), run_id="fx-1")

    # Seed invoices on Saturday 2026-01-03 (tests weekend LOCF FX, EU date, credit note, orphan)
    inv_dir = landing / "invoices" / "date=2026-01-03"
    inv_dir.mkdir(parents=True)
    inv_lines = [
        {
            "inv_id": "INV-1",
            "client_id": "CLI_0002",
            "project_id": "PRJ_0001",
            "amount": 1000.0,
            "currency": "USD",
            "issued_at": "03.01.2026",
            "paid_at": "10.01.2026",
        },
        {
            "inv_id": "INV-2",
            "client_id": "CLI_0002",
            "project_id": "PRJ_0001",
            "amount": -250.0,
            "currency": "EUR",
            "issued_at": "2026-01-03",
            "paid_at": None,
        },
        {
            "inv_id": "INV-ORPHAN",
            "client_id": "CLI_0002",
            "project_id": "PRJ_ORPHAN_999",
            "amount": 500.0,
            "currency": "EUR",
            "issued_at": "2026-01-03",
            "paid_at": None,
        },
    ]
    (inv_dir / "part-000.jsonl").write_text(
        "\n".join(json.dumps(r) for r in inv_lines) + "\n", encoding="utf-8"
    )
    ingest_invoices(spark, "2026-01-03", str(landing), str(lakehouse))

    # Seed expenses with near-duplicate exp_id & CZK conversion on weekend
    exp_dir = landing / "expenses" / "date=2026-01-03"
    exp_dir.mkdir(parents=True)
    (exp_dir / "part-000.csv").write_text(
        "exp_id,department,category,amount,currency,expense_date\n"
        "EXP-1,Engineering,Cloud Infrastructure,1000.0,CZK,2026-01-03\n"
        "EXP-1,Engineering,Cloud Infrastructure,1200.0,CZK,2026-01-03\n"
        "EXP-2,Finance,Training,200.0,EUR,2026-01-03\n",
        encoding="utf-8",
    )
    ingest_expenses(spark, "2026-01-03", str(landing), str(lakehouse))

    # Run twice for idempotency
    transform_invoices(spark, "2026-01-03", str(lakehouse), run_id="inv-1")
    inv_df, q_inv = transform_invoices(spark, "2026-01-03", str(lakehouse), run_id="inv-2")

    assert tuple(inv_df.columns) == SILVER_INVOICES_COLUMNS
    inv_map = {r["inv_id"]: r for r in inv_df.collect()}
    assert set(inv_map.keys()) == {"INV-1", "INV-2"}
    assert inv_map["INV-1"]["issued_date"] == date(2026, 1, 3)
    assert inv_map["INV-1"]["paid_date"] == date(2026, 1, 10)
    assert inv_map["INV-1"]["is_credit_note"] is False
    assert inv_map["INV-1"]["amount_eur"] == pytest.approx(900.0)
    assert inv_map["INV-2"]["is_credit_note"] is True
    assert inv_map["INV-2"]["paid_date"] is None
    assert inv_map["INV-2"]["net_amount_eur"] == pytest.approx(-250.0)

    assert q_inv.count() == 1
    assert list(q_inv.first()["dq_reasons"]) == ["orphan_project_id"]

    transform_expenses(spark, "2026-01-03", str(lakehouse), run_id="exp-1")
    exp_df, q_exp = transform_expenses(spark, "2026-01-03", str(lakehouse), run_id="exp-2")

    assert tuple(exp_df.columns) == SILVER_EXPENSES_COLUMNS
    assert q_exp.count() == 0
    exp_map = {r["exp_id"]: r["amount_eur"] for r in exp_df.collect()}
    assert exp_map["EXP-1"] == pytest.approx(40.0)  # lowest near-dup 1000 * 0.04
    assert exp_map["EXP-2"] == pytest.approx(200.0)
