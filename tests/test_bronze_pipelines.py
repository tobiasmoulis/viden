"""Unit tests for Phase 1 Bronze ingestion jobs across all domains."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest
from pyspark.sql.types import DateType, DoubleType, StringType, StructField, StructType

from spark_jobs.bronze.ingest_crm_surveys import run as run_crm_surveys
from spark_jobs.bronze.ingest_finance import ingest_fx_rates
from spark_jobs.bronze.ingest_finance import run as run_finance
from spark_jobs.bronze.ingest_hr import run as run_hr
from spark_jobs.bronze.ingest_reference import run as run_reference
from spark_jobs.common.spark_session import join_path


def _assert_bronze_metadata(df, expected_dates: set[date]) -> None:
    assert {"_batch_date", "_ingested_at", "_source_file"}.issubset(set(df.columns))
    assert (
        df.filter("_batch_date IS NULL OR _ingested_at IS NULL OR _source_file IS NULL").count()
        == 0
    )
    actual_dates = {r["_batch_date"] for r in df.select("_batch_date").distinct().collect()}
    assert actual_dates == expected_dates


def test_bronze_reference_projects_and_survey_questions(spark, tmp_path: Path) -> None:
    landing = tmp_path / "landing"
    lakehouse = tmp_path / "lakehouse"

    (landing / "projects").mkdir(parents=True)
    (landing / "projects" / "projects.csv").write_text(
        "project_id,client_id,budget_eur,status\n"
        "PRJ_0001,CLI_0009,12200.0,Active\n"
        "PRJ_0002,CLI_0064,105900.0,Completed\n",
        encoding="utf-8",
    )
    (landing / "survey_questions").mkdir(parents=True)
    (landing / "survey_questions" / "survey_questions.json").write_text(
        json.dumps(
            [
                {
                    "question_id": "Q01",
                    "dimension": "enps",
                    "scale_min": 0,
                    "scale_max": 10,
                    "prompt_text": "Recommend?",
                },
                {
                    "question_id": "Q02",
                    "dimension": "work_life_balance",
                    "scale_min": 1,
                    "scale_max": 5,
                    "prompt_text": "Balance?",
                },
            ]
        ),
        encoding="utf-8",
    )

    # Run twice to verify full-load idempotency.
    run_reference(spark, "2026-01-01", str(landing), str(lakehouse))
    run_reference(spark, "2026-01-01", str(landing), str(lakehouse))

    proj_df = spark.read.format("delta").load(join_path(str(lakehouse), "bronze", "projects"))
    q_df = spark.read.format("delta").load(join_path(str(lakehouse), "bronze", "survey_questions"))
    assert proj_df.count() == 2
    assert q_df.count() == 2
    _assert_bronze_metadata(proj_df, {date(2026, 1, 1)})
    _assert_bronze_metadata(q_df, {date(2026, 1, 1)})


def test_bronze_hr_employees_and_timesheets_idempotent(spark, tmp_path: Path) -> None:
    landing = tmp_path / "landing"
    lakehouse = tmp_path / "lakehouse"

    emp_header = (
        "emp_id,full_name,department,role,seniority_level,hourly_rate_eur,hire_date,"
        "last_promotion_date,work_mode,distance_km,manager_id,is_active,termination_date,"
        "churn_type,snapshot_date\n"
    )
    for ds, rate in [("2026-01-01", "60.0"), ("2026-01-02", "65.0")]:
        emp_dir = landing / "employees" / f"date={ds}"
        emp_dir.mkdir(parents=True)
        (emp_dir / "part-000.csv").write_text(
            emp_header
            + f"EMP_0001,Jane Doe,Engineering,Engineer,Senior,{rate},2023-01-01,"
            + f"2024-01-01,Hybrid,10.0,EMP_0099,true,,,{ds}\n",
            encoding="utf-8",
        )

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
        ts_df = spark.createDataFrame(
            [(f"TS-{ds}-1", "EMP_0001", "PRJ_0001", date.fromisoformat(ds), 8.0)],
            schema=ts_schema,
        )
        ts_df.coalesce(1).write.mode("overwrite").parquet(str(ts_dir))
        parquet_file = next(ts_dir.glob("*.parquet"))
        parquet_file.rename(ts_dir / "part-000.parquet")

    run_hr(spark, "2026-01-01", str(landing), str(lakehouse))
    run_hr(spark, "2026-01-02", str(landing), str(lakehouse))
    # Re-run day 1 to confirm no duplicate rows and day 2 stays intact.
    run_hr(spark, "2026-01-01", str(landing), str(lakehouse))

    emp_bronze = spark.read.format("delta").load(join_path(str(lakehouse), "bronze", "employees"))
    ts_bronze = spark.read.format("delta").load(join_path(str(lakehouse), "bronze", "timesheets"))

    assert emp_bronze.count() == 2
    assert ts_bronze.count() == 2
    _assert_bronze_metadata(emp_bronze, {date(2026, 1, 1), date(2026, 1, 2)})
    _assert_bronze_metadata(ts_bronze, {date(2026, 1, 1), date(2026, 1, 2)})


def test_bronze_finance_raw_dates_and_weekend_fx(spark, tmp_path: Path) -> None:
    landing = tmp_path / "landing"
    lakehouse = tmp_path / "lakehouse"

    # 2026-01-02 = Friday (has fx_rates), 2026-01-03 = Saturday (no fx_rates file)
    fx_fri = landing / "fx_rates" / "date=2026-01-02"
    fx_fri.mkdir(parents=True)
    (fx_fri / "part-000.json").write_text(
        json.dumps(
            [
                {
                    "rate_date": "2026-01-02",
                    "currency_from": "USD",
                    "currency_to": "EUR",
                    "rate": 0.92,
                }
            ]
        ),
        encoding="utf-8",
    )

    for ds, issued in [("2026-01-02", "02.01.2026"), ("2026-01-03", "2026-01-03")]:
        inv_dir = landing / "invoices" / f"date={ds}"
        inv_dir.mkdir(parents=True)
        (inv_dir / "part-000.jsonl").write_text(
            json.dumps(
                {
                    "inv_id": f"INV-{ds}",
                    "client_id": "CLI_0002",
                    "project_id": "PRJ_0001",
                    "amount": -150.0,
                    "currency": "EUR",
                    "issued_at": issued,
                    "paid_at": None,
                }
            )
            + "\n",
            encoding="utf-8",
        )

        exp_dir = landing / "expenses" / f"date={ds}"
        exp_dir.mkdir(parents=True)
        (exp_dir / "part-000.csv").write_text(
            "exp_id,department,category,amount,currency,expense_date\n"
            f"EXP-{ds},Engineering,Training,500.0,USD,{ds}\n",
            encoding="utf-8",
        )

    run_finance(spark, "2026-01-02", str(landing), str(lakehouse))
    run_finance(spark, "2026-01-03", str(landing), str(lakehouse))
    run_finance(spark, "2026-01-02", str(landing), str(lakehouse))

    fx_df = spark.read.format("delta").load(join_path(str(lakehouse), "bronze", "fx_rates"))
    inv_df = spark.read.format("delta").load(join_path(str(lakehouse), "bronze", "invoices"))
    exp_df = spark.read.format("delta").load(join_path(str(lakehouse), "bronze", "expenses"))

    assert fx_df.count() == 1
    assert inv_df.count() == 2
    assert exp_df.count() == 2

    # Raw European date string must remain untouched in Bronze.
    raw_issued = {r["issued_at"] for r in inv_df.select("issued_at").collect()}
    assert raw_issued == {"02.01.2026", "2026-01-03"}

    # Missing weekday FX file (Monday 2026-01-05) must raise FileNotFoundError.
    with pytest.raises(FileNotFoundError):
        ingest_fx_rates(spark, "2026-01-05", str(landing), str(lakehouse))


def test_bronze_crm_polymorphic_payload_and_pulse_surveys(spark, tmp_path: Path) -> None:
    landing = tmp_path / "landing"
    lakehouse = tmp_path / "lakehouse"

    # Day 1 (2026-01-01): no employee_surveys drop; Day 9 (2026-01-09): has survey drop.
    for ds in ["2026-01-01", "2026-01-09"]:
        crm_dir = landing / "crm_events" / f"date={ds}"
        crm_dir.mkdir(parents=True)
        lines = [
            json.dumps(
                {
                    "event_id": f"EVT-{ds}-1",
                    "client_id": "VIP_CORP_001",
                    "event_type": "api_call",
                    "timestamp": f"{ds}T00:01:00Z",
                    "payload": {"endpoint": "/v1/metrics", "latency_ms": 123},
                }
            ),
            json.dumps(
                {
                    "event_id": f"EVT-{ds}-2",
                    "client_id": "CLI_0002",
                    "event_type": "support_ticket",
                    "timestamp": f"{ds}T00:02:00Z",
                    "payload": {"ticket_id": "TCK-1", "severity": "critical"},
                }
            ),
        ]
        (crm_dir / "part-000.jsonl").write_text("\n".join(lines) + "\n", encoding="utf-8")

    srv_dir = landing / "employee_surveys" / "date=2026-01-09"
    srv_dir.mkdir(parents=True)
    (srv_dir / "part-000.jsonl").write_text(
        json.dumps(
            {
                "survey_id": "SRV-20260109-1-0001",
                "emp_id": "EMP_0001",
                "submitted_at": "2026-01-09T08:00:00Z",
                "enps_score": 9,
                "overall_comment": "Great team!",
                "answers": [
                    {
                        "question_id": "Q01",
                        "dimension": "enps",
                        "numeric_score": 9,
                        "written_answer": "Happy here",
                    }
                ],
            }
        )
        + "\n",
        encoding="utf-8",
    )

    run_crm_surveys(spark, "2026-01-01", str(landing), str(lakehouse))
    run_crm_surveys(spark, "2026-01-09", str(landing), str(lakehouse))
    run_crm_surveys(spark, "2026-01-09", str(landing), str(lakehouse))

    crm_df = spark.read.format("delta").load(join_path(str(lakehouse), "bronze", "crm_events"))
    srv_df = spark.read.format("delta").load(
        join_path(str(lakehouse), "bronze", "employee_surveys")
    )

    assert crm_df.count() == 4
    assert srv_df.count() == 1

    # Verify polymorphic payload JSON preserved verbatim as parseable JSON string.
    payloads = {
        r["event_type"]: json.loads(r["payload"])
        for r in crm_df.filter("_batch_date = DATE '2026-01-09'").collect()
    }
    assert payloads["api_call"]["latency_ms"] == 123
    assert payloads["support_ticket"]["severity"] == "critical"
