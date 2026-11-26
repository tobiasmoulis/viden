"""Regression tests verifying all audit bugfixes, hardening rules, and edge cases."""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from dq.framework import DQCheckResult, DQGateFailedError, write_audit_results
from dq.run_gate import run_silver_gate
from spark_jobs.bronze.ingest_clients import ingest_clients
from spark_jobs.bronze.ingest_crm_surveys import ingest_crm_events, ingest_employee_surveys
from spark_jobs.bronze.ingest_finance import ingest_expenses, ingest_fx_rates
from spark_jobs.bronze.ingest_hr import ingest_employees
from spark_jobs.bronze.ingest_reference import ingest_survey_questions
from spark_jobs.silver.transform_clients import transform_clients
from spark_jobs.silver.transform_crm_events import transform_crm_events
from spark_jobs.silver.transform_employees_scd2 import transform_employees_scd2
from spark_jobs.silver.transform_fx_rates import transform_fx_rates
from spark_jobs.silver.transform_invoices_expenses import transform_expenses
from spark_jobs.silver.transform_surveys import (
    transform_survey_questions,
    transform_surveys,
)


def test_write_audit_results_merge_idempotency_and_dedup(spark, tmp_path: Path) -> None:
    """Atomic MERGE handles duplicate keys in input and preserves other checks cleanly."""
    audit_path = str(tmp_path / "dq" / "dq_audit_results")
    now = datetime.now(timezone.utc)
    c1 = DQCheckResult("r1", date(2026, 1, 1), "silver", "tbl", "chk_a", "ERROR", "PASS", 0, 5, now)
    c2 = DQCheckResult("r2", date(2026, 1, 1), "silver", "tbl", "chk_a", "ERROR", "PASS", 0, 6, now)
    c3 = DQCheckResult("r2", date(2026, 1, 1), "silver", "tbl", "chk_b", "WARN", "PASS", 0, 6, now)

    write_audit_results(spark, [c1, c2], audit_path)
    write_audit_results(spark, [c3], audit_path)

    rows = {
        (r["check_name"], r["run_id"], r["total_count"])
        for r in spark.read.format("delta").load(audit_path).collect()
    }
    assert rows == {("chk_a", "r2", 6), ("chk_b", "r2", 6)}


def test_scd2_and_fx_rerun_earlier_date_preserves_future_history(spark, tmp_path: Path) -> None:
    """Re-running an earlier batch_date must NOT truncate later Bronze history."""
    landing = tmp_path / "landing"
    lakehouse = tmp_path / "lakehouse"

    header = (
        "emp_id,full_name,department,role,seniority_level,hourly_rate_eur,hire_date,"
        "last_promotion_date,work_mode,distance_km,manager_id,is_active,termination_date,"
        "churn_type,snapshot_date\n"
    )
    for ds, rate in [("2026-01-01", "60.0"), ("2026-01-02", "70.0")]:
        d = landing / "employees" / f"date={ds}"
        d.mkdir(parents=True)
        (d / "part-000.csv").write_text(
            header
            + f"EMP_1,Alice,Engineering,Eng,Mid,{rate},2023-01-01,2024-01-01,"
            + f"Hybrid,5.0,EMP_9,true,,,{ds}\n",
            encoding="utf-8",
        )
        ingest_employees(spark, ds, str(landing), str(lakehouse))

        fxd = landing / "fx_rates" / f"date={ds}"
        fxd.mkdir(parents=True)
        (fxd / "part-000.json").write_text(
            json.dumps(
                [
                    {"rate_date": ds, "currency_from": "USD", "currency_to": "EUR", "rate": 0.9},
                    {"rate_date": ds, "currency_from": "EUR", "currency_to": "USD", "rate": 1.1},
                    {"rate_date": ds, "currency_from": "CZK", "currency_to": "EUR", "rate": 0.04},
                    {"rate_date": ds, "currency_from": "EUR", "currency_to": "CZK", "rate": 25.0},
                    {"rate_date": ds, "currency_from": "USD", "currency_to": "CZK", "rate": 22.5},
                    {"rate_date": ds, "currency_from": "CZK", "currency_to": "USD", "rate": 0.044},
                ]
            ),
            encoding="utf-8",
        )
        ingest_fx_rates(spark, ds, str(landing), str(lakehouse))

    # Build through Day 2, then re-run Day 1 — Day 2 history must stay intact!
    transform_employees_scd2(spark, "2026-01-02", str(lakehouse), run_id="e-d2")
    scd2_after_d1_rerun, _ = transform_employees_scd2(
        spark, "2026-01-01", str(lakehouse), run_id="e-d1"
    )
    assert scd2_after_d1_rerun.count() == 2

    transform_fx_rates(spark, "2026-01-02", str(lakehouse), run_id="fx-d2")
    fx_after_d1_rerun = transform_fx_rates(spark, "2026-01-01", str(lakehouse), run_id="fx-d1")
    assert fx_after_d1_rerun.filter("rate_date = DATE '2026-01-02'").count() == 6

    # Stale FX freshness check in run_silver_gate fails on Day 3 when Day 3 is missing.
    with pytest.raises(DQGateFailedError, match="gate_freshness_rate_date"):
        run_silver_gate(spark, "2026-01-03", lakehouse_root=str(lakehouse), run_id="g-stale")


def test_expenses_dedup_prefers_valid_row_over_non_positive_near_duplicate(
    spark, tmp_path: Path
) -> None:
    """Near-duplicate exp_id with negative corrupt row must keep the valid positive row."""
    landing = tmp_path / "landing"
    lakehouse = tmp_path / "lakehouse"

    fxd = landing / "fx_rates" / "date=2026-01-01"
    fxd.mkdir(parents=True)
    (fxd / "part-000.json").write_text(
        json.dumps(
            [
                {
                    "rate_date": "2026-01-01",
                    "currency_from": "USD",
                    "currency_to": "EUR",
                    "rate": 0.9,
                },
                {
                    "rate_date": "2026-01-01",
                    "currency_from": "EUR",
                    "currency_to": "USD",
                    "rate": 1.1,
                },
                {
                    "rate_date": "2026-01-01",
                    "currency_from": "CZK",
                    "currency_to": "EUR",
                    "rate": 0.04,
                },
                {
                    "rate_date": "2026-01-01",
                    "currency_from": "EUR",
                    "currency_to": "CZK",
                    "rate": 25.0,
                },
                {
                    "rate_date": "2026-01-01",
                    "currency_from": "USD",
                    "currency_to": "CZK",
                    "rate": 22.5,
                },
                {
                    "rate_date": "2026-01-01",
                    "currency_from": "CZK",
                    "currency_to": "USD",
                    "rate": 0.044,
                },
            ]
        ),
        encoding="utf-8",
    )
    ingest_fx_rates(spark, "2026-01-01", str(landing), str(lakehouse))
    transform_fx_rates(spark, "2026-01-01", str(lakehouse), run_id="fx-1")

    exp_dir = landing / "expenses" / "date=2026-01-01"
    exp_dir.mkdir(parents=True)
    (exp_dir / "part-000.csv").write_text(
        "exp_id,department,category,amount,currency,expense_date\n"
        "EXP-DUP,Engineering,Training,-50.0,EUR,2026-01-01\n"
        "EXP-DUP,Engineering,Training,120.0,EUR,2026-01-01\n",
        encoding="utf-8",
    )
    ingest_expenses(spark, "2026-01-01", str(landing), str(lakehouse))
    valid_df, q_df = transform_expenses(spark, "2026-01-01", str(lakehouse), run_id="exp-1")

    assert q_df.count() == 0
    assert valid_df.first()["amount_eur"] == pytest.approx(120.0)


def test_crm_hardening_orphan_client_invalid_json_and_sort_merge_parity(
    spark, tmp_path: Path
) -> None:
    """CRM events quarantine orphan_client_id + invalid_payload_json and support sort_merge."""
    landing = tmp_path / "landing"
    lakehouse = tmp_path / "lakehouse"

    (landing / "clients").mkdir(parents=True)
    (landing / "clients" / "clients.csv").write_text(
        "client_id,client_name,tier\nVIP_CORP_001,VIP Global Corp,Enterprise\n",
        encoding="utf-8",
    )
    ingest_clients(spark, "2026-01-01", str(landing), str(lakehouse))
    transform_clients(spark, "2026-01-01", str(lakehouse), run_id="c-1")

    crm_dir = landing / "crm_events" / "date=2026-01-01"
    crm_dir.mkdir(parents=True)
    # Write raw JSONL with 1 valid, 1 dup (older ts), 1 orphan client, 1 broken JSON string payload
    raw_lines = [
        json.dumps(
            {
                "event_id": "EVT-1",
                "client_id": "VIP_CORP_001",
                "event_type": "api_call",
                "timestamp": "2026-01-01T01:00:00Z",
                "payload": {"endpoint": "/old", "latency_ms": 99},
            }
        ),
        json.dumps(
            {
                "event_id": "EVT-1",
                "client_id": "VIP_CORP_001",
                "event_type": "api_call",
                "timestamp": "2026-01-01T02:00:00Z",
                "payload": {"endpoint": "/latest", "latency_ms": 42},
            }
        ),
        json.dumps(
            {
                "event_id": "EVT-ORPHAN",
                "client_id": "CLI_UNKNOWN",
                "event_type": "page_view",
                "timestamp": "2026-01-01T03:00:00Z",
                "payload": {"path": "/home"},
            }
        ),
        json.dumps(
            {
                "event_id": "EVT-BADJSON",
                "client_id": "VIP_CORP_001",
                "event_type": "page_view",
                "timestamp": "2026-01-01T04:00:00Z",
                "payload": "{not-valid-json",
            }
        ),
    ]
    (crm_dir / "part-000.jsonl").write_text("\n".join(raw_lines) + "\n", encoding="utf-8")
    ingest_crm_events(spark, "2026-01-01", str(landing), str(lakehouse))

    with pytest.raises(ValueError, match="Unsupported skew_strategy"):
        transform_crm_events(spark, "2026-01-01", str(lakehouse), skew_strategy="invalid")

    v_bc, q_bc = transform_crm_events(
        spark, "2026-01-01", str(lakehouse), run_id="crm-bc", skew_strategy="broadcast"
    )
    v_sm, q_sm = transform_crm_events(
        spark, "2026-01-01", str(lakehouse), run_id="crm-sm", skew_strategy="sort_merge"
    )
    assert spark.conf.get("spark.sql.adaptive.enabled") == "false"

    assert v_bc.count() == 1
    assert v_sm.count() == 1
    assert v_sm.first()["endpoint"] == "/latest"
    assert v_sm.first()["latency_ms"] == 42

    q_reasons = {r["event_id"]: list(r["dq_reasons"]) for r in q_sm.collect()}
    assert q_reasons["EVT-ORPHAN"] == ["orphan_client_id"]
    assert q_reasons["EVT-BADJSON"] == ["invalid_payload_json"]
    assert q_bc.count() == 2


def test_surveys_hardening_wave2_fallback_prompt_text_and_parent_leaks(
    spark, tmp_path: Path
) -> None:
    """Surveys quarantine missing_prompt_text, bad parent emp/ts, unknown_question, and Wave 2."""
    landing = tmp_path / "landing"
    lakehouse = tmp_path / "lakehouse"

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
                    "question_id": "Q99",
                    "dimension": "broken",
                    "scale_min": 1,
                    "scale_max": 5,
                    "prompt_text": "   ",
                },
            ]
        ),
        encoding="utf-8",
    )
    ingest_survey_questions(spark, "2026-02-20", str(landing), str(lakehouse))
    q_dim, q_dim_bad = transform_survey_questions(spark, "2026-02-20", str(lakehouse), run_id="q-1")
    assert q_dim.count() == 1
    assert list(q_dim_bad.first()["dq_reasons"]) == ["missing_prompt_text"]

    srv_dir = landing / "employee_surveys" / "date=2026-02-20"
    srv_dir.mkdir(parents=True)
    submissions = [
        # Valid Wave 2 submission with non-standard ID (tests date fallback -> wave 2)
        {
            "survey_id": "CUSTOM_ID_001",
            "emp_id": "EMP_1",
            "submitted_at": "2026-02-20T09:00:00Z",
            "enps_score": 9,
            "overall_comment": "Great",
            "answers": [
                {
                    "question_id": "Q01",
                    "dimension": "enps",
                    "numeric_score": 9,
                    "written_answer": "Nice",
                },
                {
                    "question_id": "Q_UNKNOWN",
                    "dimension": "x",
                    "numeric_score": 3,
                    "written_answer": "Unknown",
                },
            ],
        },
        # Invalid parent (missing emp_id & bad timestamp) -> answers must NOT leak into Silver!
        {
            "survey_id": "SRV-20260220-2-0002",
            "emp_id": "  ",
            "submitted_at": "not-a-timestamp",
            "enps_score": 8,
            "overall_comment": "Bad parent",
            "answers": [
                {
                    "question_id": "Q01",
                    "dimension": "enps",
                    "numeric_score": 8,
                    "written_answer": "Should be quarantined",
                }
            ],
        },
    ]
    (srv_dir / "part-000.jsonl").write_text(
        "\n".join(json.dumps(s) for s in submissions) + "\n", encoding="utf-8"
    )
    ingest_employee_surveys(spark, "2026-02-20", str(landing), str(lakehouse))
    resp_df, ans_df, q_ans_df = transform_surveys(
        spark, "2026-02-20", str(lakehouse), run_id="srv-w2"
    )

    assert resp_df.count() == 1
    assert resp_df.first()["survey_wave"] == 2

    # Check raw malformed timestamp preserved in response quarantine
    q_resp = spark.read.format("delta").load(
        str(lakehouse / "silver_quarantine" / "survey_responses")
    )
    assert q_resp.first()["_raw_submitted_at"] == "not-a-timestamp"

    # Only Q01 from CUSTOM_ID_001 is valid; Q_UNKNOWN and bad-parent Q01 are quarantined
    assert ans_df.count() == 1
    assert ans_df.first()["survey_id"] == "CUSTOM_ID_001"
    q_ans_reasons = {
        (r["survey_id"], r["question_id"]): set(r["dq_reasons"]) for r in q_ans_df.collect()
    }
    assert q_ans_reasons[("CUSTOM_ID_001", "Q_UNKNOWN")] == {"unknown_question"}
    assert q_ans_reasons[("SRV-20260220-2-0002", "Q01")] == {
        "missing_emp_id",
        "invalid_submitted_at",
    }
