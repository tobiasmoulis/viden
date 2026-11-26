"""Unit tests for Silver CRM events & Surveys transformations."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from spark_jobs.bronze.ingest_clients import ingest_clients
from spark_jobs.bronze.ingest_crm_surveys import ingest_crm_events, ingest_employee_surveys
from spark_jobs.bronze.ingest_reference import ingest_survey_questions
from spark_jobs.silver.transform_clients import transform_clients
from spark_jobs.silver.transform_crm_events import (
    SILVER_CRM_EVENTS_COLUMNS,
    transform_crm_events,
)
from spark_jobs.silver.transform_surveys import (
    SILVER_SURVEY_ANSWERS_COLUMNS,
    SILVER_SURVEY_QUESTIONS_COLUMNS,
    SILVER_SURVEY_RESPONSES_COLUMNS,
)
from spark_jobs.silver.transform_surveys import (
    run as run_surveys,
)


def test_silver_crm_events_polymorphic_payload_enrichment_and_quarantine(
    spark, tmp_path: Path
) -> None:
    landing = tmp_path / "landing"
    lakehouse = tmp_path / "lakehouse"

    # Seed silver_clients
    (landing / "clients").mkdir(parents=True)
    (landing / "clients" / "clients.csv").write_text(
        "client_id,client_name,tier\n"
        "VIP_CORP_001,VIP Global Corp,Enterprise\n"
        "CLI_0002,Acme Corp,Gold\n",
        encoding="utf-8",
    )
    ingest_clients(spark, "2026-01-01", str(landing), str(lakehouse))
    transform_clients(spark, "2026-01-01", str(lakehouse), run_id="cli-1")

    # Seed all 5 event_type variants + 1 invalid event_type
    crm_dir = landing / "crm_events" / "date=2026-01-01"
    crm_dir.mkdir(parents=True)
    events = [
        {
            "event_id": "EVT-1",
            "client_id": "VIP_CORP_001",
            "event_type": "page_view",
            "timestamp": "2026-01-01T00:05:00Z",
            "payload": {
                "path": "/dashboard",
                "referrer": "email",
                "duration_sec": 300,
                "device": "desktop",
            },
        },
        {
            "event_id": "EVT-2",
            "client_id": "VIP_CORP_001",
            "event_type": "support_ticket",
            "timestamp": "2026-01-01T01:00:00Z",
            "payload": {
                "ticket_id": "TCK-1",
                "severity": "critical",
                "module": "billing",
                "sla_hours": 4,
            },
        },
        {
            "event_id": "EVT-3",
            "client_id": "CLI_0002",
            "event_type": "contract_renewal",
            "timestamp": "2026-01-01T02:00:00Z",
            "payload": {
                "opportunity_id": "OPP-1",
                "stage": "proposal",
                "arr_delta_eur": 25000.5,
                "probability": 0.8,
            },
        },
        {
            "event_id": "EVT-4",
            "client_id": "VIP_CORP_001",
            "event_type": "api_call",
            "timestamp": "2026-01-01T03:00:00Z",
            "payload": {
                "endpoint": "/v1/metrics",
                "status_code": 200,
                "latency_ms": 145,
                "region": "eu-west-1",
            },
        },
        {
            "event_id": "EVT-5",
            "client_id": "CLI_0002",
            "event_type": "meeting_logged",
            "timestamp": "2026-01-01T04:00:00Z",
            "payload": {
                "meeting_type": "qbr",
                "attendees_count": 5,
                "outcome": "aligned",
                "next_step_days": 14,
            },
        },
        {
            "event_id": "EVT-BAD",
            "client_id": "CLI_0002",
            "event_type": "unknown_type",
            "timestamp": "2026-01-01T05:00:00Z",
            "payload": {},
        },
    ]
    (crm_dir / "part-000.jsonl").write_text(
        "\n".join(json.dumps(e) for e in events) + "\n", encoding="utf-8"
    )
    ingest_crm_events(spark, "2026-01-01", str(landing), str(lakehouse))

    # Run twice (also verifies idempotency)
    transform_crm_events(spark, "2026-01-01", str(lakehouse), run_id="crm-1")
    valid_df, q_df = transform_crm_events(spark, "2026-01-01", str(lakehouse), run_id="crm-2")

    assert tuple(valid_df.columns) == SILVER_CRM_EVENTS_COLUMNS
    assert valid_df.count() == 5
    assert q_df.count() == 1
    assert list(q_df.first()["dq_reasons"]) == ["invalid_event_type"]

    by_id = {r["event_id"]: r for r in valid_df.collect()}
    assert by_id["EVT-1"]["client_tier"] == "Enterprise"
    assert by_id["EVT-1"]["path"] == "/dashboard"
    assert by_id["EVT-1"]["duration_sec"] == 300
    assert by_id["EVT-1"]["event_date"] == date(2026, 1, 1)
    assert by_id["EVT-2"]["severity"] == "critical"
    assert by_id["EVT-3"]["arr_delta_eur"] == pytest.approx(25000.5)
    assert by_id["EVT-3"]["client_name"] == "Acme Corp"
    assert by_id["EVT-4"]["latency_ms"] == 145
    assert by_id["EVT-5"]["attendees_count"] == 5


def test_silver_surveys_questions_dedup_explode_bounds_and_empty_wave(
    spark, tmp_path: Path
) -> None:
    landing = tmp_path / "landing"
    lakehouse = tmp_path / "lakehouse"

    # Seed survey_questions catalog
    (landing / "survey_questions").mkdir(parents=True)
    questions = [
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
    (landing / "survey_questions" / "survey_questions.json").write_text(
        json.dumps(questions), encoding="utf-8"
    )
    ingest_survey_questions(spark, "2026-01-01", str(landing), str(lakehouse))

    # Non-wave date (2026-01-01): 0 survey rows must succeed cleanly
    ingest_employee_surveys(spark, "2026-01-01", str(landing), str(lakehouse))
    resp_empty, ans_empty, q_ans_empty = run_surveys(
        spark, "2026-01-01", str(lakehouse), run_id="srv-empty"
    )
    assert resp_empty.count() == 0
    assert ans_empty.count() == 0
    assert q_ans_empty.count() == 0
    assert tuple(resp_empty.columns) == SILVER_SURVEY_RESPONSES_COLUMNS
    assert tuple(ans_empty.columns) == SILVER_SURVEY_ANSWERS_COLUMNS

    q_dim = spark.read.format("delta").load(str(lakehouse / "silver" / "silver_survey_questions"))
    assert tuple(q_dim.columns) == SILVER_SURVEY_QUESTIONS_COLUMNS
    assert q_dim.count() == 2

    # Wave 1 date (2026-01-09): duplicate survey_id, Promoter/Passive/Detractor,
    # out-of-range answer score (11), and blank written_answer.
    srv_dir = landing / "employee_surveys" / "date=2026-01-09"
    srv_dir.mkdir(parents=True)
    submissions = [
        {
            "survey_id": "SRV-20260109-1-0001",
            "emp_id": "EMP_0001",
            "submitted_at": "2026-01-09T09:00:00Z",
            "enps_score": 10,
            "overall_comment": "Love it",
            "answers": [
                {
                    "question_id": "Q01",
                    "dimension": "enps",
                    "numeric_score": 10,
                    "written_answer": "Great culture",
                },
                {
                    "question_id": "Q02",
                    "dimension": "work_life_balance",
                    "numeric_score": 4,
                    "written_answer": "   ",
                },
            ],
        },
        # Exact duplicate of SRV-20260109-1-0001
        {
            "survey_id": "SRV-20260109-1-0001",
            "emp_id": "EMP_0001",
            "submitted_at": "2026-01-09T09:00:00Z",
            "enps_score": 10,
            "overall_comment": "Love it",
            "answers": [
                {
                    "question_id": "Q01",
                    "dimension": "enps",
                    "numeric_score": 10,
                    "written_answer": "Great culture",
                },
                {
                    "question_id": "Q02",
                    "dimension": "work_life_balance",
                    "numeric_score": 4,
                    "written_answer": "   ",
                },
            ],
        },
        {
            "survey_id": "SRV-20260109-1-0002",
            "emp_id": "EMP_0002",
            "submitted_at": "2026-01-09T10:00:00Z",
            "enps_score": 7,
            "overall_comment": "Okay",
            "answers": [
                {
                    "question_id": "Q01",
                    "dimension": "enps",
                    "numeric_score": 7,
                    "written_answer": None,
                },
                {
                    "question_id": "Q02",
                    "dimension": "work_life_balance",
                    "numeric_score": 11,  # out of [1, 5] -> quarantine
                    "written_answer": "Bad score",
                },
            ],
        },
        {
            "survey_id": "SRV-20260109-1-0003",
            "emp_id": "EMP_0003",
            "submitted_at": "2026-01-09T11:00:00Z",
            "enps_score": 4,
            "overall_comment": "Burnout",
            "answers": [
                {
                    "question_id": "Q01",
                    "dimension": "enps",
                    "numeric_score": 4,
                    "written_answer": "Need balance",
                },
                {
                    "question_id": "Q02",
                    "dimension": "work_life_balance",
                    "numeric_score": 2,
                    "written_answer": "Long hours",
                },
            ],
        },
    ]
    (srv_dir / "part-000.jsonl").write_text(
        "\n".join(json.dumps(s) for s in submissions) + "\n", encoding="utf-8"
    )
    ingest_employee_surveys(spark, "2026-01-09", str(landing), str(lakehouse))

    # Run twice for idempotency
    run_surveys(spark, "2026-01-09", str(lakehouse), run_id="srv-1")
    resp_df, ans_df, q_ans_df = run_surveys(spark, "2026-01-09", str(lakehouse), run_id="srv-2")

    cats = {r["survey_id"]: (r["survey_wave"], r["enps_category"]) for r in resp_df.collect()}
    assert cats == {
        "SRV-20260109-1-0001": (1, "Promoter"),
        "SRV-20260109-1-0002": (1, "Passive"),
        "SRV-20260109-1-0003": (1, "Detractor"),
    }

    # 3 distinct submissions * 2 answers = 6 total answers -> 5 valid, 1 out-of-range quarantined.
    assert ans_df.count() == 5
    assert q_ans_df.count() == 1
    assert list(q_ans_df.first()["dq_reasons"]) == ["score_out_of_range"]

    # Blank/NULL written answers retained with has_written_comment=False
    comment_flags = {
        (r["survey_id"], r["question_id"]): r["has_written_comment"] for r in ans_df.collect()
    }
    assert comment_flags[("SRV-20260109-1-0001", "Q01")] is True
    assert comment_flags[("SRV-20260109-1-0001", "Q02")] is False
    assert comment_flags[("SRV-20260109-1-0002", "Q01")] is False
