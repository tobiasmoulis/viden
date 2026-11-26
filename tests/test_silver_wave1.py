"""Unit tests for Silver Wave 1 jobs: employees SCD2, projects, and FX rates."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from spark_jobs.bronze.ingest_finance import ingest_fx_rates
from spark_jobs.bronze.ingest_hr import ingest_employees
from spark_jobs.bronze.ingest_reference import ingest_projects
from spark_jobs.common.spark_session import join_path
from spark_jobs.silver.transform_employees_scd2 import (
    OPEN_END_SENTINEL,
    SILVER_EMPLOYEES_SCD2_COLUMNS,
    transform_employees_scd2,
)
from spark_jobs.silver.transform_fx_rates import (
    CANONICAL_CURRENCY_PAIRS,
    SILVER_FX_RATES_COLUMNS,
    transform_fx_rates,
)
from spark_jobs.silver.transform_projects import SILVER_PROJECTS_COLUMNS, transform_projects


def _write_employees_day(landing: Path, ds: str, rows: list[str]) -> None:
    day_dir = landing / "employees" / f"date={ds}"
    day_dir.mkdir(parents=True, exist_ok=True)
    header = (
        "emp_id,full_name,department,role,seniority_level,hourly_rate_eur,hire_date,"
        "last_promotion_date,work_mode,distance_km,manager_id,is_active,termination_date,"
        "churn_type,snapshot_date"
    )
    (day_dir / "part-000.csv").write_text("\n".join([header, *rows]) + "\n", encoding="utf-8")


def _emp_row(
    emp_id: str,
    name: str,
    dept: str,
    role: str,
    seniority: str,
    rate: str,
    hire: str,
    promo: str,
    mode: str,
    dist: str,
    active: str,
    term: str,
    churn: str,
    snap: str,
) -> str:
    return (
        f"{emp_id},{name},{dept},{role},{seniority},{rate},{hire},{promo},"
        f"{mode},{dist},EMP_9,{active},{term},{churn},{snap}"
    )


def test_silver_employees_scd2_change_tracking_termination_and_idempotency(
    spark, tmp_path: Path
) -> None:
    landing = tmp_path / "landing"
    lakehouse = tmp_path / "lakehouse"

    # Day 1: EMP_1 active at 60 EUR, EMP_2 active at 50 EUR
    _write_employees_day(
        landing,
        "2026-01-01",
        [
            _emp_row(
                "EMP_1",
                "Alice Smith",
                "Engineering",
                "Engineer",
                "Mid",
                "60.0",
                "2023-01-01",
                "2024-01-01",
                "Hybrid",
                "10.0",
                "true",
                "",
                "",
                "2026-01-01",
            ),
            _emp_row(
                "EMP_2",
                "Bob Jones",
                "Sales",
                "AE",
                "Senior",
                "50.0",
                "2022-05-01",
                "2023-05-01",
                "Remote",
                "0.0",
                "true",
                "",
                "",
                "2026-01-01",
            ),
        ],
    )
    # Day 2: No change for either employee (must collapse into Day 1 version)
    _write_employees_day(
        landing,
        "2026-01-02",
        [
            _emp_row(
                "EMP_1",
                "Alice Smith",
                "Engineering",
                "Engineer",
                "Mid",
                "60.0",
                "2023-01-01",
                "2024-01-01",
                "Hybrid",
                "10.0",
                "true",
                "",
                "",
                "2026-01-02",
            ),
            _emp_row(
                "EMP_2",
                "Bob Jones",
                "Sales",
                "AE",
                "Senior",
                "50.0",
                "2022-05-01",
                "2023-05-01",
                "Remote",
                "0.0",
                "true",
                "",
                "",
                "2026-01-02",
            ),
        ],
    )
    # Day 3: EMP_1 rate & role promotion, EMP_2 departs voluntarily, plus 1 invalid row
    _write_employees_day(
        landing,
        "2026-01-03",
        [
            _emp_row(
                "EMP_1",
                "Alice Smith",
                "Engineering",
                "Senior Engineer",
                "Senior",
                "75.0",
                "2023-01-01",
                "2026-01-03",
                "Hybrid",
                "10.0",
                "true",
                "",
                "",
                "2026-01-03",
            ),
            _emp_row(
                "EMP_2",
                "Bob Jones",
                "Sales",
                "AE",
                "Senior",
                "50.0",
                "2022-05-01",
                "2023-05-01",
                "Remote",
                "0.0",
                "false",
                "2026-01-03",
                "voluntary",
                "2026-01-03",
            ),
            _emp_row(
                "",
                "Invalid Emp",
                "Sales",
                "AE",
                "Mid",
                "-5.0",
                "2022-05-01",
                "2023-05-01",
                "Remote",
                "0.0",
                "true",
                "",
                "",
                "2026-01-03",
            ),
        ],
    )
    # Day 4: No further change
    _write_employees_day(
        landing,
        "2026-01-04",
        [
            _emp_row(
                "EMP_1",
                "Alice Smith",
                "Engineering",
                "Senior Engineer",
                "Senior",
                "75.0",
                "2023-01-01",
                "2026-01-03",
                "Hybrid",
                "10.0",
                "true",
                "",
                "",
                "2026-01-04",
            ),
            _emp_row(
                "EMP_2",
                "Bob Jones",
                "Sales",
                "AE",
                "Senior",
                "50.0",
                "2022-05-01",
                "2023-05-01",
                "Remote",
                "0.0",
                "false",
                "2026-01-03",
                "voluntary",
                "2026-01-04",
            ),
        ],
    )

    for ds in ["2026-01-01", "2026-01-02", "2026-01-03", "2026-01-04"]:
        ingest_employees(spark, ds, str(landing), str(lakehouse))

    # Run at Day 3 first to check quarantine, then run Day 4 twice for idempotency.
    _, q_day3 = transform_employees_scd2(spark, "2026-01-03", str(lakehouse), run_id="r3")
    assert q_day3.count() == 1

    transform_employees_scd2(spark, "2026-01-04", str(lakehouse), run_id="r4a")
    scd2_df, q_day4 = transform_employees_scd2(spark, "2026-01-04", str(lakehouse), run_id="r4b")
    assert q_day4.count() == 0
    assert tuple(scd2_df.columns) == SILVER_EMPLOYEES_SCD2_COLUMNS

    rows = [
        (
            r["emp_id"],
            r["role"],
            r["hourly_rate_eur"],
            r["is_active"],
            r["termination_date"],
            r["churn_type"],
            r["valid_from"],
            r["valid_to"],
            r["is_current"],
        )
        for r in scd2_df.orderBy("emp_id", "valid_from").collect()
    ]
    assert rows == [
        ("EMP_1", "Engineer", 60.0, True, None, None, date(2026, 1, 1), date(2026, 1, 3), False),
        (
            "EMP_1",
            "Senior Engineer",
            75.0,
            True,
            None,
            None,
            date(2026, 1, 3),
            OPEN_END_SENTINEL,
            True,
        ),
        ("EMP_2", "AE", 50.0, True, None, None, date(2026, 1, 1), date(2026, 1, 3), False),
        (
            "EMP_2",
            "AE",
            50.0,
            False,
            date(2026, 1, 3),
            "voluntary",
            date(2026, 1, 3),
            OPEN_END_SENTINEL,
            True,
        ),
    ]

    # Verify point-in-time lookup on half-open [valid_from, valid_to)
    pit_day2 = scd2_df.filter(
        "emp_id = 'EMP_1' AND DATE '2026-01-02' >= valid_from AND DATE '2026-01-02' < valid_to"
    ).first()
    pit_day3 = scd2_df.filter(
        "emp_id = 'EMP_1' AND DATE '2026-01-03' >= valid_from AND DATE '2026-01-03' < valid_to"
    ).first()
    assert pit_day2["hourly_rate_eur"] == 60.0
    assert pit_day3["hourly_rate_eur"] == 75.0


def test_silver_projects_cleansing_quarantine_and_idempotency(spark, tmp_path: Path) -> None:
    landing = tmp_path / "landing"
    lakehouse = tmp_path / "lakehouse"
    (landing / "projects").mkdir(parents=True)
    (landing / "projects" / "projects.csv").write_text(
        "project_id,client_id,budget_eur,status\n"
        " PRJ_0001 , CLI_0002 ,50000.0, Active \n"
        "PRJ_0002,CLI_0003,120000.0,Completed\n"
        ",CLI_0003,10000.0,Active\n"
        "PRJ_BAD_STATUS,CLI_0003,10000.0,Cancelled\n"
        "PRJ_BAD_BUDGET,CLI_0003,-100.0,Planned\n",
        encoding="utf-8",
    )

    ingest_projects(spark, "2026-01-01", str(landing), str(lakehouse))
    transform_projects(spark, "2026-01-01", str(lakehouse), run_id="p1")
    valid_df, quarantine_df = transform_projects(spark, "2026-01-01", str(lakehouse), run_id="p2")

    assert tuple(valid_df.columns) == SILVER_PROJECTS_COLUMNS
    assert valid_df.count() == 2
    assert quarantine_df.count() == 3

    reasons = {tuple(r["dq_reasons"]) for r in quarantine_df.collect()}
    assert ("missing_project_id",) in reasons
    assert ("invalid_project_status",) in reasons
    assert ("invalid_budget_eur",) in reasons


def test_silver_fx_rates_calendar_spine_and_weekend_locf(spark, tmp_path: Path) -> None:
    landing = tmp_path / "landing"
    lakehouse = tmp_path / "lakehouse"

    # 2026-01-02 (Friday) + 2026-01-05 (Monday); Sat 2026-01-03 & Sun 2026-01-04 omitted.
    rates_by_day = {
        "2026-01-02": {
            ("EUR", "USD"): 1.10,
            ("USD", "EUR"): 0.91,
            ("EUR", "CZK"): 25.0,
            ("CZK", "EUR"): 0.04,
            ("USD", "CZK"): 22.7,
            ("CZK", "USD"): 0.044,
        },
        "2026-01-05": {
            ("EUR", "USD"): 1.12,
            ("USD", "EUR"): 0.89,
            ("EUR", "CZK"): 25.2,
            ("CZK", "EUR"): 0.0397,
            ("USD", "CZK"): 22.5,
            ("CZK", "USD"): 0.0444,
        },
    }
    for ds, pair_rates in rates_by_day.items():
        day_dir = landing / "fx_rates" / f"date={ds}"
        day_dir.mkdir(parents=True)
        payload = [
            {"rate_date": ds, "currency_from": cf, "currency_to": ct, "rate": r}
            for (cf, ct), r in pair_rates.items()
        ]
        (day_dir / "part-000.json").write_text(json.dumps(payload), encoding="utf-8")

    for ds in ["2026-01-02", "2026-01-03", "2026-01-04", "2026-01-05"]:
        ingest_fx_rates(spark, ds, str(landing), str(lakehouse))

    # Run twice for idempotency
    transform_fx_rates(spark, "2026-01-05", str(lakehouse), run_id="fx1")
    fx_df = transform_fx_rates(spark, "2026-01-05", str(lakehouse), run_id="fx2")

    assert tuple(fx_df.columns) == SILVER_FX_RATES_COLUMNS
    # 4 calendar days x 6 currency pairs = 24 rows, 0 NULL rates.
    assert fx_df.count() == 4 * len(CANONICAL_CURRENCY_PAIRS)
    assert fx_df.filter("rate IS NULL").count() == 0

    usd_eur = {
        r["rate_date"]: (r["rate"], r["_is_forward_filled"])
        for r in fx_df.filter("currency_from = 'USD' AND currency_to = 'EUR'").collect()
    }
    assert usd_eur == {
        date(2026, 1, 2): (0.91, False),
        date(2026, 1, 3): (0.91, True),
        date(2026, 1, 4): (0.91, True),
        date(2026, 1, 5): (0.89, False),
    }

    # Verify DQ audit logged PASS records
    audit_df = spark.read.format("delta").load(join_path(str(lakehouse), "dq", "dq_audit_results"))
    assert audit_df.filter("table_name = 'silver_fx_rates_daily' AND status = 'PASS'").count() == 3
