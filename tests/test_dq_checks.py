"""Unit tests for Data Quality checks in `dq.framework` and `dq.checks`."""

from __future__ import annotations

from datetime import date

from pyspark.sql.types import DateType, DoubleType, StringType, StructField, StructType

from dq.checks import (
    check_non_empty_partition,
    check_quarantine_ratio,
    check_ratio_threshold,
    check_scd2_no_overlaps,
    check_unique_key,
    check_zero_null_eur,
)
from dq.framework import check_expected_columns


def test_check_expected_columns(spark) -> None:
    df = spark.createDataFrame([("C1", "Gold")], ["client_id", "tier"])
    ok = check_expected_columns(
        df,
        ["client_id", "tier"],
        run_id="r1",
        batch_date="2026-01-01",
        layer="silver",
        table_name="silver_clients",
    )
    assert ok.status == "PASS"
    assert ok.failed_count == 0

    bad = check_expected_columns(
        df,
        ["client_id", "tier", "missing_col"],
        run_id="r1",
        batch_date="2026-01-01",
        layer="silver",
        table_name="silver_clients",
    )
    assert bad.status == "FAIL"
    assert bad.failed_count == 1


def test_check_unique_key(spark) -> None:
    df_unique = spark.createDataFrame(
        [("E1", date(2026, 1, 1)), ("E1", date(2026, 2, 1)), ("E2", date(2026, 1, 1))],
        ["emp_id", "valid_from"],
    )
    res_ok = check_unique_key(
        df_unique,
        ["emp_id", "valid_from"],
        run_id="r1",
        batch_date="2026-01-01",
        layer="silver",
        table_name="silver_employees_scd2",
    )
    assert res_ok.status == "PASS"
    assert res_ok.failed_count == 0

    df_dup = spark.createDataFrame(
        [("E1", date(2026, 1, 1)), ("E1", date(2026, 1, 1)), ("E2", date(2026, 1, 1))],
        ["emp_id", "valid_from"],
    )
    res_fail = check_unique_key(
        df_dup,
        ["emp_id", "valid_from"],
        run_id="r1",
        batch_date="2026-01-01",
        layer="silver",
        table_name="silver_employees_scd2",
    )
    assert res_fail.status == "FAIL"
    assert res_fail.failed_count == 1


def test_check_non_empty_partition_weekday_and_weekend(spark) -> None:
    schema = StructType([StructField("id", StringType(), True)])
    empty_df = spark.createDataFrame([], schema)
    non_empty_df = spark.createDataFrame([("1",)], schema)

    # 2026-01-02 is Friday (weekday) -> empty must FAIL even with allow_weekends_empty=True.
    res_fri = check_non_empty_partition(
        empty_df,
        run_id="r1",
        batch_date="2026-01-02",
        layer="bronze",
        table_name="fx_rates",
        allow_weekends_empty=True,
    )
    assert res_fri.status == "FAIL"

    # 2026-01-03 is Saturday -> empty PASSES when allow_weekends_empty=True.
    res_sat_ok = check_non_empty_partition(
        empty_df,
        run_id="r1",
        batch_date="2026-01-03",
        layer="bronze",
        table_name="fx_rates",
        allow_weekends_empty=True,
    )
    assert res_sat_ok.status == "PASS"

    # Saturday empty FAILS when allow_weekends_empty=False.
    res_sat_fail = check_non_empty_partition(
        empty_df,
        run_id="r1",
        batch_date="2026-01-03",
        layer="bronze",
        table_name="timesheets",
        allow_weekends_empty=False,
    )
    assert res_sat_fail.status == "FAIL"

    res_non_empty = check_non_empty_partition(
        non_empty_df,
        run_id="r1",
        batch_date="2026-01-02",
        layer="bronze",
        table_name="timesheets",
    )
    assert res_non_empty.status == "PASS"
    assert res_non_empty.total_count == 1


def test_check_scd2_no_overlaps(spark) -> None:
    schema = StructType(
        [
            StructField("emp_id", StringType(), False),
            StructField("valid_from", DateType(), False),
            StructField("valid_to", DateType(), True),
        ]
    )

    # Valid contiguous half-open intervals [valid_from, valid_to)
    valid_df = spark.createDataFrame(
        [
            ("E1", date(2026, 1, 1), date(2026, 2, 1)),
            ("E1", date(2026, 2, 1), date(9999, 12, 31)),
            ("E2", date(2026, 1, 1), date(9999, 12, 31)),
        ],
        schema,
    )
    res_ok = check_scd2_no_overlaps(valid_df, run_id="r1", batch_date="2026-02-01")
    assert res_ok.status == "PASS"
    assert res_ok.failed_count == 0

    # Overlapping intervals for E1
    overlap_df = spark.createDataFrame(
        [
            ("E1", date(2026, 1, 1), date(2026, 2, 10)),
            ("E1", date(2026, 2, 1), date(9999, 12, 31)),
        ],
        schema,
    )
    res_overlap = check_scd2_no_overlaps(overlap_df, run_id="r1", batch_date="2026-02-01")
    assert res_overlap.status == "FAIL"
    assert res_overlap.failed_count == 1

    # Inverted interval (valid_from >= valid_to)
    inverted_df = spark.createDataFrame(
        [("E1", date(2026, 2, 1), date(2026, 2, 1))],
        schema,
    )
    res_inv = check_scd2_no_overlaps(inverted_df, run_id="r1", batch_date="2026-02-01")
    assert res_inv.status == "FAIL"
    assert res_inv.failed_count == 1


def test_check_zero_null_eur(spark) -> None:
    schema = StructType([StructField("amount_eur", DoubleType(), True)])
    clean_df = spark.createDataFrame([(100.0,), (-25.5,)], schema)
    dirty_df = spark.createDataFrame([(100.0,), (None,)], schema)

    assert (
        check_zero_null_eur(
            clean_df,
            "amount_eur",
            run_id="r1",
            batch_date="2026-01-01",
            layer="silver",
            table_name="silver_invoices",
        ).status
        == "PASS"
    )
    res_bad = check_zero_null_eur(
        dirty_df,
        "amount_eur",
        run_id="r1",
        batch_date="2026-01-01",
        layer="silver",
        table_name="silver_invoices",
    )
    assert res_bad.status == "FAIL"
    assert res_bad.failed_count == 1


def test_check_quarantine_ratio_and_ratio_threshold() -> None:
    ok = check_quarantine_ratio(
        valid_count=95,
        quarantined_count=5,
        max_ratio=0.08,
        run_id="r1",
        batch_date="2026-01-01",
        table_name="silver_timesheets",
    )
    assert ok.status == "PASS"
    assert ok.failed_count == 5
    assert ok.total_count == 100

    warn = check_quarantine_ratio(
        valid_count=90,
        quarantined_count=10,
        max_ratio=0.08,
        run_id="r1",
        batch_date="2026-01-01",
        table_name="silver_timesheets",
    )
    assert warn.status == "WARN"
    assert warn.severity == "WARN"

    generic = check_ratio_threshold(
        failed_count=2,
        total_count=100,
        check_name="orphan_invoice_rate",
        max_ratio=0.01,
        run_id="r1",
        batch_date="2026-01-01",
        layer="silver",
        table_name="silver_invoices",
    )
    assert generic.status == "WARN"
    assert generic.check_name == "orphan_invoice_rate"
