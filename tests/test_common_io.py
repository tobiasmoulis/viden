"""Unit tests for shared Lakehouse I/O helpers (`spark_jobs.common.io`)."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, DateType, StringType, StructField, StructType

from spark_jobs.common.io import (
    add_bronze_metadata,
    clean_str,
    landing_partition_path,
    parse_batch_date,
    read_landing_partition,
    write_delta_overwrite,
    write_delta_partition,
    write_quarantine,
)


def test_parse_batch_date_and_landing_partition_path() -> None:
    assert parse_batch_date("2026-01-14") == date(2026, 1, 14)
    assert parse_batch_date(date(2026, 1, 14)) == date(2026, 1, 14)
    with pytest.raises(ValueError):
        parse_batch_date("14.01.2026")

    path = landing_partition_path("/opt/workspace/data/landing", "fx_rates", "2026-01-05", "p.json")
    assert path == "/opt/workspace/data/landing/fx_rates/date=2026-01-05/p.json"


def test_clean_str_and_add_bronze_metadata(spark) -> None:
    df = spark.createDataFrame([("  Alice  ",), ("   ",), (None,)], ["name"])
    cleaned = df.select(clean_str("name").alias("name")).collect()
    assert [r["name"] for r in cleaned] == ["Alice", None, None]

    bronze = add_bronze_metadata(df, "2026-01-02")
    assert {"_batch_date", "_ingested_at", "_source_file"}.issubset(set(bronze.columns))
    batch_dates = {r["_batch_date"] for r in bronze.select("_batch_date").distinct().collect()}
    assert batch_dates == {date(2026, 1, 2)}


def test_read_landing_partition_present_and_missing(spark, tmp_path: Path) -> None:
    day_dir = tmp_path / "expenses" / "date=2026-01-01"
    day_dir.mkdir(parents=True)
    (day_dir / "part-000.csv").write_text("exp_id,amount\nE1,10.5\n", encoding="utf-8")

    df = read_landing_partition(
        spark,
        str(tmp_path),
        "expenses",
        "2026-01-01",
        "part-000.csv",
        "csv",
        options={"header": "true"},
    )
    assert df is not None
    assert df.count() == 1

    # Missing weekend partition allowed when allow_missing=True
    missing = read_landing_partition(
        spark,
        str(tmp_path),
        "fx_rates",
        "2026-01-03",
        "part-000.json",
        "json",
        allow_missing=True,
    )
    assert missing is None

    with pytest.raises(FileNotFoundError):
        read_landing_partition(
            spark,
            str(tmp_path),
            "fx_rates",
            "2026-01-03",
            "part-000.json",
            "json",
            allow_missing=False,
        )


def test_write_delta_partition_idempotency_and_isolation(spark, tmp_path: Path) -> None:
    target = str(tmp_path / "bronze_events")
    schema = StructType(
        [
            StructField("id", StringType(), False),
            StructField("_batch_date", DateType(), False),
        ]
    )

    day1 = spark.createDataFrame([("A", date(2026, 1, 1)), ("B", date(2026, 1, 1))], schema)
    day2 = spark.createDataFrame([("C", date(2026, 1, 2))], schema)

    write_delta_partition(spark, day1, target, "2026-01-01")
    write_delta_partition(spark, day2, target, "2026-01-02")

    # Re-run day1 with updated rows; day2 must stay untouched.
    day1_rerun = spark.createDataFrame([("A_v2", date(2026, 1, 1))], schema)
    write_delta_partition(spark, day1_rerun, target, "2026-01-01")

    result = spark.read.format("delta").load(target)
    rows = {(r["id"], r["_batch_date"]) for r in result.collect()}
    assert rows == {("A_v2", date(2026, 1, 1)), ("C", date(2026, 1, 2))}

    # Re-run day1 with empty DataFrame clears only day1.
    empty_day1 = spark.createDataFrame([], schema)
    write_delta_partition(spark, empty_day1, target, "2026-01-01")
    after_clear = spark.read.format("delta").load(target)
    assert {(r["id"], r["_batch_date"]) for r in after_clear.collect()} == {("C", date(2026, 1, 2))}


def test_write_delta_overwrite_and_write_quarantine(spark, tmp_path: Path) -> None:
    dim_path = str(tmp_path / "silver_dim")
    df1 = spark.createDataFrame([("P1",), ("P2",)], ["project_id"])
    df2 = spark.createDataFrame([("P1",)], ["project_id"])
    write_delta_overwrite(df1, dim_path)
    write_delta_overwrite(df2, dim_path)
    assert spark.read.format("delta").load(dim_path).count() == 1

    q_path = str(tmp_path / "quarantine_test")
    q_schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("dq_reasons", ArrayType(StringType()), False),
            StructField("_batch_date", DateType(), False),
        ]
    )
    q_df = spark.createDataFrame([("BAD_1", ["invalid_hours"], date(2026, 1, 1))], q_schema)
    write_quarantine(spark, q_df, q_path, "2026-01-01")
    write_quarantine(spark, q_df, q_path, "2026-01-01")
    assert spark.read.format("delta").load(q_path).count() == 1

    # Additive diagnostic columns evolve quarantine schema cleanly across partition writes.
    q_evolved_df = q_df.withColumn("_batch_date", F.to_date(F.lit("2026-01-02"))).withColumn(
        "_raw_payload", F.lit("bad-ts")
    )
    write_quarantine(spark, q_evolved_df, q_path, "2026-01-02")
    q_all = spark.read.format("delta").load(q_path)
    assert "_raw_payload" in q_all.columns
    assert q_all.count() == 2

    # Missing required quarantine columns must raise ValueError.
    bad_df = spark.createDataFrame([("X",)], ["id"]).withColumn(
        "_batch_date", F.to_date(F.lit("2026-01-01"))
    )
    with pytest.raises(ValueError, match="dq_reasons"):
        write_quarantine(spark, bad_df, q_path, "2026-01-01")
