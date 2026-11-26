"""Shared Lakehouse I/O, metadata, and idempotent Delta write helpers."""

from __future__ import annotations

import re
from collections.abc import Sequence
from datetime import date, datetime
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from spark_jobs.common.spark_session import join_path

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def parse_batch_date(batch_date: str | date) -> date:
    """Validate and parse a logical batch date (YYYY-MM-DD)."""
    if isinstance(batch_date, datetime):
        return batch_date.date()
    if isinstance(batch_date, date):
        return batch_date
    return date.fromisoformat(batch_date)


def _validate_identifier(name: str) -> str:
    if not _IDENTIFIER_RE.fullmatch(name):
        raise ValueError(f"Invalid SQL column identifier: {name!r}")
    return name


def clean_str(column_name: str) -> Column:
    """Trim whitespace and convert empty strings to NULL."""
    trimmed = F.trim(F.col(column_name))
    return F.when(F.length(trimmed) == 0, F.lit(None).cast("string")).otherwise(trimmed)


def add_bronze_metadata(df: DataFrame, batch_date: str | date) -> DataFrame:
    """Append standardized Bronze technical metadata columns."""
    iso_date = parse_batch_date(batch_date).isoformat()
    return (
        df.withColumn("_batch_date", F.to_date(F.lit(iso_date)))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )


def landing_partition_path(
    landing_root: str,
    dataset: str,
    batch_date: str | date,
    filename: str,
) -> str:
    """Build Hive-style daily landing file path: <landing_root>/<dataset>/date=YYYY-MM-DD/<file>."""
    iso_date = parse_batch_date(batch_date).isoformat()
    return join_path(landing_root, dataset, f"date={iso_date}", filename)


def path_exists(spark: SparkSession, path: str) -> bool:
    """Check whether a local filesystem path or Hadoop-supported URI exists."""
    if "://" not in path:
        return Path(path).exists()
    jvm = spark._jvm
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_path = jvm.org.apache.hadoop.fs.Path(path)
    fs = hadoop_path.getFileSystem(hadoop_conf)
    return bool(fs.exists(hadoop_path))


def read_landing_partition(
    spark: SparkSession,
    landing_root: str,
    dataset: str,
    batch_date: str | date,
    filename: str,
    file_format: str,
    schema: StructType | None = None,
    *,
    options: dict[str, str] | None = None,
    allow_missing: bool = False,
) -> DataFrame | None:
    """Read a daily Hive-partitioned landing file, optionally returning None when missing."""
    source_path = landing_partition_path(landing_root, dataset, batch_date, filename)
    if not path_exists(spark, source_path):
        if allow_missing:
            return None
        raise FileNotFoundError(f"Landing partition file not found: {source_path}")

    reader = spark.read.format(file_format)
    if schema is not None:
        reader = reader.schema(schema)
    if options:
        for key, value in options.items():
            reader = reader.option(key, value)
    return reader.load(source_path)


def write_delta_overwrite(
    df: DataFrame,
    target_path: str,
    *,
    partition_by: str | Sequence[str] | None = None,
) -> None:
    """Overwrite a Delta table idempotently (full-load dimensions / snapshots)."""
    writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if isinstance(partition_by, str):
        writer = writer.partitionBy(_validate_identifier(partition_by))
    elif partition_by:
        writer = writer.partitionBy(*(_validate_identifier(c) for c in partition_by))
    writer.save(target_path)


def write_delta_partition(
    spark: SparkSession,
    df: DataFrame,
    target_path: str,
    batch_date: str | date,
    *,
    partition_col: str = "_batch_date",
) -> None:
    """Idempotently replace a single `_batch_date` partition in a Delta table.

    - If the target table does not exist yet, creates it partitioned by `partition_col`.
    - If it exists and `df` is non-empty, replaces only `partition_col = DATE '<batch_date>'`
      atomically via `replaceWhere`.
    - If it exists and `df` is empty, deletes any stale rows for `batch_date`.
    """
    col_name = _validate_identifier(partition_col)
    if col_name not in df.columns:
        raise ValueError(f"DataFrame must contain partition column {col_name!r}")

    iso_date = parse_batch_date(batch_date).isoformat()
    predicate = f"{col_name} = DATE '{iso_date}'"

    if DeltaTable.isDeltaTable(spark, target_path):
        if df.isEmpty():
            DeltaTable.forPath(spark, target_path).delete(predicate)
        else:
            (
                df.write.format("delta")
                .mode("overwrite")
                .option("replaceWhere", predicate)
                .save(target_path)
            )
    else:
        (df.write.format("delta").mode("overwrite").partitionBy(col_name).save(target_path))


def write_quarantine(
    spark: SparkSession,
    quarantine_df: DataFrame,
    quarantine_path: str,
    batch_date: str | date,
) -> None:
    """Write invalid Silver rows idempotently to `silver_quarantine/<table>`."""
    required = {"dq_reasons", "_batch_date"}
    missing = required - set(quarantine_df.columns)
    if missing:
        raise ValueError(f"Quarantine DataFrame missing required columns: {sorted(missing)}")
    write_delta_partition(
        spark,
        quarantine_df,
        quarantine_path,
        batch_date,
        partition_col="_batch_date",
    )
