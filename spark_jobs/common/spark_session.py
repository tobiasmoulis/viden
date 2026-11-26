"""Shared SparkSession factory and Lakehouse path helpers."""

from __future__ import annotations

import os
from pathlib import Path

import pyspark
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def get_landing_root(override: str | None = None) -> str:
    """Return the configured Landing Zone root path or URI."""
    return override or os.getenv("LANDING_ROOT", "./data/landing")


def get_lakehouse_root(override: str | None = None) -> str:
    """Return the configured Lakehouse root path or URI."""
    return override or os.getenv("LAKEHOUSE_ROOT", "./lakehouse")


def join_path(base: str, *parts: str) -> str:
    """Join path segments cleanly for both local filesystems and object-store URIs."""
    clean_parts = [p.strip("/\\") for p in parts if p]
    if "://" in base:
        stripped = base.rstrip("/")
        return "/".join([stripped, *clean_parts]) if clean_parts else stripped
    return str(Path(base).joinpath(*clean_parts))


def _has_preinstalled_delta_jars() -> bool:
    """Return True if Delta Lake JARs are already installed locally in Spark/PySpark."""
    candidate_dirs: list[Path] = []
    spark_home = os.getenv("SPARK_HOME")
    if spark_home:
        candidate_dirs.append(Path(spark_home) / "jars")
    candidate_dirs.append(Path(pyspark.__file__).resolve().parent / "jars")

    return any(d.is_dir() and any(d.glob("delta-spark*.jar")) for d in candidate_dirs)


def get_spark_session(
    app_name: str,
    *,
    master: str | None = None,
    shuffle_partitions: int | None = None,
    extra_conf: dict[str, str] | None = None,
) -> SparkSession:
    """Build or retrieve a SparkSession configured for Delta Lake and optional S3A/MinIO."""
    resolved_master = master or os.getenv("SPARK_MASTER_URL") or "local[*]"
    resolved_shuffle = (
        shuffle_partitions
        if shuffle_partitions is not None
        else int(os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"))
    )

    builder = (
        SparkSession.builder.appName(app_name)
        .master(resolved_master)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(resolved_shuffle))
        .config("spark.databricks.delta.snapshotPartitions", str(resolved_shuffle))
    )

    s3_endpoint = os.getenv("S3_ENDPOINT_URL", "").strip()
    if s3_endpoint:
        builder = (
            builder.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config(
                "spark.hadoop.fs.s3a.connection.ssl.enabled",
                "true" if s3_endpoint.startswith("https://") else "false",
            )
        )
        access_key = os.getenv("AWS_ACCESS_KEY_ID", "")
        secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "")
        if access_key and secret_key:
            builder = builder.config("spark.hadoop.fs.s3a.access.key", access_key).config(
                "spark.hadoop.fs.s3a.secret.key", secret_key
            )

    event_log_enabled = (extra_conf or {}).get("spark.eventLog.enabled", "true") != "false"
    event_log_dir = os.getenv("SPARK_EVENT_LOG_DIR", "").strip()
    if event_log_enabled and event_log_dir:
        if "://" not in event_log_dir:
            local_log_path = Path(event_log_dir)
            local_log_path.mkdir(parents=True, exist_ok=True)
            resolved_log_uri = f"file://{local_log_path.resolve().as_posix()}"
        else:
            resolved_log_uri = event_log_dir
        builder = builder.config("spark.eventLog.enabled", "true").config(
            "spark.eventLog.dir", resolved_log_uri
        )

    if extra_conf:
        for key, value in extra_conf.items():
            builder = builder.config(key, value)

    if _has_preinstalled_delta_jars():
        return builder.getOrCreate()
    return configure_spark_with_delta_pip(builder).getOrCreate()
