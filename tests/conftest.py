"""Pytest fixtures for local PySpark & Delta Lake unit tests."""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from pyspark.sql import SparkSession

from spark_jobs.common.spark_session import get_spark_session


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    """Provide a local Delta-enabled SparkSession shared across the test session."""
    session = get_spark_session(
        app_name="test-clients-pipeline",
        master="local[2]",
        shuffle_partitions=2,
        extra_conf={
            "spark.ui.enabled": "false",
            "spark.eventLog.enabled": "false",
            "spark.sql.warehouse.dir": "/tmp/spark-warehouse-tests",
            "spark.databricks.delta.snapshotPartitions": "2",
        },
    )
    yield session
    session.stop()
