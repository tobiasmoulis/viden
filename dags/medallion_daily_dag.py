"""Parameterized daily Medallion Lakehouse Airflow DAG.

Wired end-to-end for:
- Landing file sensor (`sense_landing_files` using `test -s` non-empty checks)
- All 5 Bronze ingestion jobs (`bronze_ingest`)
- All 8 Silver transformation jobs (`clients`, `projects`, `employees_scd2`,
  `fx_rates`, `timesheets`, `invoices_expenses`, `crm_events`, `surveys`)
- Centralized DQ gates (`dq_bronze_gate`, `dq_silver_gate`, `dq_gold_gate`)
- Reporting Delta log readiness check (`refresh_reporting_layer`)

Remaining Gold mart tasks remain scaffolded as `EmptyOperator` placeholders
until their feature PRs are merged.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

logger = logging.getLogger(__name__)


def _on_task_failure(context: dict) -> None:
    """Log structured failure context for Airflow task alerts/troubleshooting."""
    ti = context.get("task_instance")
    exc = context.get("exception")
    logger.error(
        "Task failed: dag_id=%s task_id=%s logical_date=%s run_id=%s error=%s",
        getattr(ti, "dag_id", None),
        getattr(ti, "task_id", None),
        context.get("ds"),
        context.get("run_id"),
        exc,
    )


def _spark_submit_cmd(script_path: str, extra_args: str = "") -> str:
    """Build a standardized `spark-submit` shell command with `umask 000`."""
    cmd = (
        "umask 000 && "
        'spark-submit --master "${SPARK_MASTER_URL:-spark://spark-master:7077}" '
        f"{script_path}"
    )
    return f"{cmd} {extra_args}" if extra_args else cmd


DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=30),
    "on_failure_callback": _on_task_failure,
}


with DAG(
    dag_id="medallion_daily_pipeline",
    description="Parameterized daily Medallion Lakehouse pipeline",
    default_args=DEFAULT_ARGS,
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "medallion", "spark"],
) as dag:
    sense_landing_files = BashOperator(
        task_id="sense_landing_files",
        bash_command=(
            'ROOT="${LANDING_ROOT:-/opt/workspace/data/landing}" && '
            'DS="{{ ds }}" && '
            "for f in "
            '"clients/clients.csv" '
            '"projects/projects.csv" '
            '"survey_questions/survey_questions.json" '
            '"employees/date=${DS}/part-000.csv" '
            '"timesheets/date=${DS}/part-000.parquet" '
            '"invoices/date=${DS}/part-000.jsonl" '
            '"expenses/date=${DS}/part-000.csv" '
            '"crm_events/date=${DS}/part-000.jsonl"; do '
            'test -s "${ROOT}/${f}" || { echo "Missing or empty ${f} in ${ROOT}"; exit 1; }; '
            "done && "
            'if [ "{{ logical_date.isoweekday() }}" -le 5 ]; then '
            'test -s "${ROOT}/fx_rates/date=${DS}/part-000.json" '
            '|| { echo "Missing or empty weekday fx_rates for ${DS}"; exit 1; }; '
            "fi"
        ),
    )

    with TaskGroup(group_id="bronze_ingest") as bronze_ingest:
        ingest_clients = BashOperator(
            task_id="ingest_clients",
            bash_command=_spark_submit_cmd(
                "/opt/workspace/spark_jobs/bronze/ingest_clients.py",
                '--batch-date "{{ ds }}"',
            ),
        )
        ingest_reference = BashOperator(
            task_id="ingest_reference",
            bash_command=_spark_submit_cmd(
                "/opt/workspace/spark_jobs/bronze/ingest_reference.py",
                '--batch-date "{{ ds }}"',
            ),
        )
        ingest_hr = BashOperator(
            task_id="ingest_hr",
            bash_command=_spark_submit_cmd(
                "/opt/workspace/spark_jobs/bronze/ingest_hr.py",
                '--batch-date "{{ ds }}"',
            ),
        )
        ingest_finance = BashOperator(
            task_id="ingest_finance",
            bash_command=_spark_submit_cmd(
                "/opt/workspace/spark_jobs/bronze/ingest_finance.py",
                '--batch-date "{{ ds }}"',
            ),
        )
        ingest_crm_surveys = BashOperator(
            task_id="ingest_crm_surveys",
            bash_command=_spark_submit_cmd(
                "/opt/workspace/spark_jobs/bronze/ingest_crm_surveys.py",
                '--batch-date "{{ ds }}"',
            ),
        )

    dq_bronze_gate = BashOperator(
        task_id="dq_bronze_gate",
        bash_command=_spark_submit_cmd(
            "/opt/workspace/dq/run_gate.py",
            '--layer bronze --batch-date "{{ ds }}" --run-id "{{ run_id }}"',
        ),
    )

    with TaskGroup(group_id="silver_transform") as silver_transform:
        # Dimensions & FX rates
        transform_clients = BashOperator(
            task_id="transform_clients",
            bash_command=_spark_submit_cmd(
                "/opt/workspace/spark_jobs/silver/transform_clients.py",
                '--batch-date "{{ ds }}" --run-id "{{ run_id }}"',
            ),
        )
        transform_projects = BashOperator(
            task_id="transform_projects",
            bash_command=_spark_submit_cmd(
                "/opt/workspace/spark_jobs/silver/transform_projects.py",
                '--batch-date "{{ ds }}" --run-id "{{ run_id }}"',
            ),
        )
        transform_employees_scd2 = BashOperator(
            task_id="transform_employees_scd2",
            bash_command=_spark_submit_cmd(
                "/opt/workspace/spark_jobs/silver/transform_employees_scd2.py",
                '--batch-date "{{ ds }}" --run-id "{{ run_id }}"',
            ),
        )
        transform_fx_rates = BashOperator(
            task_id="transform_fx_rates",
            bash_command=_spark_submit_cmd(
                "/opt/workspace/spark_jobs/silver/transform_fx_rates.py",
                '--batch-date "{{ ds }}" --run-id "{{ run_id }}"',
            ),
        )

        # Facts & Surveys (depend on conformed dimensions / FX rates)
        transform_timesheets = BashOperator(
            task_id="transform_timesheets",
            bash_command=_spark_submit_cmd(
                "/opt/workspace/spark_jobs/silver/transform_timesheets.py",
                '--batch-date "{{ ds }}" --run-id "{{ run_id }}"',
            ),
        )
        transform_invoices_expenses = BashOperator(
            task_id="transform_invoices_expenses",
            bash_command=_spark_submit_cmd(
                "/opt/workspace/spark_jobs/silver/transform_invoices_expenses.py",
                '--batch-date "{{ ds }}" --run-id "{{ run_id }}"',
            ),
        )
        transform_crm_events = BashOperator(
            task_id="transform_crm_events",
            bash_command=_spark_submit_cmd(
                "/opt/workspace/spark_jobs/silver/transform_crm_events.py",
                '--batch-date "{{ ds }}" --run-id "{{ run_id }}"',
            ),
        )
        transform_surveys = BashOperator(
            task_id="transform_surveys",
            bash_command=_spark_submit_cmd(
                "/opt/workspace/spark_jobs/silver/transform_surveys.py",
                '--batch-date "{{ ds }}" --run-id "{{ run_id }}"',
            ),
        )

        transform_employees_scd2 >> transform_timesheets
        [transform_projects, transform_fx_rates] >> transform_invoices_expenses
        transform_clients >> transform_crm_events

    dq_silver_gate = BashOperator(
        task_id="dq_silver_gate",
        bash_command=_spark_submit_cmd(
            "/opt/workspace/dq/run_gate.py",
            '--layer silver --batch-date "{{ ds }}" --run-id "{{ run_id }}"',
        ),
    )

    with TaskGroup(group_id="gold_marts") as gold_marts:
        # TODO: Replace placeholders with parallel Gold mart PySpark tasks.
        gold_project_profitability_todo = EmptyOperator(task_id="gold_project_profitability_todo")
        gold_department_finance_todo = EmptyOperator(task_id="gold_department_finance_todo")
        gold_client_engagement_todo = EmptyOperator(task_id="gold_client_engagement_todo")
        gold_hr_enps_analytics_todo = EmptyOperator(task_id="gold_hr_enps_analytics_todo")
        gold_employee_churn_features_todo = EmptyOperator(
            task_id="gold_employee_churn_features_todo"
        )

    dq_gold_gate = BashOperator(
        task_id="dq_gold_gate",
        bash_command=_spark_submit_cmd(
            "/opt/workspace/dq/run_gate.py",
            '--layer gold --batch-date "{{ ds }}" --run-id "{{ run_id }}"',
        ),
    )

    # Verify Delta log availability for reporting tables (add mandatory gold/* checks as built).
    refresh_reporting_layer = BashOperator(
        task_id="refresh_reporting_layer",
        bash_command=(
            'LAKEHOUSE="${LAKEHOUSE_ROOT:-/opt/workspace/lakehouse}" && '
            'test -d "${LAKEHOUSE}/dq/dq_audit_results/_delta_log" '
            '|| { echo "Missing dq_audit_results Delta log"; exit 1; } && '
            'echo "Reporting layer Delta tables verified under ${LAKEHOUSE}"'
        ),
    )

    (
        sense_landing_files
        >> bronze_ingest
        >> dq_bronze_gate
        >> silver_transform
        >> dq_silver_gate
        >> gold_marts
        >> dq_gold_gate
        >> refresh_reporting_layer
    )
