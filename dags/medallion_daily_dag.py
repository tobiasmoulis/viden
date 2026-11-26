"""Parameterized daily Medallion Lakehouse DAG skeleton.

Reference pipeline (`clients` Bronze -> Silver) is wired end-to-end.
Remaining domain tasks, DQ gates, Gold marts, and reporting refresh are
scaffolded as `EmptyOperator` TODO placeholders matching the dependency graph
in `docs/STUDENT_ASSIGNMENT.md` (Section 4.4).
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
    description="Parameterized daily Medallion Lakehouse pipeline (starter scaffold)",
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
            'test -f "${ROOT}/${f}" || { echo "Missing ${f} in ${ROOT}"; exit 1; }; '
            "done && "
            'if [ "{{ logical_date.isoweekday() }}" -le 5 ]; then '
            'test -f "${ROOT}/fx_rates/date=${DS}/part-000.json" '
            '|| { echo "Missing weekday fx_rates for ${DS}"; exit 1; }; '
            "fi"
        ),
    )

    with TaskGroup(group_id="bronze_ingest") as bronze_ingest:
        ingest_clients = BashOperator(
            task_id="ingest_clients",
            bash_command=(
                'spark-submit --master "${SPARK_MASTER_URL:-spark://spark-master:7077}" '
                '/opt/workspace/spark_jobs/bronze/ingest_clients.py --batch-date "{{ ds }}"'
            ),
        )
        ingest_reference = BashOperator(
            task_id="ingest_reference",
            bash_command=(
                'spark-submit --master "${SPARK_MASTER_URL:-spark://spark-master:7077}" '
                '/opt/workspace/spark_jobs/bronze/ingest_reference.py --batch-date "{{ ds }}"'
            ),
        )
        ingest_hr = BashOperator(
            task_id="ingest_hr",
            bash_command=(
                'spark-submit --master "${SPARK_MASTER_URL:-spark://spark-master:7077}" '
                '/opt/workspace/spark_jobs/bronze/ingest_hr.py --batch-date "{{ ds }}"'
            ),
        )
        ingest_finance = BashOperator(
            task_id="ingest_finance",
            bash_command=(
                'spark-submit --master "${SPARK_MASTER_URL:-spark://spark-master:7077}" '
                '/opt/workspace/spark_jobs/bronze/ingest_finance.py --batch-date "{{ ds }}"'
            ),
        )
        ingest_crm_surveys = BashOperator(
            task_id="ingest_crm_surveys",
            bash_command=(
                'spark-submit --master "${SPARK_MASTER_URL:-spark://spark-master:7077}" '
                '/opt/workspace/spark_jobs/bronze/ingest_crm_surveys.py --batch-date "{{ ds }}"'
            ),
        )

    # TODO: Wire Bronze DQ gate validating mandatory daily partitions & row counts.
    dq_bronze_gate = EmptyOperator(task_id="dq_bronze_gate")

    with TaskGroup(group_id="silver_transform") as silver_transform:
        # Dimensions & FX rates
        transform_clients = BashOperator(
            task_id="transform_clients",
            bash_command=(
                'spark-submit --master "${SPARK_MASTER_URL:-spark://spark-master:7077}" '
                "/opt/workspace/spark_jobs/silver/transform_clients.py "
                '--batch-date "{{ ds }}" --run-id "{{ run_id }}"'
            ),
        )
        # TODO: Replace placeholders with actual Silver PySpark tasks.
        transform_employees_scd2_todo = EmptyOperator(task_id="transform_employees_scd2_todo")
        transform_fx_rates_todo = EmptyOperator(task_id="transform_fx_rates_todo")

        # Facts & Surveys (depend on conformed dimensions / FX rates)
        transform_timesheets_todo = EmptyOperator(task_id="transform_timesheets_todo")
        transform_invoices_expenses_todo = EmptyOperator(task_id="transform_invoices_expenses_todo")
        transform_crm_events_todo = EmptyOperator(task_id="transform_crm_events_todo")
        transform_surveys_todo = EmptyOperator(task_id="transform_surveys_todo")

        transform_employees_scd2_todo >> transform_timesheets_todo
        transform_fx_rates_todo >> transform_invoices_expenses_todo
        transform_clients >> transform_crm_events_todo

    # TODO: Wire Silver DQ gate (SCD2 no-overlap, zero NULL EUR amounts, quarantine warn ratio).
    dq_silver_gate = EmptyOperator(task_id="dq_silver_gate")

    with TaskGroup(group_id="gold_marts") as gold_marts:
        # TODO: Replace placeholders with parallel Gold mart PySpark tasks.
        gold_project_profitability_todo = EmptyOperator(task_id="gold_project_profitability_todo")
        gold_department_finance_todo = EmptyOperator(task_id="gold_department_finance_todo")
        gold_client_engagement_todo = EmptyOperator(task_id="gold_client_engagement_todo")
        gold_hr_enps_analytics_todo = EmptyOperator(task_id="gold_hr_enps_analytics_todo")
        gold_employee_churn_features_todo = EmptyOperator(
            task_id="gold_employee_churn_features_todo"
        )

    # TODO: Wire Gold DQ gate validating Gold mart schemas, primary keys & KPI sanity bounds.
    dq_gold_gate = EmptyOperator(task_id="dq_gold_gate")

    # TODO: Implement reporting cache/view refresh or verification check if applicable.
    refresh_reporting_layer = EmptyOperator(task_id="refresh_reporting_layer")

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
