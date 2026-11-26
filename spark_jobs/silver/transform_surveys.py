"""Silver transformation & quarantine job for survey questions, responses, and answers.

Builds in a single Bronze pass:
1. `silver/silver_survey_questions` (conformed 10-question catalog)
2. `silver/silver_survey_responses` (deduplicated by `survey_id`, safe regex+fallback
   `survey_wave`, `enps_category`: Promoter 9-10, Passive 7-8, Detractor 0-6)
3. `silver/silver_survey_answers` (exploded 10-element `answers` joined with question
   bounds; invalid parent identity/timestamps and out-of-range scores routed to
   `silver_quarantine/survey_answers`; blank/NULL `written_answer` retained with
   `has_written_comment: boolean`)
"""

from __future__ import annotations

import argparse
import uuid

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from dq.checks import (
    check_non_empty_partition,
    check_quarantine_ratio,
    check_ratio_threshold,
    check_unique_key,
)
from dq.framework import (
    check_expected_columns,
    check_not_null,
    enforce_dq_gate,
    write_audit_results,
)
from spark_jobs.common.io import (
    clean_str,
    parse_batch_date,
    write_delta_overwrite,
    write_delta_partition,
    write_quarantine,
)
from spark_jobs.common.spark_session import (
    get_lakehouse_root,
    get_spark_session,
    join_path,
)

SILVER_SURVEY_QUESTIONS_COLUMNS = (
    "question_id",
    "dimension",
    "scale_min",
    "scale_max",
    "prompt_text",
    "_batch_date",
    "_silver_updated_at",
)

SILVER_SURVEY_RESPONSES_COLUMNS = (
    "survey_id",
    "emp_id",
    "survey_wave",
    "submitted_at",
    "enps_score",
    "enps_category",
    "overall_comment",
    "_batch_date",
    "_silver_updated_at",
)

SILVER_SURVEY_ANSWERS_COLUMNS = (
    "survey_id",
    "emp_id",
    "survey_wave",
    "question_id",
    "dimension",
    "numeric_score",
    "written_answer",
    "has_written_comment",
    "_batch_date",
    "_silver_updated_at",
)


def derive_survey_wave(survey_id_col: Column, batch_date_col: Column) -> Column:
    """Safely extract `survey_wave` from `SRV-YYYYMMDD-<wave>-...` with date fallback."""
    extracted = F.regexp_extract(F.coalesce(survey_id_col, F.lit("")), r"^SRV-\d{8}-(\d+)-", 1)
    date_fallback = F.when(batch_date_col < F.to_date(F.lit("2026-02-01")), F.lit(1)).otherwise(
        F.lit(2)
    )
    return F.when(F.length(extracted) > 0, extracted.cast("int")).otherwise(date_fallback)


def transform_survey_questions(
    spark: SparkSession,
    batch_date: str,
    lakehouse_root: str | None = None,
    run_id: str | None = None,
) -> tuple[DataFrame, DataFrame]:
    """Transform `bronze/survey_questions` into `silver/silver_survey_questions`."""
    iso_date = parse_batch_date(batch_date).isoformat()
    resolved_lakehouse = get_lakehouse_root(lakehouse_root)
    resolved_run_id = run_id or f"manual-{iso_date}-{uuid.uuid4().hex[:8]}"

    bronze_path = join_path(resolved_lakehouse, "bronze", "survey_questions")
    silver_path = join_path(resolved_lakehouse, "silver", "silver_survey_questions")
    quarantine_path = join_path(resolved_lakehouse, "silver_quarantine", "survey_questions")
    audit_path = join_path(resolved_lakehouse, "dq", "dq_audit_results")

    bronze_df = spark.read.format("delta").load(bronze_path)

    normalized_df = bronze_df.select(
        clean_str("question_id").alias("question_id"),
        clean_str("dimension").alias("dimension"),
        F.col("scale_min").cast("int").alias("scale_min"),
        F.col("scale_max").cast("int").alias("scale_max"),
        clean_str("prompt_text").alias("prompt_text"),
        F.to_date(F.lit(iso_date)).alias("_batch_date"),
        F.col("_ingested_at"),
    )

    reason_exprs = F.array(
        F.when(F.col("question_id").isNull(), F.lit("missing_question_id")),
        F.when(F.col("dimension").isNull(), F.lit("missing_dimension")),
        F.when(F.col("prompt_text").isNull(), F.lit("missing_prompt_text")),
        F.when(
            F.col("scale_min").isNull()
            | F.col("scale_max").isNull()
            | (F.col("scale_min") >= F.col("scale_max")),
            F.lit("invalid_scale_bounds"),
        ),
    )
    evaluated_df = normalized_df.withColumn(
        "dq_reasons", F.filter(reason_exprs, lambda reason: reason.isNotNull())
    ).persist()

    try:
        quarantine_df = evaluated_df.filter(F.size("dq_reasons") > 0)

        dedup_window = Window.partitionBy("question_id").orderBy(
            F.col("_ingested_at").desc(),
            F.col("dimension").asc(),
        )
        valid_df = (
            evaluated_df.filter(F.size("dq_reasons") == 0)
            .drop("dq_reasons")
            .withColumn("_rn", F.row_number().over(dedup_window))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
            .withColumn("_silver_updated_at", F.current_timestamp())
            .select(*SILVER_SURVEY_QUESTIONS_COLUMNS)
        )

        write_delta_overwrite(valid_df, silver_path)
        write_quarantine(spark, quarantine_df, quarantine_path, iso_date)
    finally:
        evaluated_df.unpersist()

    persisted_df = spark.read.format("delta").load(silver_path)
    persisted_quarantine_df = (
        spark.read.format("delta")
        .load(quarantine_path)
        .filter(F.col("_batch_date") == F.to_date(F.lit(iso_date)))
    )
    valid_count = persisted_df.count()
    quarantined_count = persisted_quarantine_df.count()

    dq_results = [
        check_expected_columns(
            persisted_df,
            SILVER_SURVEY_QUESTIONS_COLUMNS,
            run_id=resolved_run_id,
            batch_date=iso_date,
            layer="silver",
            table_name="silver_survey_questions",
            severity="ERROR",
        ),
        check_non_empty_partition(
            persisted_df,
            run_id=resolved_run_id,
            batch_date=iso_date,
            layer="silver",
            table_name="silver_survey_questions",
            severity="ERROR",
        ),
        check_not_null(
            persisted_df,
            "question_id",
            run_id=resolved_run_id,
            batch_date=iso_date,
            layer="silver",
            table_name="silver_survey_questions",
            severity="ERROR",
        ),
        check_unique_key(
            persisted_df,
            ["question_id"],
            run_id=resolved_run_id,
            batch_date=iso_date,
            layer="silver",
            table_name="silver_survey_questions",
            severity="ERROR",
        ),
        check_quarantine_ratio(
            valid_count=valid_count,
            quarantined_count=quarantined_count,
            run_id=resolved_run_id,
            batch_date=iso_date,
            layer="silver",
            table_name="silver_survey_questions",
            severity="WARN",
        ),
    ]
    write_audit_results(spark, dq_results, audit_path)
    enforce_dq_gate(dq_results)

    return persisted_df, persisted_quarantine_df


def transform_surveys(
    spark: SparkSession,
    batch_date: str,
    lakehouse_root: str | None = None,
    run_id: str | None = None,
) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Transform `bronze/employee_surveys` for `batch_date` into Silver responses & answers."""
    iso_date = parse_batch_date(batch_date).isoformat()
    resolved_lakehouse = get_lakehouse_root(lakehouse_root)
    resolved_run_id = run_id or f"manual-{iso_date}-{uuid.uuid4().hex[:8]}"

    bronze_path = join_path(resolved_lakehouse, "bronze", "employee_surveys")
    questions_path = join_path(resolved_lakehouse, "silver", "silver_survey_questions")
    responses_path = join_path(resolved_lakehouse, "silver", "silver_survey_responses")
    answers_path = join_path(resolved_lakehouse, "silver", "silver_survey_answers")
    q_responses_path = join_path(resolved_lakehouse, "silver_quarantine", "survey_responses")
    q_answers_path = join_path(resolved_lakehouse, "silver_quarantine", "survey_answers")
    audit_path = join_path(resolved_lakehouse, "dq", "dq_audit_results")

    bronze_df = (
        spark.read.format("delta")
        .load(bronze_path)
        .filter(F.col("_batch_date") == F.to_date(F.lit(iso_date)))
    )

    cleaned_survey_id = clean_str("survey_id")
    raw_submitted_col = clean_str("submitted_at")
    normalized_df = bronze_df.select(
        cleaned_survey_id.alias("survey_id"),
        clean_str("emp_id").alias("emp_id"),
        derive_survey_wave(cleaned_survey_id, F.col("_batch_date")).alias("survey_wave"),
        raw_submitted_col.alias("_raw_submitted_at"),
        F.to_timestamp(raw_submitted_col).alias("submitted_at"),
        F.col("enps_score").cast("int").alias("enps_score"),
        clean_str("overall_comment").alias("overall_comment"),
        F.col("answers"),
        F.col("_batch_date"),
        F.col("_ingested_at"),
        F.col("_source_file"),
    )

    raw_count = normalized_df.count()

    # Deterministic deduplication by `survey_id` (per docs/architecture.md Section 5).
    dedup_window = Window.partitionBy("survey_id").orderBy(
        F.col("submitted_at").desc_nulls_last(),
        F.col("enps_score").desc_nulls_last(),
        F.col("_ingested_at").desc(),
    )
    non_null_key_df = (
        normalized_df.filter(F.col("survey_id").isNotNull())
        .withColumn("_rn", F.row_number().over(dedup_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
    null_key_df = normalized_df.filter(F.col("survey_id").isNull())
    dedup_df = non_null_key_df.unionByName(null_key_df).persist()

    try:
        dedup_count = dedup_df.count()
        duplicate_count = max(0, raw_count - dedup_count)

        # 1. Response-level evaluation & eNPS category derivation
        enps_category = (
            F.when((F.col("enps_score") >= 9) & (F.col("enps_score") <= 10), F.lit("Promoter"))
            .when((F.col("enps_score") >= 7) & (F.col("enps_score") <= 8), F.lit("Passive"))
            .when((F.col("enps_score") >= 0) & (F.col("enps_score") <= 6), F.lit("Detractor"))
            .otherwise(F.lit(None).cast("string"))
        )
        resp_reasons = F.array(
            F.when(F.col("survey_id").isNull(), F.lit("missing_survey_id")),
            F.when(F.col("emp_id").isNull(), F.lit("missing_emp_id")),
            F.when(F.col("submitted_at").isNull(), F.lit("invalid_submitted_at")),
            F.when(
                F.col("enps_score").isNull()
                | (F.col("enps_score") < 0)
                | (F.col("enps_score") > 10),
                F.lit("enps_score_out_of_range"),
            ),
        )
        eval_resp_df = dedup_df.drop("answers").withColumn(
            "dq_reasons", F.filter(resp_reasons, lambda r: r.isNotNull())
        )

        # Preserve raw `_raw_submitted_at` string in quarantine for debugging invalid timestamps.
        q_resp_df = eval_resp_df.filter(F.size("dq_reasons") > 0)
        valid_resp_df = (
            eval_resp_df.filter(F.size("dq_reasons") == 0)
            .drop("dq_reasons", "_raw_submitted_at")
            .withColumn("enps_category", enps_category)
            .withColumn("_silver_updated_at", F.current_timestamp())
            .select(*SILVER_SURVEY_RESPONSES_COLUMNS)
        )

        # 2. Exploded answer-level evaluation against `silver_survey_questions` scale bounds
        questions_dim_df = (
            spark.read.format("delta")
            .load(questions_path)
            .select(
                F.col("question_id"),
                F.col("dimension").alias("_ref_dimension"),
                F.col("scale_min"),
                F.col("scale_max"),
                F.lit(True).alias("_question_matched"),
            )
        )

        exploded_df = dedup_df.select(
            F.col("survey_id"),
            F.col("emp_id"),
            F.col("survey_wave"),
            F.col("submitted_at"),
            F.col("_raw_submitted_at"),
            F.col("_batch_date"),
            F.col("_ingested_at"),
            F.col("_source_file"),
            F.explode("answers").alias("_ans"),
        ).select(
            F.col("survey_id"),
            F.col("emp_id"),
            F.col("survey_wave"),
            F.col("submitted_at"),
            F.col("_raw_submitted_at"),
            clean_str("_ans.question_id").alias("question_id"),
            clean_str("_ans.dimension").alias("_raw_dimension"),
            F.col("_ans.numeric_score").cast("int").alias("numeric_score"),
            clean_str("_ans.written_answer").alias("written_answer"),
            F.col("_batch_date"),
            F.col("_ingested_at"),
            F.col("_source_file"),
        )

        joined_ans_df = exploded_df.join(
            F.broadcast(questions_dim_df), on="question_id", how="left"
        ).withColumn("dimension", F.coalesce(F.col("_ref_dimension"), F.col("_raw_dimension")))

        ans_reasons = F.array(
            F.when(F.col("survey_id").isNull(), F.lit("missing_survey_id")),
            F.when(F.col("emp_id").isNull(), F.lit("missing_emp_id")),
            F.when(F.col("submitted_at").isNull(), F.lit("invalid_submitted_at")),
            F.when(F.col("question_id").isNull(), F.lit("missing_question_id")),
            F.when(
                F.col("question_id").isNotNull() & F.col("_question_matched").isNull(),
                F.lit("unknown_question"),
            ),
            F.when(
                F.col("_question_matched").isNotNull()
                & (
                    F.col("numeric_score").isNull()
                    | (F.col("numeric_score") < F.col("scale_min"))
                    | (F.col("numeric_score") > F.col("scale_max"))
                ),
                F.lit("score_out_of_range"),
            ),
        )
        eval_ans_df = joined_ans_df.withColumn(
            "dq_reasons", F.filter(ans_reasons, lambda r: r.isNotNull())
        )

        q_ans_df = eval_ans_df.filter(F.size("dq_reasons") > 0).drop(
            "_raw_dimension", "_ref_dimension", "_question_matched"
        )

        ans_dedup_window = Window.partitionBy("survey_id", "question_id").orderBy(
            F.col("numeric_score").desc_nulls_last(),
            F.col("written_answer").asc_nulls_last(),
            F.col("_ingested_at").desc(),
        )
        valid_ans_df = (
            eval_ans_df.filter(F.size("dq_reasons") == 0)
            .drop(
                "dq_reasons",
                "_raw_dimension",
                "_ref_dimension",
                "_question_matched",
                "_raw_submitted_at",
                "submitted_at",
                "scale_min",
                "scale_max",
            )
            .withColumn("_rn", F.row_number().over(ans_dedup_window))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
            .withColumn("has_written_comment", F.col("written_answer").isNotNull())
            .withColumn("_silver_updated_at", F.current_timestamp())
            .select(*SILVER_SURVEY_ANSWERS_COLUMNS)
        )

        write_delta_partition(spark, valid_resp_df, responses_path, iso_date)
        write_quarantine(spark, q_resp_df, q_responses_path, iso_date)
        write_delta_partition(spark, valid_ans_df, answers_path, iso_date)
        write_quarantine(spark, q_ans_df, q_answers_path, iso_date)
    finally:
        dedup_df.unpersist()

    persisted_resp_df = (
        spark.read.format("delta")
        .load(responses_path)
        .filter(F.col("_batch_date") == F.to_date(F.lit(iso_date)))
    )
    persisted_q_resp_df = (
        spark.read.format("delta")
        .load(q_responses_path)
        .filter(F.col("_batch_date") == F.to_date(F.lit(iso_date)))
    )
    persisted_ans_df = (
        spark.read.format("delta")
        .load(answers_path)
        .filter(F.col("_batch_date") == F.to_date(F.lit(iso_date)))
    )
    persisted_q_ans_df = (
        spark.read.format("delta")
        .load(q_answers_path)
        .filter(F.col("_batch_date") == F.to_date(F.lit(iso_date)))
    )

    resp_valid_count = persisted_resp_df.count()
    resp_q_count = persisted_q_resp_df.count()
    ans_valid_count = persisted_ans_df.count()
    ans_q_count = persisted_q_ans_df.count()
    missing_comment_count = persisted_ans_df.filter(~F.col("has_written_comment")).count()

    dq_results = [
        check_expected_columns(
            persisted_resp_df,
            SILVER_SURVEY_RESPONSES_COLUMNS,
            run_id=resolved_run_id,
            batch_date=iso_date,
            layer="silver",
            table_name="silver_survey_responses",
            severity="ERROR",
        ),
        check_not_null(
            persisted_resp_df,
            "survey_id",
            run_id=resolved_run_id,
            batch_date=iso_date,
            layer="silver",
            table_name="silver_survey_responses",
            severity="ERROR",
        ),
        check_not_null(
            persisted_resp_df,
            "emp_id",
            run_id=resolved_run_id,
            batch_date=iso_date,
            layer="silver",
            table_name="silver_survey_responses",
            severity="ERROR",
        ),
        check_not_null(
            persisted_resp_df,
            "submitted_at",
            run_id=resolved_run_id,
            batch_date=iso_date,
            layer="silver",
            table_name="silver_survey_responses",
            severity="ERROR",
        ),
        check_unique_key(
            persisted_resp_df,
            ["survey_id"],
            run_id=resolved_run_id,
            batch_date=iso_date,
            layer="silver",
            table_name="silver_survey_responses",
            severity="ERROR",
        ),
        check_quarantine_ratio(
            valid_count=resp_valid_count,
            quarantined_count=resp_q_count,
            max_ratio=0.08,
            run_id=resolved_run_id,
            batch_date=iso_date,
            layer="silver",
            table_name="silver_survey_responses",
            severity="WARN",
        ),
        check_ratio_threshold(
            failed_count=duplicate_count,
            total_count=raw_count,
            check_name="duplicate_rate",
            max_ratio=0.10,
            run_id=resolved_run_id,
            batch_date=iso_date,
            layer="silver",
            table_name="silver_survey_responses",
            severity="WARN",
        ),
        check_expected_columns(
            persisted_ans_df,
            SILVER_SURVEY_ANSWERS_COLUMNS,
            run_id=resolved_run_id,
            batch_date=iso_date,
            layer="silver",
            table_name="silver_survey_answers",
            severity="ERROR",
        ),
        check_not_null(
            persisted_ans_df,
            "survey_id",
            run_id=resolved_run_id,
            batch_date=iso_date,
            layer="silver",
            table_name="silver_survey_answers",
            severity="ERROR",
        ),
        check_not_null(
            persisted_ans_df,
            "question_id",
            run_id=resolved_run_id,
            batch_date=iso_date,
            layer="silver",
            table_name="silver_survey_answers",
            severity="ERROR",
        ),
        check_not_null(
            persisted_ans_df,
            "numeric_score",
            run_id=resolved_run_id,
            batch_date=iso_date,
            layer="silver",
            table_name="silver_survey_answers",
            severity="ERROR",
        ),
        check_unique_key(
            persisted_ans_df,
            ["survey_id", "question_id"],
            run_id=resolved_run_id,
            batch_date=iso_date,
            layer="silver",
            table_name="silver_survey_answers",
            severity="ERROR",
        ),
        check_quarantine_ratio(
            valid_count=ans_valid_count,
            quarantined_count=ans_q_count,
            max_ratio=0.08,
            run_id=resolved_run_id,
            batch_date=iso_date,
            layer="silver",
            table_name="silver_survey_answers",
            severity="WARN",
        ),
        check_ratio_threshold(
            failed_count=missing_comment_count,
            total_count=ans_valid_count,
            check_name="missing_written_comment_rate",
            max_ratio=0.05,
            run_id=resolved_run_id,
            batch_date=iso_date,
            layer="silver",
            table_name="silver_survey_answers",
            severity="WARN",
        ),
    ]
    write_audit_results(spark, dq_results, audit_path)
    enforce_dq_gate(dq_results)

    return persisted_resp_df, persisted_ans_df, persisted_q_ans_df


def run(
    spark: SparkSession,
    batch_date: str,
    lakehouse_root: str | None = None,
    run_id: str | None = None,
) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Build `silver_survey_questions`, `silver_survey_responses`, and `silver_survey_answers`."""
    q_dim_df, _ = transform_survey_questions(
        spark,
        batch_date=batch_date,
        lakehouse_root=lakehouse_root,
        run_id=run_id,
    )
    resp_df, ans_df, q_ans_df = transform_surveys(
        spark,
        batch_date=batch_date,
        lakehouse_root=lakehouse_root,
        run_id=run_id,
    )
    print(
        f"Silver surveys complete for batch_date={batch_date}: "
        f"questions={q_dim_df.count()}, responses={resp_df.count()}, "
        f"answers={ans_df.count()}, quarantined_answers={q_ans_df.count()}"
    )
    return resp_df, ans_df, q_ans_df


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build silver_survey_questions, silver_survey_responses, and answers."
    )
    parser.add_argument("--batch-date", required=True, help="Logical batch date (YYYY-MM-DD).")
    parser.add_argument("--lakehouse-root", default=None, help="Override Lakehouse root path.")
    parser.add_argument("--run-id", default=None, help="Orchestrator run identifier.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    spark = get_spark_session("silver-transform-surveys")
    try:
        run(spark, args.batch_date, args.lakehouse_root, args.run_id)
    finally:
        spark.stop()
