# Changelog

All notable changes to the CorpTech Enterprise Spark Lakehouse & Medallion Pipeline are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

---

## [Unreleased] — `feat/hardening-bugfixes-perf` (2026-11-26)

Full codebase hardening pass addressing concurrency, backfill safety, data quality completeness, Delta schema evolution, Docker/Airflow reliability, and Spark execution performance.

### Fixed

- **Concurrency-safe DQ audit writes (`dq/framework.py`)**: Replaced the non-atomic `DELETE` + `APPEND` sequence in `write_audit_results()` with an atomic Delta `MERGE` keyed on `(batch_date, layer, table_name, check_name)`, with input deduplication and exponential backoff + PID-jittered retries on Delta optimistic concurrency conflicts (`ConcurrentAppendException`, `ConcurrentDeleteReadException`, `MetadataChangedException`, `ProtocolChangedException`). Also removed unused `_escape_sql_literal`.
- **SCD2 & FX earlier-date re-run truncation (`spark_jobs/silver/transform_employees_scd2.py`, `spark_jobs/silver/transform_fx_rates.py`)**: Computed `effective_end_date = max(batch_date, max(_batch_date) in Bronze)` so re-running an earlier historical date in Airflow or manual recovery never wipes newer SCD2 employee versions or newer daily FX rates. Added regex-guarded ISO date parsing (`^\d{4}-\d{2}-\d{2}$`) to prevent malformed date crashes under ANSI mode.
- **Quarantine additive schema evolution (`spark_jobs/common/io.py`)**: Added opt-in `merge_schema: bool = False` to `write_delta_partition()` and enabled `merge_schema=True` in `write_quarantine()` so adding diagnostic/lineage columns to `silver_quarantine/*` tables evolves Delta schemas cleanly across existing partitions while main Bronze/Silver contract tables remain strict.
- **Silver CRM Events validation & skew parity (`spark_jobs/silver/transform_crm_events.py`)**:
  - Moved the broadcast `silver_clients` lookup (with `_client_matched` sentinel) before the quarantine split so `orphan_client_id` rows are quarantined instead of leaking into Silver with `NULL` client metadata.
  - Added `invalid_payload_json` quarantine validation via `MapType(StringType(), StringType())` parsing.
  - Upgraded `event_id` deduplication tie-breaking to deterministic `(event_timestamp DESC, _ingested_at DESC, payload_raw ASC NULLS LAST)`.
  - Explicitly disabled AQE/skewJoin when `skew_strategy="sort_merge"` is invoked programmatically in the same SparkSession.
  - Added 3 extra job-level DQ checks (`non_empty_partition`, `not_null_client_id`, `not_null_event_timestamp`).
- **Silver Surveys strictness & wave parsing (`spark_jobs/silver/transform_surveys.py`)**:
  - Replaced fragile `element_at(split(survey_id, '-'), 3)` with `derive_survey_wave()` (`^SRV-\d{8}-(\d+)-` regex with date-based fallback so Wave 1/Wave 2 derivation never returns `NULL` or throws).
  - Quarantined `missing_prompt_text` in `silver_survey_questions`, added deterministic secondary dedup ordering, and logged `non_empty_partition` + `quarantine_ratio`.
  - Preserved raw `_raw_submitted_at` strings in `silver_quarantine/survey_responses` for debugging malformed timestamps.
  - Quarantined `missing_emp_id` and `invalid_submitted_at` on exploded answers (`silver_survey_answers`) so answers from quarantined parent surveys cannot leak into Silver; added `_question_matched` sentinel, deterministic 3-column dedup ordering, `_ingested_at`/`_source_file` lineage in quarantine, and `not_null` checks on `emp_id`, `submitted_at`, `question_id`, and `numeric_score`.
- **Silver Expenses near-duplicate tie-breaker (`spark_jobs/silver/transform_invoices_expenses.py`)**: Updated `exp_id` deduplication to rank valid positive-amount rows before `amount ASC` (matching `transform_timesheets.py`) so a corrupt duplicate does not shadow a valid record.
- **Centralized Silver DQ gate completeness (`dq/run_gate.py`)**: Added `gate_freshness_rate_date` on `silver_fx_rates_daily` (verifies `rate_date == batch_date` exists) and added `not_null_cols` enforcement for `silver_crm_events` (`client_id`, `event_timestamp`), `silver_survey_responses` (`emp_id`, `submitted_at`), and `silver_survey_answers` (`numeric_score`), expanding the strict Silver gate from 50 to **56 mandatory checks**.
- **Docker, Airflow & R2 defaults (`docker-compose.yml`, `docker/airflow/Dockerfile`, `scripts/download_data.py`, `dags/medallion_daily_dag.py`)**:
  - Added `umask 000` to `spark-master` (matching `spark-worker` and `spark-history`) to prevent cross-container Delta write permission errors.
  - Set `spark.driver.bindAddress 0.0.0.0` in `docker/airflow/Dockerfile` for reliable client-mode `spark-submit` from Airflow containers.
  - Updated fallback `R2_PREFIX` from `course-2026` to `course-2026/archives` in `docker-compose.yml` and `scripts/download_data.py`.
  - Updated the stale top docstring in `dags/medallion_daily_dag.py` to accurately reflect all 8 wired Silver tasks.

### Performance

- **Delta snapshot metadata parallelism (`spark_jobs/common/spark_session.py`)**: Configured `spark.databricks.delta.snapshotPartitions` to match `spark.sql.shuffle.partitions` (default `8` instead of Delta's default `50`), significantly reducing per-transaction metadata task overhead on local/Docker clusters.
- **Eliminated redundant Bronze scans (`spark_jobs/silver/transform_*.py`)**: Persisted evaluated/deduplicated DataFrames with `try ... finally: unpersist()` and read post-write persisted quarantine tables instead of lazily re-scanning uncached Bronze inputs during quarantine writes and DQ counts across `transform_employees_scd2`, `transform_fx_rates`, `transform_projects`, `transform_timesheets`, `transform_invoices_expenses`, `transform_crm_events`, and `transform_surveys`.

### Testing & Verification

- Added `tests/test_hardening_regressions.py` (5 comprehensive end-to-end regression tests covering atomic MERGE audit dedup, SCD2/FX earlier-date re-run preservation & stale FX gate detection, expenses valid-first tie-breaker, CRM orphan/invalid-JSON/sort-merge parity, and Surveys `missing_prompt_text`/Wave 2 fallback/parent-leak quarantine).
- Extended `tests/test_common_io.py` with additive schema evolution test coverage for `write_quarantine()`.
- Verified live on the 14-day Lakehouse (`2026-01-01` – `2026-01-14`): all 7 updated Silver jobs (`174.32s` total), live `2026-01-01` earlier-date re-run preserving all `1,204` SCD2 versions & `84` FX rows (`33.05s`), strict Silver DQ gate **56/56 PASS** (`31.61s`), `dq_audit_results` (`105/105` distinct PKs, `0` Silver FAILs), `umask 0000` on `spark-master`, Airflow DAG import (`23` tasks), and exact `SANITY_CHECKS.md` counts across all 11 Silver tables.

---

## Merged Pull Requests (History)

### PR #6 — `feat/silver-crm-surveys` (`4eb100a` / merge `472bfcf`, 2026-11-26)
- Implemented `silver_crm_events` (20 polymorphic JSON payload fields, broadcast + AQE skew mitigation for `VIP_CORP_001` = 45.05% of traffic) and `silver_survey_questions` / `silver_survey_responses` / `silver_survey_answers` (`explode()`, score bounds quarantine, `has_written_comment`).
- Wired CRM & Surveys into `dags/medallion_daily_dag.py`, promoted all 11 Silver tables to mandatory in `dq/run_gate.py`, and documented real 14-day Spark UI skew metrics & screenshots (`docs/images/crm_skew_baseline.png`, `docs/images/crm_skew_optimized.png`) in `docs/architecture.md`.

### PR #5 — `feat/dq-gates-airflow-umask` (`9965f1f` / merge `52d78ae`, 2026-11-26)
- Added `umask 000` on `spark-worker`/`spark-history`, centralized `dq/run_gate.py` (`bronze`, `silver`, `gold`), check-level audit scoping in `dq/framework.py`, `_spark_submit_cmd` & `test -s` sensor in `dags/medallion_daily_dag.py`, and `tests/test_dq_gate.py`.

### PR #4 — `feat/silver-wave2-facts` (`9c0a559` / merge `75697a2`, 2026-11-26)
- Implemented `silver_timesheets` (dedup, quarantine, zombie timesheet detection), `silver_invoices` (dual ISO/`DD.MM.YYYY` dates, `left_anti` orphan quarantine, credit notes, FX→EUR), `silver_expenses`, and `tests/test_silver_wave2.py`.

### PR #3 — `feat/silver-hr-scd2` (`569f892` / merge `34cc68c`, 2026-11-26)
- Implemented `silver_employees_scd2` (`[valid_from, valid_to)` half-open intervals, `9999-12-31` sentinel), `silver_projects`, `silver_fx_rates_daily` (calendar spine + LOCF weekend fill), `snapshotPartitions=2` in `tests/conftest.py`, and `tests/test_silver_wave1.py`.

### PR #2 — `feat/phase-1-bronze` (`d7e64da` / merge `936b3c2`, 2026-11-26)
- Implemented all 10 Bronze ingestion jobs (`ingest_reference.py`, `ingest_hr.py`, `ingest_finance.py`, `ingest_crm_surveys.py`), wired Bronze tasks & `sense_landing_files` in `dags/medallion_daily_dag.py`, and added `tests/test_bronze_pipelines.py`.

### PR #1 — `feat/phase-0-contracts-helpers` (`c5c173b` / merge `48921e9`, 2026-11-26)
- Added shared I/O helpers (`spark_jobs/common/io.py`), extended `dq/framework.py` and `dq/checks.py`, locked data contracts in `docs/architecture.md`, refactored `clients` pipeline, fixed `PATH` in `docker/spark/Dockerfile`, and added `tests/test_common_io.py` & `tests/test_dq_checks.py`.
