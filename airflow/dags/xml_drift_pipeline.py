"""
xml_drift_pipeline.py — xml-drift-lakehouse Airflow DAG
=========================================================
Full pipeline — Phase 1 + Phase 2 RAG field mapping:

  parse_xmls
      ↓
  schema_diff          ← detect structural drift vs known baseline
      ↓
  rag_mapping          ← only fires if drift detected (short-circuit otherwise)
      ↓
  dbt_staging
      ↓
  dbt_intermediate
      ↓
  dbt_mart
      ↓
  dbt_test
      ↓
  cleanup_staging

Schedule: daily at 06:00 UTC
Each task only runs if the previous one succeeded.

Environment variables (set in docker/.env or Airflow Variables):
  LLM_PROVIDER        claude | ollama (default: ollama)
  ANTHROPIC_API_KEY   required if LLM_PROVIDER=claude
  OLLAMA_MODEL        ollama model name (default: llama3)
  OLLAMA_HOST         ollama host (default: http://localhost:11434)

"""

import json
import os
import shutil
from datetime import timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.dates import days_ago

# ── Paths (inside container) ───────────────────────────────────────────────────
PROJECT_DIR  = Path("/opt/project")
INGESTION    = PROJECT_DIR / "ingestion"
DATA_SAMPLE  = PROJECT_DIR / "data" / "sample"
OUTPUT_DIR   = PROJECT_DIR / "output"
DBT_DIR      = PROJECT_DIR / "dbt"
DIFF_DIR     = OUTPUT_DIR / "schema_diff"
DUCKDB_PATH  = OUTPUT_DIR / "lakehouse.duckdb"

# ── Default args ───────────────────────────────────────────────────────────────
default_args = {
    "owner":            "gianfranco",
    "depends_on_past":  False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry":   False,
}

# ── DAG ────────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="xml_drift_pipeline",
    description=(
        "xml-drift-lakehouse: parse XMLs → schema diff → RAG mapping "
        "→ dbt staging → intermediate → mart → tests"
    ),
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="0 6 * * *",    # daily at 06:00 UTC
    catchup=False,
    max_active_runs=1,
    tags=["xml-drift", "lakehouse", "dbt", "rag"],
) as dag:

    # ── Task 1: Parse XMLs → Parquet ──────────────────────────────────────────
    parse_xmls = BashOperator(
        task_id="parse_xmls",
        bash_command=(
            f"python {INGESTION}/parser.py "
            f"--src {DATA_SAMPLE} "
            f"--dst {OUTPUT_DIR}"
        ),
        doc_md="""
        Reads all XMLs from data/sample/.
        Parses both variants (DetailedInvoice + SummaryInvoice).
        Writes invoices.parquet + line_items.parquet to output/.
        Idempotent — same input always produces same output.

        """,
    )

    # ── Task 2: Schema diff ───────────────────────────────────────────────────
    schema_diff = BashOperator(
        task_id="schema_diff",
        bash_command=(
            f"python {INGESTION}/schema_diff.py "
            f"--src {DATA_SAMPLE} "
            f"--out {DIFF_DIR} "
            f"--run-date {{{{ ds }}}}"
        ),
        doc_md="""
        Scans all XMLs and compares observed schema against known baseline.
        Detects: new fields, variant drift, type conflicts.
        Writes diff.json + diff.md to output/schema_diff/{run_date}/.
        If no drift: downstream RAG task is short-circuited.
        """,
    )

    # ── Task 3: Check for drift (short-circuit) ───────────────────────────────
    def _check_drift(**context) -> bool:
        """
        Returns True if drift was detected → rag_mapping runs.
        Returns False if no drift → rag_mapping is skipped.
        ShortCircuitOperator propagates the skip downstream.
        """
        run_date  = context["ds"]
        diff_path = DIFF_DIR / run_date / "diff.json"

        if not diff_path.exists():
            print(f"No diff file found at {diff_path} — skipping RAG")
            return False

        diff = json.loads(diff_path.read_text())
        has_drift = diff.get("has_drift", False)

        if has_drift:
            new_count = len(diff.get("new_fields", []))
            print(f"Drift detected — {new_count} new field(s) → triggering RAG mapper")
        else:
            print("No drift detected → RAG mapper skipped")

        return has_drift

    check_drift = ShortCircuitOperator(
        task_id="check_drift",
        python_callable=_check_drift,
        doc_md="""
        Reads diff.json from schema_diff output.
        If has_drift=False: short-circuits, skips rag_mapping.
        If has_drift=True: proceeds to rag_mapping.
        """,
    )

    # ── Task 4: RAG field mapping ─────────────────────────────────────────────
    rag_mapping = BashOperator(
        task_id="rag_mapping",
        bash_command=(
            f"python {INGESTION}/rag_mapper.py "
            f"--diff {DIFF_DIR}/{{{{ ds }}}}/diff.json "
            f"--db {DUCKDB_PATH}"
        ),
        env={
            # Pass LLM config from Airflow environment to the task
            "LLM_PROVIDER":      os.getenv("LLM_PROVIDER",      "ollama"),
            "ANTHROPIC_API_KEY": os.getenv("ANTHROPIC_API_KEY", ""),
            "OLLAMA_MODEL":      os.getenv("OLLAMA_MODEL",      "llama3"),
            "OLLAMA_HOST":       os.getenv("OLLAMA_HOST",       "http://localhost:11434"),
            "CONFIDENCE_AUTO":   os.getenv("CONFIDENCE_AUTO",   "0.90"),
            "CONFIDENCE_REVIEW": os.getenv("CONFIDENCE_REVIEW", "0.70"),
        },
        doc_md="""
        Sends each new/unknown field to the LLM for semantic mapping.
        Scores confidence: auto (>=0.90), review (>=0.70), human (<0.70).
        Writes all decisions to mapping_registry in lakehouse.duckdb.

        LLM provider set via LLM_PROVIDER env var:
          - ollama: local, free, no API key needed
          - claude: Anthropic API, best quality, needs ANTHROPIC_API_KEY

        """,
    )

    # ── Task 5: dbt staging ───────────────────────────────────────────────────
    dbt_staging = BashOperator(
        task_id="dbt_staging",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"dbt run --select staging "
            f"--profiles-dir {DBT_DIR} "
            f"--project-dir {DBT_DIR}"
        ),
        doc_md="""
        Builds stg_detailed_invoice and stg_summary_invoice views.
        Reads directly from Parquet via read_parquet().
        """,
        trigger_rule="all_done",   # runs even if check_drift short-circuited rag_mapping
    )

    # ── Task 6: dbt intermediate ──────────────────────────────────────────────
    dbt_intermediate = BashOperator(
        task_id="dbt_intermediate",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"dbt run --select intermediate "
            f"--profiles-dir {DBT_DIR} "
            f"--project-dir {DBT_DIR}"
        ),
        doc_md="""
        Builds int_invoices_deduped view.
        Deduplicates on invoice_id + line_number, latest action_datetime wins.
        """,
    )

    # ── Task 7: dbt mart ──────────────────────────────────────────────────────
    dbt_mart = BashOperator(
        task_id="dbt_mart",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"dbt run --select marts "
            f"--profiles-dir {DBT_DIR} "
            f"--project-dir {DBT_DIR}"
        ),
        doc_md="""
        Builds mart_invoices as a DuckDB table.
        Final analytical layer — both variants reconciled, enriched.
        """,
    )

    # ── Task 8: dbt tests ─────────────────────────────────────────────────────
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"dbt test "
            f"--profiles-dir {DBT_DIR} "
            f"--project-dir {DBT_DIR}"
        ),
        doc_md="""
        Runs all DQ tests across staging, intermediate, and mart.
        Pipeline fails here if data quality gates are not met.
        """,
    )

    # ── Task 9: cleanup staging Parquets ──────────────────────────────────────
    def _cleanup_staging():
        for folder in ["invoices", "line_items"]:
            path = OUTPUT_DIR / folder
            if path.exists():
                shutil.rmtree(str(path))
                print(f"Removed: {path}")
        print("Staging Parquets cleaned up.")

    cleanup_staging = PythonOperator(
        task_id="cleanup_staging",
        python_callable=_cleanup_staging,
        doc_md="""
        Removes staging Parquets from the previous run before parsing begins.
        Ensures each run starts clean with no stale data from failed previous runs.
        """,
    )

    # ── Dependencies ───────────────────────────────────────────────────────────
    #
    # cleanup_staging
    #      ↓
    #  parse_xmls
    #      ↓
    #  schema_diff
    #      ↓
    #  check_drift ──(no drift)──→ [skips rag_mapping]
    #      ↓ (drift)                      ↓
    #  rag_mapping              (trigger_rule: none_failed)
    #      ↓                             ↓
    #      └─────────────────────────────┘
    #                    ↓
    #              dbt_staging
    #                    ↓
    #            dbt_intermediate
    #                    ↓
    #               dbt_mart
    #                    ↓
    #               dbt_test
    #
    cleanup_staging >> parse_xmls >> schema_diff >> check_drift >> rag_mapping >> dbt_staging
    check_drift >> dbt_staging   # direct path when short-circuited
    dbt_staging >> dbt_intermediate >> dbt_mart >> dbt_test 
