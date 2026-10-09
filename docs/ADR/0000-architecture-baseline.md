# ADR-0000: Architecture baseline (retrospective)

- Status: Accepted (retrospective)
- Date: 2026-10-08
- Scope: Records decisions taken before ADRs were adopted in this repo, so later ADRs have a stable reference point. This is a recap of what exists, not a design proposal.

## Context

xml-drift-lakehouse is a personal portfolio project. It is not production experience. It reproduces, with a fictional schema (domain `fieldops-demo.io`), the shape of a problem solved in production by the OpenInvoice pipeline at Vital Energy: XML invoices arriving in two structural variants with different field names and node ordering, handled schema-on-read.

In production the stack was Glue + Athena with hand-written views. Here DuckDB stands in for Athena and dbt Core stands in for the hand-written views. The private repo (`adaptive-lakehouse`) carries the full-fidelity schema and is out of scope for this ADR.

## Decisions recorded

**D1. Schema-on-read parsing.** `ingestion/parser.py` uses lxml + Polars and writes one Parquet file per entity (`invoices`, `line_items`). Two variants are handled: DetailedInvoice (about 60.2%) and SummaryInvoice (about 39.8%).

**D2. Surrogate keys.** SHA-256. The invoice key is built from `source_file + invoice_id`; the line key adds `line_number`. `invoice_id` is the system-assigned root XML attribute. `document_number` is the vendor-assigned invoice number and is not a unique identifier.

**D3. Landing zone.** Local Parquet, unpartitioned, rewritten on each run, read by dbt through `read_parquet(...)` in `sources.yml`.

**D4. dbt layering.** Staging models per variant, an intermediate deduplication layer using `QUALIFY`, and a mart that does `UNION ALL` with `COALESCE` reconciliation, materialized as a table.

**D5. Dual engine.** The same dbt models run on DuckDB (dev) and Snowflake with no SQL changes. Snowflake is loaded by `scripts/load_to_snowflake.py`. Snowflake primary keys are metadata only, so downstream logic must not assume uniqueness from them.

**D6. LLM-assisted drift handling.** A schema diff engine detects new fields, variant drift and type conflicts. Field mapping goes through Claude API or Ollama (local) with the full schema corpus injected in the prompt. Routing is confidence-tiered: 0.90 and above is auto_approved, 0.70 and above is flagged_review, below 0.70 is pending_human. Decisions are written to an immutable mapping registry in DuckDB. Vector retrieval is planned and not implemented.

**D7. Orchestration and BI.** An Airflow DAG in Docker Compose: parse, schema diff, drift gate (ShortCircuitOperator), RAG mapping, dbt staging, intermediate, mart, dbt test. Apache Superset reads the DuckDB mart.

**D8. Quality.** 51 dbt data quality tests across the layers, plus 29 pytest integration tests (`tests/test_rag_flow.py`) that run without an API key.

## Known gaps and deviations at this date

- `scripts/load_to_snowflake.py` uses pandas (`write_pandas`). The "no pandas" convention applies to new code only.
- `tests/test_parser.py` is a script and contains no test functions.
- No incremental loads: the Parquet landing zone is rewritten in full each run.
- `dbt/profiles.yml` is tracked despite a `.gitignore` rule that never matched (inline comment is invalid in `.gitignore`). It holds no literal secrets; Snowflake values come from `env_var(...)`.
- `sources.yml` describes `invoice_id` as "Document number", which contradicts D2. Documentation fix pending.
- Version drift: `docker/requirements.txt` pins `duckdb==1.1.3`, while the local virtualenv runs a newer DuckDB. Neither is currently declared authoritative.

## Consequences

- ADRs live in `docs/adr/`. Numbering is independent per repo and is never mirrored from another repo.
- ADR-0001 onward are forward-looking and record decisions before implementation.
