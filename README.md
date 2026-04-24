# xml-drift-lakehouse
![Status](https://img.shields.io/badge/status-work%20in%20progress-yellow)
![License](https://img.shields.io/badge/license-MIT-blue)
![Stack](https://img.shields.io/badge/stack-Python%20%7C%20DuckDB%20%7C%20dbt%20%7C%20Airflow-informational)

> Work in progress. The architecture, patterns and folder structure are documented and reflect real production experience. Implementation is being built incrementally. Feedback and contributions welcome.

A portable, schema-on-read toolkit for ingesting XML data sources with structural drift into a lakehouse architecture.

Built for the real-world problem: XML that has changed shape over time, has multiple structural variants, and cannot be ingested with a hard-coded schema without breaking.

---

## The Problem

Most XML ingestion tools assume a stable, known schema. Real-world XML does not behave that way.

Over years of production use, XML sources accumulate:
- Field names that changed between versions
- Node structures that appear as scalars in some records and arrays in others
- Optional elements that are present in some variants and absent in others
- Hierarchical paths that reorganized across system upgrades

Hard-coding a schema against drifted XML means constant maintenance, silent data loss, or pipeline failures. This toolkit takes a different approach.

---

## The Approach

**Schema-on-read with surrogate key propagation.**

Instead of defining what the XML should look like, the pipeline discovers what it actually looks like at ingestion time. Each structural variant is absorbed automatically and reconciled at the analytical layer.

Key patterns:

- **Schema-on-read ingestion** — no hard-coded field mapping at the ingestion stage
- **Surrogate key injection** — a deterministic SHA-256 key is injected at ingestion and propagated through all child records to maintain join integrity across entities
- **Relational explosion** — hierarchical XML nodes are flattened into separate tables preserving parent-child relationships
- **Variant reconciliation** — multiple structural variants of the same entity are unified at the analytical layer via UNION ALL and COALESCE across divergent field names
- **Defensive casting** — CAST/COALESCE/NULLIF patterns handle empty strings, null fields, and type mismatches across variants

---

## Architecture

```
XML Source
    |
    v
[ Ingestion Layer ]
  - XML parsed via lxml
  - Surrogate key injected (SHA-256 of source_file + invoice_id)
  - Stored as Parquet in landing zone
    |
    v
[ Schema Discovery ]
  - Schema-on-read variant detection
  - Structural variants auto-discovered
  - Field coverage report generated per variant
    |
    v
[ Relational Layer ]
  - Hierarchical nodes exploded into tables
  - Parent-child joins via surrogate key
  - Both variants handled transparently
    |
    v
[ Reconciliation Layer ]
  - Variant UNION ALL with COALESCE on divergent fields
  - Deduplication: latest status wins per invoice + line
  - Defensive type casting
  - Consumer-ready analytical table
    |
    v
[ Analytics ]
  - Clean, queryable DuckDB table
  - BI / reporting ready
```

---

## XML Variants

The dataset contains two structurally distinct invoice types, both handled transparently by the pipeline:

| Feature | DetailedInvoice | SummaryInvoice |
|---------|----------------|----------------|
| Distribution | 60.2% (938 files) | 39.8% (620 files) |
| Line items | Full unit economics | Lean |
| ProductDescription / ServiceCode | Yes | No |
| LineSubTotal / LinePretaxTotal | Yes | No |
| Allocation position | Inside each LineEntry | Header level |
| PeriodDate | Line level | Header level |
| Tax entries | Yes (sparse) | No |
| Early payment terms | Yes (sparse) | No |

The reconciliation logic uses `COALESCE` across variants in the mart layer — the same pattern used in the production Athena views this project is based on.

---

## Local Stack

This toolkit runs entirely locally without cloud dependencies, making it portable and easy to evaluate.

| Layer | Tool |
|-------|------|
| XML parsing | Python + lxml |
| Data anonymization | Faker |
| Storage | Local Parquet files |
| Query engine | DuckDB |
| Transformation | dbt Core + dbt-duckdb |
| Orchestration | Apache Airflow (Docker) |
| Visualization | Apache Superset (coming) |

### Cloud Portability

The local stack is deliberately designed to mirror the architectural patterns of any major cloud platform. Each component maps cleanly to cloud-native equivalents without requiring changes to the core transformation logic.

| Local | AWS | Azure | GCP |
|-------|-----|-------|-----|
| DuckDB | Amazon Athena | Synapse Serverless | BigQuery |
| Local Parquet | S3 + Glue | ADLS Gen2 + Synapse | GCS + Dataproc |
| dbt Core | dbt Core (any platform) | dbt Core (any platform) | dbt Core (any platform) |
| Airflow (Docker) | MWAA / Glue Workflows | Azure Data Factory | Cloud Composer |

The transformation logic, surrogate key patterns, and reconciliation models are cloud-agnostic by design. Deploying to any cloud platform requires infrastructure configuration only, not logic changes.

---

## Repository Structure

```
xml-drift-lakehouse/
├── data/
│   └── sample/              # Sanitized XML samples (2 variants, 1558 files)
├── ingestion/
│   ├── sanitize.py          # Anonymize raw XMLs with Faker (idempotent)
│   ├── remap.py             # Remap to public domain-agnostic schema
│   ├── verify.py            # Pre-commit sensitive data scanner
│   ├── parser.py            # Schema-on-read XML to Parquet
│   └── schema_discovery.py  # Structural profiler — field coverage per variant
├── dbt/
│   ├── models/
│   │   ├── staging/         # stg_detailed_invoice, stg_summary_invoice
│   │   ├── intermediate/    # int_invoices_deduped
│   │   └── marts/           # mart_invoices (final analytical table)
│   └── tests/
├── airflow/
│   └── dags/
│       └── xml_drift_pipeline.py  # Full pipeline DAG
├── docker/
│   ├── docker-compose.yml   # Airflow + Postgres stack
│   ├── Dockerfile           # Custom Airflow image with project deps
│   └── requirements.txt
├── output/                  # Generated — not committed
└── docs/
    └── schema_report.md     # Living schema documentation (auto-generated)
```

---

## Quickstart

### Local (Python)

```bash
# Clone and setup
git clone https://github.com/gdcur/xml-drift-lakehouse
cd xml-drift-lakehouse
python -m venv .venv && source .venv/bin/activate
pip install lxml polars dbt-duckdb faker

# Run ingestion
python ingestion/parser.py --src ./data/sample --dst ./output

# Run dbt transformations + tests
cd dbt && dbt run && dbt test
```

### With Airflow (Docker)

```bash
cd docker

# First time only
docker compose build
docker compose run --rm airflow-init

# Start
docker compose up -d

# Open UI: http://localhost:8080  (admin / admin)
# Trigger DAG: xml_drift_pipeline
```

---

## dbt Lineage

```
stg_detailed_invoice ---+
                        +--> int_invoices_deduped --> mart_invoices
stg_summary_invoice  ---+
```

| Model | Type | Description |
|-------|------|-------------|
| `stg_detailed_invoice` | view | Staged DetailedInvoice — full unit economics |
| `stg_summary_invoice` | view | Staged SummaryInvoice — lean lines, header allocation |
| `int_invoices_deduped` | view | Deduplicated — latest status per invoice_id + line_number |
| `mart_invoices` | table | Final analytical table — both variants reconciled |

**51 DQ tests** — `not_null`, `unique`, `accepted_values` across all layers.

---

## Data Sanitization

The XML samples in `data/sample/` are fully anonymized:

- All company names, addresses, invoice IDs replaced with Faker-generated values
- Dates shifted by a deterministic random offset per invoice
- Location names replaced with generic fake names
- Domain namespace remapped from production schema to `fieldops-demo.io`
- O&G-specific terminology renamed to generic industry-agnostic equivalents
- PII elements (named individuals, phone numbers) replaced

The sanitization pipeline (`sanitize.py -> remap.py -> verify.py`) is **fully idempotent** — the same input always produces the same output. A `--strict` flag on `verify.py` can be wired into a pre-commit hook.

---

## Roadmap

### Phase 1 — Core Pipeline

- [x] XML anonymization pipeline (sanitize, remap, verify)
- [x] Schema-on-read parser with surrogate key injection
- [x] Schema discovery — field coverage report per variant
- [x] Parquet landing zone
- [x] dbt staging models — one per structural variant
- [x] dbt intermediate — deduplication layer
- [x] dbt mart — UNION ALL + COALESCE reconciliation
- [x] 51 DQ tests across all layers
- [x] Airflow DAG (Docker)
- [ ] Apache Superset dashboard
- [ ] Incremental loads

### Phase 2 — RAG-Assisted Schema Intelligence

- [ ] Embed field-level semantic knowledge from known XML variants into a vector store
- [ ] RAG lookup layer: identify unknown variants by semantic similarity rather than structural matching
- [ ] LLM-assisted mapping suggestion for new or unseen field names
- [ ] Hybrid execution: deterministic reconciliation for known variants, RAG fallback for drift beyond known patterns
- [ ] Evaluation framework: measure mapping accuracy across synthetic drift scenarios

---

## Background

This toolkit grew out of a production problem involving large-scale XML ingestion where the source data had accumulated years of structural drift: multiple variants, divergent field names, and no stable schema to code against.

The patterns here — schema-on-read, surrogate key propagation, variant reconciliation — are generalized from that experience and designed to work with any XML source that has accumulated structural drift over time, regardless of industry or platform.

The local stack (DuckDB + dbt + Airflow) replicates the same architectural patterns without cloud dependencies, making the approach portable across environments and deployable to any cloud platform without changes to the core logic.

---

## AI Assistance

This project was built with AI assistance (Claude by Anthropic) for code generation and scaffolding. All architectural decisions, data modeling choices, schema design, and quality validation were reviewed, challenged, and directed by the author.

The core problem — schema-on-read XML processing with structural drift — is derived from real production work. The AI accelerated implementation; the engineering judgment is human.

---

## Related

- [ercot-plan-ranker](https://github.com/gdcur/ercot-plan-ranker) — A production-style lakehouse demo using the same patterns applied to ERCOT electricity market data

---

## Author

Gianfranco — Data Engineer
[github.com/gdcur](https://github.com/gdcur)

---

## License

MIT
