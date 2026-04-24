# xml-drift-lakehouse
![Status](https://img.shields.io/badge/status-work%20in%20progress-yellow)
![License](https://img.shields.io/badge/license-MIT-blue)
![Stack](https://img.shields.io/badge/stack-Python%20%7C%20DuckDB%20%7C%20dbt%20%7C%20Airflow-informational)

A production-grade, schema-on-read XML ingestion toolkit with automated structural drift detection and AI-assisted field mapping.

Built as a portfolio project to demonstrate real-world data engineering patterns: immutable raw storage, replayable transformations, and a RAG pipeline that handles schema evolution without manual intervention.

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
- **RAG-assisted drift detection** — new or unknown fields are semantically mapped to the known schema using an LLM, with confidence-tiered routing to auto-approve, human review, or pipeline pause

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
[ Schema Diff ]
  - Today's observed schema compared against known baseline
  - New fields, variant drift, type conflicts detected
  - diff.json + diff.md written for audit trail
    |
    v
[ RAG Field Mapping ]  ← only fires if drift detected
  - New fields sent to LLM with full schema context
  - Confidence-tiered routing:
      ≥ 0.90  auto_approved  → pipeline continues
      ≥ 0.70  flagged_review → pipeline continues, flag logged
      < 0.70  pending_human  → logged, awaits human decision
  - All decisions written to mapping_registry (DuckDB)
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

### Why partitioned final output?

Each run produces an isolated partition (`dt=2026-04-24/`). If a mapping decision was wrong:

1. Delete only that partition: `rm -rf output/final/dt=2026-04-24/`
2. Correct the mapping in `mapping_registry`
3. Re-run dbt for that date only
4. All other partitions untouched

This is the lakehouse principle: raw data is immutable, transformations are replayable, no full reprocessing needed.

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

## Phase 2 in Action — Real Run Output

Schema drift detected on a live run with 1,558 XML files:

```
⚠️  Schema drift detected:
  New fields:    5
  Variant drift: 1
  Type conflicts: 0
  → RAG mapper will be triggered
```

The 5 unknown fields included both a deliberately injected test field (`InvoiceAmount`) and 4 fields already present in the XML feed but not covered by the baseline (`Initial`, `RequestedBy`, `Date`, `OrderLineRef`). The system found all of them in one pass.

We ran the same drift against both supported LLM providers on the same data, changing only `LLM_PROVIDER` in `.env`. No other modifications.

### Ollama (llama3.1) — tries harder, more willing to commit

```
[InvoiceAmount]  → line_total          (0.80)  ⚠️  REVIEW
[Initial]        → vendor_total        (0.50)  🛑 HUMAN REQUIRED
[RequestedBy]    → vendor_entity_name  (0.80)  ⚠️  REVIEW
[Date]           → document_date       (0.85)  ⚠️  REVIEW
[OrderLineRef]   → alloc_total         (0.81)  ⚠️  REVIEW

✅ Auto-approved: 0  |  ⚠️ Flagged review: 4  |  🛑 Human required: 1  |  ❓ Unknown: 0
```

### Claude (claude-haiku) — more conservative, more honest

```
[InvoiceAmount]  → UNKNOWN             (0.00)  🛑 HUMAN REQUIRED
  "Position is 'Party' — doesn't match header/line/allocation.
   Can't map despite name similarity to invoice_total."

[Initial]        → UNKNOWN             (0.00)  🛑 HUMAN REQUIRED
  "Single-character string at Contact/ActionSource position.
   No standard invoice field matches this pattern."

[RequestedBy]    → UNKNOWN             (0.00)  🛑 HUMAN REQUIRED
  "Person name (e.g. 'Martin, Shaun') at LineEntry position.
   Schema has no requester/person field at line level."

[Date]           → document_date       (0.82)  ⚠️  REVIEW
  "Date type with generic name in CrossReference position —
   aligns with document_date as the primary invoice date."

[OrderLineRef]   → line_number         (0.75)  ⚠️  REVIEW
  "Integer with examples 218, 973 — likely a line item reference,
   similar to line_number which sequentially identifies lines."

✅ Auto-approved: 0  |  ⚠️ Flagged review: 2  |  🛑 Human required: 3  |  ❓ Unknown: 3
```

### What this comparison shows

| Field | Ollama | Claude | Who's right? |
|-------|--------|--------|--------------|
| `InvoiceAmount` | `line_total` 0.80 | UNKNOWN | **Claude** — field is in a `Party` node, not a financial total |
| `Initial` | `vendor_total` 0.50 | UNKNOWN | **Claude** — it's a contact initial, not an amount |
| `RequestedBy` | `vendor_entity_name` 0.80 | UNKNOWN | **Claude** — person name at line level, no match in schema |
| `Date` | `document_date` 0.85 | `document_date` 0.82 | **Both agree** |
| `OrderLineRef` | `alloc_total` 0.81 | `line_number` 0.75 | **Claude** — integer reference maps better to line_number |

**Claude is more conservative and structurally precise.** Where Ollama forced a mapping, Claude cited the position mismatch and refused to guess. A wrong auto-mapping that passes review is worse than a human flag that gets resolved correctly.

**Ollama is more aggressive.** It found a candidate for every field (zero unknowns), which means fewer human reviews — but at the cost of questionable mappings like `Initial` → `vendor_total`.

Neither is universally better. The right choice depends on your tolerance for false positives vs false negatives:
- **Ollama**: fewer human interruptions, higher risk of silent wrong mappings
- **Claude**: more human reviews on genuinely ambiguous fields, lower risk of wrong auto-approvals

---

## Mapping Registry

Every decision is written to `mapping_registry` in DuckDB:

```sql
SELECT source_field, mapped_to, decision_type, confidence, llm_reasoning
FROM mapping_registry
WHERE run_date = '2026-04-24'
ORDER BY confidence DESC;
```

To correct a wrong mapping:

```sql
UPDATE mapping_registry
SET    status        = 'overridden',
       mapped_to     = 'invoice_total',
       overridden_by = 'manual: InvoiceAmount is header total, not line total'
WHERE  source_field = 'InvoiceAmount'
AND    run_date     = '2026-04-24';
```

Then delete the affected partition and re-run dbt. Raw data unchanged.

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
| LLM mapping | Ollama (local) or Claude API |

### Cloud Portability

The local stack is deliberately designed to mirror the architectural patterns of any major cloud platform. Each component maps cleanly to cloud-native equivalents without requiring changes to the core transformation logic.

| Local | AWS | Azure | GCP |
|-------|-----|-------|-----|
| DuckDB | Amazon Athena | Synapse Serverless | BigQuery |
| Local Parquet | S3 + Glue | ADLS Gen2 + Synapse | GCS + Dataproc |
| dbt Core | dbt Core (any platform) | dbt Core (any platform) | dbt Core (any platform) |
| Airflow (Docker) | MWAA / Glue Workflows | Azure Data Factory | Cloud Composer |

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
│   ├── schema_discovery.py  # Structural profiler — field coverage per variant
│   ├── schema_diff.py       # Compare today's schema vs known baseline
│   └── rag_mapper.py        # LLM field mapper + confidence scorer + registry
├── dbt/
│   ├── models/
│   │   ├── staging/         # stg_detailed_invoice, stg_summary_invoice
│   │   ├── intermediate/    # int_invoices_deduped
│   │   └── marts/           # mart_invoices (final analytical table)
│   └── tests/
├── airflow/
│   └── dags/
│       └── xml_drift_pipeline.py  # Full pipeline DAG with RAG branch
├── docker/
│   ├── docker-compose.yml   # Airflow + Postgres stack
│   ├── Dockerfile           # Custom Airflow image with project deps
│   └── requirements.txt
├── tests/
│   └── test_rag_flow.py     # 33 integration tests — no API key needed
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
pip install lxml polars dbt-duckdb faker duckdb

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

### LLM Configuration

Set in `docker/.env` before starting:

**Option A — Ollama (free, local, no API key)**

```bash
# Install Ollama: https://ollama.ai
# Pull any model: ollama pull llama3.1
LLM_PROVIDER=ollama
OLLAMA_MODEL=llama3.1        # or mistral-nemo, gemma2 — any pulled model works
OLLAMA_HOST=http://host.docker.internal:11434   # Mac/Windows
# OLLAMA_HOST=http://172.17.0.1:11434           # Linux
```

**Option B — Claude API (better mapping quality)**

```bash
LLM_PROVIDER=claude
ANTHROPIC_API_KEY=sk-ant-...
```

### Run tests (no API key, no Ollama needed)

```bash
python tests/test_rag_flow.py -v
# → 33 tests, 0 failures
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

- [x] Schema diff engine — detect new fields, variant drift, type conflicts
- [x] LLM-assisted field mapping — Claude API + Ollama (local) support
- [x] Confidence-tiered routing — auto_approved / flagged_review / pending_human
- [x] Mapping registry — immutable audit trail in DuckDB
- [x] Airflow integration — ShortCircuitOperator, RAG branch wired into full pipeline
- [x] 33 integration tests — full RAG flow, no API key needed
- [x] Ollama vs Claude comparison — same drift, both providers tested live
- [ ] Flask human review UI — approve/reject pending mappings via web interface
- [ ] Airflow sensor — block pipeline on pending_human until resolved
- [ ] Corpus self-improvement — approved mappings feed back into baseline

### Phase 3 — Observability

- [ ] Apache Superset dashboard
- [ ] Incremental loads
- [ ] Drift trend reporting

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
