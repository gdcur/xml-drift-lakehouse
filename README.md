# xml-drift-lakehouse

A production-grade, schema-on-read XML ingestion toolkit with automated structural drift detection and AI-assisted field mapping.

Built as a portfolio project to demonstrate real-world data engineering patterns: immutable raw storage, replayable transformations, and a RAG pipeline that handles schema evolution without manual intervention.

---

## The Problem

XML feeds from vendors are never stable. Field names change. New fields appear. A field that was `InvoiceTotal` in Q1 becomes `InvoiceAmt` in Q3. A field that only appeared in detailed invoices starts appearing in summary invoices.

In production, the naive solution is to maintain hand-crafted transformation logic — CTEs, COALESCE chains, XML parsing rules — and update them manually every time a vendor sends a new structure. At scale (750K+ invoices, 10-year history, 14 structural variants), this becomes a maintenance burden and a reliability risk.

This toolkit automates the detection and mapping of structural drift:

1. **Detect** — compare today's observed XML schema against the known baseline
2. **Map** — send new fields to an LLM with full schema context for semantic mapping
3. **Score** — assign confidence and route to auto-approve, human review, or pipeline pause
4. **Audit** — every decision recorded in an immutable registry, replayable per partition

---

## Architecture

```
Daily Run (Airflow)
    │
    ├─ parse_xmls          Parse all XMLs → Parquet (schema-on-read)
    │
    ├─ schema_diff         Compare today's schema vs known baseline
    │                      → output/schema_diff/{date}/diff.json
    │                      → output/schema_diff/{date}/diff.md
    │
    ├─ check_drift         ShortCircuitOperator
    │                      → no drift: skip rag_mapping, continue to dbt
    │                      → drift detected: proceed to rag_mapping
    │
    ├─ rag_mapping         (only fires if drift detected)
    │   ├─ Full schema corpus + new field metadata → LLM prompt
    │   ├─ LLM suggests mapping + confidence + reasoning
    │   └─ Confidence tier decision:
    │       ├─ ≥ 0.90  auto_approved  → pipeline continues
    │       ├─ ≥ 0.70  flagged_review → pipeline continues, flag logged
    │       └─ < 0.70  pending_human  → logged, pipeline uses best guess
    │                                   (PRIVATE: Airflow sensor blocks here)
    ├─ dbt_staging         stg_detailed_invoice, stg_summary_invoice
    ├─ dbt_intermediate    int_invoices_deduped
    ├─ dbt_mart            mart_invoices
    ├─ dbt_test            data quality assertions
    └─ cleanup_staging     remove staging Parquets after successful mart build
```

### Why partitioned final output?

Each run produces an isolated partition (`dt=2026-04-24/`). If a mapping decision was wrong:

1. Delete only that partition: `rm -rf output/final/dt=2026-04-24/`
2. Correct the mapping in `mapping_registry`
3. Re-run dbt for that date only
4. All other partitions untouched

This is the lakehouse principle: raw data is immutable, transformations are replayable, no full reprocessing needed.

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

The 5 unknown fields were: `InvoiceAmount` (added to test the trigger), plus `Initial`, `RequestedBy`, `Date`, and `OrderLineRef` — fields that were already present in the XML feed but not covered by the baseline. The system found all of them in one pass.

We ran the same drift against both providers on the same data, with only `LLM_PROVIDER` changed in `.env`. No other modifications.

---

### Ollama (llama3.1) — tries harder, more willing to commit

```
[InvoiceAmount]  → line_total          (0.80)  ⚠️  REVIEW
[Initial]        → vendor_total        (0.50)  🛑 HUMAN REQUIRED
[RequestedBy]    → vendor_entity_name  (0.80)  ⚠️  REVIEW
[Date]           → document_date       (0.85)  ⚠️  REVIEW
[OrderLineRef]   → alloc_total         (0.81)  ⚠️  REVIEW

✅ Auto-approved: 0  |  ⚠️ Flagged review: 4  |  🛑 Human required: 1  |  ❓ Unknown: 0
```

---

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

---

### What this comparison shows

| Field | Ollama | Claude | Who's right? |
|-------|--------|--------|--------------|
| `InvoiceAmount` | `line_total` 0.80 | UNKNOWN | **Claude** — field is in a `Party` node, not a financial total |
| `Initial` | `vendor_total` 0.50 | UNKNOWN | **Claude** — it's a contact initial, not an amount |
| `RequestedBy` | `vendor_entity_name` 0.80 | UNKNOWN | **Claude** — person name at line level, no match in schema |
| `Date` | `document_date` 0.85 | `document_date` 0.82 | **Both agree** |
| `OrderLineRef` | `alloc_total` 0.81 | `line_number` 0.75 | **Claude** — integer reference maps better to line_number |

**Claude is more conservative and structurally precise.** Where Ollama forced a mapping, Claude cited the position mismatch and refused to guess. That's actually the correct behavior — a wrong auto-mapping that passes review is worse than a human flag that gets resolved correctly.

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

Registry schema:

```
id              INTEGER    — auto-increment
run_date        DATE       — partition key
source_field    VARCHAR    — new field name as seen in XML
source_path     VARCHAR    — XML path / parent elements
source_type     VARCHAR    — inferred type (decimal, string, date, integer)
mapped_to       VARCHAR    — suggested known field name
confidence      DOUBLE     — LLM confidence score (0.0–1.0)
decision_type   VARCHAR    — auto_approved | flagged_review | pending_human
llm_provider    VARCHAR    — ollama | claude
llm_reasoning   VARCHAR    — LLM explanation
status          VARCHAR    — active | pending | overridden
overridden_by   VARCHAR    — human override note
created_at      TIMESTAMP
updated_at      TIMESTAMP
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

## Setup

### Prerequisites

- Python 3.11+
- Docker + Docker Compose
- One of:
  - **Ollama** (free, local) — https://ollama.ai
  - **Anthropic API key** (better quality) — https://console.anthropic.com

### Install

```bash
git clone https://github.com/YOUR_USERNAME/xml-drift-lakehouse.git
cd xml-drift-lakehouse
pip install -r requirements.txt
```

### LLM Configuration

**Option A — Ollama (free, local, no API key)**

Install Ollama, then pull any model:

```bash
ollama pull llama3.1       # recommended — good balance of quality and speed
ollama pull mistral-nemo   # lighter and faster
ollama pull gemma2         # good alternative
```

Any model you have pulled will work. Set in `docker/.env`:

```bash
LLM_PROVIDER=ollama
OLLAMA_MODEL=llama3.1      # or any model you pulled
OLLAMA_HOST=http://host.docker.internal:11434   # Mac/Windows
# OLLAMA_HOST=http://172.17.0.1:11434           # Linux
```

**Option B — Claude API (better mapping quality)**

```bash
LLM_PROVIDER=claude
ANTHROPIC_API_KEY=sk-ant-...
```

Cost note: Phase 2 only fires when an unknown field is detected. On a stable feed this is rare — typically 5-10 fields on first run, then almost never. Each mapping call is ~500 tokens. Cost per run: fractions of a cent.

### Run tests (no API key, no Ollama needed)

```bash
python tests/test_rag_flow.py -v
# → 33 tests, 0 failures
```

### Run schema diff standalone

```bash
python ingestion/schema_diff.py --src ./data/sample
# → output/schema_diff/{today}/diff.json
# → output/schema_diff/{today}/diff.md
```

### Run RAG mapper standalone

```bash
# With Ollama
LLM_PROVIDER=ollama OLLAMA_MODEL=llama3.1 python ingestion/rag_mapper.py

# With Claude
LLM_PROVIDER=claude ANTHROPIC_API_KEY=sk-... python ingestion/rag_mapper.py

# Dry run — no DB writes, for inspection
LLM_PROVIDER=ollama python ingestion/rag_mapper.py --dry-run
```

### Full pipeline (Docker + Airflow)

```bash
docker compose up -d
# Airflow UI: http://localhost:8080
# Username: airflow / Password: airflow
```

Trigger the DAG manually or wait for the 06:00 UTC schedule. On first run with unknown fields, `rag_mapping` will fire and write decisions to `mapping_registry`.

---

## Components

| File | Description |
|------|-------------|
| `ingestion/parser.py` | XML parser — handles both DetailedInvoice and SummaryInvoice structural variants |
| `ingestion/schema_diff.py` | Compares observed XML schema vs baseline, produces `diff.json` + `diff.md` |
| `ingestion/rag_mapper.py` | LLM field mapper + confidence scorer + mapping_registry writer |
| `airflow/dags/xml_drift_pipeline.py` | Airflow DAG — full pipeline with ShortCircuitOperator for RAG branch |
| `dbt/` | dbt models: staging → intermediate → mart |
| `tests/test_rag_flow.py` | 33 integration tests — full RAG flow with mock LLM, no API key needed |

---

## Project Structure

```
xml-drift-lakehouse/
├── ingestion/
│   ├── parser.py               # XML → Parquet
│   ├── schema_diff.py          # Schema drift detection
│   └── rag_mapper.py           # LLM field mapper + registry
├── dbt/
│   ├── models/staging/
│   ├── models/intermediate/
│   ├── models/mart/
│   └── tests/
├── airflow/
│   └── dags/xml_drift_pipeline.py
├── docker/
│   ├── docker-compose.yml
│   └── .env                    # LLM config goes here
├── data/
│   └── sample/                 # Test XML files
├── output/                     # Generated (gitignored)
│   ├── schema_diff/
│   └── lakehouse.duckdb
└── tests/
    └── test_rag_flow.py        # 33 integration tests
```

---

## Design Decisions

**Why LLM-only and not vector embeddings?**
The public repo sends the full schema corpus directly in the LLM prompt. This requires no local embedding model, no PyTorch, no GPU — runs anywhere with just Ollama or an API key. The corpus is ~30 fields, well within context window limits. The private repo upgrades this to `sentence-transformers` + pgvector for pre-computed embeddings and faster lookups at scale.

**Why DuckDB for the registry?**
Zero infrastructure. The same DuckDB instance used for local Athena-replacement is used for the registry. In production this would be Postgres + pgvector.

**Why not trust LLM confidence alone?**
LLMs are confident about things they shouldn't be. The confidence score from the LLM is one signal — the system also considers type and position independently. A field where the LLM is confident but structurally mismatched will still land in `flagged_review`. The AI suggests, the system validates.

**Why ShortCircuitOperator and not always running RAG?**
On most daily runs there is no drift — the XML structure is stable. Running an LLM call on every pipeline execution would be wasteful and add latency. The short-circuit means the RAG branch adds zero overhead on clean runs.

**Why partition by run_date?**
Because the alternative is full reprocessing when a mapping is corrected. With partitioned output, a correction affects only the day(s) where the wrong mapping was applied. This is the same pattern used in production lakehouses (Hive, Iceberg, Delta Lake).

---

## Phase 1 Coverage

Phase 1 covers XML parsing and the full dbt model stack. See commit history for:
- `parser.py` — dual-variant XML parsing with schema-on-read
- `stg_detailed_invoice.sql`, `stg_summary_invoice.sql`
- `int_invoices_deduped.sql` — SCD Type 1 deduplication
- `mart_invoices.sql` — unified reconciled view
- dbt data quality tests

---

## Private Repo Upgrade Path

Every simplification in this public repo is marked with `# PRIVATE:` comments in the code. The upgrade path:

| Public (this repo) | Private (production) |
|-------------------|---------------------|
| LLM-only mapping (full corpus in prompt) | `sentence-transformers` + pgvector |
| DuckDB registry | Postgres + pgvector |
| Pipeline continues on `pending_human` | Airflow sensor blocks until human resolves |
| No UI for human review | Flask approval UI |
| Single output directory | Date-partitioned output with `promote_to_final` |

---

## License

MIT
