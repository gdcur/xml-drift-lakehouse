# ADR-0002: Business-key identity and file lineage

- Status: Proposed
- Date: 2026-10-08
- Supersedes: ADR-0000 decision D2 (surrogate keys)
- Related: ADR-0001 (Iceberg table format)

## Context

Today the surrogate key `sk` is a truncated SHA-256 (16 hex characters) of `source_file`, `invoice_id` and, for lines, `line_number` (ADR-0000, D2). Because the file name is part of the hash, the same invoice delivered in a different file gets a different `sk`.

The pipeline is currently a one-shot full rebuild: every run re-reads every XML and rewrites the Parquet files. Daily movements were never implemented. Once invoices arrive over several days, the same `invoice_id` will appear again in a later file with updated content, for example `inv/day1/003.xml` and `inv/day4/003.xml`. The file name may or may not repeat. The identity of a record must therefore come from the business data, not from the file that carried it.

## Decision

### 1. Identity comes from `invoice_id`

`invoice_id` is the system-assigned number on the XML root. It identifies an invoice regardless of file name or folder.

- Invoice key: `sk` = truncated SHA-256 of `invoice_id`.
- Line key: `line_sk` = truncated SHA-256 of `invoice_id` and `line_number`.
- Column names stay as they are; only the values change.
- The upsert key in Iceberg (ADR-0001) is `invoice_id` for invoices and `(invoice_id, line_number)` for lines.

### 2. File lineage is stored as data, not identity

Every row records where it came from:

- `source_file`: the file name, for example `003.xml`.
- `source_path`: the path relative to the input root, for example `day4/003.xml`, so two files with the same name remain distinguishable and the original document can be found quickly, both for our own checks and for business users.
- `ingested_at`: recorded per file or per input batch, not once per run, so it can order versions.

The current table keeps the lineage of the latest version of each invoice. Earlier versions, and the paths they came from, remain available through Iceberg snapshots (time travel).

### 3. Which version wins

When the same `invoice_id` appears more than once, the latest version wins: greatest `action_datetime` first, then latest `ingested_at`, then `source_path` as a final deterministic tie-break. This replaces the current behavior in which a run-wide timestamp made the tie-break meaningless.

## Consequences

- Breaking change: every `sk` and `line_sk` value changes. Affected: `parser.py`, the dbt models and their tests, the Snowflake load, and the README. All 51 dbt tests must still pass after the change.
- Re-delivery of an unchanged invoice becomes a no-op upsert instead of a second row.
- Truncation to 16 hex characters is kept for continuity. At this data size collision risk is negligible, and the choice is recorded here so it is deliberate.
- ADR-0000 D2 is superseded by this ADR. ADR-0000 itself is left unedited, apart from its status log.

## Open questions

1. Whether to hash `invoice_id` alone, or also include a vendor or client code if `invoice_id` is only unique within a source system. In the fictional dataset it is unique; to be confirmed against the schema.
2. The exact input root used to compute `source_path`.

## Status log

- 2026-10-08: Proposed.
