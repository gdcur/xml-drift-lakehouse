# ADR-0001: Apache Iceberg as the lakehouse table format

- Status: Proposed
- Date: 2026-10-08
- Supersedes: none
- Depends on: ADR-0000 (baseline)

## Context

The landing zone is plain Parquet (ADR-0000, D3): unpartitioned, fully rewritten on each run, with no table metadata, no snapshots and no schema history. The roadmap lists "Incremental loads" as open, and the schema-drift workflow (D6) has no table-level way to record that a column was added.

Apache Iceberg addresses these gaps: snapshot isolation, hidden partitioning, schema evolution that does not rewrite data, and time travel. Athena has native Iceberg table support, and Athena was used in production on the OpenInvoice pipeline. This is a portfolio project, so the Athena link is a natural extension of a tool I used in production. It is not production experience with Iceberg, and all documentation must say so.

## Decision drivers

1. Close the roadmap item credibly and in a short time.
2. Stay reproducible: a fresh clone must run offline with no cloud account.
3. Keep a defensible link to the Athena production background.
4. Do not regress the existing Parquet path, the Snowflake target or the 51 dbt tests.

## Options considered

**A. Athena only.** Strongest link to the production story. Requires an AWS account and S3, and fresh clones cannot run it.

**B. PyIceberg + DuckDB only.** Fully local and reproducible. No link to Athena.

**C. Hybrid.** PyIceberg with a local SQL catalog (SQLite) and a local warehouse directory as the default. An optional Athena/Glue leg uses the same writer against a Glue catalog on S3.

## Decision

Option C.

### Design

- **Tables:** `invoices` and `line_items`, keyed on the existing surrogate keys (`sk`, ADR-0000 D2).
- **Writer:** a new `ingestion/iceberg_writer.py`. `parse_xmls` keeps producing Parquet. The writer reads it with Polars, converts to Arrow and merges into Iceberg on `sk`. No pandas in this module.
- **Upsert vs rebuild:** use PyIceberg's upsert if the pinned version supports it. Otherwise fall back to a partition-scoped overwrite. The spike (below) decides.
- **Partitioning:** `month(document_date)` as the only partition field. `variant` was considered and rejected as a partition field: it has two values with a 60/40 skew, so it prunes little. At about 550k line items the partitioning is demonstrative, not a performance need, and the README must say so.
- **Schema evolution:** when the schema diff engine (D6) approves a new field, the writer applies `ADD COLUMN` to the Iceberg table instead of rewriting data.
- **Reads in dbt:** a new source reads Iceberg through DuckDB `iceberg_scan`, behind an environment flag. The Parquet source remains the default. The Snowflake target is untouched.
- **Catalogs:** SQLite (`sqlite:///...`) locally. Glue for the optional Athena leg.
- **Athena leg:** minimal infrastructure (S3 bucket, Glue database, Athena workgroup), documented queries, time travel, and maintenance commands. Clearly labeled optional.

### Spike required before implementation

These are assumptions to verify, not facts:

1. Which DuckDB version is authoritative: the pin (1.1.3) or a deliberate bump. Confirm what the `iceberg` extension supports in the chosen version. The design only needs DuckDB to read Iceberg, since writes go through PyIceberg.
2. How `iceberg_scan` is pointed at a table created through a SQLite catalog (current metadata file resolution, moved-path handling).
3. Whether the pinned PyIceberg version supports upsert.
4. The physical type of `document_date` in the current Parquet output, since the month transform needs a date or timestamp type.

## Consequences

- Positive: snapshots and time travel on the landing zone, incremental loads become possible, schema drift gets a table-level record, and the Athena story gets a concrete artifact.
- Negative: new dependencies (pyiceberg, pyarrow), a second read path to keep in parity, and extra CI surface.
- The parity risk is mitigated by tests: Parquet vs Iceberg row counts, `sk` uniqueness after upsert, partition spec assertions, and a schema-evolution test.

## Out of scope

Compaction scheduling, row-level deletes, Snowflake Iceberg tables, and any change to the resume. The resume decision comes last and keeps the portfolio-vs-production distinction intact.

## Status log

- 2026-10-08: Proposed. Becomes Accepted when the spike confirms the four assumptions above.
