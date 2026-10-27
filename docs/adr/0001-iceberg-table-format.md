# ADR-0001: Apache Iceberg as the lakehouse table format

- Status: Accepted
- Date: 2026-10-08
- Updated: 2026-10-09
- Supersedes: none
- Depends on: ADR-0000 (baseline), ADR-0002 (business-key identity and file lineage)

## Context

The landing zone is plain Parquet (ADR-0000, D3): unpartitioned and fully rewritten on each run, with no table metadata, no snapshots and no schema history. This is by design: the original pipeline was built as a full load, re-reads every XML file on every run, and does not handle changes to invoices after they were loaded.

To manage daily variations (details, statuses, amendments), this project introduces an Iceberg layer, which suits the required upserts well. Iceberg provides snapshot isolation, hidden partitioning, schema evolution that does not rewrite data, and time travel. Amazon Athena has native Iceberg table support, and Athena is the engine of the real-world pipeline this project is modeled on (ADR-0000). An optional Athena leg is therefore a natural extension of the same design, described below. Iceberg itself is new in this project.

To exercise upserts, the repo now has a reproducible multi-day dataset (`scripts/daily_generator.py`, published as `data/daily/`, 6 days and 40 XML files). It contains new invoices, updated invoices (same `invoice_id`, changed content, valid approval workflow steps), exact re-deliveries, lines added or removed by an update, and new header fields that appear mid-run. Its `manifest.csv` is the expected result for the tests.

## Decision drivers

1. Close the roadmap item credibly and in a short time.
2. Stay reproducible: a fresh clone must run offline with no cloud account.
3. Keep a link to Amazon Athena with the Glue catalog, the engine of the pipeline this project is modeled on, so the design is shown to carry over from the local setup to a managed cloud engine.
4. Do not regress the existing Parquet path, the Snowflake target or the 51 dbt tests.

## Options considered

**A. Athena only.** Strongest link to the real pipeline this project is modeled on. Requires an AWS account and S3, and fresh clones cannot run it.

**B. PyIceberg + DuckDB only.** Fully local and reproducible. No link to Athena.

**C. Hybrid.** PyIceberg with a local SQL catalog (SQLite) and a local warehouse directory as the default. An optional Athena/Glue leg uses the same writer against a Glue catalog on S3.

## Decision

Option C.

Rationale: it keeps the project runnable by anyone from a fresh clone, with no cloud account, no cost and tests that run in seconds, which option A cannot offer. It also shows that the same design runs on a managed cloud engine, which option B cannot show. The price is a second target to maintain, so the Athena leg is kept small and optional.

### Design

- **Tables:** `invoices` and `line_items`.
- **Identity and upsert keys:** from ADR-0002, not from the file-based `sk` of ADR-0000. Invoices are keyed on `invoice_id`; line items on `(invoice_id, line_number)`. The identifier columns must be declared as required (non-null) in the Iceberg schema.
- **Writer:** a new `ingestion/iceberg_writer.py`. `parse_xmls` keeps producing Parquet. The writer reads it with Polars, converts to Arrow, casts to the table schema and upserts. No pandas in this module.
- **Fixed schema:** the Iceberg schema is declared explicitly in the writer and is not inferred from each batch (see spike result 4). New fields approved by the schema diff engine (ADR-0000, D6) are added with `ALTER TABLE ... ADD COLUMN`, never by re-inferring.
- **Removed lines:** an update can remove a line. A plain upsert on `(invoice_id, line_number)` would leave the removed line behind as a stale row. The writer therefore replaces all lines of an invoice when a new version arrives: delete by `invoice_id`, then append the new lines. The test dataset covers this case.
- **Partitioning:** `invoices` is partitioned by `month(document_date)`. `variant` was considered and rejected as a partition field: it has two values with a 60/40 skew, so it prunes almost nothing. `line_items` is not partitioned (decided 2026-10-09): a line has no date of its own, and copying the invoice date into every line would add a column to keep in sync for no real gain at this data size. At about 550k line items the partitioning is demonstrative, not a performance need, and the README must say so.
- **Reads in dbt:** a new source reads Iceberg through DuckDB `iceberg_scan`, behind an environment flag. The Parquet source remains the default. The Snowflake target is untouched. The source finds the table through a version hint file written by the writer (see "Decisions taken after the spike").
- **Catalogs:** SQLite (`sqlite:///...`) locally. Glue for the optional Athena leg.
- **Athena leg:** minimal infrastructure (S3 bucket, Glue database, Athena workgroup), documented queries, time travel, and maintenance commands. Clearly labeled optional.

### Versions

- DuckDB: bump from the 1.1.3 pin to the 1.5.x line (tested: 1.5.6). Reasons: 1.1.3 has no wheel for Python 3.14, cannot read the table when the warehouse path carries a `file://` scheme, and has no snapshot time travel. Updating `docker/requirements.txt` is a separate step.
- PyIceberg 0.12.0, installed with the `pyiceberg-core` extra.
- dbt-core 1.11.8 and dbt-duckdb 1.10.1 stay as they are.

## Spike results (verified by running them on 2026-10-09)

1. **DuckDB versions.** The `iceberg` extension is read-only in both versions tested, which is enough because PyIceberg does all the writing. 1.1.3 reads PyIceberg tables only when the warehouse path has no `file://` scheme and has no snapshot time travel. 1.5.6 reads both forms and supports time travel (10 rows at snapshot 1, 5 after the delete in the spike).
2. **dbt compatibility.** With dbt-duckdb 1.10.1 and dbt-core 1.11.8, `dbt build` passes all 51 tests and 4 models on both DuckDB 1.5.6 and 1.1.3, after the staging bug fix on `main` (commit 9e6a1ea: `stg_summary_invoice` filtered on the wrong variant).
3. **SQLite catalog and `iceberg_scan`.** The SQLite catalog does not write `version-hint.text`, so scanning the table directory fails. Two ways work: pass the current metadata file from the catalog (`table.metadata_location`), or have the writer write `version-hint.text` itself. `allow_moved_paths` works only on a table directory with a version hint, not on a direct metadata file in 1.1.3.
4. **PyIceberg upsert.** Confirmed in 0.12.0: changed rows update, a repeated batch changes nothing, new rows insert, and `ADD COLUMN` works without rewriting data (old rows read as null). Three requirements: the identifier column must be required; writes to a month-partitioned table need the `pyiceberg-core` extra; Arrow batches must be cast to the table schema before writing. Upsert is copy-on-write, so each run adds a delete snapshot and an append snapshot.
5. **Physical types.** `document_date` is a VARCHAR in ISO `YYYY-MM-DD` form in the current Parquet output; casting to DATE works and month partitioning works after the cast. `line_items` has no `document_date` column. The parser also emits Arrow null-typed columns when a column is all null in a run (20 of them in the sample run), and PyIceberg rejects them. Types therefore vary between runs, which is why a fixed explicit schema is required.

## Decisions taken after the spike (2026-10-09)

1. **`line_items` is not partitioned.** Only `invoices` is partitioned, by month.
2. **The dbt source finds the table through a version hint.** After every write, the writer creates or updates `version-hint.text` in the table's metadata folder. The dbt source then points at the fixed table folder and passes `version_name_format='%s%s.metadata.json'`, plus `allow_moved_paths` so the same table can be read from the host and from the Docker/Airflow containers, where the absolute paths differ. The alternative (the writer publishing the path of the newest metadata file for dbt to read) was rejected because it adds state that must travel with every run and does not solve moved paths. This step is needed only for the local DuckDB read path: PyIceberg and Athena ask their catalog (SQLite or Glue) which metadata file is current.
3. **The writer uses a fixed explicit schema**, for the reasons in spike result 5.

## Implementation notes (2026-10-26)

Found while writing the load path (step W2, `ingestion/iceberg_writer.py --load`):

1. **`Table.upsert` is not used.** In PyIceberg 0.12.0 it fails on files written before an `ADD COLUMN` (the existing rows come back without the new column and the cast to the batch schema raises). The schema evolution test in `tests/test_iceberg_load.py` reproduces it. The writer does what upsert does underneath, delete by key and append, as one transaction per table.
2. **Write order.** `line_items` is written first and `invoices` last, so the invoice row is the commit marker. PyIceberg has no multi-table transaction on a SQL catalog. A run that dies in between leaves lines from a newer version than their invoice; `--check` flags it and re-running the batch repairs it.
3. **Re-delivery.** The same content delivered in another file moves the lineage columns of the one existing row (ADR-0002: the table keeps the lineage of the latest version). The same file again, or re-parsed later, is a no-op: `ingested_at` is excluded from the comparison.
4. **Late older versions.** A delivery with an older `action_datetime` than the stored one is ignored ("stale"), even when it is ingested later.
5. **Fixed schema, evolving table.** The batch is cast to the table's *current* schema, so columns added with `ADD COLUMN` are filled with null when a batch does not carry them. Unknown batch columns are dropped; new fields enter through the schema diff engine.
6. **Type contract for dbt.** Iceberg stores real `DATE` and `TIMESTAMP` columns, while the Parquet path and Snowflake hand the models text. The mart relies on the text form (`left(document_date, 4)`, `coalesce` with `null::varchar`), and DuckDB refuses to mix `DATE` and `VARCHAR`. The Iceberg branch of the staging models therefore casts the temporal columns back to the same ISO text, so the models stay identical on every source. Found by running `dbt build` with `XDL_SOURCE=iceberg` on the full daily set.
7. **Verification.** DuckDB reading through `version-hint.text` is covered by `test_duckdb_reads_through_the_hint`, which skips when the `iceberg` extension cannot be downloaded.

## Consequences

- Positive: snapshots and time travel on the landing zone, incremental loads become possible, schema drift gets a table-level record, earlier versions of an invoice stay reachable through snapshots, and the Athena story gets a concrete artifact.
- Negative: new dependencies (pyiceberg, pyarrow), a second read path to keep in parity, a DuckDB version bump, and extra CI surface.
- The parity risk is mitigated by tests: Parquet vs Iceberg row counts, key uniqueness after upsert, removed lines not left behind, repeated batches changing nothing, partition spec assertions, and a schema-evolution test.

## Out of scope

Compaction scheduling, row-level deletes beyond the per-invoice line replacement, and Snowflake Iceberg tables.

## Status log

- 2026-10-08: Proposed.
- 2026-10-09: Updated with spike results, the version decision and the dependency on ADR-0002.
- 2026-10-09: Three open decisions taken (unpartitioned `line_items`, version hint file, fixed explicit schema). Remains Proposed until reviewed.
- 2026-10-09: Accepted after review.
- 2026-10-26: Write path implemented (W2); implementation notes added. `Table.upsert` replaced by transactional delete + append.
