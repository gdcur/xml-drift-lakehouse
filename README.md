# xml-drift-lakehouse

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
- **Surrogate key injection** — a timestamp-based epoch key is injected at ingestion, propagated through all child records to maintain join integrity across entities
- **Relational explosion** — hierarchical XML nodes are flattened into separate tables preserving parent-child relationships
- **Variant reconciliation** — multiple structural variants of the same entity are unified at the analytical layer via UNION ALL and COALESCE across divergent field names
- **Defensive casting** — CAST/COALESCE/NULLIF patterns handle empty strings, null fields and type mismatches across variants

---

## Architecture

```
XML Source
    |
    v
[ Ingestion Layer ]
  - XML parsed via xmltodict
  - Surrogate key injected (epoch timestamp)
  - Stored as JSON / Parquet in landing zone
    |
    v
[ Schema Discovery ]
  - DynamicFrame-style schema-on-read
  - Structural variants auto-discovered
  - Raw tables generated per variant
    |
    v
[ Relational Layer ]
  - Hierarchical nodes exploded into tables
  - Parent-child joins via surrogate key
  - Partitioned by date for incremental loads
    |
    v
[ Reconciliation Layer ]
  - Variant UNION ALL with COALESCE on divergent fields
  - Defensive type casting
  - Consumer-ready analytical views
    |
    v
[ Analytics ]
  - Clean, queryable datasets
  - BI / reporting ready
```

---

## Local Stack

This toolkit runs entirely locally without cloud dependencies, making it portable and easy to evaluate.

| Layer | Tool |
|-------|------|
| XML parsing | Python, xmltodict |
| Storage | Local Parquet files |
| Query engine | DuckDB |
| Transformation | dbt Core |
| Orchestration | Apache Airflow (Docker) |
| Visualization | Apache Superset |

The architecture is deliberately compatible with cloud-native equivalents:

| Local | Cloud equivalent |
|-------|-----------------|
| DuckDB | Amazon Athena |
| Local Parquet | Amazon S3 + AWS Glue |
| dbt Core | dbt Core on any platform |
| Airflow (Docker) | AWS Glue Workflows / MWAA |

---

## Repository Structure

```
xml-drift-lakehouse/
├── ingestion/
│   ├── parser.py          # XML to JSON/Parquet with surrogate key injection
│   ├── schema_discovery.py # Schema-on-read, variant detection
│   └── compaction.py      # Small file compaction for query performance
├── models/
│   ├── staging/           # Raw variant tables, one per structural variant
│   ├── intermediate/      # Exploded relational tables
│   └── marts/             # Reconciled consumer-ready views
├── dbt/
│   ├── models/            # dbt transformation models
│   ├── tests/             # dbt data quality tests
│   └── dbt_project.yml
├── airflow/
│   └── dags/              # Pipeline orchestration DAGs
├── docker/
│   └── docker-compose.yml # Full local stack
├── data/
│   └── sample/            # Sample XML files with intentional schema drift
└── docs/
    └── architecture.md    # Architecture decisions and patterns
```

---

## Quickstart

```bash
# Clone the repository
git clone https://github.com/gdcur/xml-drift-lakehouse
cd xml-drift-lakehouse

# Start the local stack
docker compose up -d

# Run ingestion on sample data
python ingestion/parser.py --source data/sample/

# Run dbt transformations
cd dbt && dbt run

# Open Superset dashboard
# http://localhost:8088
```

---

## Roadmap

- [x] Core XML parser with surrogate key injection
- [x] Schema-on-read variant detection
- [x] Local Parquet storage layer
- [ ] dbt staging and mart models
- [ ] Multi-variant UNION ALL reconciliation layer
- [ ] Airflow DAG for end-to-end orchestration
- [ ] dbt data quality tests and freshness checks
- [ ] Sample dataset with 3+ structural variants demonstrating drift
- [ ] Documentation of surrogate key propagation pattern
- [ ] AWS Glue/Athena deployment guide

---

## Background

This toolkit grew out of a production problem involving large-scale XML ingestion where the source data had accumulated years of structural drift: multiple variants, divergent field names, and no stable schema to code against.

The patterns here — schema-on-read, surrogate key propagation, variant reconciliation — are generalized from that experience and designed to work with any XML source that has accumulated structural drift over time, regardless of industry or platform.

The local stack (DuckDB + dbt + Airflow) replicates the same architectural patterns without cloud dependencies, making the approach portable across environments.

---

## Related

- [ercot-plan-ranker](https://github.com/gdcur/ercot-plan-ranker) — A production-style lakehouse demo using the same patterns applied to ERCOT electricity market data

---

## License

MIT