# Databricks notebook source
# MAGIC %md
# MAGIC # xml-drift-lakehouse on Databricks
# MAGIC
# MAGIC Same XML invoices (`data/daily`, 40 files, 25 invoices) as the DuckDB / Iceberg / dbt version,
# MAGIC processed with Spark and Delta Lake in Unity Catalog: **raw → curated → consumer**.
# MAGIC The notebook ends by reconciling the result with the DuckDB side.
# MAGIC
# MAGIC **Before running:** upload `daily_xml.zip` (made by `databricks/make_landing_zip.py`) to the volume
# MAGIC `workspace.xml_drift.landing`. Optional, for the reconciliation cell: also upload `reconcile_duckdb.csv`
# MAGIC (made by `databricks/export_duckdb_reference.py`).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Setup (not part of the pipeline): schema, volume, stage the sample files
# MAGIC Unity Catalog names objects as `catalog.schema.object`. A volume is a governed folder of files.

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.xml_drift")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.xml_drift.landing")

import os, zipfile

BASE = "/Volumes/workspace/xml_drift/landing"
with zipfile.ZipFile(f"{BASE}/daily_xml.zip") as z:   # stage the sample data; in production files land here directly
    z.extractall(BASE)

xml_files = [os.path.join(d, f) for d, _, fs in os.walk(f"{BASE}/daily") for f in fs if f.endswith(".xml")]
print(len(xml_files), "xml files")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Raw: each file stored untouched, plus lineage
# MAGIC One row per file: the XML text, a content hash, the source path, and the invoice id and variant read from the text.
# MAGIC `MERGE` on (path, hash) makes the load idempotent: running it twice adds nothing.

# COMMAND ----------

from pyspark.sql import functions as F

SRC = "/Volumes/workspace/xml_drift/landing/daily/day_*/*.xml"
RAW = "workspace.xml_drift.raw_xml"

raw_new = (
    spark.read.format("text").option("wholetext", "true").load(SRC)   # one row per file
    .select(
        F.col("_metadata.file_path").alias("source_path"),             # lineage from Spark itself
        F.regexp_extract(F.col("_metadata.file_path"), r"(day_\d+/[^/]+)$", 1).alias("source_file"),
        F.col("value").alias("xml"),
        F.sha2(F.col("value"), 256).alias("content_sha256"),
        F.regexp_extract(F.col("value"), r'invoice_id="(\d+)"', 1).alias("invoice_id"),
        F.when(F.col("value").contains("<fops:DetailedInvoice"), "DetailedInvoice")
         .when(F.col("value").contains("<fops:SummaryInvoice"), "SummaryInvoice")
         .alias("variant"),
        F.current_timestamp().alias("ingested_at"),
    )
)

raw_new.limit(0).write.mode("ignore").saveAsTable(RAW)   # create the table only if it doesn't exist
raw_new.createOrReplaceTempView("raw_new")

spark.sql(f"""
    MERGE INTO {RAW} t
    USING raw_new n
      ON t.source_path = n.source_path AND t.content_sha256 = n.content_sha256
    WHEN NOT MATCHED THEN INSERT *
""")

display(spark.sql(f"""
    SELECT variant, count(*) AS files, count(DISTINCT invoice_id) AS invoices
    FROM {RAW} GROUP BY variant
"""))
print(spark.table(RAW).count(), "rows in raw_xml")   # expected: 40

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Curated (a): schema-on-read of the header with `from_xml`
# MAGIC The schema is declared here, at read time, as a superset of both variants. Elements that are not in it are ignored
# MAGIC (same as the Python parser). `ignoreNamespace` drops the `fops:` prefix.

# COMMAND ----------

PARTY = ("array<struct<_PartyRole:string,"
         " Entity:struct<_EntityCodeType:string, EntityCode:string, EntityName:string>,"
         " Location:struct<LocationCode:string, LocationName:string,"
         " Address:struct<City:string, StateProvinceCode:string, CountryCode:string>>>>")

HEADER = ("struct<DocumentNumber:string, DocumentDate:string, DocumentType:string,"
          " SubmissionMethod:string, CurrencyCode:string, Total:double, VendorTotal:double,"
          " LineCount:int, Notes:string, PeriodDate:string, DiscountTotal:double,"
          " Action:array<struct<ActionType:string, Status:string, TransactionDateTime:string>>,"
          f" Party:{PARTY}>")

SCHEMA = (f"_invoice_id string,"
          f" DetailedInvoice struct<DocumentHeader:{HEADER}>,"
          f" SummaryInvoice struct<DocumentHeader:{HEADER}>")

(spark.table(RAW)
   .select("source_path", "source_file", "invoice_id", "variant", "ingested_at",
           F.from_xml("xml", SCHEMA, {"ignoreNamespace": "true"}).alias("d"))
   .createOrReplaceTempView("parsed"))

hdr = spark.sql("""
WITH x AS (
  SELECT *, coalesce(d.DetailedInvoice.DocumentHeader, d.SummaryInvoice.DocumentHeader) AS h FROM parsed
), y AS (
  SELECT *, filter(h.Party, p -> p._PartyRole = 'Vendor')[0] AS v,
            filter(h.Party, p -> p._PartyRole = 'Client')[0] AS c FROM x
)
SELECT invoice_id, variant, source_file, source_path, ingested_at,
       h.DocumentNumber            AS document_number,
       to_date(h.DocumentDate)     AS document_date,
       h.DocumentType              AS document_type,
       h.SubmissionMethod          AS submission_method,
       h.CurrencyCode              AS currency_code,
       h.Total                     AS total,
       h.VendorTotal               AS vendor_total,
       h.LineCount                 AS line_count,
       h.Notes                     AS notes,
       h.PeriodDate                AS period_date,
       h.Action[0].ActionType      AS action_type,
       h.Action[0].Status          AS action_status,
       to_timestamp(h.Action[0].TransactionDateTime) AS action_datetime,
       v.Entity.EntityCode         AS vendor_entity_code,
       v.Entity.EntityName         AS vendor_entity_name,
       v.Location.Address.City     AS vendor_city,
       c.Entity.EntityCode         AS client_entity_code,
       c.Entity.EntityName         AS client_entity_name,
       c.Location.Address.City     AS client_city
FROM y
""")
hdr.createOrReplaceTempView("hdr")

print(hdr.count(), "parsed header rows")   # expected: 40
print(hdr.filter("document_number IS NULL OR total IS NULL OR action_datetime IS NULL").count(), "rows with a missing key field")   # expected: 0
display(hdr.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Curated (b): line items
# MAGIC `LineEntry` and `Allocation` are repeated elements, so they arrive as arrays and are exploded to one row per line.
# MAGIC Surrogate keys use the same recipe as the Python parser: first 16 hex characters of `sha256` over the parts joined with `|`.

# COMMAND ----------

ALLOC = ("struct<AllocationRate:double, Total:double, CostCenter:string, ProjectCode:string,"
         " WorkOrder:string, OrderReference:string,"
         " SiteReference:struct<SiteRefName:string>,"
         " AccountInfo:struct<AccountMajor:struct<AccountCode:string, AccountType:string>,"
         " AccountMinor:struct<AccountCode:string>>>")

LINE = ("struct<LineNumber:int, Quantity:double, Units:string, UnitPrice:double,"
        " LineSubTotal:double, LinePretaxTotal:double, Total:double,"
        " PeriodStartDate:string, PeriodDate:string, Notes:string,"
        f" ChargeClass:string, PurchaseCategory:string, Allocation:array<{ALLOC}>>")

LINES = f"struct<Allocation:array<{ALLOC}>, LineEntry:array<{LINE}>>"

SCHEMA_FULL = (f"_invoice_id string,"
               f" DetailedInvoice struct<DocumentHeader:{HEADER}, DocumentLines:{LINES}>,"
               f" SummaryInvoice struct<DocumentHeader:{HEADER}, DocumentLines:{LINES}>")

(spark.table(RAW)
   .select("source_path", "source_file", "invoice_id", "variant", "ingested_at",
           F.from_xml("xml", SCHEMA_FULL, {"ignoreNamespace": "true"}).alias("d"))
   .createOrReplaceTempView("parsed_full"))

lines = spark.sql("""
WITH x AS (
  SELECT invoice_id, variant, source_file, source_path, ingested_at,
         coalesce(d.DetailedInvoice.DocumentLines, d.SummaryInvoice.DocumentLines) AS dl
  FROM parsed_full
), e AS (
  SELECT *, explode(dl.LineEntry) AS le FROM x
), a AS (
  SELECT *, coalesce(le.Allocation[0], dl.Allocation[0]) AS al FROM e
)
SELECT
  substring(sha2(concat_ws('|', invoice_id, cast(le.LineNumber AS string)), 256), 1, 16) AS sk,
  substring(sha2(invoice_id, 256), 1, 16)                                               AS invoice_sk,
  substring(sha2(concat_ws('|', invoice_id, source_path), 256), 1, 16)                  AS version_sk,
  invoice_id, variant, source_file, source_path, ingested_at,
  le.LineNumber        AS line_number,
  le.Quantity          AS quantity,
  le.Units             AS units,
  le.UnitPrice         AS unit_price,
  le.LineSubTotal      AS line_subtotal,
  le.LinePretaxTotal   AS line_pretax_total,
  le.Total             AS line_total,
  to_date(le.PeriodStartDate) AS period_start_date,
  to_date(le.PeriodDate)      AS period_date,
  le.Notes             AS notes,
  le.ChargeClass       AS charge_class,
  le.PurchaseCategory  AS purchase_category,
  al.AllocationRate    AS alloc_rate,
  al.Total             AS alloc_total,
  al.CostCenter        AS alloc_cost_center,
  al.ProjectCode       AS alloc_project_code,
  al.WorkOrder         AS alloc_work_order,
  al.OrderReference    AS alloc_order_ref,
  al.SiteReference.SiteRefName          AS alloc_site_ref_name,
  al.AccountInfo.AccountMajor.AccountCode AS alloc_account_major,
  al.AccountInfo.AccountMinor.AccountCode AS alloc_account_minor,
  al.AccountInfo.AccountMajor.AccountType AS alloc_account_type
FROM a
""")
lines.createOrReplaceTempView("lines_all")

print(lines.count(), "line rows (all deliveries, before picking the latest version)")   # expected: 228
print(lines.filter("line_number IS NULL OR line_total IS NULL").count(), "rows with a missing key field")   # expected: 0
display(spark.sql("SELECT variant, count(*) lines, count(alloc_cost_center) with_alloc FROM lines_all GROUP BY variant"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Curated (c): latest version per invoice, written as managed Delta tables
# MAGIC Rule from ADR-0002: latest `action_datetime`, then `ingested_at`, then `source_path`. Curated is derived from the
# MAGIC immutable raw table, so it is rebuilt with `CREATE OR REPLACE TABLE ... AS SELECT`.

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE workspace.xml_drift.curated_invoices AS
SELECT * EXCEPT (rn) FROM (
  SELECT
    substring(sha2(invoice_id, 256), 1, 16)                              AS sk,
    substring(sha2(concat_ws('|', invoice_id, source_path), 256), 1, 16) AS version_sk,
    *,
    row_number() OVER (PARTITION BY invoice_id
                       ORDER BY action_datetime DESC, ingested_at DESC, source_path DESC) AS rn
  FROM hdr
) WHERE rn = 1
""")

spark.sql("""
CREATE OR REPLACE TABLE workspace.xml_drift.curated_line_items AS
SELECT l.*
FROM lines_all l
JOIN workspace.xml_drift.curated_invoices i
  ON l.invoice_id = i.invoice_id AND l.source_path = i.source_path
""")

print(spark.table("workspace.xml_drift.curated_invoices").count(), "invoices")      # expected: 25
print(spark.table("workspace.xml_drift.curated_line_items").count(), "line items")  # expected: 147
print(spark.sql("SELECT count(*) - count(DISTINCT sk) FROM workspace.xml_drift.curated_line_items").first()[0],
      "duplicate line keys")                                                         # expected: 0

display(spark.sql("""
  SELECT invoice_id, source_file, action_type, action_status, line_count
  FROM workspace.xml_drift.curated_invoices WHERE invoice_id = '900000003'
"""))   # expected: day_005/6242173772.xml, Reject, Disputed

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Reconciliation with DuckDB / Iceberg
# MAGIC Per invoice: key, winning file, header total, number of lines, sum of line totals, and an md5 over the line keys.
# MAGIC `reconcile_duckdb.csv` comes from `databricks/export_duckdb_reference.py`. Every count below should be 0.

# COMMAND ----------

import os

REF = "/Volumes/workspace/xml_drift/landing/reconcile_duckdb.csv"

if not os.path.exists(REF):
    print("Reference file not found. Upload output/reconcile_duckdb.csv to the landing volume to run this check.")
else:
    ref = (spark.read.option("header", True).csv(REF)
             .select("invoice_id", F.col("sk").alias("r_sk"), F.col("variant").alias("r_variant"),
                     F.col("file_name").alias("r_file"), F.col("action_type").alias("r_atype"),
                     F.col("action_status").alias("r_astatus"),
                     F.col("total").cast("double").alias("r_total"),
                     F.col("n_lines").cast("long").alias("r_lines"),
                     F.col("lines_total").cast("double").alias("r_lines_total"),
                     F.col("line_keys_md5").alias("r_md5")))

    dbx = spark.sql("""
    SELECT i.invoice_id, i.sk, i.variant,
           regexp_extract(i.source_path, '([^/]+)$', 1) AS file_name,
           i.action_type, i.action_status,
           round(i.total, 2) AS total,
           count(l.sk) AS n_lines,
           round(sum(l.line_total), 2) AS lines_total,
           md5(concat_ws(',', sort_array(collect_list(l.sk)))) AS line_keys_md5
    FROM workspace.xml_drift.curated_invoices i
    LEFT JOIN workspace.xml_drift.curated_line_items l ON i.invoice_id = l.invoice_id
    GROUP BY i.invoice_id, i.sk, i.variant, i.source_path, i.action_type, i.action_status, i.total
    """)

    j = dbx.join(ref, "invoice_id", "full_outer")

    checks = {
      "only in Databricks":      F.col("r_sk").isNull(),
      "only in DuckDB":          F.col("sk").isNull(),
      "invoice key (sk)":        ~F.col("sk").eqNullSafe(F.col("r_sk")),
      "variant":                 ~F.col("variant").eqNullSafe(F.col("r_variant")),
      "winning file":            ~F.col("file_name").eqNullSafe(F.col("r_file")),
      "action type":             ~F.col("action_type").eqNullSafe(F.col("r_atype")),
      "action status":           ~F.col("action_status").eqNullSafe(F.col("r_astatus")),
      "header total":            F.abs(F.col("total") - F.col("r_total")) > 0.005,
      "number of lines":         ~F.col("n_lines").eqNullSafe(F.col("r_lines")),
      "sum of line totals":      F.abs(F.col("lines_total") - F.col("r_lines_total")) > 0.005,
      "line keys (md5)":         ~F.col("line_keys_md5").eqNullSafe(F.col("r_md5")),
    }

    print(f"Databricks: {dbx.count()} invoices, {dbx.agg(F.sum('n_lines')).first()[0]} lines")
    print(f"DuckDB:     {ref.count()} invoices, {ref.agg(F.sum('r_lines')).first()[0]} lines\n")
    for name, cond in checks.items():
        print(f"{name:22s} differences: {j.filter(cond).count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Consumer: a Unity Catalog view, the contract for analytics
# MAGIC Port of the dbt `mart_invoices` logic for the columns this Spark schema captures (identity, totals, workflow,
# MAGIC vendor/client, line economics, allocation) plus the derived metrics. Tax, early-payment and cross-reference columns
# MAGIC of the dbt mart are not in the Spark schema yet.

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW workspace.xml_drift.mart_invoices AS
SELECT
  l.sk AS line_sk, l.invoice_sk, i.source_file, i.variant, i.invoice_id,
  i.document_number, i.document_date, i.document_type, i.submission_method, i.currency_code,
  i.total AS invoice_total, i.vendor_total, i.line_count,
  i.action_type, i.action_status, i.action_datetime,
  i.vendor_entity_code, i.vendor_entity_name, i.vendor_city,
  i.client_entity_code, i.client_entity_name, i.client_city,
  l.line_number, l.charge_class, l.purchase_category,
  coalesce(l.period_date, i.period_date) AS period_date,
  l.period_start_date, l.quantity, l.units, l.unit_price,
  l.line_subtotal, l.line_pretax_total, l.line_total,
  CASE WHEN l.unit_price IS NOT NULL AND l.unit_price > 0 THEN l.unit_price
       WHEN l.line_total IS NOT NULL AND l.quantity IS NOT NULL AND l.quantity > 0
            THEN l.line_total / l.quantity END                    AS effective_unit_price,
  coalesce(l.line_total, i.total)                                 AS effective_line_total,
  l.alloc_rate, l.alloc_total, l.alloc_cost_center, l.alloc_project_code,
  l.alloc_work_order, l.alloc_order_ref, l.alloc_site_ref_name,
  l.alloc_account_major, l.alloc_account_type,
  (l.alloc_cost_center IS NOT NULL)    AS has_cost_center,
  (l.alloc_project_code IS NOT NULL)   AS has_project_code,
  (i.action_status = 'Approved')       AS is_approved,
  year(i.document_date)                AS document_year,
  month(i.document_date)               AS document_month
FROM workspace.xml_drift.curated_line_items l
JOIN workspace.xml_drift.curated_invoices  i ON l.invoice_id = i.invoice_id
""")

display(spark.sql("""
SELECT variant, document_year,
       count(*)                                  AS `rows`,
       count(DISTINCT invoice_id)                AS invoices,
       round(sum(effective_line_total), 2)       AS sum_line_total,
       round(sum(effective_unit_price), 2)       AS sum_unit_price,
       count_if(is_approved)                     AS approved,
       count_if(has_cost_center)                 AS with_cc
FROM workspace.xml_drift.mart_invoices
GROUP BY variant, document_year ORDER BY variant, document_year
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC The same aggregation on the dbt `mart_invoices` (DuckDB reading the Iceberg tables) gives identical numbers:
# MAGIC
# MAGIC | variant | year | rows | invoices | sum_line_total | sum_unit_price | approved | with_cc |
# MAGIC |---|---|---|---|---|---|---|---|
# MAGIC | DetailedInvoice | 2020 | 7 | 1 | 1089341.93 | 21516.76 | 0 | 7 |
# MAGIC | DetailedInvoice | 2021 | 7 | 2 | 2431444.22 | 45303.81 | 0 | 7 |
# MAGIC | DetailedInvoice | 2022 | 13 | 2 | 1836504.81 | 42329.83 | 8 | 13 |
# MAGIC | DetailedInvoice | 2024 | 55 | 9 | 16633217.94 | 298105.70 | 0 | 55 |
# MAGIC | DetailedInvoice | 2025 | 7 | 1 | 1335825.84 | 28854.78 | 0 | 7 |
# MAGIC | SummaryInvoice | 2020 | 9 | 1 | 734538.82 | 26492.48 | 0 | 9 |
# MAGIC | SummaryInvoice | 2021 | 4 | 1 | 770791.78 | 28417.29 | 0 | 4 |
# MAGIC | SummaryInvoice | 2023 | 10 | 2 | 885368.91 | 44038.87 | 5 | 10 |
# MAGIC | SummaryInvoice | 2024 | 35 | 6 | 4503247.73 | 164862.06 | 0 | 35 |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Drift: which elements appeared after day 1
# MAGIC The raw layer keeps the XML untouched, so structure can be inspected without parsing it. The daily data introduces two
# MAGIC new fields; both are outside the `from_xml` schema, so curated ignores them until the schema is extended.
# MAGIC A plain hash of each file's element set (second query) is noisy here, because optional elements vary file to file.

# COMMAND ----------

display(spark.sql("""
WITH e AS (
  SELECT variant,
         regexp_extract(source_file, 'day_([0-9]+)', 1) AS day,
         explode(array_distinct(regexp_extract_all(xml, '<fops:([A-Za-z]+)', 1))) AS element
  FROM workspace.xml_drift.raw_xml
)
SELECT variant, element, min(day) AS first_seen_day, count(*) AS files
FROM e
GROUP BY variant, element
HAVING min(day) > '001'
ORDER BY first_seen_day, variant, element
"""))   # expected: OriginalSystem from day 002, InvoiceAmount from day 004

display(spark.sql("""
WITH t AS (
  SELECT variant,
         regexp_extract(source_file, 'day_([0-9]+)', 1) AS day,
         substring(sha2(concat_ws(',', array_sort(array_distinct(
             regexp_extract_all(xml, '<fops:([A-Za-z]+)', 1)))), 256), 1, 12) AS fingerprint
  FROM workspace.xml_drift.raw_xml
)
SELECT day, variant, count(*) AS files, count(DISTINCT fingerprint) AS distinct_structures
FROM t GROUP BY day, variant ORDER BY day, variant
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Delta: transaction log and time travel

# COMMAND ----------

display(spark.sql("""
  SELECT version, timestamp, operation,
         operationMetrics['numOutputRows'] AS output_rows
  FROM (DESCRIBE HISTORY workspace.xml_drift.raw_xml)
  ORDER BY version
"""))

for v in (0, 1):
    n = spark.sql(f"SELECT count(*) FROM workspace.xml_drift.raw_xml VERSION AS OF {v}").first()[0]
    print(f"raw_xml version {v}: {n} rows")
print("raw_xml now:", spark.table("workspace.xml_drift.raw_xml").count(), "rows")

display(spark.sql("DESCRIBE DETAIL workspace.xml_drift.curated_invoices")
        .select("format", "numFiles", "sizeInBytes", "location"))
