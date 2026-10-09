"""
iceberg_writer.py - xml-drift-lakehouse Iceberg layer (ADR-0001, ADR-0002)
==========================================================================
Explicit Iceberg table definitions for the parser output, a local SQL
catalog (SQLite) and an --init command that creates the tables.

Step W1: table definitions and --init only. No data writes yet.

  <warehouse>/
    catalog.db               SQLite catalog
    lakehouse/invoices/      partitioned by month(document_date)
    lakehouse/line_items/    unpartitioned

Schemas are declared here, never inferred from a batch: the parser infers
types per run and writes all-null columns with a null type, which Iceberg
format v2 rejects (ADR-0001, spike result 5). Column names match the
Parquet output of parser.py.

Keys (ADR-0002):
  invoices    identifier = invoice_id
  line_items  identifier = (invoice_id, line_number)

Usage:
    python ingestion/iceberg_writer.py --init
    python ingestion/iceberg_writer.py --init --warehouse ./output/iceberg
"""

import argparse
from pathlib import Path

from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC, PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.transforms import MonthTransform
from pyiceberg.types import (
    DateType,
    DoubleType,
    IcebergType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
    TimestamptzType,
)

NAMESPACE = "lakehouse"
INVOICES = f"{NAMESPACE}.invoices"
LINE_ITEMS = f"{NAMESPACE}.line_items"
DEFAULT_WAREHOUSE = "./output/iceberg"

S, D, L, DT, TS, TSZ = StringType(), DoubleType(), LongType(), DateType(), TimestampType(), TimestamptzType()

# ── Column definitions (Parquet column order) ─────────────────────────────────

INVOICE_COLUMNS: list[tuple[str, IcebergType]] = [
    ("sk",                      S),
    ("version_sk",              S),
    ("ingested_at",             TSZ),   # ISO with +00:00 offset, set per file
    ("source_file",             S),
    ("source_path",             S),
    ("variant",                 S),
    ("invoice_id",              S),
    ("document_number",         S),
    ("document_date",           DT),
    ("document_type",           S),
    ("submission_method",       S),
    ("currency_code",           S),
    ("total",                   D),
    ("vendor_total",            D),
    ("line_count",              L),
    ("notes",                   S),
    ("period_date",             DT),
    ("discount_total",          D),
    ("action_type",             S),
    ("action_status",           S),
    ("action_datetime",         TS),    # no offset in the XML
    ("vendor_entity_code",      S),
    ("vendor_entity_code_type", S),
    ("vendor_entity_name",      S),
    ("vendor_location_code",    S),
    ("vendor_location_name",    S),
    ("vendor_city",             S),
    ("vendor_state",            S),
    ("vendor_country_code",     S),
    ("client_entity_code",      S),
    ("client_entity_code_type", S),
    ("client_entity_name",      S),
    ("client_location_code",    S),
    ("client_location_name",    S),
    ("client_city",             S),
    ("client_state",            S),
    ("client_country_code",     S),
]

LINE_ITEM_COLUMNS: list[tuple[str, IcebergType]] = [
    ("sk",                   S),
    ("invoice_sk",           S),
    ("version_sk",           S),
    ("ingested_at",          TSZ),
    ("source_file",          S),
    ("source_path",          S),
    ("variant",              S),
    ("invoice_id",           S),
    ("line_number",          L),
    ("service_code",         S),
    ("product_description",  S),
    ("product_category",     S),
    ("quantity",             D),
    ("units",                S),
    ("unit_price",           D),
    ("line_subtotal",        D),
    ("line_pretax_total",    D),
    ("line_total",           D),
    ("period_start_date",    DT),
    ("period_date",          DT),
    ("notes",                S),
    ("charge_class",         S),
    ("purchase_category",    S),
    ("category_code",        S),
    ("category_scheme",      S),
    ("discount_total",       D),
    ("discount_rate",        D),
    ("cross_ref_doc_number", S),
    ("cross_ref_doc_type",   S),
    ("cross_ref_date",       DT),
    ("tax_type",             S),
    ("tax_total",            D),
    ("tax_exempt_code",      S),
    ("early_pay_due_date",   DT),
    ("early_pay_days_due",   L),
    ("early_pay_eligible",   S),    # Y / N flag, kept as text
    ("alloc_rate",           D),
    ("alloc_total",          D),
    ("alloc_cost_center",    S),
    ("alloc_project_code",   S),
    ("alloc_work_order",     S),
    ("alloc_order_ref",      S),
    ("alloc_site_ref_name",  S),
    ("alloc_account_major",  S),
    ("alloc_account_minor",  S),
    ("alloc_account_type",   S),
]

INVOICE_KEYS = ("invoice_id",)
LINE_ITEM_KEYS = ("invoice_id", "line_number")


def build_schema(columns: list[tuple[str, IcebergType]], keys: tuple[str, ...]) -> Schema:
    """Schema with field ids 1..n in column order; identifier columns are required."""
    fields = [
        NestedField(field_id=i, name=name, field_type=ftype, required=name in keys)
        for i, (name, ftype) in enumerate(columns, start=1)
    ]
    ids = [f.field_id for f in fields if f.name in keys]
    return Schema(*fields, identifier_field_ids=ids)


INVOICES_SCHEMA = build_schema(INVOICE_COLUMNS, INVOICE_KEYS)
LINE_ITEMS_SCHEMA = build_schema(LINE_ITEM_COLUMNS, LINE_ITEM_KEYS)

INVOICES_SPEC = PartitionSpec(
    PartitionField(
        source_id=INVOICES_SCHEMA.find_field("document_date").field_id,
        field_id=1000,
        transform=MonthTransform(),
        name="document_date_month",
    )
)
LINE_ITEMS_SPEC = UNPARTITIONED_PARTITION_SPEC

TABLES = {
    INVOICES: (INVOICES_SCHEMA, INVOICES_SPEC),
    LINE_ITEMS: (LINE_ITEMS_SCHEMA, LINE_ITEMS_SPEC),
}

# ── Catalog ───────────────────────────────────────────────────────────────────

def get_catalog(warehouse: str | Path = DEFAULT_WAREHOUSE) -> SqlCatalog:
    """SQLite catalog at <warehouse>/catalog.db.

    The warehouse path is absolute and scheme-less: DuckDB 1.1.3 cannot read
    tables whose metadata carries file:// paths (ADR-0001, spike result 1).
    """
    wh = Path(warehouse).resolve()
    wh.mkdir(parents=True, exist_ok=True)
    return SqlCatalog(NAMESPACE, uri=f"sqlite:///{wh}/catalog.db", warehouse=str(wh))


def init_tables(catalog: SqlCatalog) -> dict[str, Table]:
    """Create the namespace and both tables if missing. Safe to run twice."""
    catalog.create_namespace_if_not_exists(NAMESPACE)
    tables = {}
    for ident, (schema, spec) in TABLES.items():
        existed = catalog.table_exists(ident)
        table = catalog.create_table_if_not_exists(ident, schema=schema, partition_spec=spec)
        if existed and table.schema().as_struct() != schema.as_struct():
            print(f"  [WARN] {ident} exists with a different schema; left unchanged")
        print(f"  {ident:24s} {'exists' if existed else 'created'}  {table.metadata_location}")
        tables[ident] = table
    return tables

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(description="Iceberg tables for xml-drift-lakehouse")
    p.add_argument("--warehouse", default=DEFAULT_WAREHOUSE,
                   help="Local warehouse folder; the catalog is <warehouse>/catalog.db")
    p.add_argument("--init", action="store_true",
                   help="Create the namespace and tables if they do not exist")
    args = p.parse_args()

    if not args.init:
        p.error("nothing to do: pass --init")

    print(f"\nWarehouse: {Path(args.warehouse).resolve()}")
    init_tables(get_catalog(args.warehouse))


if __name__ == "__main__":
    main()
