"""
iceberg_writer.py - xml-drift-lakehouse Iceberg layer (ADR-0001, ADR-0002)
==========================================================================
Explicit Iceberg table definitions for the parser output, a local SQL
catalog (SQLite), and a loader that upserts parser batches by business key.

W1: table definitions and --init.
W2: --load (upsert, latest version wins, lines replaced per invoice),
    version-hint.text for DuckDB, and --check for key/version consistency.

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
    python ingestion/iceberg_writer.py --load --src ./output --check
"""

import argparse
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.expressions import In
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC, PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.transforms import MonthTransform
from pyiceberg.types import (
    BooleanType,
    DateType,
    DoubleType,
    FloatType,
    IcebergType,
    IntegerType,
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


def init_tables(catalog: SqlCatalog, verbose: bool = True) -> dict[str, Table]:
    """Create the namespace and both tables if missing. Safe to run twice."""
    catalog.create_namespace_if_not_exists(NAMESPACE)
    tables = {}
    for ident, (schema, spec) in TABLES.items():
        existed = catalog.table_exists(ident)
        table = catalog.create_table_if_not_exists(ident, schema=schema, partition_spec=spec)
        if verbose and existed and table.schema().as_struct() != schema.as_struct():
            print(f"  [WARN] {ident} exists with a different schema; left unchanged")
        if verbose:
            print(f"  {ident:24s} {'exists' if existed else 'created'}  {table.metadata_location}")
        tables[ident] = table
    return tables

# ── Batch load (step W2) ──────────────────────────────────────────────────────

# Columns that say where and when a version arrived, not what it says (ADR-0002).
LINEAGE_COLS = ("ingested_at", "source_file", "source_path", "version_sk")

# Which delivery wins when an invoice arrives again (ADR-0002, section 3):
# greatest action_datetime, then latest ingested_at, then source_path.
VERSION_ORDER = ("action_datetime", "ingested_at", "source_path")

_MIN_NAIVE = datetime(1900, 1, 1)
_MIN_UTC = datetime(1900, 1, 1, tzinfo=timezone.utc)


def _pl_type(t: IcebergType) -> pl.DataType:
    if isinstance(t, StringType):
        return pl.Utf8
    if isinstance(t, DoubleType):
        return pl.Float64
    if isinstance(t, FloatType):
        return pl.Float32
    if isinstance(t, LongType):
        return pl.Int64
    if isinstance(t, IntegerType):
        return pl.Int32
    if isinstance(t, BooleanType):
        return pl.Boolean
    if isinstance(t, DateType):
        return pl.Date
    if isinstance(t, TimestamptzType):
        return pl.Datetime("us", "UTC")
    if isinstance(t, TimestampType):
        return pl.Datetime("us")
    raise TypeError(f"unsupported Iceberg type for conform(): {t}")


def conform(df: pl.DataFrame, schema: Schema, what: str) -> pl.DataFrame:
    """Cast a parser batch to the table schema, column by column.

    The schema is the table's current one, so columns added later with
    ADD COLUMN are filled with null when the batch does not carry them.
    Columns the table does not know are dropped (new fields enter through
    the schema diff engine and ADD COLUMN, never through a batch).
    A value that existed but did not survive its cast is data loss, so it
    raises instead of becoming null silently.
    """
    typed = {f.name: _pl_type(f.field_type) for f in schema.fields}
    if df.height == 0:
        return pl.DataFrame(schema=typed)

    exprs = []
    for name, pt in typed.items():
        if name not in df.columns or df.schema[name] == pl.Null:
            exprs.append(pl.lit(None, dtype=pt).alias(name))
            continue
        col = pl.col(name)
        if pt == pl.Date:
            expr = col.cast(pl.Utf8).str.to_date(strict=False)
        elif isinstance(pt, pl.Datetime):
            expr = col.cast(pl.Utf8).str.to_datetime(
                time_unit="us", time_zone=pt.time_zone, strict=False)
        else:
            expr = col.cast(pt, strict=False)
        exprs.append(expr.alias(name))
    out = df.select(exprs)

    for name in typed:
        if name in df.columns and df.schema[name] != pl.Null:
            lost = out[name].null_count() - df[name].null_count()
            if lost:
                raise ValueError(f"{what}.{name}: {lost} value(s) lost in the cast to {typed[name]}")
    return out


def to_arrow(df: pl.DataFrame, table: Table) -> pa.Table:
    """Polars -> Arrow, cast to the table's physical schema (required fields included)."""
    target = schema_to_pyarrow(table.schema(), include_field_ids=False)
    return df.to_arrow().cast(target)


def read_table(table: Table, ids: list[str] | None = None, chunk: int = 500) -> pl.DataFrame:
    """Read a table (or just the given invoice_ids) into Polars."""
    if ids is None:
        return pl.from_arrow(table.scan().to_arrow())
    frames = [
        pl.from_arrow(table.scan(row_filter=In("invoice_id", ids[i:i + chunk])).to_arrow())
        for i in range(0, len(ids), chunk)
    ]
    return pl.concat(frames) if frames else pl.DataFrame(schema={f.name: _pl_type(f.field_type) for f in table.schema().fields})


def _fingerprints(inv: pl.DataFrame, lines: pl.DataFrame) -> pl.DataFrame:
    """One hash per invoice over its header and its ordered lines.

    ingested_at is left out: re-parsing the same file later must not count as
    a change. source_path stays in: the same content delivered in another file
    moves the lineage (ADR-0002), so that is a change.
    """
    h_cols = [c for c in inv.columns if c != "ingested_at"]
    l_cols = [c for c in lines.columns if c != "ingested_at"]
    head = inv.select("invoice_id", pl.struct(h_cols).hash().cast(pl.Utf8).alias("h"))
    body = (
        lines.sort("invoice_id", "line_number")
        .with_columns(pl.struct(l_cols).hash().cast(pl.Utf8).alias("lh"))
        .group_by("invoice_id", maintain_order=True)
        .agg(pl.col("lh").str.concat("|").alias("ls"))
    )
    return (
        head.join(body, on="invoice_id", how="left")
        .select("invoice_id", (pl.col("h") + "/" + pl.col("ls").fill_null("")).alias("fp"))
    )


def _is_newer_or_equal(inc: pl.DataFrame, cur: pl.DataFrame) -> pl.DataFrame:
    """For each incoming invoice: does it exist, and does it win by VERSION_ORDER?"""
    cur_k = cur.select(
        "invoice_id",
        pl.lit(True).alias("_exists"),
        *[pl.col(c).alias(f"_cur_{c}") for c in VERSION_ORDER],
    )
    j = inc.select("invoice_id", *VERSION_ORDER).join(cur_k, on="invoice_id", how="left")
    dt, c_dt = pl.col("action_datetime").fill_null(_MIN_NAIVE), pl.col("_cur_action_datetime").fill_null(_MIN_NAIVE)
    ing = pl.col("ingested_at").fill_null(pl.lit(_MIN_UTC, dtype=pl.Datetime("us", "UTC")))
    c_ing = pl.col("_cur_ingested_at").fill_null(pl.lit(_MIN_UTC, dtype=pl.Datetime("us", "UTC")))
    sp, c_sp = pl.col("source_path").fill_null(""), pl.col("_cur_source_path").fill_null("")
    newer = (dt > c_dt) | ((dt == c_dt) & ((ing > c_ing) | ((ing == c_ing) & (sp >= c_sp))))
    return j.select(
        "invoice_id",
        pl.col("_exists").fill_null(False).alias("exists"),
        newer.alias("newer"),
    )


def load_batch(catalog: SqlCatalog, inv_df: pl.DataFrame, line_df: pl.DataFrame) -> dict[str, int]:
    """Upsert one parser batch into the Iceberg tables (ADR-0001, ADR-0002).

    Per invoice_id, in order:
      1. inside the batch, only the winning version is kept (VERSION_ORDER);
      2. against the table, an older delivery is ignored ("stale") and an
         identical one is a no-op ("unchanged");
      3. otherwise the invoice is inserted or updated, and ALL its lines are
         replaced (delete by invoice_id, then append), so a line removed by an
         update does not linger.
    Lines are written before the invoice row. The invoice row is the commit
    marker: if the run dies in between, the invoice still shows its old
    version, and a rerun redoes the replacement.
    """
    tables = init_tables(catalog, verbose=False)
    inv_t, line_t = tables[INVOICES], tables[LINE_ITEMS]

    inv = conform(inv_df, inv_t.schema(), "invoices")
    lines = conform(line_df, line_t.schema(), "line_items")

    for what, df, keys in (("invoices", inv, INVOICE_KEYS), ("line_items", lines, LINE_ITEM_KEYS)):
        for k in keys:
            if df.height and df[k].null_count():
                raise ValueError(f"{what}.{k} has null values; refusing to load")
    if lines.height and not set(lines["invoice_id"]).issubset(set(inv["invoice_id"])):
        raise ValueError("line_items contain invoice_ids that are not in the invoice batch")

    stats = {"received": inv.height, "new": 0, "updated": 0, "unchanged": 0, "stale": 0,
             "superseded": 0, "lines_written": 0}
    if inv.height == 0:
        return stats

    # 1. one winning version per invoice inside the batch
    inv = inv.sort(*VERSION_ORDER, descending=True, nulls_last=True).unique("invoice_id", keep="first", maintain_order=True)
    stats["superseded"] = stats["received"] - inv.height
    lines = lines.join(inv.select("invoice_id", "version_sk"), on=["invoice_id", "version_sk"], how="semi")

    # 2. compare with what the table holds
    ids = inv["invoice_id"].to_list()
    cur_inv = read_table(inv_t, ids)
    cur_lines = read_table(line_t, ids)
    verdict = _is_newer_or_equal(inv, cur_inv)
    cur_fp = _fingerprints(cur_inv, cur_lines).rename({"fp": "cur_fp"}) if cur_inv.height else None
    in_fp = _fingerprints(inv, lines).rename({"fp": "in_fp"})

    plan = verdict.join(in_fp, on="invoice_id")
    plan = plan.join(cur_fp, on="invoice_id", how="left") if cur_fp is not None else plan.with_columns(pl.lit(None, dtype=pl.Utf8).alias("cur_fp"))
    plan = plan.with_columns(
        pl.when(~pl.col("exists")).then(pl.lit("new"))
        .when(~pl.col("newer")).then(pl.lit("stale"))
        .when(pl.col("in_fp") == pl.col("cur_fp")).then(pl.lit("unchanged"))
        .otherwise(pl.lit("updated")).alias("verdict")
    )
    for v in ("new", "updated", "unchanged", "stale"):
        stats[v] = plan.filter(pl.col("verdict") == v).height

    accept = plan.filter(pl.col("verdict").is_in(["new", "updated"]))
    if accept.height == 0:
        return stats
    accept_ids = accept["invoice_id"].to_list()
    replace_ids = accept.filter(pl.col("verdict") == "updated")["invoice_id"].to_list()

    # 3. write: lines first, invoice row last. Each table commits delete + append
    #    as ONE transaction. Table.upsert is not used: in PyIceberg 0.12 it fails
    #    on files written before an ADD COLUMN, while delete + append (what upsert
    #    does underneath) does not.
    new_lines = lines.filter(pl.col("invoice_id").is_in(accept_ids))
    new_inv = inv.filter(pl.col("invoice_id").is_in(accept_ids))
    _replace(line_t, replace_ids, to_arrow(new_lines, line_t) if new_lines.height else None)
    _replace(inv_t, replace_ids, to_arrow(new_inv, inv_t))
    stats["lines_written"] = new_lines.height

    for t in (inv_t, line_t):
        write_version_hint(t)
    return stats


def _replace(table: Table, delete_ids: list[str], rows: pa.Table | None) -> None:
    """Delete rows by invoice_id and append new rows in a single commit."""
    if not delete_ids and rows is None:
        return
    with table.transaction() as tx:
        if delete_ids:
            tx.delete(delete_filter=In("invoice_id", delete_ids))
        if rows is not None:
            tx.append(rows)


def write_version_hint(table: Table) -> Path:
    """Write version-hint.text next to the current metadata file (ADR-0001).

    The SQLite catalog does not write one, and DuckDB iceberg_scan needs it to
    find the current metadata when pointed at the table folder. The hint holds
    the metadata file name without '.metadata.json', which matches
    version_name_format='%s%s.metadata.json' on the DuckDB side.
    """
    loc = table.metadata_location
    path = Path(loc[len("file://"):] if loc.startswith("file://") else loc)
    hint = path.parent / "version-hint.text"
    hint.write_text(path.name.removesuffix(".metadata.json"))
    return hint


def check_consistency(catalog: SqlCatalog) -> list[str]:
    """Return a list of problems; empty means the tables agree (ADR-0002)."""
    inv = read_table(catalog.load_table(INVOICES))
    lines = read_table(catalog.load_table(LINE_ITEMS))
    problems = []
    if inv.height != inv["invoice_id"].n_unique():
        problems.append("invoices: duplicate invoice_id")
    if lines.height != lines.select("invoice_id", "line_number").unique().height:
        problems.append("line_items: duplicate (invoice_id, line_number)")
    orphan = lines.join(inv.select("invoice_id"), on="invoice_id", how="anti")
    if orphan.height:
        problems.append(f"line_items: {orphan.height} row(s) without an invoice")
    skew = (
        lines.join(inv.select("invoice_id", pl.col("version_sk").alias("inv_version")), on="invoice_id")
        .filter(pl.col("version_sk") != pl.col("inv_version"))
    )
    if skew.height:
        problems.append(f"line_items: {skew.height} row(s) from a different version than their invoice")
    return problems

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(description="Iceberg tables for xml-drift-lakehouse")
    p.add_argument("--warehouse", default=DEFAULT_WAREHOUSE,
                   help="Local warehouse folder; the catalog is <warehouse>/catalog.db")
    p.add_argument("--init", action="store_true",
                   help="Create the namespace and tables if they do not exist")
    p.add_argument("--load", action="store_true",
                   help="Upsert the parser output found in --src into the tables")
    p.add_argument("--src", default="./output",
                   help="Parser output folder holding invoices/ and line_items/ (default ./output)")
    p.add_argument("--check", action="store_true",
                   help="Check key uniqueness and invoice/line version consistency")
    args = p.parse_args()

    if not (args.init or args.load or args.check):
        p.error("nothing to do: pass --init, --load and/or --check")

    print(f"\nWarehouse: {Path(args.warehouse).resolve()}")
    catalog = get_catalog(args.warehouse)

    if args.init:
        init_tables(catalog)

    if args.load:
        src = Path(args.src)
        inv_p = src / "invoices" / "invoices.parquet"
        line_p = src / "line_items" / "line_items.parquet"
        if not inv_p.exists():
            raise SystemExit(f"not found: {inv_p} (run the parser first)")
        inv_df = pl.read_parquet(inv_p)
        line_df = pl.read_parquet(line_p) if line_p.exists() else pl.DataFrame()
        stats = load_batch(catalog, inv_df, line_df)
        print("  " + "  ".join(f"{k}={v}" for k, v in stats.items()))

    if args.check:
        problems = check_consistency(catalog)
        print("  consistency: OK" if not problems else "  consistency: FAILED\n    " + "\n    ".join(problems))
        if problems:
            raise SystemExit(1)


if __name__ == "__main__":
    main()
