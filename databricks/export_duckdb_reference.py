"""Export a per-invoice summary of the Iceberg tables for the Databricks reconciliation.

One row per invoice: key, variant, winning file, workflow state, header total, number of
lines, sum of line totals, and an md5 over the sorted line keys. The notebook builds the
same summary from its curated tables and counts the differences.

    python databricks/export_duckdb_reference.py     # writes output/reconcile_duckdb.csv

Needs the Iceberg tables (bash scripts/replay_daily.sh) and the DuckDB iceberg extension
(downloaded on first use, so internet access is needed once).
"""
import argparse
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--warehouse", default=str(ROOT / "output" / "iceberg"))
    ap.add_argument("--out", default=str(ROOT / "output" / "reconcile_duckdb.csv"))
    args = ap.parse_args()

    wh = Path(args.warehouse) / "lakehouse"
    opt = "version_name_format='%s%s.metadata.json', allow_moved_paths=true"
    con = duckdb.connect()
    con.sql("install iceberg; load iceberg")
    con.sql(f"""
        copy (
          select i.invoice_id, i.sk, i.variant,
                 regexp_extract(i.source_path, '([^/]+)$', 1) as file_name,
                 i.action_type, i.action_status,
                 round(i.total, 2) as total,
                 count(l.sk) as n_lines,
                 round(sum(l.line_total), 2) as lines_total,
                 md5(string_agg(l.sk, ',' order by l.sk)) as line_keys_md5
          from iceberg_scan('{wh}/invoices', {opt}) i
          left join iceberg_scan('{wh}/line_items', {opt}) l
            on i.invoice_id = l.invoice_id
          group by all
          order by i.invoice_id
        ) to '{args.out}' (header)
    """)
    print(con.sql(f"select count(*) as invoices, sum(n_lines) as lines from '{args.out}'"))


if __name__ == "__main__":
    main()
