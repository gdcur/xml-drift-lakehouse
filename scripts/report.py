"""
report.py — xml-drift-lakehouse business analytics summary
===========================================================
Prints aggregated business figures from mart_invoices.
Runs against the local DuckDB lakehouse — no BI tool needed.

Usage:
    python report.py
    python report.py --db output/lakehouse.duckdb
    python report.py --top 10
"""

import argparse
from pathlib import Path

try:
    import duckdb
except ImportError:
    print("duckdb not installed. Run: pip install duckdb")
    raise SystemExit(1)


# ── Helpers ───────────────────────────────────────────────────────────────────

def separator(title: str = "", width: int = 52) -> None:
    if title:
        print(f"\n────────── {title} {'─' * max(0, width - len(title) - 12)}")
    else:
        print(f"{'─' * width}")


def fmt_currency(val) -> str:
    if val is None:
        return f"{'N/A':>15}"
    return f"${float(val):>14,.2f}"


def fmt_pct(val) -> str:
    if val is None:
        return "   N/A"
    return f"{float(val):>5.1f}%"


# ── Report ────────────────────────────────────────────────────────────────────

def report(db_path: str, top_n: int) -> None:
    path = Path(db_path)
    if not path.exists():
        print(f"Database not found: {path.resolve()}")
        print("Run the DAG first to generate the lakehouse.")
        raise SystemExit(1)

    con = duckdb.connect(str(path), read_only=True)

    # ── Overall summary ───────────────────────────────────────────────────────
    separator("invoice summary")
    row = con.execute("""
        SELECT
            COUNT(DISTINCT invoice_id)              AS total_invoices,
            COUNT(*)                                AS total_line_items,
            SUM(invoice_total)                      AS total_spend,
            AVG(invoice_total)                      AS avg_invoice_value,
            MIN(document_date)                      AS earliest_date,
            MAX(document_date)                      AS latest_date
        FROM mart_invoices
    """).fetchone()

    if not row or row[0] == 0:
        print("  No data found. Run the DAG first.")
        con.close()
        return

    print(f"  Total invoices     : {row[0]:,}")
    print(f"  Total line items   : {row[1]:,}")
    print(f"  Total spend        : {fmt_currency(row[2])}")
    print(f"  Avg invoice value  : {fmt_currency(row[3])}")
    print(f"  Date range         : {row[4]}  →  {row[5]}")

    # ── By year ───────────────────────────────────────────────────────────────
    separator("spend by year")
    rows = con.execute("""
        SELECT
            document_year,
            COUNT(DISTINCT invoice_id)  AS invoices,
            SUM(invoice_total)          AS total_spend
        FROM mart_invoices
        WHERE document_year IS NOT NULL
        GROUP BY document_year
        ORDER BY document_year
    """).fetchall()

    print(f"  {'Year':<6} {'Invoices':>8}   {'Total Spend':>14}")
    print(f"  {'─'*6} {'─'*8}   {'─'*14}")
    for r in rows:
        print(f"  {str(r[0]):<6} {r[1]:>8,}   {fmt_currency(r[2])}")

    # ── By vendor ─────────────────────────────────────────────────────────────
    separator(f"top {top_n} vendors by spend")
    rows = con.execute(f"""
        SELECT
            vendor_entity_name,
            COUNT(DISTINCT invoice_id)  AS invoices,
            SUM(invoice_total)          AS total_spend
        FROM mart_invoices
        WHERE vendor_entity_name IS NOT NULL
        GROUP BY vendor_entity_name
        ORDER BY total_spend DESC
        LIMIT {top_n}
    """).fetchall()

    print(f"  {'Vendor':<30} {'Invoices':>8}   {'Total Spend':>14}")
    print(f"  {'─'*30} {'─'*8}   {'─'*14}")
    for r in rows:
        print(f"  {str(r[0]):<30} {r[1]:>8,}   {fmt_currency(r[2])}")

    # ── By status ─────────────────────────────────────────────────────────────
    separator("by approval status")
    total_spend = con.execute("SELECT SUM(invoice_total) FROM mart_invoices").fetchone()[0]
    rows = con.execute("""
        SELECT
            action_status,
            COUNT(DISTINCT invoice_id)  AS invoices,
            SUM(invoice_total)          AS total_spend
        FROM mart_invoices
        WHERE action_status IS NOT NULL
        GROUP BY action_status
        ORDER BY total_spend DESC
    """).fetchall()

    print(f"  {'Status':<16} {'Invoices':>8}   {'Total Spend':>14}   {'Share':>6}")
    print(f"  {'─'*16} {'─'*8}   {'─'*14}   {'─'*6}")
    for r in rows:
        pct = (float(r[2]) / float(total_spend) * 100) if total_spend else None
        print(f"  {str(r[0]):<16} {r[1]:>8,}   {fmt_currency(r[2])}   {fmt_pct(pct)}")

    # ── By variant ────────────────────────────────────────────────────────────
    separator("by invoice variant")
    rows = con.execute("""
        SELECT
            variant,
            COUNT(DISTINCT invoice_id)  AS invoices,
            COUNT(*)                    AS line_items,
            SUM(invoice_total)          AS total_spend
        FROM mart_invoices
        GROUP BY variant
        ORDER BY total_spend DESC
    """).fetchall()

    print(f"  {'Variant':<20} {'Invoices':>8}   {'Lines':>6}   {'Total Spend':>14}")
    print(f"  {'─'*20} {'─'*8}   {'─'*6}   {'─'*14}")
    for r in rows:
        print(f"  {str(r[0]):<20} {r[1]:>8,}   {r[2]:>6,}   {fmt_currency(r[3])}")

    # ── By document type ──────────────────────────────────────────────────────
    separator("by document type")
    rows = con.execute("""
        SELECT
            document_type,
            COUNT(DISTINCT invoice_id)  AS invoices,
            SUM(invoice_total)          AS total_spend
        FROM mart_invoices
        WHERE document_type IS NOT NULL
        GROUP BY document_type
        ORDER BY total_spend DESC
    """).fetchall()

    print(f"  {'Type':<20} {'Invoices':>8}   {'Total Spend':>14}")
    print(f"  {'─'*20} {'─'*8}   {'─'*14}")
    for r in rows:
        print(f"  {str(r[0]):<20} {r[1]:>8,}   {fmt_currency(r[2])}")

    # ── Spend pivot: status × year ───────────────────────────────────────────
    separator("spend by status × year")
    pivot_sql = """
        PIVOT (
            SELECT
                action_status,
                document_year,
                SUM(invoice_total) AS total_spend
            FROM mart_invoices
            WHERE action_status IS NOT NULL
              AND document_year IS NOT NULL
            GROUP BY action_status, document_year
        )
        ON document_year
        USING SUM(total_spend)
        ORDER BY action_status
    """
    cursor = con.execute(pivot_sql)
    cols = [c[0] for c in cursor.description]
    years = [c for c in cols if c != 'action_status']
    rows = cursor.fetchall()

    print(f"  {'Status':<16} " + "  ".join(f"{str(y):>15}" for y in years))
    print(f"  {'─'*16} " + "  ".join('─'*15 for _ in years))
    for r in rows:
        status = str(r[0])
        vals = "  ".join(f"{'N/A':>15}" if v is None else f"{fmt_currency(v):>15}" for v in r[1:])
        print(f"  {status:<16} {vals}")

    # ── Pipeline info ─────────────────────────────────────────────────────────
    separator("pipeline info")
    info = con.execute("""
        SELECT
            MAX(ingested_at)            AS last_run,
            COUNT(DISTINCT source_file) AS source_files
        FROM mart_invoices
    """).fetchone()

    last_run = str(info[0])[:19].replace("T", " ") if info[0] else "unknown"
    print(f"  Last run         : {last_run} UTC")
    print(f"  Source files     : {info[1]} XML files")
    print(f"  Database         : {path.resolve()}")
    separator()

    con.close()


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="xml-drift-lakehouse business report")
    parser.add_argument("--db",  default="output/lakehouse.duckdb", help="Path to DuckDB file")
    parser.add_argument("--top", type=int, default=5, help="Top N vendors to show (default: 5)")
    args = parser.parse_args()

    report(args.db, args.top)


if __name__ == "__main__":
    main()
