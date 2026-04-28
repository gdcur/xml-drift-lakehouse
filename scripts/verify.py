"""
verify.py — xml-drift-lakehouse result checker
===============================================
Queries the DuckDB lakehouse after a pipeline run and prints a clean summary.
No DuckDB CLI needed — just run:

    python verify.py

Optional arguments:
    --db    path to lakehouse.duckdb (default: ./output/lakehouse.duckdb)
    --rows  number of sample rows to show (default: 5)
"""

import argparse
import sys
from pathlib import Path

try:
    import duckdb
except ImportError:
    print("duckdb not installed. Run: pip install duckdb")
    sys.exit(1)


def separator(title: str = "") -> None:
    if title:
        print(f"\n{'─' * 10} {title} {'─' * (38 - len(title))}")
    else:
        print("─" * 50)


def verify(db_path: str, sample_rows: int = 5) -> None:
    path = Path(db_path)
    if not path.exists():
        print(f"No database found at {db_path}")
        print("Run the Airflow DAG first, then try again.")
        sys.exit(1)

    con = duckdb.connect(db_path, read_only=True)

    # ── Check tables exist ────────────────────────────────────────────────────
    tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
    required = {"mart_invoices", "mapping_registry"}
    missing  = required - tables

    if missing:
        print(f"Tables not found: {missing}")
        print("The pipeline may not have completed successfully.")
        con.close()
        sys.exit(1)

    # ── mart_invoices ─────────────────────────────────────────────────────────
    separator("mart_invoices")

    counts = con.execute("""
        SELECT
            COUNT(*)                                          AS total_rows,
            COUNT(DISTINCT invoice_id)                        AS unique_invoices,
            COALESCE(SUM(CASE WHEN variant = 'DetailedInvoice' THEN 1 ELSE 0 END), 0) AS detailed,
            COALESCE(SUM(CASE WHEN variant = 'SummaryInvoice'  THEN 1 ELSE 0 END), 0) AS summary
        FROM mart_invoices
    """).fetchone()

    print(f"  Total rows       : {counts[0]:,}")
    print(f"  Unique invoices  : {counts[1]:,}")
    print(f"  DetailedInvoice  : {counts[2]:,}")
    print(f"  SummaryInvoice   : {counts[3]:,}")

    # Status distribution
    statuses = con.execute("""
        SELECT action_status, COUNT(*) AS cnt
        FROM mart_invoices
        WHERE action_status IS NOT NULL
        GROUP BY action_status
        ORDER BY cnt DESC
        LIMIT 5
    """).fetchall()

    if statuses:
        print(f"\n  Status distribution:")
        for status, cnt in statuses:
            print(f"    {status:<20} {cnt:,}")

    # Sample rows
    separator(f"sample invoices (first {sample_rows})")
    rows = con.execute(f"""
        SELECT
            invoice_id,
            document_number,
            document_date,
            vendor_entity_name,
            invoice_total,
            action_status
        FROM mart_invoices
        ORDER BY ingested_at DESC
        LIMIT {sample_rows}
    """).fetchall()

    if not rows:
        print("  No rows found.")
    else:
        header = f"  {'invoice_id':<12} {'document_number':<16} {'date':<12} {'vendor':<25} {'total':>12}  status"
        print(header)
        print(f"  {'-'*12} {'-'*16} {'-'*12} {'-'*25} {'-'*12}  {'-'*12}")
        for r in rows:
            print(f"  {str(r[0] or ''):<12} {str(r[1] or ''):<16} {str(r[2] or ''):<12} {str(r[3] or ''):<25} {str(r[4] or ''):>12}  {str(r[5] or '')}")


    # ── mapping_registry ──────────────────────────────────────────────────────
    separator("mapping_registry")

    reg_count  = con.execute("SELECT COUNT(*) FROM mapping_registry").fetchone()[0]
    open_count = con.execute(
        "SELECT COUNT(*) FROM mapping_registry WHERE status != 'resolved'"
    ).fetchone()[0]

    if reg_count == 0:
        print("  Empty — no drift detected on last run.")
        print("  To test drift: open any XML in data/sample/ and add an unknown field,")
        print("  then re-trigger the DAG.")
    else:
        resolved_count = reg_count - open_count
        print(f"  Total entries    : {reg_count}")
        print(f"  Open (unresolved): {open_count}")
        print(f"  Resolved         : {resolved_count}")

        if open_count == 0:
            print("\n  All mapped fields have been resolved.")
        else:
            # Show open fields only
            open_rows = con.execute("""
                SELECT
                    source_field,
                    mapped_to,
                    decision_type,
                    confidence,
                    llm_reasoning,
                    status
                FROM mapping_registry
                WHERE status != 'resolved'
                ORDER BY confidence DESC
            """).fetchall()

            separator("fields requiring action")
            for row in open_rows:
                icon = {"auto_approved": "✅", "flagged_review": "⚠️ ", "pending_human": "🛑"}.get(row[2], "  ")
                print(f"  {icon} {row[0]:<25} → {row[1]:<25} ({row[3]:.2f})  [{row[5]}]")
                print(f"     {row[4][:300]}")

            print()
            print("  To resolve a field after applying the mapping:")
            print("  1. Add the field to BASELINE_FIELDS in ingestion/schema_diff.py")
            print("  2. Add an alias in ingestion/parser.py")
            print("  3. Mark as resolved:")
            print()
            if open_rows:
                for row in open_rows:
                    print(f"     python verify.py --resolve {row[0]}")
            print()
            print("  Then re-run the DAG to confirm no drift remains.")


    separator()
    print(f"  Database: {path.resolve()}")
    con.close()


def resolve(db_path: str, field_name: str) -> None:
    """Mark a field as resolved in mapping_registry."""
    path = Path(db_path)
    if not path.exists():
        print(f"No database found at {db_path}")
        sys.exit(1)

    con = duckdb.connect(db_path, read_only=False)
    result = con.execute(
        "SELECT COUNT(*) FROM mapping_registry WHERE source_field = ? AND status != 'resolved'",
        [field_name]
    ).fetchone()[0]

    if result == 0:
        print(f"  No open entry found for '{field_name}' — already resolved or not in registry.")
        con.close()
        return

    con.execute("""
        UPDATE mapping_registry
        SET status = 'resolved',
            updated_at = current_timestamp
        WHERE source_field = ?
        AND status != 'resolved'
    """, [field_name])

    print(f"  Resolved: '{field_name}' marked as resolved in mapping_registry.")
    print(f"  Remember to:")
    print(f"    1. Add '{field_name}' to BASELINE_FIELDS in ingestion/schema_diff.py")
    print(f"    2. Add alias in ingestion/parser.py")
    print(f"    3. Re-run the DAG to confirm no drift remains")
    con.close()


def main():
    parser = argparse.ArgumentParser(
        description="Verify xml-drift-lakehouse pipeline results"
    )
    parser.add_argument(
        "--db",
        default="./output/lakehouse.duckdb",
        help="Path to lakehouse.duckdb (default: ./output/lakehouse.duckdb)"
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=5,
        help="Number of sample rows to display (default: 5)"
    )
    parser.add_argument(
        "--resolve",
        metavar="FIELD_NAME",
        help="Mark a field as resolved after applying the mapping"
    )
    args = parser.parse_args()

    if args.resolve:
        resolve(args.db, args.resolve)
    else:
        verify(args.db, args.rows)


if __name__ == "__main__":
    main()
