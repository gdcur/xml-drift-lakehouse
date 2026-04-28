"""
load_to_snowflake.py — xml-drift-lakehouse
==========================================
Loads the two processed Parquet files into Snowflake raw tables.
Run once after ingestion, or whenever the source data changes.

Usage:
    python scripts/load_to_snowflake.py
    python scripts/load_to_snowflake.py --db output/lakehouse.duckdb

Requires environment variables (set in .env):
    SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD,
    SNOWFLAKE_ROLE, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA
"""

import os
import argparse
from pathlib import Path

import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas


# ── Config ────────────────────────────────────────────────────────────────────

DEFAULT_INVOICES    = "output/invoices/invoices.parquet"
DEFAULT_LINE_ITEMS  = "output/line_items/line_items.parquet"


def get_snowflake_conn():
    required = [
        "SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_WAREHOUSE", "SNOWFLAKE_DATABASE"
    ]
    missing = [k for k in required if not os.environ.get(k)]
    if missing:
        raise EnvironmentError(f"Missing environment variables: {', '.join(missing)}")

    return snowflake.connector.connect(
        account   = os.environ["SNOWFLAKE_ACCOUNT"],
        user      = os.environ["SNOWFLAKE_USER"],
        password  = os.environ["SNOWFLAKE_PASSWORD"],
        role      = os.environ.get("SNOWFLAKE_ROLE", "SYSADMIN"),
        warehouse = os.environ["SNOWFLAKE_WAREHOUSE"],
        database  = os.environ["SNOWFLAKE_DATABASE"],
        schema    = os.environ.get("SNOWFLAKE_SCHEMA", "PUBLIC"),
    )


def ensure_raw_schema(cursor):
    """Create RAW schema if it doesn't exist."""
    cursor.execute("CREATE SCHEMA IF NOT EXISTS RAW")
    print("  ✓ RAW schema ready")


def load_parquet(conn, parquet_path: Path, table_name: str):
    """Load a Parquet file into a Snowflake table (replace)."""
    print(f"\n  Loading {parquet_path.name} → RAW.{table_name}")

    if not parquet_path.exists():
        raise FileNotFoundError(f"Parquet file not found: {parquet_path}")

    df = pd.read_parquet(parquet_path)

    # Snowflake requires uppercase column names
    df.columns = [c.upper() for c in df.columns]

    # Ensure all object columns are strings (Snowflake doesn't like mixed types)
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].astype(str).replace("None", None)

    cursor = conn.cursor()
    cursor.execute(f"USE SCHEMA RAW")

    # Drop and recreate for clean load
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

    success, nchunks, nrows, _ = write_pandas(
        conn,
        df,
        table_name=table_name,
        schema="RAW",
        auto_create_table=True,
        overwrite=True,
    )

    if success:
        print(f"  ✓ {nrows:,} rows loaded into RAW.{table_name} ({nchunks} chunk(s))")
    else:
        raise RuntimeError(f"Failed to load {table_name}")

    cursor.close()
    return nrows


def main():
    parser = argparse.ArgumentParser(description="Load Parquet files into Snowflake raw tables")
    parser.add_argument("--invoices",   default=DEFAULT_INVOICES,   help="Path to invoices.parquet")
    parser.add_argument("--line-items", default=DEFAULT_LINE_ITEMS, help="Path to line_items.parquet")
    args = parser.parse_args()

    invoices_path   = Path(args.invoices)
    line_items_path = Path(args.line_items)

    print("=" * 55)
    print("  xml-drift-lakehouse — Snowflake loader")
    print("=" * 55)
    print(f"\n  Account  : {os.environ.get('SNOWFLAKE_ACCOUNT', '(not set)')}")
    print(f"  Database : {os.environ.get('SNOWFLAKE_DATABASE', '(not set)')}")
    print(f"  Warehouse: {os.environ.get('SNOWFLAKE_WAREHOUSE', '(not set)')}")

    print("\n  Connecting to Snowflake...")
    conn = get_snowflake_conn()
    print("  ✓ Connected")

    cursor = conn.cursor()
    cursor.execute(f"USE DATABASE {os.environ['SNOWFLAKE_DATABASE']}")
    ensure_raw_schema(cursor)
    cursor.close()

    total_rows = 0
    total_rows += load_parquet(conn, invoices_path,   "INVOICES")
    total_rows += load_parquet(conn, line_items_path, "LINE_ITEMS")

    conn.close()

    print(f"\n{'=' * 55}")
    print(f"  Done. {total_rows:,} total rows loaded into Snowflake.")
    print(f"  Run: dbt run --target snowflake")
    print(f"{'=' * 55}\n")


if __name__ == "__main__":
    main()
