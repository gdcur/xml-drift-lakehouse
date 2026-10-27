#!/usr/bin/env python
"""
iceberg_demo.py - show what the Iceberg landing zone holds (ADR-0001, ADR-0002)
===============================================================================
Prints a short report from the Iceberg tables built by iceberg_writer.py:

  1. what the tables hold now
  2. the snapshot log (one entry per change)
  3. the invoices that were revised most often
  4. the history of one invoice, read back with time travel
  5. which lines were added or removed between its first and last version

If the warehouse is empty, the 6-day set in data/daily is replayed first.

Usage:
    python scripts/iceberg_demo.py
    python scripts/iceberg_demo.py --invoice 900000003
    python scripts/iceberg_demo.py --warehouse output/iceberg --daily data/daily
"""

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
from pyiceberg.expressions import EqualTo

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "ingestion"))

import iceberg_writer as iw  # noqa: E402
from parser import process_file  # noqa: E402


def heading(text: str) -> None:
    print(f"\n{text}\n{'-' * len(text)}")


def clock(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%H:%M:%S")


def snapshots(table) -> list:
    return sorted(table.snapshots(), key=lambda s: s.timestamp_ms)


def props(snap) -> dict:
    return dict(getattr(snap.summary, "additional_properties", {}) or {}) if snap.summary else {}


def operation(snap) -> str:
    op = getattr(snap.summary, "operation", None)
    return getattr(op, "value", str(op)) if op else "?"


def scan(table, snapshot_id, invoice_id, columns) -> pl.DataFrame:
    arrow = table.scan(row_filter=EqualTo("invoice_id", invoice_id), selected_fields=columns,
                       snapshot_id=snapshot_id).to_arrow()
    return pl.from_arrow(arrow)


def replay(catalog, daily: Path) -> None:
    days = sorted(p for p in daily.iterdir() if p.is_dir())
    if not days:
        raise SystemExit(f"no day folders in {daily}")
    print(f"Warehouse is empty: replaying {len(days)} days from {daily}")
    for day in days:
        headers, lines = [], []
        for f in sorted(day.glob("*.xml")):
            h, ls = process_file(f, datetime.now(timezone.utc).isoformat(), f.relative_to(daily).as_posix())
            if h:
                headers.append(h)
                lines.extend(ls)
        s = iw.load_batch(
            catalog,
            pl.DataFrame(headers, infer_schema_length=len(headers)),
            pl.DataFrame(lines, infer_schema_length=len(lines)) if lines else pl.DataFrame(),
        )
        print(f"  {day.name}: {s['received']} invoices -> new {s['new']}, updated {s['updated']}, "
              f"unchanged {s['unchanged']}, stale {s['stale']}")


def revisions(inv_t) -> dict[str, list[str]]:
    """For every invoice: the distinct source files it came from, in order of arrival."""
    seen: dict[str, list[str]] = {}
    for snap in snapshots(inv_t):
        arrow = inv_t.scan(selected_fields=("invoice_id", "source_path"), snapshot_id=snap.snapshot_id).to_arrow()
        for inv_id, path in zip(arrow["invoice_id"].to_pylist(), arrow["source_path"].to_pylist()):
            if path not in seen.setdefault(inv_id, []):
                seen[inv_id].append(path)
    return seen


def timeline(inv_t, line_t, invoice_id: str) -> list[dict]:
    states, last = [], None
    for snap in snapshots(inv_t):
        df = scan(inv_t, snap.snapshot_id, invoice_id,
                  ("source_path", "action_type", "action_status", "action_datetime"))
        if df.height == 0:      # the delete half of a replace: the invoice is briefly absent
            continue
        row = df.row(0, named=True)
        line_snap = line_t.snapshot_as_of_timestamp(snap.timestamp_ms)
        line_id = line_snap.snapshot_id if line_snap else None
        n_lines = scan(line_t, line_id, invoice_id, ("line_number",)).height if line_id else 0
        key = (row["source_path"], row["action_type"], row["action_status"], n_lines)
        if key != last:
            states.append({**row, "at": clock(snap.timestamp_ms), "lines": n_lines,
                           "line_set": line_set(line_t, line_id, invoice_id)})
            last = key
    return states


def line_set(line_t, snapshot_id, invoice_id) -> set[tuple]:
    """(line_number, label, line_total) for every line; Summary invoices have no product description."""
    if snapshot_id is None:
        return set()
    df = scan(line_t, snapshot_id, invoice_id, ("line_number", "product_description", "charge_class", "line_total"))
    # a scan returns columns in table order, so pick them by name
    rows = df.select("line_number", "product_description", "charge_class", "line_total").iter_rows()
    return {(n, desc or cls or "(no description)", total) for n, desc, cls, total in rows}


def describe_change(prev: dict | None, cur: dict) -> str:
    if prev is None:
        return "first delivery"
    parts = []
    before = (prev["action_type"], prev["action_status"])
    after = (cur["action_type"], cur["action_status"])
    if before != after or prev["action_datetime"] != cur["action_datetime"]:
        parts.append(f"workflow {before[0]}/{before[1]} -> {after[0]}/{after[1]}")
    if prev["line_set"] != cur["line_set"]:
        parts.append("lines changed")
    return ", ".join(parts) or "same content, delivered again"


def main() -> None:
    p = argparse.ArgumentParser(description="Report on the Iceberg landing zone")
    p.add_argument("--warehouse", default=str(ROOT / "output" / "iceberg"))
    p.add_argument("--daily", default=str(ROOT / "data" / "daily"))
    p.add_argument("--invoice", default=None, help="invoice_id to follow (default: the most revised one)")
    args = p.parse_args()

    catalog = iw.get_catalog(args.warehouse)
    tables = iw.init_tables(catalog, verbose=False)
    inv_t, line_t = tables[iw.INVOICES], tables[iw.LINE_ITEMS]
    if inv_t.current_snapshot() is None:
        replay(catalog, Path(args.daily))
        inv_t, line_t = catalog.load_table(iw.INVOICES), catalog.load_table(iw.LINE_ITEMS)

    # 1. now ------------------------------------------------------------------
    inv, lines = iw.read_table(inv_t), iw.read_table(line_t)
    variants = ", ".join(f"{r[0]} {r[1]}" for r in inv.group_by("variant").len().sort("variant").iter_rows())
    heading("1. What the tables hold now")
    print(f"  warehouse     {Path(args.warehouse).resolve()}")
    print(f"  invoices      {inv.height} rows, one per invoice_id  ({variants})")
    print(f"  line_items    {lines.height} rows, one per (invoice_id, line_number)")
    print(f"  snapshots     invoices {len(inv_t.snapshots())}, line_items {len(line_t.snapshots())}")
    print(f"  partitioning  invoices by month(document_date); line_items unpartitioned")
    problems = iw.check_consistency(catalog)
    print(f"  consistency   {'OK: keys unique, every line matches its invoice version' if not problems else problems}")

    # 2. snapshot log -----------------------------------------------------------
    heading("2. Snapshot log of the invoices table (one entry per change)")
    print("  An update is stored as delete + append, so it shows up as two or three entries.")
    print(f"  {'#':>3}  {'time (UTC)':<10}  {'operation':<10}  {'rows after':>10}")
    for i, snap in enumerate(snapshots(inv_t), start=1):
        print(f"  {i:>3}  {clock(snap.timestamp_ms):<10}  {operation(snap):<10}  {props(snap).get('total-records', '?'):>10}")

    # 3. most revised -----------------------------------------------------------
    seen = revisions(inv_t)
    ranked = sorted(seen.items(), key=lambda kv: (-len(kv[1]), kv[0]))
    heading("3. Invoices revised most often (distinct files they arrived in)")
    for inv_id, paths in ranked[:5]:
        print(f"  {inv_id}  {len(paths)} versions   {' -> '.join(paths)}")

    # 4. one invoice over time ----------------------------------------------------
    target = args.invoice or ranked[0][0]
    if target not in seen:
        raise SystemExit(f"invoice {target} is not in the table")
    states = timeline(inv_t, line_t, target)
    heading(f"4. History of invoice {target}, read back with time travel")
    print(f"  {'time':<9} {'arrived in':<24} {'action':<22} {'lines':>5}  what changed")
    for i, st in enumerate(states):
        action = f"{st['action_type']}/{st['action_status']}"
        print(f"  {st['at']:<9} {st['source_path']:<24} {action:<22} {st['lines']:>5}  "
              f"{describe_change(states[i - 1] if i else None, st)}")

    # 5. what changed in the lines ----------------------------------------------------
    heading(f"5. Line changes between first and last version of {target}")
    if len(states) < 2:
        print("  only one version: nothing to compare")
    else:
        first, last = states[0]["line_set"], states[-1]["line_set"]
        removed, added = sorted(first - last), sorted(last - first)
        print(f"  first version {len(first)} lines, last version {len(last)} lines")
        for n, label, total in removed:
            print(f"  - line {n}: {label:<14} {total:>12,.2f}")
        for n, label, total in added:
            print(f"  + line {n}: {label:<14} {total:>12,.2f}")
        if not removed and not added:
            same_header = all((st["action_type"], st["action_status"], st["action_datetime"])
                              == (states[0]["action_type"], states[0]["action_status"], states[0]["action_datetime"])
                              for st in states)
            print("  no change: the invoice was only delivered again" if same_header
                  else "  lines are identical; only the workflow state changed")
    print()


if __name__ == "__main__":
    main()
