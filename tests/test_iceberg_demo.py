"""
test_iceberg_demo.py - smoke test for scripts/iceberg_demo.py
=============================================================
Runs the demo on an empty temporary warehouse (it replays data/daily first)
and checks the report. Skipped when pyiceberg is not installed.

Run:
    python -m pytest tests/test_iceberg_demo.py -v
"""

import subprocess
import sys
from pathlib import Path

import pytest

pytest.importorskip("pyiceberg")

ROOT = Path(__file__).parent.parent


def run_demo(warehouse: Path, *extra: str) -> str:
    done = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "iceberg_demo.py"), "--warehouse", str(warehouse), *extra],
        capture_output=True, text=True, timeout=120,
    )
    assert done.returncode == 0, done.stderr
    return done.stdout


def test_demo_replays_and_reports(tmp_path):
    out = run_demo(tmp_path / "wh")
    assert "Warehouse is empty: replaying 6 days" in out
    assert "25 rows, one per invoice_id" in out and "147 rows" in out
    assert "OK: keys unique" in out
    assert "first delivery" in out and "workflow" in out
    assert "- line 5:" in out and "+ line 5:" in out         # the invoice whose line was replaced


def test_demo_on_existing_warehouse_and_redelivered_invoice(tmp_path):
    run_demo(tmp_path / "wh")
    out = run_demo(tmp_path / "wh", "--invoice", "900000008")   # only ever re-delivered
    assert "Warehouse is empty" not in out
    assert "same content, delivered again" in out
    assert "no change: the invoice was only delivered again" in out
