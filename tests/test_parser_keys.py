"""
test_parser_keys.py - ADR-0002 key tests for ingestion/parser.py
=================================================================
Uses the committed multi-day set in data/daily (see manifest.csv).

Run:
    python -m pytest tests/test_parser_keys.py -v
"""

import csv
import hashlib
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "ingestion"))

from parser import process_file, surrogate  # noqa: E402

DAILY = ROOT / "data" / "daily"
TS = "2026-01-01T00:00:00+00:00"


def parse(rel: str):
    return process_file(DAILY / rel, TS, rel)


def first_update():
    """Return (first file, later file) for the first updated invoice in the manifest."""
    with open(DAILY / "manifest.csv", newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    upd = next(r for r in rows if r["event"] == "update")
    orig = next(r for r in rows if r["invoice_id"] == upd["invoice_id"] and r["event"] == "new")
    return orig["file"], upd["file"]


def test_surrogate_hashes_only_given_parts():
    expected = hashlib.sha256("900000001|3".encode()).hexdigest()[:16]
    assert surrogate("900000001", "3") == expected
    assert surrogate("900000001") == hashlib.sha256(b"900000001").hexdigest()[:16]


def test_sk_ignores_file_and_version_sk_tracks_delivery():
    first, later = first_update()
    h1, lines1 = parse(first)
    h2, lines2 = parse(later)
    assert h1["invoice_id"] == h2["invoice_id"]
    assert h1["sk"] == h2["sk"] == surrogate(h1["invoice_id"])
    assert h1["version_sk"] != h2["version_sk"]
    assert h2["source_path"] == later
    assert h2["source_file"] == Path(later).name


def test_line_items_carry_invoice_keys():
    first, _ = first_update()
    header, lines = parse(first)
    assert lines
    for ln in lines:
        assert ln["invoice_sk"] == header["sk"]
        assert ln["version_sk"] == header["version_sk"]
        assert ln["source_path"] == header["source_path"]
        assert ln["sk"] == surrogate(header["invoice_id"], str(ln["line_number"]))
    assert len({ln["sk"] for ln in lines}) == len(lines)
