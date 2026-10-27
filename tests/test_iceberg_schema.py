"""
test_iceberg_schema.py - Iceberg table definitions (ADR-0001, ADR-0002)
=======================================================================
Runs --init twice on a temporary warehouse and checks schema, identifier
fields and partition spec. Skipped when pyiceberg is not installed.

Run:
    python -m pytest tests/test_iceberg_schema.py -v
"""

import sys
from pathlib import Path

import pytest

pytest.importorskip("pyiceberg")

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "ingestion"))

import iceberg_writer as iw  # noqa: E402
from parser import process_file  # noqa: E402
from pyiceberg.transforms import MonthTransform  # noqa: E402


@pytest.fixture
def tables(tmp_path):
    catalog = iw.get_catalog(tmp_path / "wh")
    iw.init_tables(catalog)
    first = {i: catalog.load_table(i).metadata_location for i in iw.TABLES}
    iw.init_tables(catalog)  # second run must be a no-op
    assert {i: catalog.load_table(i).metadata_location for i in iw.TABLES} == first
    return {i: catalog.load_table(i) for i in iw.TABLES}


def test_schemas_match_parser_output(tables):
    headers, lines = [], []
    for f in sorted((ROOT / "data" / "sample").glob("*.xml")):
        h, ls = process_file(f, "2026-01-01T00:00:00+00:00")
        headers.append(h)
        lines.extend(ls)
    inv_cols = list(dict.fromkeys(k for h in headers for k in h))
    line_cols = list(dict.fromkeys(k for ln in lines for k in ln))
    assert [f.name for f in tables[iw.INVOICES].schema().fields] == inv_cols
    assert [f.name for f in tables[iw.LINE_ITEMS].schema().fields] == line_cols
    assert (len(inv_cols), len(line_cols)) == (37, 46)


def test_no_null_types_and_key_types(tables):
    for t in tables.values():
        assert all(str(f.field_type) not in ("null", "unknown") for f in t.schema().fields)
    inv = tables[iw.INVOICES].schema()
    assert str(inv.find_field("document_date").field_type) == "date"
    assert str(inv.find_field("action_datetime").field_type) == "timestamp"
    assert str(tables[iw.LINE_ITEMS].schema().find_field("line_number").field_type) == "long"


def test_identifier_fields_are_required(tables):
    expected = {iw.INVOICES: {"invoice_id"}, iw.LINE_ITEMS: {"invoice_id", "line_number"}}
    for ident, keys in expected.items():
        schema = tables[ident].schema()
        assert set(schema.identifier_field_names()) == keys
        assert {f.name for f in schema.fields if f.required} == keys


def test_partition_specs(tables):
    inv = tables[iw.INVOICES]
    (field,) = inv.spec().fields
    assert isinstance(field.transform, MonthTransform)
    assert inv.schema().find_field(field.source_id).name == "document_date"
    assert tables[iw.LINE_ITEMS].spec().is_unpartitioned()
