"""
test_rag_flow.py — xml-drift-lakehouse integration tests
=========================================================
Tests the full RAG pipeline end-to-end without an LLM API key or Ollama.

Test coverage:
  - TestSchemaDiff     : compute_diff() against synthetic XMLs
  - TestScoreConfidence: composite confidence scoring
  - TestMappingRegistry: DuckDB save/retrieve/query
  - TestRagFlow        : full pipeline integration (diff → map → registry)

Run:
    python tests/test_rag_flow.py
    python tests/test_rag_flow.py -v
"""

import json
import os
import sys
import tempfile
import textwrap
import unittest
from datetime import date
from pathlib import Path

# ── Add ingestion to path ─────────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).parent.parent / "ingestion"))

import duckdb
from schema_diff import compute_diff, render_diff_report, BASELINE_FIELDS
from rag_mapper import (
    BaseLLMMapper, score_confidence,
    init_registry, save_decision, process_diff, CORPUS,
)

# ── Namespace ─────────────────────────────────────────────────────────────────
NS = "http://www.fieldops-demo.io/xml/ns"


# ── XML fixtures ──────────────────────────────────────────────────────────────

def _xml_detailed(doc_num: str = "INV-001", extra_fields: str = "") -> str:
    return textwrap.dedent(f"""\
        <?xml version="1.0" encoding="UTF-8"?>
        <fops:StandardBillingDocument xmlns:fops="{NS}">
          <fops:DetailedInvoice>
            <fops:DocumentHeader>
              <fops:DocumentNumber>{doc_num}</fops:DocumentNumber>
              <fops:DocumentDate>2026-04-23</fops:DocumentDate>
              <fops:DocumentType>Invoice</fops:DocumentType>
              <fops:CurrencyCode>USD</fops:CurrencyCode>
              <fops:Total>1250.00</fops:Total>
              <fops:VendorTotal>1250.00</fops:VendorTotal>
              <fops:Status>Approved</fops:Status>
              {extra_fields}
            </fops:DocumentHeader>
            <fops:DocumentLines>
              <fops:LineEntry>
                <fops:LineNumber>1</fops:LineNumber>
                <fops:Quantity>10.0</fops:Quantity>
                <fops:UnitPrice>125.00</fops:UnitPrice>
                <fops:Total>1250.00</fops:Total>
              </fops:LineEntry>
            </fops:DocumentLines>
          </fops:DetailedInvoice>
        </fops:StandardBillingDocument>
    """)


def _xml_summary(doc_num: str = "INV-002", extra_fields: str = "") -> str:
    return textwrap.dedent(f"""\
        <?xml version="1.0" encoding="UTF-8"?>
        <fops:StandardBillingDocument xmlns:fops="{NS}">
          <fops:SummaryInvoice>
            <fops:DocumentHeader>
              <fops:DocumentNumber>{doc_num}</fops:DocumentNumber>
              <fops:DocumentDate>2026-04-23</fops:DocumentDate>
              <fops:Total>8800.00</fops:Total>
              <fops:VendorTotal>8800.00</fops:VendorTotal>
              <fops:Status>Pending</fops:Status>
              {extra_fields}
            </fops:DocumentHeader>
            <fops:DocumentLines>
              <fops:LineEntry>
                <fops:LineNumber>1</fops:LineNumber>
                <fops:Total>8800.00</fops:Total>
              </fops:LineEntry>
            </fops:DocumentLines>
          </fops:SummaryInvoice>
        </fops:StandardBillingDocument>
    """)


# ── Mock LLM mapper ───────────────────────────────────────────────────────────

class MockLLMMapper(BaseLLMMapper):
    """
    Deterministic mock — no API call, no Ollama needed.
    Returns predictable mappings for known test fields.
    """

    KNOWN = {
        "InvoiceAmount":  ("invoice_total",      0.92, "Amount field maps to invoice_total"),
        "InvoiceAmt":     ("invoice_total",      0.88, "Abbreviation maps to invoice_total"),
        "VendorCode":     ("vendor_entity_code", 0.85, "Code suffix matches entity_code pattern"),
        "DocNumber":      ("invoice_id",         0.91, "Abbreviation of document number"),
        "RequestedBy":    ("vendor_entity_name", 0.80, "Person name maps to entity name"),
        "SupplierRating": ("UNKNOWN",            0.10, "No matching field in schema"),
        "InternalNotes":  ("UNKNOWN",            0.08, "Free-text field not in baseline"),
    }

    def suggest_mapping(self, new_field: dict) -> dict:
        fname = new_field["field_name"]
        if fname in self.KNOWN:
            mapped, conf, reason = self.KNOWN[fname]
            return {"mapped_to": mapped, "confidence": conf, "reasoning": reason}
        return {
            "mapped_to":  "UNKNOWN",
            "confidence": 0.15,
            "reasoning":  "No confident match found",
        }


# ── Test: schema_diff ─────────────────────────────────────────────────────────

class TestSchemaDiff(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.src    = Path(self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()

    def _write(self, filename: str, content: str):
        (self.src / filename).write_text(content)

    def test_no_drift_known_fields_only(self):
        self._write("inv_001.xml", _xml_detailed())
        self._write("inv_002.xml", _xml_summary())
        diff = compute_diff(self.src)
        self.assertFalse(diff["has_drift"],
            f"Expected no drift but got: {diff['new_fields']}")
        self.assertEqual(diff["new_fields"],    [])
        self.assertEqual(diff["variant_drift"], [])
        self.assertEqual(diff["type_conflicts"],[])

    def test_detects_new_field(self):
        self._write("inv_001.xml", _xml_detailed(
            extra_fields="<fops:InvoiceAmount>1250.00</fops:InvoiceAmount>"
        ))
        diff = compute_diff(self.src)
        self.assertTrue(diff["has_drift"])
        names = [f["field_name"] for f in diff["new_fields"]]
        self.assertIn("InvoiceAmount", names)

    def test_detects_multiple_new_fields(self):
        extras = """
            <fops:InvoiceAmount>1250.00</fops:InvoiceAmount>
            <fops:VendorCode>V-001</fops:VendorCode>
            <fops:InternalNotes>Reviewed</fops:InternalNotes>
        """
        self._write("inv_001.xml", _xml_detailed(extra_fields=extras))
        diff  = compute_diff(self.src)
        names = {f["field_name"] for f in diff["new_fields"]}
        self.assertGreaterEqual(
            len(names & {"InvoiceAmount", "VendorCode", "InternalNotes"}), 3
        )

    def test_known_fields_not_flagged_as_new(self):
        self._write("inv_001.xml", _xml_detailed(
            extra_fields="<fops:InvoiceAmount>999.00</fops:InvoiceAmount>"
        ))
        diff      = compute_diff(self.src)
        new_names = {f["field_name"] for f in diff["new_fields"]}
        for known in BASELINE_FIELDS:
            self.assertNotIn(known, new_names,
                f"Known field '{known}' incorrectly flagged as new")

    def test_new_field_has_required_keys(self):
        self._write("inv_001.xml", _xml_detailed(
            extra_fields="<fops:InvoiceAmount>1250.00</fops:InvoiceAmount>"
        ))
        diff  = compute_diff(self.src)
        field = next(f for f in diff["new_fields"] if f["field_name"] == "InvoiceAmount")
        for key in ("field_name", "observed_type", "parents", "variants", "example_values"):
            self.assertIn(key, field, f"Missing key: {key}")

    def test_empty_directory(self):
        diff = compute_diff(self.src)
        self.assertEqual(diff["files_scanned"], 0)
        self.assertFalse(diff["has_drift"])

    def test_malformed_xml_skipped(self):
        (self.src / "bad.xml").write_text("<unclosed>")
        self._write("good.xml", _xml_detailed())
        diff = compute_diff(self.src)
        self.assertGreaterEqual(diff["files_scanned"], 1)

    def test_files_scanned_count(self):
        self._write("inv_001.xml", _xml_detailed())
        self._write("inv_002.xml", _xml_summary())
        diff = compute_diff(self.src)
        self.assertEqual(diff["files_scanned"], 2)

    def test_render_no_drift(self):
        self._write("inv_001.xml", _xml_detailed())
        diff   = compute_diff(self.src)
        report = render_diff_report(diff)
        self.assertIn("No Schema Drift", report)

    def test_render_drift(self):
        self._write("inv_001.xml", _xml_detailed(
            extra_fields="<fops:InvoiceAmount>1250.00</fops:InvoiceAmount>"
        ))
        diff   = compute_diff(self.src)
        report = render_diff_report(diff)
        self.assertIn("Schema Drift Detected", report)
        self.assertIn("InvoiceAmount", report)

    def test_deterministic(self):
        extras = "<fops:InvoiceAmount>1250.00</fops:InvoiceAmount>"
        self._write("inv_001.xml", _xml_detailed(extra_fields=extras))
        diff1 = compute_diff(self.src)
        diff2 = compute_diff(self.src)
        self.assertEqual(diff1["has_drift"],     diff2["has_drift"])
        self.assertEqual(diff1["files_scanned"], diff2["files_scanned"])
        names1 = sorted(f["field_name"] for f in diff1["new_fields"])
        names2 = sorted(f["field_name"] for f in diff2["new_fields"])
        self.assertEqual(names1, names2)


# ── Test: score_confidence ────────────────────────────────────────────────────

class TestScoreConfidence(unittest.TestCase):

    def test_unknown_returns_zero(self):
        field = {"field_name": "Foo", "observed_type": "string", "parents": []}
        self.assertEqual(score_confidence(field, "UNKNOWN", 0.9), 0.0)

    def test_high_llm_confidence_scores_well(self):
        field = {
            "field_name":    "InvoiceTotal",
            "observed_type": "decimal",
            "parents":       ["DocumentHeader"],
        }
        score = score_confidence(field, "invoice_total", 0.95)
        self.assertGreater(score, 0.60)

    def test_low_llm_confidence_lowers_score(self):
        field = {
            "field_name":    "InvoiceTotal",
            "observed_type": "decimal",
            "parents":       ["DocumentHeader"],
        }
        high = score_confidence(field, "invoice_total", 0.95)
        low  = score_confidence(field, "invoice_total", 0.20)
        self.assertGreater(high, low)

    def test_type_mismatch_lowers_score(self):
        field_str = {"field_name": "X", "observed_type": "string", "parents": []}
        field_dec = {"field_name": "X", "observed_type": "decimal", "parents": []}
        score_str = score_confidence(field_str, "invoice_total", 0.80)
        score_dec = score_confidence(field_dec, "invoice_total", 0.80)
        self.assertLess(score_str, score_dec)

    def test_score_in_valid_range(self):
        field = {"field_name": "X", "observed_type": "decimal", "parents": []}
        score = score_confidence(field, "invoice_total", 0.80)
        self.assertGreaterEqual(score, 0.0)
        self.assertLessEqual(score, 1.0)

    def test_unmapped_field_uses_llm_weight(self):
        field = {"field_name": "WeirdField", "observed_type": "string", "parents": []}
        score = score_confidence(field, "some_unknown_not_in_corpus", 0.80)
        self.assertGreater(score, 0.0)


# ── Test: mapping registry ────────────────────────────────────────────────────

class TestMappingRegistry(unittest.TestCase):

    def setUp(self):
        fd, self.db_path = tempfile.mkstemp(suffix=".duckdb")
        os.close(fd)
        os.unlink(self.db_path)
        self.con = init_registry(self.db_path)

    def tearDown(self):
        self.con.close()
        Path(self.db_path).unlink(missing_ok=True)

    def _make_decision(self, **overrides) -> dict:
        d = {
            "run_date":      str(date.today()),
            "source_field":  "InvoiceAmount",
            "source_path":   "DocumentHeader",
            "source_type":   "decimal",
            "mapped_to":     "invoice_total",
            "confidence":    0.88,
            "decision_type": "auto_approved",
            "llm_provider":  "mock",
            "llm_reasoning": "Amount field maps to invoice_total",
            "status":        "active",
        }
        d.update(overrides)
        return d

    def test_save_and_retrieve(self):
        save_decision(self.con, self._make_decision())
        rows = self.con.execute(
            "SELECT source_field, mapped_to FROM mapping_registry"
        ).fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][0], "InvoiceAmount")
        self.assertEqual(rows[0][1], "invoice_total")

    def test_multiple_decisions(self):
        for i in range(3):
            save_decision(self.con, self._make_decision(
                source_field=f"Field{i}",
                mapped_to="invoice_total" if i % 2 == 0 else "line_total",
            ))
        count = self.con.execute("SELECT COUNT(*) FROM mapping_registry").fetchone()[0]
        self.assertEqual(count, 3)

    def test_upsert_same_field_same_date(self):
        save_decision(self.con, self._make_decision(mapped_to="invoice_total"))
        save_decision(self.con, self._make_decision(mapped_to="line_total"))
        count = self.con.execute("SELECT COUNT(*) FROM mapping_registry").fetchone()[0]
        self.assertEqual(count, 1)
        row = self.con.execute("SELECT mapped_to FROM mapping_registry").fetchone()
        self.assertEqual(row[0], "line_total")

    def test_decision_types_persisted(self):
        for dt, status in [
            ("auto_approved",  "active"),
            ("flagged_review", "active"),
            ("pending_human",  "pending"),
        ]:
            save_decision(self.con, self._make_decision(
                source_field=f"Field_{dt}",
                decision_type=dt,
                status=status,
            ))
        types = [r[0] for r in self.con.execute(
            "SELECT decision_type FROM mapping_registry ORDER BY id"
        ).fetchall()]
        self.assertIn("auto_approved",  types)
        self.assertIn("flagged_review", types)
        self.assertIn("pending_human",  types)

    def test_query_pending(self):
        save_decision(self.con, self._make_decision(
            source_field="SupplierRating",
            mapped_to="UNKNOWN",
            decision_type="pending_human",
            status="pending",
        ))
        save_decision(self.con, self._make_decision(
            source_field="InvoiceAmount",
            decision_type="auto_approved",
            status="active",
        ))
        pending = self.con.execute(
            "SELECT source_field FROM mapping_registry WHERE status = 'pending'"
        ).fetchall()
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0][0], "SupplierRating")

    def test_persist_across_reconnect(self):
        save_decision(self.con, self._make_decision())
        self.con.close()
        con2 = duckdb.connect(self.db_path)
        rows = con2.execute("SELECT source_field FROM mapping_registry").fetchall()
        con2.close()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][0], "InvoiceAmount")


# ── Test: full RAG flow ───────────────────────────────────────────────────────

class TestRagFlow(unittest.TestCase):

    def setUp(self):
        self.xml_dir = tempfile.TemporaryDirectory()
        self.src     = Path(self.xml_dir.name)
        fd, self.db_path = tempfile.mkstemp(suffix=".duckdb")
        os.close(fd)
        os.unlink(self.db_path)

    def tearDown(self):
        self.xml_dir.cleanup()
        Path(self.db_path).unlink(missing_ok=True)

    def _write(self, filename: str, content: str):
        (self.src / filename).write_text(content)

    def test_no_drift_no_mapping(self):
        self._write("inv_001.xml", _xml_detailed())
        diff = compute_diff(self.src)
        self.assertFalse(diff["has_drift"])
        decisions = process_diff(diff, MockLLMMapper(), con=None, dry_run=True)
        self.assertEqual(decisions, [])

    def test_dry_run_no_db_written(self):
        self._write("inv_001.xml", _xml_detailed(
            extra_fields="<fops:InvoiceAmount>1250.00</fops:InvoiceAmount>"
        ))
        diff      = compute_diff(self.src)
        decisions = process_diff(diff, MockLLMMapper(), con=None, dry_run=True)
        self.assertGreater(len(decisions), 0)
        self.assertFalse(Path(self.db_path).exists())

    def test_high_confidence_auto_approved(self):
        self._write("inv_001.xml", _xml_detailed(
            extra_fields="<fops:InvoiceAmount>1250.00</fops:InvoiceAmount>"
        ))
        diff = compute_diff(self.src)
        con  = init_registry(self.db_path)
        decisions = process_diff(diff, MockLLMMapper(), con, dry_run=False)
        con.close()
        decision = next(
            (d for d in decisions if d["source_field"] == "InvoiceAmount"), None
        )
        self.assertIsNotNone(decision)
        self.assertEqual(decision["mapped_to"], "invoice_total")
        self.assertIn(decision["decision_type"], ("auto_approved", "flagged_review"))
        self.assertEqual(decision["status"], "active")

    def test_unknown_field_pending_human(self):
        self._write("inv_001.xml", _xml_detailed(
            extra_fields="<fops:SupplierRating>4.5</fops:SupplierRating>"
        ))
        diff = compute_diff(self.src)
        con  = init_registry(self.db_path)
        decisions = process_diff(diff, MockLLMMapper(), con, dry_run=False)
        con.close()
        decision = next(
            (d for d in decisions if d["source_field"] == "SupplierRating"), None
        )
        self.assertIsNotNone(decision)
        self.assertEqual(decision["mapped_to"],     "UNKNOWN")
        self.assertEqual(decision["decision_type"], "pending_human")
        self.assertEqual(decision["status"],        "pending")

    def test_decisions_written_to_db(self):
        extras = """
            <fops:InvoiceAmount>1250.00</fops:InvoiceAmount>
            <fops:SupplierRating>4.5</fops:SupplierRating>
        """
        self._write("inv_001.xml", _xml_detailed(extra_fields=extras))
        diff = compute_diff(self.src)
        con  = init_registry(self.db_path)
        decisions = process_diff(diff, MockLLMMapper(), con, dry_run=False)
        con.close()
        con2  = duckdb.connect(self.db_path)
        count = con2.execute("SELECT COUNT(*) FROM mapping_registry").fetchone()[0]
        con2.close()
        self.assertEqual(count, len(decisions))

    def test_two_variants_both_parsed(self):
        self._write("detailed.xml", _xml_detailed(
            extra_fields="<fops:InvoiceAmount>1250.00</fops:InvoiceAmount>"
        ))
        self._write("summary.xml", _xml_summary())
        diff = compute_diff(self.src)
        self.assertIn("DetailedInvoice", diff["variant_counts"])
        self.assertIn("SummaryInvoice",  diff["variant_counts"])


# ── Runner ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse as _ap
    _p = _ap.ArgumentParser()
    _p.add_argument("-v", "--verbose", action="store_true")
    _args, _ = _p.parse_known_args()

    loader = unittest.TestLoader()
    suite  = unittest.TestSuite()
    for cls in [TestSchemaDiff, TestScoreConfidence, TestMappingRegistry, TestRagFlow]:
        suite.addTests(loader.loadTestsFromTestCase(cls))

    runner = unittest.TextTestRunner(verbosity=2 if _args.verbose else 1)
    result = runner.run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
