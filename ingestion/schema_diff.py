"""
schema_diff.py — xml-drift-lakehouse schema diff engine
=========================================================
Compares today's discovered XML schema against the known baseline
and produces a structured diff that feeds the RAG mapper.

A diff is produced when:
  - A new element is found (never seen before)
  - A known element appears in a new position (different parent)
  - A known element appears in a new variant (was DetailedInvoice only, now SummaryInvoice)

The diff is written to:
  - output/schema_diff/{run_date}/diff.json   ← machine-readable, feeds rag_mapper.py
  - output/schema_diff/{run_date}/diff.md     ← human-readable, for audit trail

If no diff is found, the file is still written with empty new_fields[]
so the Airflow task can always check it.

Usage:
    python schema_diff.py                            # compare ./data/sample vs baseline
    python schema_diff.py --src ./data/sample
    python schema_diff.py --src ./data/sample --run-date 2026-04-24
    python schema_diff.py --src ./data/sample --baseline ./docs/schema_baseline.json
"""

import argparse
import json
from collections import defaultdict
from datetime import date
from pathlib import Path

from lxml import etree

# ── Namespace ─────────────────────────────────────────────────────────────────
NS   = "http://www.fieldops-demo.io/xml/ns"
FOPS = f"{{{NS}}}"

# ── Baseline schema ───────────────────────────────────────────────────────────
# This is the ground truth — fields we know, their types, positions, variants
BASELINE_FIELDS = {
    # field_name: {type, position, variants, description}

#    # ── Custom / drifted fields — add confirmed mappings here ──────────────────

#    "DocumentNbr": {
#        "type":        "string",
#        "position":    "header",
#        "variants":    ["both"],
#        "description": "Vendor abbreviation for DocumentNumber"
#    },
#    "DocNumber": {
#        "type":        "string",
#        "position":    "header",
#        "variants":    ["both"],
#        "description": "Vendor abbreviation for DocumentNumber"
#    },

    # ── Header fields ──────────────────────────────────────────────────────────
    "DocumentNumber":       {"type": "string",  "position": "header", "variants": ["both"],           "description": "Invoice document number as assigned by the vendor — not guaranteed unique across vendors"},
    "DocumentDate":         {"type": "date",    "position": "header", "variants": ["both"],           "description": "Invoice date"},
    "DocumentType":         {"type": "string",  "position": "header", "variants": ["both"],           "description": "Type of document (invoice, credit note etc)"},
    "SubmissionMethod":     {"type": "string",  "position": "header", "variants": ["both"],           "description": "How document was submitted"},
    "CurrencyCode":         {"type": "string",  "position": "header", "variants": ["both"],           "description": "ISO currency code"},
    "Total":                {"type": "decimal", "position": "header", "variants": ["both"],           "description": "Total monetary value of the invoice"},
    "VendorTotal":          {"type": "decimal", "position": "header", "variants": ["both"],           "description": "Total as submitted by vendor"},
    "LineCount":            {"type": "integer", "position": "header", "variants": ["both"],           "description": "Number of line items"},
    "Notes":                {"type": "string",  "position": "header", "variants": ["both"],           "description": "Free text notes"},
    "PeriodDate":           {"type": "date",    "position": "header", "variants": ["both"],           "description": "Service period date"},
    "DiscountTotal":        {"type": "decimal", "position": "header", "variants": ["both"],           "description": "Total discount amount"},
    "ActionType":           {"type": "string",  "position": "header", "variants": ["both"],           "description": "Type of workflow action"},
    "Status":               {"type": "string",  "position": "header", "variants": ["both"],           "description": "Approval workflow status"},
    "TransactionDateTime":  {"type": "datetime","position": "header", "variants": ["both"],           "description": "Timestamp of last workflow action"},
    "TransactionIdentifier":{"type": "string",  "position": "header", "variants": ["both"],           "description": "Unique transaction identifier"},
    "EntityCode":           {"type": "string",  "position": "header", "variants": ["both"],           "description": "Company identifier code"},
    "EntityName":           {"type": "string",  "position": "header", "variants": ["both"],           "description": "Company name"},
    "LocationCode":         {"type": "string",  "position": "header", "variants": ["both"],           "description": "Site/location identifier"},
    "LocationName":         {"type": "string",  "position": "header", "variants": ["both"],           "description": "Site/location name"},
    "AddressLine":          {"type": "string",  "position": "header", "variants": ["both"],           "description": "Street address"},
    "City":                 {"type": "string",  "position": "header", "variants": ["both"],           "description": "City name"},
    "StateProvinceCode":    {"type": "string",  "position": "header", "variants": ["both"],           "description": "State or province code"},
    "CountryCode":          {"type": "string",  "position": "header", "variants": ["both"],           "description": "ISO country code"},
    "PostalZipCode":        {"type": "string",  "position": "header", "variants": ["both"],           "description": "Postal or ZIP code"},
    "PhoneNumber":          {"type": "string",  "position": "header", "variants": ["both"],           "description": "Phone number"},
    "EmailAddress":         {"type": "string",  "position": "header", "variants": ["both"],           "description": "Email address"},
    "DepartmentName":       {"type": "string",  "position": "header", "variants": ["both"],           "description": "Department name"},
    "FirstName":            {"type": "string",  "position": "header", "variants": ["both"],           "description": "Contact first name"},
    "LastName":             {"type": "string",  "position": "header", "variants": ["both"],           "description": "Contact last name"},
    "StateProvince":        {"type": "string",  "position": "header", "variants": ["both"],           "description": "State or province full name"},
    "Country":              {"type": "string",  "position": "header", "variants": ["both"],           "description": "Country full name"},
    "LocationPath":         {"type": "string",  "position": "header", "variants": ["both"],           "description": "Full location path"},
    "ContentType":          {"type": "string",  "position": "header", "variants": ["both"],           "description": "Content type identifier"},

    # ── Line fields ────────────────────────────────────────────────────────────
    "LineNumber":           {"type": "integer", "position": "line",   "variants": ["both"],               "description": "Line item number within invoice"},
    "ChargeClass":          {"type": "string",  "position": "line",   "variants": ["both"],               "description": "Classification of charge (Itemized, Non-Itemized)"},
    "PurchaseCategory":     {"type": "string",  "position": "line",   "variants": ["both"],               "description": "Purchase category code"},
    "Quantity":             {"type": "decimal", "position": "line",   "variants": ["both"],               "description": "Quantity of units"},
    "Units":                {"type": "string",  "position": "line",   "variants": ["DetailedInvoice"],    "description": "Unit of measure"},
    "UnitPrice":            {"type": "decimal", "position": "line",   "variants": ["both"],               "description": "Price per unit"},
    "LineSubTotal":         {"type": "decimal", "position": "line",   "variants": ["DetailedInvoice"],    "description": "Line subtotal before tax"},
    "LinePretaxTotal":      {"type": "decimal", "position": "line",   "variants": ["DetailedInvoice"],    "description": "Line total before tax"},
    "PeriodStartDate":      {"type": "date",    "position": "line",   "variants": ["DetailedInvoice"],    "description": "Start of service period"},
    "ServiceCode":          {"type": "string",  "position": "line",   "variants": ["DetailedInvoice"],    "description": "Service item code"},
    "ProductDescription":   {"type": "string",  "position": "line",   "variants": ["DetailedInvoice"],    "description": "Description of product or service"},
    "ProductCategory":      {"type": "string",  "position": "line",   "variants": ["DetailedInvoice"],    "description": "Product or service category"},
    "CategoryCode":         {"type": "string",  "position": "line",   "variants": ["both"],               "description": "Categorization code"},
    "TaxType":              {"type": "string",  "position": "line",   "variants": ["DetailedInvoice"],    "description": "Type of tax applied"},
    "ExemptCode":           {"type": "string",  "position": "line",   "variants": ["DetailedInvoice"],    "description": "Tax exemption code"},
    "DueDate":              {"type": "date",    "position": "line",   "variants": ["DetailedInvoice"],    "description": "Payment due date"},
    "DaysDue":              {"type": "integer", "position": "line",   "variants": ["DetailedInvoice"],    "description": "Number of days until payment due"},
    "EligibleFlag":         {"type": "string",  "position": "line",   "variants": ["DetailedInvoice"],    "description": "Early payment eligibility flag"},

    # ── Allocation fields ──────────────────────────────────────────────────────
    "AllocationRate":       {"type": "decimal", "position": "allocation", "variants": ["both"],           "description": "Allocation rate or percentage"},
    "CostCenter":           {"type": "string",  "position": "allocation", "variants": ["both"],           "description": "Cost center identifier"},
    "ProjectCode":          {"type": "string",  "position": "allocation", "variants": ["DetailedInvoice"],"description": "Project or BudgetNumber code"},
    "WorkOrder":            {"type": "string",  "position": "allocation", "variants": ["DetailedInvoice"],"description": "Work order number"},
    "OrderReference":       {"type": "string",  "position": "allocation", "variants": ["DetailedInvoice"],"description": "Purchase order reference"},
    "SiteRefName":          {"type": "string",  "position": "allocation", "variants": ["both"],           "description": "Site or location reference name"},
    "AccountCode":          {"type": "string",  "position": "allocation", "variants": ["both"],           "description": "Account code for cost allocation"},
}

# Fields that are structural containers — never flagged as new
# Also includes fields dropped or renamed in remap.py
STRUCTURAL_ELEMENTS = {
    "StandardBillingDocument", "DetailedInvoice", "SummaryInvoice",
    "DocumentHeader", "DocumentLines", "LineEntry", "Allocation",
    "ServiceItem", "Party", "Entity", "Location", "Address",
    "Action", "ActionSource", "Contact", "Department",
    "Attachments", "Attachment", "SiteReference",
    "TaxEntry", "EarlyPayment", "CrossReference",
    "Category", "Discount", "Metadata", "RefData", "RefInfo",
    "Notice", "AccountInfo", "AccountMajor", "AccountMinor",
    # Dropped in remap.py — may still appear in sanitized files
    "GeographicLocation", "ActionDestination",
    # Renamed in remap.py — old names may appear in sanitized files
    "ProductServiceDescription", "ProductServiceCategory",
    # Sparse/noise fields
    "Description", "Value", "AccountType",
    "ContactNameOrLocation", "NoticeType", "LocationPath", "ContentType",
    "AccountCode",  # nested inside AccountMajor/AccountMinor — never a standalone new field
}


# ── XML walker ────────────────────────────────────────────────────────────────

def lname(elem) -> str:
    return etree.QName(elem.tag).localname


def detect_variant(root) -> str:
    if lname(root) != "StandardBillingDocument":
        return "Unknown"
    for child in root:
        n = lname(child)
        if n in ("DetailedInvoice", "SummaryInvoice"):
            return n
    return "Unknown"


def infer_type(elem) -> str:
    """
    Infer data type from element text content.
    Conservative — prefers string over integer for ID-like fields.
    """
    text = (elem.text or "").strip()
    if not text:
        return "unknown"
    import re
    # Date
    if re.match(r'^\d{4}-\d{2}-\d{2}$', text):
        return "date"
    if re.match(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}', text):
        return "datetime"
    # Decimal (must have decimal point)
    if re.match(r'^-?\d+\.\d+$', text):
        return "decimal"
    # Integer — only if very short number (quantity, count) not a code/ID
    if re.match(r'^\d+$', text) and len(text) <= 4:
        return "integer"
    # Long numeric strings (IDs, codes, zip codes) → string
    return "string"


def walk_fields(elem, parent_lname="", result=None) -> dict:
    """Walk element tree collecting field metadata."""
    if result is None:
        result = defaultdict(lambda: {
            "types": set(),
            "parents": set(),
            "example_values": [],
            "count": 0
        })

    n = lname(elem)
    if n in STRUCTURAL_ELEMENTS:
        for child in elem:
            walk_fields(child, n, result)
        return result

    # This is a leaf/data field
    text = (elem.text or "").strip()
    if text:
        result[n]["types"].add(infer_type(elem))
        result[n]["parents"].add(parent_lname)
        result[n]["count"] += 1
        if len(result[n]["example_values"]) < 3:
            result[n]["example_values"].append(text)

    for child in elem:
        walk_fields(child, n, result)

    return result


# ── Diff engine ───────────────────────────────────────────────────────────────

def compute_diff(src_dir: Path) -> dict:
    """
    Scan all XMLs and compute diff against BASELINE_FIELDS.
    Returns structured diff dict.
    """
    xml_files = sorted(src_dir.glob("*.xml"))
    parser    = etree.XMLParser(recover=True, remove_comments=True)

    # Aggregate field observations across all files
    all_fields   = defaultdict(lambda: {
        "types": set(), "parents": set(),
        "example_values": [], "count": 0,
        "variants": set(), "files": set()
    })

    variant_counts = defaultdict(int)

    for f in xml_files:
        try:
            tree = etree.parse(str(f), parser)
        except Exception:
            continue

        root    = tree.getroot()
        variant = detect_variant(root)
        if variant == "Unknown":
            continue

        variant_counts[variant] += 1
        fields = walk_fields(root)

        for field_name, meta in fields.items():
            all_fields[field_name]["types"].update(meta["types"])
            all_fields[field_name]["parents"].update(meta["parents"])
            all_fields[field_name]["count"] += meta["count"]
            all_fields[field_name]["variants"].add(variant)
            all_fields[field_name]["files"].add(f.name)
            if len(all_fields[field_name]["example_values"]) < 3:
                all_fields[field_name]["example_values"].extend(
                    meta["example_values"][:3 - len(all_fields[field_name]["example_values"])]
                )

    # Compute diff
    new_fields      = []
    variant_drift   = []
    type_conflicts  = []

    for field_name, meta in all_fields.items():
        if field_name in STRUCTURAL_ELEMENTS:
            continue

        dominant_type = max(meta["types"], key=lambda t: list(meta["types"]).count(t)) \
                        if meta["types"] else "unknown"

        if field_name not in BASELINE_FIELDS:
            # Genuinely new field
            new_fields.append({
                "field_name":     field_name,
                "observed_type":  dominant_type,
                "all_types":      list(meta["types"]),
                "parents":        list(meta["parents"]),
                "variants":       list(meta["variants"]),
                "example_values": meta["example_values"][:3],
                "file_count":     len(meta["files"]),
                "occurrence_count": meta["count"],
            })
        else:
            baseline = BASELINE_FIELDS[field_name]

            # Check variant drift — field appears in new variant
            baseline_variants = set(baseline["variants"])
            if "both" not in baseline_variants:
                new_variants = meta["variants"] - baseline_variants
                if new_variants:
                    variant_drift.append({
                        "field_name":      field_name,
                        "baseline_variants": list(baseline_variants),
                        "new_variants":    list(new_variants),
                        "example_values":  meta["example_values"][:3],
                    })

            # Check type conflict
            if dominant_type != "unknown" and dominant_type != baseline["type"]:
                # Allow integer/decimal interchangeability
                numeric = {"integer", "decimal"}
                if not (dominant_type in numeric and baseline["type"] in numeric):
                    type_conflicts.append({
                        "field_name":    field_name,
                        "baseline_type": baseline["type"],
                        "observed_type": dominant_type,
                        "all_types":     list(meta["types"]),
                        "example_values": meta["example_values"][:3],
                    })

    return {
        "run_date":       str(date.today()),
        "files_scanned":  sum(variant_counts.values()),
        "variant_counts": dict(variant_counts),
        "has_drift":      len(new_fields) > 0 or len(variant_drift) > 0 or len(type_conflicts) > 0,
        "new_fields":     new_fields,
        "variant_drift":  variant_drift,
        "type_conflicts": type_conflicts,
        "baseline_field_count": len(BASELINE_FIELDS),
        "observed_field_count": len(all_fields),
    }


# ── Report renderer ───────────────────────────────────────────────────────────

def render_diff_report(diff: dict) -> str:
    lines = []
    a = lines.append

    a("# Schema Diff Report")
    a(f"\nRun date: `{diff['run_date']}`")
    a(f"Files scanned: `{diff['files_scanned']}`")
    a(f"Baseline fields: `{diff['baseline_field_count']}`")
    a(f"Observed fields: `{diff['observed_field_count']}`\n")

    if not diff["has_drift"]:
        a("## ✅ No Schema Drift Detected")
        a("\nAll observed fields match the known baseline. RAG mapper not triggered.")
        return "\n".join(lines)

    a("## ⚠️ Schema Drift Detected\n")

    if diff["new_fields"]:
        a(f"### New Fields ({len(diff['new_fields'])})\n")
        a("Fields never seen before — will be sent to RAG mapper for mapping suggestion.\n")
        a("| Field | Type | Variants | Files | Example Values |")
        a("|-------|------|----------|------:|----------------|")
        for f in diff["new_fields"]:
            examples = ", ".join(f["example_values"][:2])
            variants = ", ".join(f["variants"])
            a(f"| `{f['field_name']}` | {f['observed_type']} | {variants} | {f['file_count']} | {examples} |")

    if diff["variant_drift"]:
        a(f"\n### Variant Drift ({len(diff['variant_drift'])})\n")
        a("Known fields appearing in new variants.\n")
        a("| Field | Baseline Variants | New Variants |")
        a("|-------|-------------------|--------------|")
        for f in diff["variant_drift"]:
            a(f"| `{f['field_name']}` | {', '.join(f['baseline_variants'])} | {', '.join(f['new_variants'])} |")

    if diff["type_conflicts"]:
        a(f"\n### Type Conflicts ({len(diff['type_conflicts'])})\n")
        a("Fields where observed type differs from baseline type.\n")
        a("| Field | Baseline Type | Observed Type | Examples |")
        a("|-------|--------------|---------------|----------|")
        for f in diff["type_conflicts"]:
            examples = ", ".join(f["example_values"][:2])
            a(f"| `{f['field_name']}` | {f['baseline_type']} | {f['observed_type']} | {examples} |")

    return "\n".join(lines)


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Compute schema diff against known baseline")
    parser.add_argument("--src",      default="./data/sample", help="XML source folder")
    parser.add_argument("--out",      default="./output/schema_diff", help="Diff output folder")
    parser.add_argument("--run-date", default=str(date.today()), help="Run date (YYYY-MM-DD)")
    args = parser.parse_args()

    src_dir  = Path(args.src).resolve()
    out_dir  = Path(args.out) / args.run_date
    out_dir.mkdir(parents=True, exist_ok=True)

    xml_files = list(src_dir.glob("*.xml"))
    if not xml_files:
        print(f"No XML files found in {src_dir}")
        return

    print(f"Computing schema diff for {len(xml_files)} file(s)...")
    diff = compute_diff(src_dir)

    # Write JSON (machine-readable — feeds rag_mapper.py)
    json_path = out_dir / "diff.json"
    with open(json_path, "w") as f:
        json.dump(diff, f, indent=2)

    # Write Markdown (human-readable — audit trail)
    md_path = out_dir / "diff.md"
    md_path.write_text(render_diff_report(diff), encoding="utf-8")

    # Console summary
    if diff["has_drift"]:
        print(f"\n⚠️  Schema drift detected:")
        print(f"   New fields:     {len(diff['new_fields'])}")
        print(f"   Variant drift:  {len(diff['variant_drift'])}")
        print(f"   Type conflicts: {len(diff['type_conflicts'])}")
        print(f"\n   → RAG mapper will be triggered")
    else:
        print(f"\n✅ No schema drift — baseline matches observed schema")
        print(f"   → RAG mapper will be skipped")

    print(f"\nDiff written to: {out_dir}")


if __name__ == "__main__":
    main()
