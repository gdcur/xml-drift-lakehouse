"""
schema_discovery.py — xml-drift-lakehouse schema profiler
==========================================================
Scans all public XMLs and produces a structural report:

  - Variant distribution (DetailedInvoice / SummaryInvoice)
  - Field presence map: which fields appear in which variant
  - Field coverage %: how often each field is populated
  - Unexpected elements: anything not in the known schema
  - Attribute inventory: all attributes seen per element

Usage:
    python schema_discovery.py                      # scan ./data/sample
    python schema_discovery.py --src ./data/sample
    python schema_discovery.py --src ./data/sample --out ./docs/schema_report.md
"""

import argparse
import json
from collections import defaultdict
from pathlib import Path

from lxml import etree

# ── Namespace ─────────────────────────────────────────────────────────────────
NS   = "http://www.fieldops-demo.io/xml/ns"
FOPS = f"{{{NS}}}"

# ── Known schema — all fields we expect from parser.py ────────────────────────
KNOWN_HEADER_FIELDS = {
    "DocumentNumber", "DocumentDate", "DocumentType", "SubmissionMethod",
    "CurrencyCode", "Total", "VendorTotal", "LineCount", "Notes",
    "PeriodDate", "DiscountTotal",
    "Action", "ActionType", "Status", "TransactionDateTime", "TransactionIdentifier",
    "Party", "Entity", "EntityCode", "EntityName",
    "Location", "LocationCode", "LocationName",
    "Address", "AddressLine", "City", "StateProvince", "StateProvinceCode",
    "Country", "CountryCode", "PostalZipCode", "PhoneNumber", "EmailAddress",
    "Department", "DepartmentName", "CurrentOwner", "Contact",
    "CrossReference", "DocumentCrossReference",
}

KNOWN_LINE_FIELDS = {
    "LineEntry", "LineNumber", "ServiceItem", "ServiceCode",
    "ProductServiceDescription", "ProductServiceCategory",
    "Quantity", "Units", "UnitPrice", "Notes",
    "PeriodStartDate", "PeriodDate",
    "LineSubTotal", "LinePretaxTotal", "Total",
    "Category", "CategoryCode",
    "Discount", "AllocationRate",
    "Allocation", "CostCenter", "ProjectCode", "WorkOrder", "OrderReference",
    "SiteReference", "SiteRefName", "GeographicLocation",
    "AccountInfo", "AccountMajor", "AccountMinor", "AccountCode",
    "AccountType", "Description",
    "Metadata", "RefData", "Value",
    "CrossReference", "ChargeClass", "PurchaseCategory",
    "Notice", "NoticeType", "RefInfo",
    "ContactNameOrLocation", "LineDescription",
}

KNOWN_ELEMENTS = KNOWN_HEADER_FIELDS | KNOWN_LINE_FIELDS | {
    "StandardBillingDocument", "DetailedInvoice", "SummaryInvoice",
    "DocumentHeader", "DocumentLines", "Attachments", "Attachment",
    "LocationPath", "ContentType", "ActionSource", "FirstName", "LastName",
    # Payment terms
    "EarlyPayment", "DueDate", "DaysDue", "EligibleFlag", "Date",
    # Tax
    "TaxEntry", "TaxType", "ExemptCode",
    # Misc
    "Initial", "RequestedBy", "OrderLineRef",
    # Dropped in remap but may appear in sanitized files
    "ActionDestination", "GeographicLocation",
    # Renamed in remap
    "ProductServiceDescription", "ProductServiceCategory",
    "ProductDescription", "ProductCategory",
}

# ── Helpers ───────────────────────────────────────────────────────────────────

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


def walk_fields(elem, path="", result=None):
    """
    Recursively walk element tree, collecting:
      - full xpath-style paths
      - element localnames
      - attribute names per element
    """
    if result is None:
        result = {"paths": set(), "elements": set(), "attrs": defaultdict(set)}

    n = lname(elem)
    current_path = f"{path}/{n}" if path else n

    result["paths"].add(current_path)
    result["elements"].add(n)

    for attr_name in elem.attrib:
        result["attrs"][n].add(attr_name)

    # Track populated vs present
    if elem.text and elem.text.strip():
        result["paths"].add(f"{current_path}[populated]")

    for child in elem:
        walk_fields(child, current_path, result)

    return result

# ── Main scanner ──────────────────────────────────────────────────────────────

def scan(src_dir: Path) -> dict:
    xml_files = sorted(src_dir.glob("*.xml"))
    if not xml_files:
        return {}

    stats = {
        "total_files":     0,
        "variants":        defaultdict(int),
        "skipped":         [],
        # per variant: element → {present, populated, total}
        "field_coverage":  defaultdict(lambda: defaultdict(lambda: {"present": 0, "populated": 0})),
        # per variant: set of all paths seen
        "all_paths":       defaultdict(set),
        # unexpected elements
        "unexpected":      defaultdict(set),   # element → set of files
        # attribute inventory
        "attributes":      defaultdict(lambda: defaultdict(set)),  # variant → elem → attrs
        # structural notes
        "header_alloc_files":  [],   # SummaryInvoice with header-level Allocation
        "multi_alloc_lines":   [],   # lines with more than 1 Allocation
        "max_lines_per_inv":   0,
        "max_allocs_per_line": 0,
    }

    parser = etree.XMLParser(recover=True, remove_comments=True)

    for f in xml_files:
        stats["total_files"] += 1
        try:
            tree = etree.parse(str(f), parser)
        except Exception as e:
            stats["skipped"].append(f"{f.name}: {e}")
            continue

        root    = tree.getroot()
        variant = detect_variant(root)

        if variant == "Unknown":
            stats["skipped"].append(f"{f.name}: unknown variant")
            continue

        stats["variants"][variant] += 1

        # Walk full tree
        walked = walk_fields(root)
        stats["all_paths"][variant].update(walked["paths"])

        # Track field coverage
        for elem_name in walked["elements"]:
            stats["field_coverage"][variant][elem_name]["present"] += 1
        for path in walked["paths"]:
            if "[populated]" in path:
                elem_name = path.split("/")[-1].replace("[populated]", "")
                stats["field_coverage"][variant][elem_name]["populated"] += 1

        # Attribute inventory
        for elem_name, attrs in walked["attrs"].items():
            stats["attributes"][variant][elem_name].update(attrs)

        # Unexpected elements
        for elem_name in walked["elements"]:
            if elem_name not in KNOWN_ELEMENTS:
                stats["unexpected"][elem_name].add(f.name)

        # Structural notes
        inv_elem = next((c for c in root if lname(c) in
                        ("DetailedInvoice", "SummaryInvoice")), None)
        if inv_elem is not None:
            doc_lines = inv_elem.find(f"{FOPS}DocumentLines")
            if doc_lines is not None:
                # Header-level allocation in SummaryInvoice
                header_allocs = doc_lines.findall(f"{FOPS}Allocation")
                if header_allocs and variant == "SummaryInvoice":
                    stats["header_alloc_files"].append(f.name)

                # Line count
                lines = doc_lines.findall(f"{FOPS}LineEntry")
                stats["max_lines_per_inv"] = max(
                    stats["max_lines_per_inv"], len(lines))

                # Multi-allocation lines
                for line in lines:
                    allocs = line.findall(f"{FOPS}Allocation")
                    stats["max_allocs_per_line"] = max(
                        stats["max_allocs_per_line"], len(allocs))
                    if len(allocs) > 1:
                        stats["multi_alloc_lines"].append(
                            f"{f.name} line {line.find(f'{FOPS}LineNumber').text if line.find(f'{FOPS}LineNumber') is not None else '?'}")

    return stats


# ── Report renderer ───────────────────────────────────────────────────────────

def render_report(stats: dict, src_dir: Path) -> str:
    variants     = stats["variants"]
    total        = stats["total_files"]
    coverage     = stats["field_coverage"]
    unexpected   = stats["unexpected"]
    attributes   = stats["attributes"]

    lines = []
    a = lines.append

    a("# Schema Discovery Report")
    a(f"\nSource: `{src_dir}`\n")

    # ── Variant distribution
    a("## Variant Distribution\n")
    a(f"| Variant | Files | % |")
    a(f"|---------|------:|--:|")
    for v, count in sorted(variants.items()):
        a(f"| {v} | {count} | {count/total*100:.1f}% |")
    a(f"| **Total** | **{total}** | **100%** |")
    if stats["skipped"]:
        a(f"\n⚠️ Skipped {len(stats['skipped'])} file(s):")
        for s in stats["skipped"][:10]:
            a(f"  - {s}")

    # ── Field coverage per variant
    a("\n## Field Coverage by Variant\n")
    a("> % = files where field is populated / files where field is present\n")

    all_fields = sorted(set(
        f for v in coverage for f in coverage[v]
    ))

    header = "| Field |" + "".join(f" {v} |" for v in sorted(variants))
    sep    = "|-------|" + "".join("------:|" for _ in variants)
    a(header)
    a(sep)

    for field in all_fields:
        row = f"| `{field}` |"
        for v in sorted(variants):
            fc = coverage[v].get(field)
            if fc is None:
                row += " — |"
            else:
                present   = fc["present"]
                populated = fc["populated"]
                pct = f"{populated/present*100:.0f}%" if present > 0 else "0%"
                row += f" {pct} ({present}/{total}) |"
        a(row)

    # ── Structural notes
    a("\n## Structural Notes\n")
    a(f"- Max line items per invoice: **{stats['max_lines_per_inv']}**")
    a(f"- Max allocations per line: **{stats['max_allocs_per_line']}**")
    if stats["header_alloc_files"]:
        a(f"- SummaryInvoice files with header-level Allocation: "
          f"**{len(stats['header_alloc_files'])}**")
    if stats["multi_alloc_lines"]:
        a(f"- Lines with multiple Allocations: **{len(stats['multi_alloc_lines'])}**")
        for m in stats["multi_alloc_lines"][:5]:
            a(f"  - {m}")

    # ── Unexpected elements
    if unexpected:
        a("\n## ⚠️ Unexpected Elements\n")
        a("Elements found in XMLs not in known schema — review before building dbt models:\n")
        a("| Element | Files |")
        a("|---------|------:|")
        for elem, files in sorted(unexpected.items()):
            a(f"| `{elem}` | {len(files)} |")
    else:
        a("\n## ✅ No Unexpected Elements\n")
        a("All elements match known schema.")

    # ── Attribute inventory
    a("\n## Attribute Inventory\n")
    for v in sorted(variants):
        a(f"### {v}\n")
        a("| Element | Attributes |")
        a("|---------|-----------|")
        for elem, attrs in sorted(attributes[v].items()):
            if attrs:
                a(f"| `{elem}` | {', '.join(f'`{x}`' for x in sorted(attrs))} |")

    return "\n".join(lines)


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Profile XML structure across all public sample files")
    parser.add_argument("--src", default="./data/sample", help="Public XML folder")
    parser.add_argument("--out", default=None,
                        help="Write markdown report to file (default: print to stdout)")
    args = parser.parse_args()

    src_dir = Path(args.src).resolve()
    xml_files = list(src_dir.glob("*.xml"))

    if not xml_files:
        print(f"No XML files found in {src_dir}")
        return

    print(f"Scanning {len(xml_files)} file(s) in {src_dir}...")
    stats  = scan(src_dir)
    report = render_report(stats, src_dir)

    if args.out:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(report, encoding="utf-8")
        print(f"Report written to {out_path}")
    else:
        print(report)


if __name__ == "__main__":
    main()
