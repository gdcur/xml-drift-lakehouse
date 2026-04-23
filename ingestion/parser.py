"""
parser.py — xml-drift-lakehouse ingestion layer
=================================================
Reads public-domain XMLs (fieldops-demo.io namespace) and produces
two Parquet datasets with surrogate key injection:

  output/
    invoices/          — one row per invoice (header fields)
    line_items/        — one row per line entry (with invoice FK)

Handles both variants transparently:
  DetailedInvoice  — full unit economics, ServiceItem, AccountInfo
  SummaryInvoice   — lean lines, header-level Allocation, no unit economics

Surrogate key pattern: {ingested_at, source_file, invoice_id, ...}
All fields nullable — schema-on-read, no variant is forced to fit the other.

Usage:
    python parser.py                        # parse ./public → ./output
    python parser.py --src ./public --dst ./output
    python parser.py --src ./public --dst ./output --fmt delta  # future
"""

import argparse
import hashlib
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from lxml import etree
import polars as pl

# ── Namespace ─────────────────────────────────────────────────────────────────
NS   = "http://www.fieldops-demo.io/xml/ns"
FOPS = f"{{{NS}}}"

# ── Helpers ───────────────────────────────────────────────────────────────────

def tag(localname: str) -> str:
    return f"{FOPS}{localname}"


def get(elem, *path, attr=None, default=None) -> str | None:
    """
    Navigate a chain of child localnames from elem and return text or attribute.
    Returns default if any step is missing.
    """
    cur = elem
    for step in path:
        cur = cur.find(tag(step))
        if cur is None:
            return default
    if attr:
        return cur.attrib.get(attr, default)
    return (cur.text or "").strip() or default


def getall(elem, *path) -> list[etree._Element]:
    """Return all matching children at the last step of path."""
    cur = elem
    for step in path[:-1]:
        cur = cur.find(tag(step))
        if cur is None:
            return []
    return cur.findall(tag(path[-1])) if cur is not None else []


def surrogate(source_file: str, invoice_id: str, extra: str = "") -> str:
    """Deterministic surrogate key: sha256(source_file + invoice_id + extra)[:16]."""
    raw = f"{source_file}|{invoice_id}|{extra}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def parse_float(val: str | None) -> float | None:
    try:
        return float(val) if val is not None else None
    except (ValueError, TypeError):
        return None


def parse_int(val: str | None) -> int | None:
    try:
        return int(val) if val is not None else None
    except (ValueError, TypeError):
        return None

# ── Variant detection ─────────────────────────────────────────────────────────

def detect_variant(root) -> tuple[str, etree._Element | None]:
    """
    Returns (variant_name, invoice_element).
    variant_name: 'DetailedInvoice' | 'SummaryInvoice' | 'Unknown'
    """
    lname = etree.QName(root.tag).localname
    if lname != "StandardBillingDocument":
        return "Unknown", None
    for child in root:
        clname = etree.QName(child.tag).localname
        if clname == "DetailedInvoice":
            return "DetailedInvoice", child
        if clname == "SummaryInvoice":
            return "SummaryInvoice", child
    return "Unknown", None

# ── Party extraction ──────────────────────────────────────────────────────────

def extract_party(header: etree._Element, role: str) -> dict[str, Any]:
    """Extract a Vendor or Client party block from DocumentHeader."""
    prefix = role.lower()   # 'vendor' or 'client'
    for party in header.findall(tag("Party")):
        if party.attrib.get("PartyRole") != role:
            continue
        return {
            f"{prefix}_entity_code":      get(party, "Entity", "EntityCode"),
            f"{prefix}_entity_code_type": get(party, "Entity", "EntityCode", attr="EntityCodeType"),
            f"{prefix}_entity_name":      get(party, "Entity", "EntityName"),
            f"{prefix}_location_code":    get(party, "Location", "LocationCode"),
            f"{prefix}_location_name":    get(party, "Location", "LocationName"),
            f"{prefix}_city":             get(party, "Location", "Address", "City"),
            f"{prefix}_state":            get(party, "Location", "Address", "StateProvinceCode"),
            f"{prefix}_country_code":     get(party, "Location", "Address", "CountryCode"),
        }
    return {}

# ── Allocation extraction ─────────────────────────────────────────────────────

def extract_allocation(alloc: etree._Element | None) -> dict[str, Any]:
    """Flatten one Allocation element."""
    if alloc is None:
        return {}
    return {
        "alloc_rate":          parse_float(get(alloc, "AllocationRate")),
        "alloc_total":         parse_float(get(alloc, "Total")),
        "alloc_cost_center":   get(alloc, "CostCenter"),
        "alloc_project_code":  get(alloc, "ProjectCode"),
        "alloc_work_order":    get(alloc, "WorkOrder"),
        "alloc_order_ref":     get(alloc, "OrderReference"),
        "alloc_site_ref_name": get(alloc, "SiteReference", "SiteRefName"),
        "alloc_account_major": get(alloc, "AccountInfo", "AccountMajor", "AccountCode"),
        "alloc_account_minor": get(alloc, "AccountInfo", "AccountMinor", "AccountCode"),
        "alloc_account_type":  get(alloc, "AccountInfo", "AccountMajor", "AccountType"),
    }

# ── Invoice header extraction ─────────────────────────────────────────────────

def extract_header(inv_elem: etree._Element, variant: str,
                   source_file: str, ingested_at: str) -> dict[str, Any]:
    """Extract DocumentHeader into a flat dict."""
    header = inv_elem.find(tag("DocumentHeader"))
    if header is None:
        return {}

    invoice_id = get(header, "DocumentNumber")
    sk = surrogate(source_file, invoice_id or "")

    rec: dict[str, Any] = {
        # Surrogate key wrapper
        "sk":               sk,
        "ingested_at":      ingested_at,
        "source_file":      source_file,
        "variant":          variant,
        "invoice_db_id":    inv_elem.attrib.get("id"),

        # Core header fields
        "invoice_id":       invoice_id,
        "document_date":    get(header, "DocumentDate"),
        "document_type":    get(header, "DocumentType"),
        "submission_method":get(header, "SubmissionMethod"),
        "currency_code":    get(header, "CurrencyCode"),
        "total":            parse_float(get(header, "Total")),
        "vendor_total":     parse_float(get(header, "VendorTotal")),
        "line_count":       parse_int(get(header, "LineCount")),
        "notes":            get(header, "Notes"),
        "period_date":      get(header, "PeriodDate"),         # SummaryInvoice header-level
        "discount_total":   parse_float(get(header, "DiscountTotal")),

        # Action (first only — truncated by remap.py)
        "action_type":      get(header, "Action", "ActionType"),
        "action_status":    get(header, "Action", "Status"),
        "action_datetime":  get(header, "Action", "TransactionDateTime"),
    }

    # Party blocks
    rec.update(extract_party(header, "Vendor"))
    rec.update(extract_party(header, "Client"))

    return rec

# ── Line item extraction ──────────────────────────────────────────────────────

def extract_line_items(inv_elem: etree._Element, variant: str,
                       invoice_id: str, source_file: str,
                       ingested_at: str) -> list[dict[str, Any]]:
    """
    Extract all LineEntry elements into flat dicts.

    Key structural difference:
      DetailedInvoice — Allocation is INSIDE each LineEntry
      SummaryInvoice  — Allocation may appear at DocumentLines level (header allocation)
                        AND/OR inside LineEntry (usually minimal)
    """
    doc_lines = inv_elem.find(tag("DocumentLines"))
    if doc_lines is None:
        return []

    invoice_sk = surrogate(source_file, invoice_id or "")

    # SummaryInvoice: capture header-level allocation (before any LineEntry)
    header_alloc = doc_lines.find(tag("Allocation"))

    rows = []
    for line in doc_lines.findall(tag("LineEntry")):
        line_num = get(line, "LineNumber")
        line_sk  = surrogate(source_file, invoice_id or "", line_num or "")

        # Line-level allocation — prefer line's own, fall back to header alloc
        _line_alloc = line.find(tag("Allocation"))
        line_alloc  = _line_alloc if _line_alloc is not None else header_alloc

        rec: dict[str, Any] = {
            # Keys
            "sk":            line_sk,
            "invoice_sk":    invoice_sk,
            "ingested_at":   ingested_at,
            "source_file":   source_file,
            "variant":       variant,
            "invoice_id":    invoice_id,
            "line_number":   parse_int(line_num),

            # DetailedInvoice fields (None for SummaryInvoice)
            "service_code":          get(line, "ServiceItem", "ServiceCode"),
            "product_description":   get(line, "ServiceItem", "ProductDescription"),
            "product_category":      get(line, "ServiceItem", "ProductCategory"),
            "quantity":              parse_float(get(line, "Quantity")),
            "units":                 get(line, "Units"),
            "unit_price":            parse_float(get(line, "UnitPrice")),
            "line_subtotal":         parse_float(get(line, "LineSubTotal")),
            "line_pretax_total":     parse_float(get(line, "LinePretaxTotal")),
            "line_total":            parse_float(get(line, "Total")),
            "period_start_date":     get(line, "PeriodStartDate"),
            "period_date":           get(line, "PeriodDate"),
            "notes":                 get(line, "Notes"),
            "charge_class":          get(line, "ChargeClass"),
            "purchase_category":     get(line, "PurchaseCategory"),

            # Category
            "category_code":         get(line, "Category", "CategoryCode"),
            "category_scheme":       line.find(tag("Category")).attrib.get(
                                         "CategorizationScheme")
                                     if line.find(tag("Category")) is not None else None,

            # Discount
            "discount_total":        parse_float(get(line, "Discount", "Total")),
            "discount_rate":         parse_float(get(line, "Discount", "AllocationRate")),

            # Cross reference
            "cross_ref_doc_number":  get(line, "CrossReference", "DocumentNumber"),
            "cross_ref_doc_type":    get(line, "CrossReference", "DocumentType"),
            "cross_ref_date":        get(line, "CrossReference", "Date"),

            # Tax (first entry only)
            "tax_type":              get(line, "TaxEntry", "TaxType"),
            "tax_total":             parse_float(get(line, "TaxEntry", "Total")),
            "tax_exempt_code":       get(line, "TaxEntry", "ExemptCode"),

            # Early payment
            "early_pay_due_date":    get(line, "EarlyPayment", "DueDate"),
            "early_pay_days_due":    parse_int(get(line, "EarlyPayment", "DaysDue")),
            "early_pay_eligible":    get(line, "EarlyPayment", "EligibleFlag"),
        }

        # Allocation (flattened)
        rec.update(extract_allocation(line_alloc))

        rows.append(rec)

    return rows

# ── File processor ────────────────────────────────────────────────────────────

def process_file(path: Path, ingested_at: str) -> tuple[dict, list[dict]]:
    """Parse one XML. Returns (header_row, [line_item_rows])."""
    parser = etree.XMLParser(recover=True, remove_comments=True)
    try:
        tree = etree.parse(str(path), parser)
    except Exception as e:
        print(f"  [SKIP] Cannot parse {path.name}: {e}")
        return {}, []

    root     = tree.getroot()
    variant, inv_elem = detect_variant(root)

    if variant == "Unknown" or inv_elem is None:
        print(f"  [SKIP] Unknown variant in {path.name}")
        return {}, []

    source_file = path.name
    header      = extract_header(inv_elem, variant, source_file, ingested_at)
    invoice_id  = header.get("invoice_id")
    lines       = extract_line_items(inv_elem, variant, invoice_id or "",
                                     source_file, ingested_at)

    return header, lines

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Parse fieldops XMLs → Parquet")
    parser.add_argument("--src", default="./public", help="Public XML folder")
    parser.add_argument("--dst", default="./output", help="Parquet output folder")
    args = parser.parse_args()

    src_dir = Path(args.src).resolve()
    dst_dir = Path(args.dst).resolve()
    (dst_dir / "invoices").mkdir(parents=True, exist_ok=True)
    (dst_dir / "line_items").mkdir(parents=True, exist_ok=True)

    xml_files = sorted(src_dir.glob("*.xml"))
    if not xml_files:
        print(f"No XML files found in {src_dir}")
        return

    ingested_at = datetime.now(timezone.utc).isoformat()
    print(f"\nParsing {len(xml_files)} file(s)  {src_dir} → {dst_dir}\n")

    all_headers    = []
    all_line_items = []
    counts = {"DetailedInvoice": 0, "SummaryInvoice": 0, "skipped": 0}

    for f in xml_files:
        header, lines = process_file(f, ingested_at)
        if not header:
            counts["skipped"] += 1
            continue
        all_headers.append(header)
        all_line_items.extend(lines)
        counts[header.get("variant", "skipped")] += 1
        print(f"  [{header['variant']:20s}] {f.name}  "
              f"lines={len(lines)}")

    if not all_headers:
        print("No records parsed.")
        return

    # ── Write Parquet ─────────────────────────────────────────────────────────
    inv_df  = pl.DataFrame(all_headers,  infer_schema_length=len(all_headers))
    line_df = pl.DataFrame(all_line_items, infer_schema_length=len(all_line_items)) \
              if all_line_items else pl.DataFrame()

    inv_path  = dst_dir / "invoices"  / "invoices.parquet"
    line_path = dst_dir / "line_items" / "line_items.parquet"

    inv_df.write_parquet(inv_path)
    if not line_df.is_empty():
        line_df.write_parquet(line_path)

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"""
Done.
  DetailedInvoice : {counts['DetailedInvoice']:>6}
  SummaryInvoice  : {counts['SummaryInvoice']:>6}
  Skipped         : {counts['skipped']:>6}
  Total lines     : {len(all_line_items):>6}

  invoices.parquet  → {inv_path}  ({inv_df.shape[0]} rows × {inv_df.shape[1]} cols)
  line_items.parquet→ {line_path}  ({line_df.shape[0] if not line_df.is_empty() else 0} rows × {line_df.shape[1] if not line_df.is_empty() else 0} cols)
""")


if __name__ == "__main__":
    main()
