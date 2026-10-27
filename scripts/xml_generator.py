"""
xml_generator.py — Parametric XML invoice generator for xml-drift-lakehouse
=============================================================================
Generates synthetic XML invoice files in both DetailedInvoice and SummaryInvoice
structural variants, following the fieldops-demo.io namespace used in the project.

Field names match the BASELINE_FIELDS in schema_diff.py exactly, so a clean
run produces no drift. Use --drift-pct to inject unknown fields and trigger
the RAG mapper.

Node order is randomized per file by default -- the same fields appear in
different positions across files, replicating real vendor variance.

Parameters:
    --count         Total number of files to generate (default: 3000)
    --out           Output directory (default: ./generated)
    --date-from     First invoice date, YYYY-MM-DD (default: 2020-01-01)
    --date-to       Last invoice date, YYYY-MM-DD (default: 2024-12-31)
    --max-amount    Maximum invoice total amount (default: 50000.00)
    --max-lines     Maximum number of line items per invoice (default: 10)
    --detailed-pct  Percentage of DetailedInvoice variant, 0-100 (default: 60)
    --seed          Random seed for reproducibility (default: 42)
    --drift-pct     Percentage of files that include unknown drift fields (default: 0)
    --no-shuffle    Disable node order shuffling (useful for debugging)

Suggested workflow:
    # Step 1 — stable feed, no drift, all green
    python xml_generator.py --count 3000 --out ./data/sample --seed 42

    # Step 2 — introduce drift, RAG mapper fires
    python xml_generator.py --count 3000 --out ./data/sample --seed 42 --drift-pct 5

    # Reproduce the exact test dataset used in the README:
    python xml_generator.py \\
        --count 3000 --out ./generated \\
        --date-from 2020-01-01 --date-to 2024-12-31 \\
        --max-amount 50000 --max-lines 10 \\
        --detailed-pct 60 --seed 42
"""

import argparse
import random
import string
from datetime import date, timedelta, datetime
from pathlib import Path

# ── Namespace ─────────────────────────────────────────────────────────────────
NS = "http://www.fieldops-demo.io/xml/ns"

# ── Reference data pools ──────────────────────────────────────────────────────
VENDOR_NAMES = [
    "Acme Energy Services", "Basin Logistics LLC", "Delta Field Ops",
    "Summit Equipment Co", "Horizon Drilling Inc", "Apex Production Services",
    "Valley Maintenance Group", "Crest Industrial Supply", "Pioneer Well Services",
    "Redstone Field Services", "Cascade Pipeline Co", "Mesa Engineering LLC",
    "Ridgeline Contractors", "Lakeview Equipment Rental", "Ironwood Field Services",
]

FIRST_NAMES = ["James", "Maria", "Robert", "Sarah", "Michael",
               "Jennifer", "David", "Linda", "Carlos", "Angela"]

LAST_NAMES  = ["Smith", "Johnson", "Williams", "Brown", "Jones",
               "Garcia", "Miller", "Davis", "Rodriguez", "Wilson"]

DEPARTMENTS = ["Operations", "Finance", "Procurement", "Engineering",
               "Maintenance", "Logistics", "HSE", "Contracts"]

CITIES = ["Houston", "Denver", "Midland", "Oklahoma City", "Calgary",
          "Dallas", "Tulsa", "Bakersfield", "Anchorage", "Bismarck"]

STATES = [("TX", "Texas"), ("CO", "Colorado"), ("OK", "Oklahoma"),
          ("CA", "California"), ("ND", "North Dakota"), ("WY", "Wyoming")]

PRODUCT_DESCRIPTIONS = [
    "Drilling services", "Transportation and logistics", "Equipment rental",
    "Maintenance services", "Field inspection", "Chemical supply",
    "Wireline services", "Cementing services", "Production testing",
    "Safety equipment supply", "Consulting services", "Pump installation",
    "Pipeline inspection", "Valve replacement", "Generator rental",
]

SERVICE_CODES    = [f"SVC-{i:04d}" for i in range(1, 30)]
PRODUCT_CATS     = ["EQUIP", "LABOR", "MATERIAL", "SERVICES", "TRANSPORT", "CHEMICAL"]
CATEGORY_CODES   = [f"CAT-{i:03d}" for i in range(1, 20)]
PURCHASE_CATS    = ["CAPEX", "OPEX", "MAINTENANCE", "EMERGENCY"]
CHARGE_CLASSES   = ["Itemized", "Non-Itemized"]
UNITS            = ["HR", "DAY", "BBL", "GAL", "FT", "TON", "EA", "LB", "MO", "JOB"]
TAX_TYPES        = ["SALES", "USE", "EXEMPT", "VAT"]
EXEMPT_CODES     = ["EX-001", "EX-002", "NONE"]
ELIGIBLE_FLAGS   = ["Y", "N"]
ACTION_TYPES     = ["Submit", "Approve", "Reject", "Pay"]
STATUSES = ["Approved", "Pending", "Rejected", "Disputed",
            "Cancelled", "On Hold", "Submitted", "Re-Submitted"]
CONTENT_TYPES    = ["StandardBillingDocument", "CreditNote", "Adjustment"]
COST_CENTERS     = [f"CC-{i:04d}" for i in range(1, 51)]
PROJECT_CODES    = [f"PRJ-{y}-{i:04d}" for y in range(2020, 2025) for i in range(1, 21)]
WORK_ORDERS      = [f"WO-{i:05d}" for i in range(1, 101)]
SITE_REFS        = [f"SITE-{i:03d}" for i in range(1, 30)]
ACCOUNT_CODES    = [f"GL-{c}" for c in ["6100", "6200", "6300", "6400", "7100", "7200", "8100"]]
LOCATION_CODES   = [f"LOC-{i:04d}" for i in range(1, 50)]
ENTITY_CODES     = [f"ENT-{i:03d}" for i in range(1, 20)]

# Drift fields — injected only when --drift-pct > 0
# These are NOT in BASELINE_FIELDS so they trigger schema_diff + RAG mapper
DRIFT_FIELDS = [
    ("InvoiceAmount",   "{amount:.2f}"),
    ("RequestedBy",     "{name}"),
    ("InternalRef",     "{ref}"),
    ("ApprovalCode",    "{code}"),
    ("BudgetReference", "{ref}"),
    ("OriginalSystem",  "{system}"),
]


# ── Helpers ───────────────────────────────────────────────────────────────────

def rand_date(rng: random.Random, date_from: date, date_to: date) -> str:
    delta = (date_to - date_from).days
    return str(date_from + timedelta(days=rng.randint(0, delta)))

def rand_datetime(rng: random.Random, date_from: date, date_to: date) -> str:
    d = date_from + timedelta(days=rng.randint(0, (date_to - date_from).days))
    return f"{d}T{rng.randint(0,23):02d}:{rng.randint(0,59):02d}:{rng.randint(0,59):02d}"

def rand_doc_number(rng: random.Random) -> str:
    return f"{rng.choice(['INV','DOC','BILL','SI'])}-{rng.randint(1000000, 9999999)}"

def rand_vendor(rng: random.Random) -> tuple[str, str]:
    name = rng.choice(VENDOR_NAMES)
    vid  = "V" + "".join(rng.choices(string.digits, k=6))
    return vid, name

def rand_address(rng: random.Random) -> dict:
    state_code, state_name = rng.choice(STATES)
    return {
        "AddressLine":     f"{rng.randint(100,9999)} {rng.choice(['Main','Oak','Industrial','Commerce'])} {rng.choice(['St','Ave','Blvd','Rd'])}",
        "City":            rng.choice(CITIES),
        "StateProvinceCode": state_code,
        "StateProvince":   state_name,
        "PostalZipCode":   f"{rng.randint(10000,99999)}",
        "CountryCode":     "US",
        "Country":         "United States",
    }

def rand_contact(rng: random.Random) -> dict:
    return {
        "FirstName":      rng.choice(FIRST_NAMES),
        "LastName":       rng.choice(LAST_NAMES),
        "DepartmentName": rng.choice(DEPARTMENTS),
        "PhoneNumber":    f"+1-{rng.randint(200,999)}-{rng.randint(100,999)}-{rng.randint(1000,9999)}",
        "EmailAddress":   f"contact{rng.randint(1,999)}@fieldops-demo.io",
    }

def node(tag: str, value: str) -> str:
    return f"    <fops:{tag}>{value}</fops:{tag}>"

def shuffle_nodes(nodes: list[str], rng: random.Random, shuffle: bool) -> str:
    if shuffle:
        rng.shuffle(nodes)
    return "\n".join(nodes)


# ── Drift node builder ────────────────────────────────────────────────────────

def build_drift_nodes(rng: random.Random, total: float) -> list[str]:
    tag, tpl = rng.choice(DRIFT_FIELDS)
    value = tpl.format(
        amount=total,
        name=f"{rng.choice(LAST_NAMES)}, {rng.choice(FIRST_NAMES)}",
        ref=f"REF-{rng.randint(1000, 9999)}",
        code=f"APV-{rng.randint(100, 999)}",
        system=rng.choice(["SAP", "Oracle", "JDE", "Maximo", "Ariba"]),
    )
    return [node(tag, value)]


# ── Header node builder ───────────────────────────────────────────────────────

def build_header_nodes(
    rng: random.Random,
    doc_num: str,
    doc_date: str,
    date_from: date,
    date_to: date,
    vid: str,
    vname: str,
    status: str,
    total: float,
    vendor_total: float,
    n_lines: int,
    drift_nodes: list[str],
) -> list[str]:
    addr    = rand_address(rng)
    contact = rand_contact(rng)

    # Flat header fields
    flat_nodes = [
        node("DocumentNumber",    doc_num),
        node("DocumentDate",      doc_date),
        node("DocumentType",      rng.choice(["Invoice", "CreditNote", "Adjustment"])),
        node("SubmissionMethod",  rng.choice(["EDI", "Portal", "Email", "API"])),
        node("CurrencyCode",      "USD"),
        node("Total",             f"{total:.2f}"),
        node("VendorTotal",       f"{vendor_total:.2f}"),
        node("LineCount",         str(n_lines)),
        node("PeriodDate",        rand_date(rng, date_from, date_to)),
        node("ContentType",       rng.choice(CONTENT_TYPES)),
    ]
    if rng.random() < 0.4:
        flat_nodes.append(node("Notes", f"Invoice ref {rng.randint(1000,9999)}"))
    if rng.random() < 0.3:
        flat_nodes.append(node("DiscountTotal", f"{round(total * rng.uniform(0.01, 0.05), 2):.2f}"))
    flat_nodes.extend(drift_nodes)

    # Action block — nested, not shuffled internally (parser uses fixed paths)
    action_block = (
        f"    <fops:Action>\n"
        f"      <fops:ActionType>{rng.choice(ACTION_TYPES)}</fops:ActionType>\n"
        f"      <fops:Status>{status}</fops:Status>\n"
        f"      <fops:TransactionDateTime>{rand_datetime(rng, date_from, date_to)}</fops:TransactionDateTime>\n"
        f"      <fops:TransactionIdentifier>TXN-{rng.randint(10000000, 99999999)}</fops:TransactionIdentifier>\n"
        f"    </fops:Action>"
    )

    # Vendor Party block
    vendor_block = (
        f"    <fops:Party PartyRole=\"Vendor\">\n"
        f"      <fops:Entity EntityCodeType=\"VendorID\">\n"
        f"        <fops:EntityCode>{vid}</fops:EntityCode>\n"
        f"        <fops:EntityName>{vname}</fops:EntityName>\n"
        f"      </fops:Entity>\n"
        f"      <fops:Location>\n"
        f"        <fops:LocationCode>{rng.choice(LOCATION_CODES)}</fops:LocationCode>\n"
        f"        <fops:LocationName>Site {rng.randint(1,50)}</fops:LocationName>\n"
        f"        <fops:Address AddressType=\"Mailing\">\n"
        f"          <fops:AddressLine>{addr['AddressLine']}</fops:AddressLine>\n"
        f"          <fops:City>{addr['City']}</fops:City>\n"
        f"          <fops:StateProvinceCode>{addr['StateProvinceCode']}</fops:StateProvinceCode>\n"
        f"          <fops:StateProvince>{addr['StateProvince']}</fops:StateProvince>\n"
        f"          <fops:PostalZipCode>{addr['PostalZipCode']}</fops:PostalZipCode>\n"
        f"          <fops:CountryCode>{addr['CountryCode']}</fops:CountryCode>\n"
        f"          <fops:Country>{addr['Country']}</fops:Country>\n"
        f"        </fops:Address>\n"
        f"      </fops:Location>\n"
        f"      <fops:Contact>\n"
        f"        <fops:FirstName>{contact['FirstName']}</fops:FirstName>\n"
        f"        <fops:LastName>{contact['LastName']}</fops:LastName>\n"
        f"        <fops:DepartmentName>{contact['DepartmentName']}</fops:DepartmentName>\n"
        f"        <fops:PhoneNumber>{contact['PhoneNumber']}</fops:PhoneNumber>\n"
        f"        <fops:EmailAddress>{contact['EmailAddress']}</fops:EmailAddress>\n"
        f"      </fops:Contact>\n"
        f"    </fops:Party>"
    )

    # Client Party block (buyer side)
    client_addr = rand_address(rng)
    client_block = (
        f"    <fops:Party PartyRole=\"Client\">\n"
        f"      <fops:Entity EntityCodeType=\"ClientID\">\n"
        f"        <fops:EntityCode>{rng.choice(ENTITY_CODES)}</fops:EntityCode>\n"
        f"        <fops:EntityName>Client {rng.randint(1,20)}</fops:EntityName>\n"
        f"      </fops:Entity>\n"
        f"      <fops:Location>\n"
        f"        <fops:LocationCode>{rng.choice(LOCATION_CODES)}</fops:LocationCode>\n"
        f"        <fops:LocationName>Office {rng.randint(1,10)}</fops:LocationName>\n"
        f"        <fops:Address AddressType=\"Mailing\">\n"
        f"          <fops:City>{client_addr['City']}</fops:City>\n"
        f"          <fops:StateProvinceCode>{client_addr['StateProvinceCode']}</fops:StateProvinceCode>\n"
        f"          <fops:CountryCode>{client_addr['CountryCode']}</fops:CountryCode>\n"
        f"        </fops:Address>\n"
        f"      </fops:Location>\n"
        f"    </fops:Party>"
    )

    return flat_nodes, action_block, vendor_block, client_block


# ── Allocation node builder ───────────────────────────────────────────────────

def build_allocation_xml(rng: random.Random, rate: float, amt: float, detailed: bool) -> str:
    """Build one Allocation element matching parser's expected structure."""
    site_ref = rng.choice(SITE_REFS)
    account  = rng.choice(ACCOUNT_CODES)
    lines = [
        f"          <fops:AllocationRate>{rate:.2f}</fops:AllocationRate>",
        f"          <fops:Total>{amt:.2f}</fops:Total>",
        f"          <fops:CostCenter>{rng.choice(COST_CENTERS)}</fops:CostCenter>",
        f"          <fops:SiteReference>\n            <fops:SiteRefName>{site_ref}</fops:SiteRefName>\n          </fops:SiteReference>",
        f"          <fops:AccountInfo>\n            <fops:AccountMajor>\n              <fops:AccountCode>{account}</fops:AccountCode>\n              <fops:AccountType>EXPENSE</fops:AccountType>\n            </fops:AccountMajor>\n          </fops:AccountInfo>",
    ]
    if detailed:
        lines += [
            f"          <fops:ProjectCode>{rng.choice(PROJECT_CODES)}</fops:ProjectCode>",
            f"          <fops:WorkOrder>{rng.choice(WORK_ORDERS)}</fops:WorkOrder>",
            f"          <fops:OrderReference>PO-{rng.randint(10000,99999)}</fops:OrderReference>",
        ]
    return "        <fops:Allocation>\n" + "\n".join(lines) + "\n        </fops:Allocation>"


def build_allocations_block(
    rng: random.Random,
    line_amount: float,
    detailed: bool,
    shuffle: bool,
) -> str:
    """Build one or two Allocation elements."""
    if rng.random() < 0.3:
        rate1 = round(rng.uniform(20, 80), 2)
        rate2 = round(100 - rate1, 2)
        amt1  = round(line_amount * rate1 / 100, 2)
        amt2  = round(line_amount - amt1, 2)
        return (
            build_allocation_xml(rng, rate1, amt1, detailed) + "\n" +
            build_allocation_xml(rng, rate2, amt2, detailed)
        )
    else:
        return build_allocation_xml(rng, 100.0, line_amount, detailed)


# ── Line node builders ────────────────────────────────────────────────────────

def build_line_nodes_detailed(
    rng: random.Random,
    line_num: int,
    line_amt: float,
    subtotal: float,
    qty: float,
    unit_price: float,
    svc_start: str,
) -> list[str]:
    nodes = [
        node("LineNumber",          str(line_num)),
        node("ProductDescription",  rng.choice(PRODUCT_DESCRIPTIONS)),
        node("ChargeClass",         rng.choice(CHARGE_CLASSES)),
        node("PurchaseCategory",    rng.choice(PURCHASE_CATS)),
        node("Quantity",            f"{qty:.2f}"),
        node("Units",               rng.choice(UNITS)),
        node("UnitPrice",           f"{unit_price:.2f}"),
        node("LineSubTotal",        f"{subtotal:.2f}"),
        node("LinePretaxTotal",     f"{line_amt:.2f}"),
        node("Total",               f"{line_amt:.2f}"),
        node("PeriodStartDate",     svc_start),
        node("PeriodDate",          svc_start),
        node("ServiceCode",         rng.choice(SERVICE_CODES)),
        node("ProductCategory",     rng.choice(PRODUCT_CATS)),
        node("CategoryCode",        rng.choice(CATEGORY_CODES)),
    ]
    # Optional line fields
    if rng.random() < 0.3:
        nodes.append(node("TaxType",    rng.choice(TAX_TYPES)))
    if rng.random() < 0.2:
        nodes.append(node("ExemptCode", rng.choice(EXEMPT_CODES)))
    if rng.random() < 0.25:
        nodes.append(node("EligibleFlag", rng.choice(ELIGIBLE_FLAGS)))
    if rng.random() < 0.2:
        due_days = rng.randint(15, 90)
        nodes.append(node("DaysDue", str(due_days)))
    return nodes


def build_line_nodes_summary(
    rng: random.Random,
    line_num: int,
    line_amt: float,
    qty: float,
    unit_price: float,
    date_from: date,
    date_to: date,
) -> list[str]:
    return [
        node("LineNumber",         str(line_num)),
        node("ProductDescription", rng.choice(PRODUCT_DESCRIPTIONS)),
        node("ChargeClass",        rng.choice(CHARGE_CLASSES)),
        node("PurchaseCategory",   rng.choice(PURCHASE_CATS)),
        node("Quantity",           f"{qty:.2f}"),
        node("UnitPrice",          f"{unit_price:.2f}"),
        node("Total",              f"{line_amt:.2f}"),
        node("PeriodDate",         rand_date(rng, date_from, date_to)),
        node("CategoryCode",       rng.choice(CATEGORY_CODES)),
    ]


# ── XML builders ──────────────────────────────────────────────────────────────

def build_detailed_invoice(
    rng: random.Random,
    date_from: date,
    date_to: date,
    max_amount: float,
    max_lines: int,
    drift: bool,
    shuffle: bool,
    invoice_id: int
) -> str:
    doc_num    = rand_doc_number(rng)
    doc_date   = rand_date(rng, date_from, date_to)
    vid, vname = rand_vendor(rng)
    status     = rng.choice(STATUSES)
    n_lines    = rng.randint(1, max_lines)

    line_blocks = []
    total = 0.0

    for i in range(1, n_lines + 1):
        qty        = round(rng.uniform(1, 100), 2)
        unit_price = round(rng.uniform(10, max_amount / max(n_lines, 1)), 2)
        subtotal   = round(qty * unit_price, 2)
        tax_rate   = rng.uniform(0, 0.08) if rng.random() < 0.3 else 0.0
        line_amt   = round(subtotal * (1 + tax_rate), 2)
        total     += line_amt
        svc_start  = rand_date(rng, date_from, date_to)

        line_nodes  = build_line_nodes_detailed(rng, i, line_amt, subtotal, qty, unit_price, svc_start)
        alloc_xml   = build_allocations_block(rng, line_amt, detailed=True, shuffle=shuffle)
        line_inner  = shuffle_nodes(line_nodes, rng, shuffle)

        line_blocks.append(
            f"      <fops:LineEntry>\n{line_inner}\n{alloc_xml}\n      </fops:LineEntry>"
        )

    total        = round(total, 2)
    vendor_total = round(total * rng.uniform(0.98, 1.02), 2)
    drift_nodes  = build_drift_nodes(rng, total) if drift else []

    flat_nodes, action_block, vendor_block, client_block = build_header_nodes(
        rng, doc_num, doc_date, date_from, date_to,
        vid, vname, status, total, vendor_total, n_lines, drift_nodes
    )
    flat_inner   = shuffle_nodes(flat_nodes, rng, shuffle)
    lines_inner  = "\n".join(line_blocks)

    return (
        f'<?xml version="1.0" encoding="UTF-8"?>\n'
        f'<fops:StandardBillingDocument xmlns:fops="{NS}" invoice_id="{invoice_id}">\n'
        f'  <fops:DetailedInvoice>\n'
        f'    <fops:DocumentHeader>\n'
        f'{flat_inner}\n'
        f'{action_block}\n'
        f'{vendor_block}\n'
        f'{client_block}\n'
        f'    </fops:DocumentHeader>\n'
        f'    <fops:DocumentLines>\n{lines_inner}\n    </fops:DocumentLines>\n'
        f'  </fops:DetailedInvoice>\n'
        f'</fops:StandardBillingDocument>\n'
    )


def build_summary_invoice(
    rng: random.Random,
    date_from: date,
    date_to: date,
    max_amount: float,
    max_lines: int,
    drift: bool,
    shuffle: bool,
    invoice_id: int
) -> str:
    doc_num    = rand_doc_number(rng)
    doc_date   = rand_date(rng, date_from, date_to)
    vid, vname = rand_vendor(rng)
    status     = rng.choice(STATUSES)
    n_lines    = rng.randint(1, max_lines)

    line_blocks = []
    total = 0.0

    for i in range(1, n_lines + 1):
        qty        = round(rng.uniform(1, 50), 2)
        unit_price = round(rng.uniform(10, max_amount / max(n_lines, 1)), 2)
        line_amt   = round(qty * unit_price, 2)
        total     += line_amt
        line_nodes  = build_line_nodes_summary(rng, i, line_amt, qty, unit_price, date_from, date_to)
        line_inner  = shuffle_nodes(line_nodes, rng, shuffle)
        line_blocks.append(
            f"      <fops:LineEntry>\n{line_inner}\n      </fops:LineEntry>"
        )

    total        = round(total, 2)
    vendor_total = round(total * rng.uniform(0.98, 1.02), 2)
    drift_nodes  = build_drift_nodes(rng, total) if drift else []

    flat_nodes, action_block, vendor_block, client_block = build_header_nodes(
        rng, doc_num, doc_date, date_from, date_to,
        vid, vname, status, total, vendor_total, n_lines, drift_nodes
    )
    flat_inner   = shuffle_nodes(flat_nodes, rng, shuffle)
    lines_inner  = "\n".join(line_blocks)
    alloc_xml    = build_allocations_block(rng, total, detailed=False, shuffle=shuffle)

    return (
        f'<?xml version="1.0" encoding="UTF-8"?>\n'
        f'<fops:StandardBillingDocument xmlns:fops="{NS}" invoice_id="{invoice_id}">\n'
        f'  <fops:SummaryInvoice>\n'
        f'    <fops:DocumentHeader>\n'
        f'{flat_inner}\n'
        f'{action_block}\n'
        f'{vendor_block}\n'
        f'{client_block}\n'
        f'    </fops:DocumentHeader>\n'
        f'    <fops:DocumentLines>\n'
        f'{alloc_xml}\n'
        f'{lines_inner}\n'
        f'    </fops:DocumentLines>\n'
        f'  </fops:SummaryInvoice>\n'
        f'</fops:StandardBillingDocument>\n'
    )


# ── CLI ────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Parametric XML invoice generator for xml-drift-lakehouse"
    )
    parser.add_argument("--count",        type=int,   default=3000,          help="Total files to generate")
    parser.add_argument("--out",          type=str,   default="./generated", help="Output directory")
    parser.add_argument("--date-from",    type=str,   default="2020-01-01",  help="First invoice date YYYY-MM-DD")
    parser.add_argument("--date-to",      type=str,   default="2024-12-31",  help="Last invoice date YYYY-MM-DD")
    parser.add_argument("--max-amount",   type=float, default=50000.0,       help="Max invoice total")
    parser.add_argument("--max-lines",    type=int,   default=10,            help="Max line items per invoice")
    parser.add_argument("--detailed-pct", type=int,   default=60,            help="Pct of DetailedInvoice variant (0-100)")
    parser.add_argument("--seed",         type=int,   default=42,            help="Random seed for reproducibility")
    parser.add_argument("--drift-pct",    type=int,   default=0,             help="Pct of files with unknown drift fields (0-100)")
    parser.add_argument("--no-shuffle",   action="store_true",               help="Disable node order shuffling")
    args = parser.parse_args()

    date_from = date.fromisoformat(args.date_from)
    date_to   = date.fromisoformat(args.date_to)
    out_dir   = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    shuffle   = not args.no_shuffle

    rng = random.Random(args.seed)

    n_detailed = int(args.count * args.detailed_pct / 100)
    n_summary  = args.count - n_detailed
    n_drift    = int(args.count * args.drift_pct / 100)

    print(f"Generating {args.count} XML files:")
    print(f"  DetailedInvoice:  {n_detailed}")
    print(f"  SummaryInvoice:   {n_summary}")
    print(f"  With drift:       {n_drift}")
    print(f"  Node shuffling:   {'enabled' if shuffle else 'disabled'}")
    print(f"  Output:           {out_dir}")
    print(f"  Date range:       {date_from} to {date_to}")
    print(f"  Max amount:       ${args.max_amount:,.2f}")
    print(f"  Max lines:        {args.max_lines}")
    print(f"  Seed:             {args.seed}")
    print()

    variants = (
        [("detailed", False)] * max(0, n_detailed - n_drift // 2) +
        [("detailed", True)]  * (n_drift // 2) +
        [("summary",  False)] * max(0, n_summary - (n_drift - n_drift // 2)) +
        [("summary",  True)]  * (n_drift - n_drift // 2)
    )
    while len(variants) < args.count:
        variants.append(("detailed" if rng.random() < args.detailed_pct / 100 else "summary", False))
    variants = variants[:args.count]
    rng.shuffle(variants)

    for i, (variant, drift) in enumerate(variants, 1):
        invoice_id = 900000000 + i
        if variant == "detailed":
            xml = build_detailed_invoice(rng, date_from, date_to, args.max_amount, args.max_lines, drift, shuffle, invoice_id)
        else:
            xml = build_summary_invoice(rng, date_from, date_to, args.max_amount, args.max_lines, drift, shuffle, invoice_id)

        doc_id = rng.randint(1000000000, 9999999999)
        fname  = out_dir / f"{doc_id}.xml"
        fname.write_text(xml, encoding="utf-8")

        if i % 500 == 0 or i == args.count:
            print(f"  {i}/{args.count} files written...")

    print(f"\nDone. {args.count} files written to {out_dir}")
    print(f"\nTo use with xml-drift-lakehouse:")
    print(f"  # Stable run (no drift):")
    print(f"  python ingestion/parser.py --src {out_dir} --dst ./output")
    print(f"  python ingestion/schema_diff.py --src {out_dir}")
    print(f"")
    print(f"  # Drift run (re-generate with --drift-pct 5):")
    print(f"  python xml_generator.py --count {args.count} --out {out_dir} --seed {args.seed} --drift-pct 5")


if __name__ == "__main__":
    main()
