"""
daily_generator.py - Multi-day invoice movements for xml-drift-lakehouse
========================================================================
Builds a series of daily folders (day_001, day_002, ...) from a base set of
XML invoices. Day 1 is an unchanged copy of the base folder, so it matches
data/sample exactly. Every later day contains:

  - new invoices (invoice_id continues after the highest existing id)
  - updated invoices: an earlier invoice re-issued with the SAME invoice_id,
    a later action datetime and a few random internal changes
  - re-deliveries: an earlier file copied unchanged (should change nothing)

The generator imports xml_generator.py and never modifies it.

Parameters:
    --base               Folder with the day-1 XML files (default: data/sample)
    --out                Output root, must be empty or absent (default: data/daily)
    --days               Number of days to generate, including day 1 (default: 4)
    --new-per-day        New invoices per day (default: 3)
    --updates-per-day    Invoices updated per day (default: 2)
    --lookback-days      Updates are drawn from invoices last seen within this
                         many days before (default: 15)
    --mutations          Max random internal changes per updated invoice; each
                         one is picked from: workflow (valid next approval
                         step), line_amount, add_line,
                         remove_line (default: 2)
    --new-fields         Number of NEW header nodes introduced over the whole
                         run, 0 to 6 (default: 0)
    --redeliver-per-day  Unchanged re-deliveries per day (default: 1)
    --keep-name-pct      Pct of updates that keep the previous file name
                         (default: 50)
    --start-date         Calendar date of day 1 (default: 2025-01-01)
    --seed               Random seed (default: 42)

Output: day_NNN/*.xml plus manifest.csv describing every event, which can be
used as the expected result when testing upserts.

Example:
    python scripts/daily_generator.py --days 4 --new-fields 1 --seed 42
"""

import argparse
import copy
import csv
import random
import re
import shutil
import sys
from datetime import date, timedelta
from pathlib import Path

from lxml import etree

sys.path.insert(0, str(Path(__file__).resolve().parent))
import xml_generator as g  # noqa: E402

NS = {"f": g.NS}
OPS = ["workflow", "line_amount", "add_line", "remove_line"]


def fq(tag):
    return f"{{{g.NS}}}{tag}"


def money(x):
    return f"{x:.2f}"


def line_entries(root):
    return root.find(".//f:DocumentLines", NS).findall("f:LineEntry", NS)


def rebalance(allocs, amount):
    """Re-split an amount across allocations, keeping their rates."""
    running = 0.0
    for i, a in enumerate(allocs):
        rate = float(a.findtext("f:AllocationRate", namespaces=NS))
        if i == len(allocs) - 1:
            val = round(amount - running, 2)
        else:
            val = round(amount * rate / 100, 2)
            running += val
        a.find("f:Total", NS).text = money(val)


def recalc(root, rng):
    """Make header totals, line count and allocations consistent again."""
    detailed = root.find("f:DetailedInvoice", NS) is not None
    lines_el = root.find(".//f:DocumentLines", NS)
    entries = lines_el.findall("f:LineEntry", NS)
    total = round(sum(float(e.findtext("f:Total", namespaces=NS)) for e in entries), 2)
    header = root.find(".//f:DocumentHeader", NS)
    header.find("f:Total", NS).text = money(total)
    header.find("f:VendorTotal", NS).text = money(round(total * rng.uniform(0.98, 1.02), 2))
    header.find("f:LineCount", NS).text = str(len(entries))
    if detailed:
        for e in entries:
            rebalance(e.findall("f:Allocation", NS), float(e.findtext("f:Total", namespaces=NS)))
    else:
        rebalance(lines_el.findall("f:Allocation", NS), total)


def rescale_line(e, rng):
    qty = max(0.01, round(float(e.findtext("f:Quantity", namespaces=NS)) * rng.uniform(0.5, 1.5), 2))
    unit = float(e.findtext("f:UnitPrice", namespaces=NS))
    e.find("f:Quantity", NS).text = money(qty)
    sub_el = e.find("f:LineSubTotal", NS)
    if sub_el is not None:
        old_sub = float(sub_el.text)
        old_tot = float(e.findtext("f:Total", namespaces=NS))
        ratio = old_tot / old_sub if old_sub else 1.0
        sub = round(qty * unit, 2)
        amt = round(sub * ratio, 2)
        sub_el.text = money(sub)
        e.find("f:LinePretaxTotal", NS).text = money(amt)
        e.find("f:Total", NS).text = money(amt)
    else:
        e.find("f:Total", NS).text = money(round(qty * unit, 2))


WORKFLOW = {
    ("Submit", "Submitted"):    [("Submit", "Pending"), ("Submit", "On Hold"),
                                 ("Approve", "Approved"), ("Reject", "Rejected")],
    ("Submit", "Pending"):      [("Approve", "Approved"), ("Reject", "Rejected"),
                                 ("Submit", "On Hold"), ("Reject", "Disputed")],
    ("Submit", "Re-Submitted"): [("Submit", "Pending"), ("Approve", "Approved"),
                                 ("Reject", "Rejected")],
    ("Submit", "On Hold"):      [("Submit", "Pending"), ("Submit", "Re-Submitted"),
                                 ("Reject", "Cancelled")],
    ("Reject", "Rejected"):     [("Submit", "Re-Submitted"), ("Reject", "Disputed"),
                                 ("Reject", "Cancelled")],
    ("Reject", "Disputed"):     [("Submit", "Re-Submitted"), ("Approve", "Approved"),
                                 ("Reject", "Cancelled")],
    ("Approve", "Approved"):    [("Pay", "Approved"), ("Reject", "Disputed")],
    ("Pay", "Approved"):        [],
    ("Reject", "Cancelled"):    [],
}


def get_state(root):
    at = root.find(".//f:Action/f:ActionType", NS).text
    st = root.find(".//f:Action/f:Status", NS).text
    return at, st


def set_state(root, at, st):
    root.find(".//f:Action/f:ActionType", NS).text = at
    root.find(".//f:Action/f:Status", NS).text = st


def op_workflow(root, rng):
    """Move the invoice to a valid next step of the approval workflow."""
    old = get_state(root)
    if old in WORKFLOW:
        options = WORKFLOW[old]
    else:
        same_status = [k for k in WORKFLOW if k[1] == old[1]]
        options = WORKFLOW[same_status[0]] if same_status else [("Submit", "Re-Submitted")]
    if not options:
        return None
    new = rng.choice(options)
    set_state(root, *new)
    return f"workflow:{old[0]}/{old[1]}>{new[0]}/{new[1]}"


def op_line_amount(root, rng):
    e = rng.choice(line_entries(root))
    rescale_line(e, rng)
    return f"line_amount:{e.findtext('f:LineNumber', namespaces=NS)}"


def op_add_line(root, rng):
    entries = line_entries(root)
    last = entries[-1]
    new = copy.deepcopy(last)
    n = len(entries) + 1
    new.find("f:LineNumber", NS).text = str(n)
    new.find("f:ProductDescription", NS).text = rng.choice(g.PRODUCT_DESCRIPTIONS)
    last.addnext(new)
    rescale_line(new, rng)
    return f"add_line:{n}"


def op_remove_line(root, rng):
    entries = line_entries(root)
    if len(entries) < 2:
        return None
    last = entries[-1]
    num = last.findtext("f:LineNumber", namespaces=NS)
    last.getparent().remove(last)
    return f"remove_line:{num}"


OP_FUNCS = {
    "workflow": op_workflow,
    "line_amount": op_line_amount,
    "add_line": op_add_line,
    "remove_line": op_remove_line,
}


def stamp(root, day_date, rng):
    """Set a new action datetime and transaction id, so the newest version wins."""
    t = root.find(".//f:Action/f:TransactionDateTime", NS)
    t.text = f"{day_date}T{rng.randint(0, 23):02d}:{rng.randint(0, 59):02d}:{rng.randint(0, 59):02d}"
    root.find(".//f:Action/f:TransactionIdentifier", NS).text = f"TXN-{rng.randint(10000000, 99999999)}"


def add_field(root, tag, rng):
    header = root.find(".//f:DocumentHeader", NS)
    if header.find(f"f:{tag}", NS) is not None:
        return None
    tpl = dict(g.DRIFT_FIELDS)[tag]
    total = float(header.findtext("f:Total", namespaces=NS))
    value = tpl.format(
        amount=total,
        name=f"{rng.choice(g.LAST_NAMES)}, {rng.choice(g.FIRST_NAMES)}",
        ref=f"REF-{rng.randint(1000, 9999)}",
        code=f"APV-{rng.randint(100, 999)}",
        system=rng.choice(["SAP", "Oracle", "JDE", "Maximo", "Ariba"]),
    )
    el = etree.SubElement(header, fq(tag))
    el.text = value
    el.tail = "\n    "
    return f"new_field:{tag}"


def main():
    p = argparse.ArgumentParser(description="Multi-day invoice movements generator")
    p.add_argument("--base", default="data/sample")
    p.add_argument("--out", default="data/daily")
    p.add_argument("--days", type=int, default=4)
    p.add_argument("--new-per-day", type=int, default=3)
    p.add_argument("--updates-per-day", type=int, default=2)
    p.add_argument("--lookback-days", type=int, default=15)
    p.add_argument("--mutations", type=int, default=2)
    p.add_argument("--new-fields", type=int, default=0)
    p.add_argument("--redeliver-per-day", type=int, default=1)
    p.add_argument("--keep-name-pct", type=int, default=50)
    p.add_argument("--start-date", default="2025-01-01")
    p.add_argument("--seed", type=int, default=42)
    a = p.parse_args()

    if not 0 <= a.new_fields <= len(g.DRIFT_FIELDS):
        sys.exit(f"--new-fields must be between 0 and {len(g.DRIFT_FIELDS)}")
    out = Path(a.out)
    if out.exists() and any(out.iterdir()):
        sys.exit(f"Output folder {out} is not empty")
    base = Path(a.base)
    base_files = sorted(base.glob("*.xml"))
    if not base_files:
        sys.exit(f"No XML files found in {base}")

    rng = random.Random(a.seed)
    start = date.fromisoformat(a.start_date)

    day1 = out / "day_001"
    day1.mkdir(parents=True)
    registry = {}
    events = []
    for f in base_files:
        shutil.copy2(f, day1 / f.name)
        inv = int(re.search(r'invoice_id="(\d+)"', f.read_text(encoding="utf-8")).group(1))
        rel = f"day_001/{f.name}"
        registry[inv] = {"day": 1, "rel": rel}
        events.append((1, inv, "new", "", rel))
    next_id = max(registry) + 1

    tags = rng.sample([t for t, _ in g.DRIFT_FIELDS], a.new_fields)
    intro_day = {t: rng.randint(2, max(2, a.days)) for t in tags}
    active_fields = []

    for day in range(2, a.days + 1):
        ddate = start + timedelta(days=day - 1)
        dd = out / f"day_{day:03d}"
        dd.mkdir()
        used = set()

        def fresh_name():
            while True:
                name = f"{rng.randint(1000000000, 9999999999)}.xml"
                if name not in used:
                    used.add(name)
                    return name

        docs = []

        pool = sorted(i for i, v in registry.items() if day - a.lookback_days <= v["day"] < day)
        for inv in rng.sample(pool, min(a.updates_per_day, len(pool))):
            prev_rel = registry[inv]["rel"]
            tree = etree.parse(str(out / prev_rel))
            root = tree.getroot()
            k = rng.randint(1, max(1, a.mutations))
            applied = []
            for op in rng.sample(OPS, min(k, len(OPS))):
                r = OP_FUNCS[op](root, rng)
                if r:
                    applied.append(r)
            if get_state(root) not in WORKFLOW:
                r = op_workflow(root, rng)
                if r:
                    applied.append(r)
            if not applied:
                applied.append(op_line_amount(root, rng))
            recalc(root, rng)
            stamp(root, ddate, rng)
            prev_name = prev_rel.split("/")[-1]
            if rng.random() * 100 < a.keep_name_pct and prev_name not in used:
                used.add(prev_name)
                name = prev_name
            else:
                name = fresh_name()
            docs.append({"tree": tree, "name": name, "inv": inv, "event": "update", "ops": applied})

        for _ in range(a.new_per_day):
            detailed = rng.random() < 0.6
            builder = g.build_detailed_invoice if detailed else g.build_summary_invoice
            xml = builder(rng, ddate - timedelta(days=30), ddate, 50000.0, 10, False, True, next_id)
            tree = etree.ElementTree(etree.fromstring(xml.encode("utf-8")))
            root = tree.getroot()
            set_state(root, "Submit", "Submitted")
            applied = []
            for tag in active_fields:
                if rng.random() < 0.5:
                    r = add_field(root, tag, rng)
                    if r:
                        applied.append(r)
            stamp(root, ddate, rng)
            docs.append({"tree": tree, "name": fresh_name(), "inv": next_id, "event": "new", "ops": applied})
            next_id += 1

        for tag, d in intro_day.items():
            if d == day and docs:
                for doc in rng.sample(docs, min(2, len(docs))):
                    r = add_field(doc["tree"].getroot(), tag, rng)
                    if r:
                        doc["ops"].append(r)
                active_fields.append(tag)

        for doc in docs:
            path = dd / doc["name"]
            doc["tree"].write(str(path), xml_declaration=True, encoding="UTF-8")
            rel = f"day_{day:03d}/{doc['name']}"
            registry[doc["inv"]] = {"day": day, "rel": rel}
            events.append((day, doc["inv"], doc["event"], ";".join(doc["ops"]), rel))

        touched = {d["inv"] for d in docs}
        pool2 = sorted(i for i, v in registry.items() if v["day"] < day and i not in touched)
        for inv in rng.sample(pool2, min(a.redeliver_per_day, len(pool2))):
            prev_rel = registry[inv]["rel"]
            prev_name = prev_rel.split("/")[-1]
            if prev_name in used:
                name = fresh_name()
            else:
                used.add(prev_name)
                name = prev_name
            shutil.copy2(out / prev_rel, dd / name)
            events.append((day, inv, "redelivery", "", f"day_{day:03d}/{name}"))

    with open(out / "manifest.csv", "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["day", "invoice_id", "event", "ops", "file"])
        w.writerows(events)

    counts = {}
    for e in events:
        counts[e[2]] = counts.get(e[2], 0) + 1
    print(f"Generated {a.days} days in {out}")
    print(f"  Events: {counts}")
    print(f"  New fields introduced: {intro_day if intro_day else 'none'}")
    print(f"  Manifest: {out / 'manifest.csv'}")


if __name__ == "__main__":
    main()
