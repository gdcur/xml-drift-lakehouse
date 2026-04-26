import sys
from pathlib import Path
from ingestion.parser import process_file

path = Path(sys.argv[1]) if len(sys.argv) > 1 else sorted(Path("public").glob("*.xml"))[0]
print(f"\nParsing: {path.name}\n")

header, lines = process_file(path, "2026-01-01T00:00:00+00:00")

print("── Header fields ──────────────────────────────")
for k in ("sk", "invoice_id", "invoice_db_id", "document_number", "variant", "source_file"):
    print(f"  {k:<20} = {header.get(k)}")

print(f"\n── First line item ────────────────────────────")
if lines:
    for k in ("sk", "invoice_sk", "invoice_id", "line_number"):
        print(f"  {k:<20} = {lines[0].get(k)}")
print()