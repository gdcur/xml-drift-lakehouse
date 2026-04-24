"""
rag_mapper.py — xml-drift-lakehouse RAG field mapper (public, lightweight)
===========================================================================
Takes the diff produced by schema_diff.py and for each new/unknown field:

  1. Builds a rich context (name + type + position + examples)
  2. Sends the full known schema corpus + new field to the LLM in one call
  3. LLM suggests mapping + confidence + reasoning
  4. Scores confidence (LLM score + type match + position match)
  5. Writes decisions to mapping_registry (DuckDB table)

No local embeddings, no PyTorch, no sentence-transformers.
The LLM does the full semantic matching in one prompt.

LLM providers (set via env var LLM_PROVIDER):
  - claude : Anthropic Claude API (needs ANTHROPIC_API_KEY)
  - ollama : Local Ollama (needs Ollama running, set OLLAMA_MODEL)

Confidence tiers:
  >= 0.90  → auto-approved, pipeline continues
  >= 0.70  → flagged for review, pipeline continues with suggestion
  <  0.70  → pending human decision (logged, pipeline uses best guess)

Usage:
    python rag_mapper.py
    python rag_mapper.py --diff ./output/schema_diff/2026-04-24/diff.json
    python rag_mapper.py --dry-run

Environment variables:
    LLM_PROVIDER        claude | ollama (default: ollama)
    ANTHROPIC_API_KEY   required if LLM_PROVIDER=claude
    OLLAMA_MODEL        model name (default: llama3)
    OLLAMA_HOST         Ollama host (default: http://localhost:11434)
"""

import argparse
import json
import os
import time
from abc import ABC, abstractmethod
from datetime import date
from pathlib import Path

import duckdb

# ── Confidence thresholds ─────────────────────────────────────────────────────
CONFIDENCE_AUTO   = float(os.getenv("CONFIDENCE_AUTO",   "0.90"))
CONFIDENCE_REVIEW = float(os.getenv("CONFIDENCE_REVIEW", "0.70"))

# ── Known schema corpus ───────────────────────────────────────────────────────
# Sent to LLM as context for semantic matching.
CORPUS = [
    # (field_name, type, position, description)
    ("invoice_total",         "decimal",  "header",     "total monetary value of the invoice"),
    ("vendor_total",          "decimal",  "header",     "total amount as submitted by the vendor"),
    ("line_total",            "decimal",  "line",       "total amount for a single line item"),
    ("line_subtotal",         "decimal",  "line",       "line item subtotal before tax"),
    ("line_pretax_total",     "decimal",  "line",       "line item total before tax is applied"),
    ("invoice_id",            "string",   "header",     "unique invoice document identifier"),
    ("document_date",         "date",     "header",     "date the invoice was issued"),
    ("document_type",         "string",   "header",     "type classification of the document"),
    ("currency_code",         "string",   "header",     "ISO currency code for monetary values"),
    ("vendor_entity_name",    "string",   "header",     "name of the vendor or supplier company"),
    ("client_entity_name",    "string",   "header",     "name of the client or buyer company"),
    ("vendor_entity_code",    "string",   "header",     "identifier code for the vendor"),
    ("client_entity_code",    "string",   "header",     "identifier code for the client"),
    ("line_number",           "integer",  "line",       "sequential number of the line item"),
    ("quantity",              "decimal",  "line",       "quantity or count of units"),
    ("unit_price",            "decimal",  "line",       "price per unit for the line item"),
    ("units",                 "string",   "line",       "unit of measure for quantity"),
    ("service_code",          "string",   "line",       "code identifying the service or product"),
    ("product_description",   "string",   "line",       "description of the product or service"),
    ("product_category",      "string",   "line",       "category of the product or service"),
    ("charge_class",          "string",   "line",       "charge type: itemized or non-itemized"),
    ("cost_center",           "string",   "allocation", "cost center for accounting allocation"),
    ("project_code",          "string",   "allocation", "project or AFE code for cost allocation"),
    ("work_order",            "string",   "allocation", "work order reference number"),
    ("order_reference",       "string",   "allocation", "purchase order reference number"),
    ("alloc_rate",            "decimal",  "allocation", "allocation rate or percentage"),
    ("alloc_total",           "decimal",  "allocation", "total amount allocated to this cost object"),
    ("period_date",           "date",     "line",       "service period date"),
    ("period_start_date",     "date",     "line",       "start date of the service period"),
    ("action_status",         "string",   "header",     "approval workflow status"),
    ("action_datetime",       "datetime", "header",     "timestamp of the last workflow action"),
    ("tax_type",              "string",   "line",       "type of tax applied"),
    ("tax_total",             "decimal",  "line",       "total tax amount"),
    ("tax_exempt_code",       "string",   "line",       "tax exemption code"),
    ("due_date",              "date",     "line",       "payment due date"),
    ("days_due",              "integer",  "line",       "number of days until payment is due"),
    ("cross_ref_doc_number",  "string",   "line",       "reference to a related document number"),
    ("cross_ref_doc_type",    "string",   "line",       "type of the referenced document"),
    ("discount_total",        "decimal",  "line",       "total discount amount applied"),
    ("discount_rate",         "decimal",  "line",       "discount rate or percentage applied"),
]


# ── LLM interface ─────────────────────────────────────────────────────────────

class BaseLLMMapper(ABC):
    """Abstract base — one method, two implementations."""

    @abstractmethod
    def suggest_mapping(self, new_field: dict) -> dict:
        """
        Returns:
            {
                "mapped_to":  str,    # known field_name or "UNKNOWN"
                "confidence": float,  # 0.0-1.0
                "reasoning":  str,    # one sentence
            }
        """
        pass

    def _build_prompt(self, new_field: dict) -> str:
        examples = ", ".join(new_field.get("example_values", [])[:3]) or "N/A"
        parents  = ", ".join(new_field.get("parents", ["unknown"]))
        variants = ", ".join(new_field.get("variants", ["unknown"]))

        corpus_lines = "\n".join([
            f"  {name:30s} | {ftype:10s} | {pos:12s} | {desc}"
            for name, ftype, pos, desc in CORPUS
        ])

        return f"""You are a data engineering assistant. Your job is to map a newly
discovered XML field to the closest matching field in a known schema.

NEW FIELD:
  name:     {new_field['field_name']}
  type:     {new_field['observed_type']}
  position: {parents}
  variants: {variants}
  examples: {examples}

KNOWN SCHEMA (field_name | type | position | description):
{corpus_lines}

TASK:
Determine which known field this new field most likely represents.
Consider:
  - Common abbreviations: amt=amount, qty=quantity, desc=description, num=number, pct=percent
  - Data type must be compatible (decimal cannot map to string)
  - Position matters: header fields map to header fields, line to line
  - If no reasonable match exists: return UNKNOWN

Respond ONLY with valid JSON, no markdown formatting:
{{
  "mapped_to": "<field_name from known schema, or UNKNOWN>",
  "confidence": <0.0 to 1.0>,
  "reasoning": "<one sentence>"
}}"""


class ClaudeMapper(BaseLLMMapper):
    """Claude API implementation — best quality."""

    def __init__(self):
        self.api_key = os.getenv("ANTHROPIC_API_KEY")
        if not self.api_key:
            raise ValueError("ANTHROPIC_API_KEY not set")
        self.model = os.getenv("CLAUDE_MODEL", "claude-sonnet-4-20250514")

    def suggest_mapping(self, new_field: dict) -> dict:
        import urllib.request
        prompt  = self._build_prompt(new_field)
        payload = json.dumps({
            "model":      self.model,
            "max_tokens": 256,
            "messages":   [{"role": "user", "content": prompt}]
        }).encode()

        req = urllib.request.Request(
            "https://api.anthropic.com/v1/messages",
            data=payload,
            headers={
                "Content-Type":      "application/json",
                "x-api-key":         self.api_key,
                "anthropic-version": "2023-06-01",
            }
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                data     = json.loads(resp.read())
                raw_text = data["content"][0]["text"].strip()
                raw_text = raw_text.replace("```json", "").replace("```", "").strip()
                result   = json.loads(raw_text)
                return {
                    "mapped_to":  result.get("mapped_to",  "UNKNOWN"),
                    "confidence": float(result.get("confidence", 0.0)),
                    "reasoning":  result.get("reasoning",  ""),
                }
        except Exception as e:
            return {"mapped_to": "UNKNOWN", "confidence": 0.0,
                    "reasoning": f"Claude API error: {e}"}


class OllamaMapper(BaseLLMMapper):
    """Ollama local LLM implementation — free, no API key."""

    def __init__(self):
        self.model = os.getenv("OLLAMA_MODEL", "llama3")
        self.host  = os.getenv("OLLAMA_HOST",  "http://localhost:11434")

    def suggest_mapping(self, new_field: dict) -> dict:
        import urllib.request
        prompt  = self._build_prompt(new_field)
        payload = json.dumps({
            "model":  self.model,
            "prompt": prompt,
            "stream": False,
            "format": "json",
        }).encode()

        req = urllib.request.Request(
            f"{self.host}/api/generate",
            data=payload,
            headers={"Content-Type": "application/json"}
        )
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                data     = json.loads(resp.read())
                raw_text = data.get("response", "{}").strip()
                raw_text = raw_text.replace("```json", "").replace("```", "").strip()
                result   = json.loads(raw_text)
                return {
                    "mapped_to":  result.get("mapped_to",  "UNKNOWN"),
                    "confidence": float(result.get("confidence", 0.0)),
                    "reasoning":  result.get("reasoning",  ""),
                }
        except Exception as e:
            return {"mapped_to": "UNKNOWN", "confidence": 0.0,
                    "reasoning": f"Ollama error: {e}"}


def get_llm_mapper() -> BaseLLMMapper:
    provider = os.getenv("LLM_PROVIDER", "ollama").lower()
    if provider == "claude":
        print("LLM provider: Claude (Anthropic API)")
        return ClaudeMapper()
    elif provider == "ollama":
        print(f"LLM provider: Ollama ({os.getenv('OLLAMA_MODEL', 'llama3')})")
        return OllamaMapper()
    raise ValueError(f"Unknown LLM_PROVIDER: {provider}. Use 'claude' or 'ollama'.")


# ── Confidence scorer ─────────────────────────────────────────────────────────

def score_confidence(new_field: dict, mapped_to: str, llm_confidence: float) -> float:
    """
    Composite confidence:
      llm_confidence * 0.50
      type_match     * 0.30
      position_match * 0.20

    """
    if mapped_to == "UNKNOWN":
        return 0.0

    # Find mapped field in corpus
    corpus_map = {name: (ftype, pos) for name, ftype, pos, _ in CORPUS}
    if mapped_to not in corpus_map:
        return llm_confidence * 0.50

    mapped_type, mapped_pos = corpus_map[mapped_to]

    # Type compatibility
    numeric    = {"integer", "decimal"}
    obs_type   = new_field.get("observed_type", "unknown")
    type_match = 1.0 if mapped_type == obs_type else \
                 0.7 if (mapped_type in numeric and obs_type in numeric) else 0.0

    # Position match
    parents      = new_field.get("parents", [])
    position_map = {
        "header":     ["DocumentHeader", "DocumentNumber", "DocumentDate",
                       "DocumentType", "CurrencyCode"],
        "line":       ["LineEntry", "LineNumber", "DocumentLines", "Quantity"],
        "allocation": ["Allocation", "AllocationRate", "CostCenter"],
    }
    obs_pos = "unknown"
    for pos, triggers in position_map.items():
        if any(p in triggers for p in parents):
            obs_pos = pos
            break
    position_match = 1.0 if mapped_pos == obs_pos else 0.5

    score = (
        llm_confidence * 0.50 +
        type_match     * 0.30 +
        position_match * 0.20
    )
    return round(min(score, 1.0), 4)


# ── Mapping registry ──────────────────────────────────────────────────────────

def init_registry(db_path: str) -> duckdb.DuckDBPyConnection:
    """
    Initialize mapping_registry table in DuckDB.
    """
    con = duckdb.connect(db_path)
    con.execute("""
        CREATE TABLE IF NOT EXISTS mapping_registry (
            id             INTEGER,
            run_date       DATE,
            source_field   VARCHAR,
            source_path    VARCHAR,
            source_type    VARCHAR,
            mapped_to      VARCHAR,
            confidence     DOUBLE,
            decision_type  VARCHAR,
            llm_provider   VARCHAR,
            llm_reasoning  TEXT,
            status         VARCHAR DEFAULT 'active',
            overridden_by  VARCHAR,
            created_at     TIMESTAMP DEFAULT current_timestamp,
            updated_at     TIMESTAMP DEFAULT current_timestamp
        )
    """)
    con.execute("""
        CREATE SEQUENCE IF NOT EXISTS mapping_registry_seq START 1
    """)
    return con


def save_decision(con, decision: dict) -> None:
    """Upsert — same source_field + run_date = overwrite."""
    con.execute("""
        DELETE FROM mapping_registry
        WHERE source_field = ? AND run_date = ?
    """, [decision["source_field"], decision["run_date"]])

    con.execute("""
        INSERT INTO mapping_registry (
            id, run_date, source_field, source_path, source_type,
            mapped_to, confidence, decision_type, llm_provider,
            llm_reasoning, status
        ) VALUES (
            nextval('mapping_registry_seq'),
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
    """, [
        decision["run_date"],
        decision["source_field"],
        decision["source_path"],
        decision["source_type"],
        decision["mapped_to"],
        decision["confidence"],
        decision["decision_type"],
        decision["llm_provider"],
        decision["llm_reasoning"],
        decision["status"],
    ])


# ── Main ──────────────────────────────────────────────────────────────────────

def process_diff(diff: dict, llm: BaseLLMMapper,
                 con, dry_run: bool = False) -> list[dict]:
    new_fields   = diff.get("new_fields", [])
    if not new_fields:
        print("No new fields to map.")
        return []

    llm_provider = os.getenv("LLM_PROVIDER", "ollama")
    decisions    = []

    print(f"\nMapping {len(new_fields)} new field(s)...\n")

    for field in new_fields:
        fname = field["field_name"]
        print(f"  [{fname}]")

        llm_result = llm.suggest_mapping(field)
        confidence = score_confidence(field, llm_result["mapped_to"],
                                      llm_result["confidence"])

        print(f"    → {llm_result['mapped_to']}  "
              f"(confidence: {confidence:.2f})")
        print(f"    {llm_result['reasoning']}")

        if confidence >= CONFIDENCE_AUTO:
            decision_type = "auto_approved"
            status        = "active"
            tier          = "✅ AUTO"
        elif confidence >= CONFIDENCE_REVIEW:
            decision_type = "flagged_review"
            status        = "active"
            tier          = "⚠️  REVIEW"
        else:
            decision_type = "pending_human"
            status        = "pending"
            tier          = "🛑 HUMAN REQUIRED"

        print(f"    {tier}\n")

        decision = {
            "run_date":      str(diff["run_date"]),
            "source_field":  fname,
            "source_path":   "/".join(field.get("parents", [])),
            "source_type":   field.get("observed_type", "unknown"),
            "mapped_to":     llm_result["mapped_to"],
            "confidence":    confidence,
            "decision_type": decision_type,
            "llm_provider":  llm_provider,
            "llm_reasoning": llm_result["reasoning"],
            "status":        status,
        }
        decisions.append(decision)

        if not dry_run:
            save_decision(con, decision)

        time.sleep(0.3)  # gentle rate limiting

    return decisions


def main():
    parser = argparse.ArgumentParser(
        description="RAG field mapper — semantically map new XML fields to known schema")
    parser.add_argument("--diff",    default=None,
                        help="Path to diff.json (default: output/schema_diff/{today}/diff.json)")
    parser.add_argument("--db",      default="./output/lakehouse.duckdb",
                        help="DuckDB path for mapping_registry")
    parser.add_argument("--dry-run", action="store_true",
                        help="Run without writing to mapping_registry")
    args = parser.parse_args()

    # Resolve diff path
    diff_path = Path(args.diff) if args.diff else \
                Path(f"./output/schema_diff/{date.today()}/diff.json")

    if not diff_path.exists():
        print(f"No diff found at {diff_path}")
        print("Run schema_diff.py first.")
        return

    diff = json.loads(diff_path.read_text())

    if not diff.get("has_drift"):
        print("✅ No schema drift — RAG mapper not needed.")
        return

    print(f"Schema drift detected:")
    print(f"  New fields:    {len(diff.get('new_fields', []))}")
    print(f"  Variant drift: {len(diff.get('variant_drift', []))}")
    if args.dry_run:
        print("\n[DRY RUN — no writes to mapping_registry]\n")

    llm = get_llm_mapper()
    con = init_registry(args.db) if not args.dry_run else None

    decisions = process_diff(diff, llm, con, dry_run=args.dry_run)

    if decisions:
        auto    = sum(1 for d in decisions if d["decision_type"] == "auto_approved")
        review  = sum(1 for d in decisions if d["decision_type"] == "flagged_review")
        human   = sum(1 for d in decisions if d["decision_type"] == "pending_human")
        unknown = sum(1 for d in decisions if d["mapped_to"] == "UNKNOWN")

        print("─" * 50)
        print("Summary:")
        print(f"  ✅ Auto-approved:   {auto}")
        print(f"  ⚠️  Flagged review: {review}")
        print(f"  🛑 Human required: {human}")
        print(f"  ❓ Unknown:         {unknown}")

        if human > 0:
            print(f"\n  {human} field(s) pending human review.")
            print("  Query: SELECT * FROM mapping_registry WHERE status = 'pending'")

        if not args.dry_run:
            print(f"\nDecisions written to: {args.db} → mapping_registry")


if __name__ == "__main__":
    main()
