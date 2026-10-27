"""
test_iceberg_load.py - Iceberg write path (ADR-0001, ADR-0002)
==============================================================
Replays the committed multi-day set (data/daily, see manifest.csv) through
parser.process_file and iceberg_writer.load_batch on a temporary warehouse.
Skipped when pyiceberg is not installed.

Run:
    python -m pytest tests/test_iceberg_load.py -v
"""

import csv
import sys
from pathlib import Path

import polars as pl
import pytest
from polars.testing import assert_frame_equal

pytest.importorskip("pyiceberg")

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "ingestion"))

import iceberg_writer as iw  # noqa: E402
from parser import process_file  # noqa: E402

DAILY = ROOT / "data" / "daily"
TOUCHED = "900000004"   # new day 1, updated day 2 (workflow change), later days too
LINE_CHANGE = "900000003"  # day 3: remove_line:5;add_line:5


def ts(day: int) -> str:
    return f"2026-01-{day:02d}T00:00:00+00:00"


def manifest() -> list[dict]:
    with open(DAILY / "manifest.csv", newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def files_of_day(day: int) -> list[str]:
    return [r["file"] for r in manifest() if int(r["day"]) == day]


def parse_files(rels: list[str], ingested_at: str) -> tuple[pl.DataFrame, pl.DataFrame]:
    headers, lines = [], []
    for rel in rels:
        h, ls = process_file(DAILY / rel, ingested_at, rel)
        headers.append(h)
        lines.extend(ls)
    return (pl.DataFrame(headers, infer_schema_length=len(headers)),
            pl.DataFrame(lines, infer_schema_length=len(lines)) if lines else pl.DataFrame())


def load_day(catalog, day: int, ingested_at: str | None = None) -> dict:
    return iw.load_batch(catalog, *parse_files(files_of_day(day), ingested_at or ts(day)))


@pytest.fixture
def catalog(tmp_path):
    return iw.get_catalog(tmp_path / "wh")


def read(catalog, ident):
    return iw.read_table(catalog.load_table(ident))


def meta(catalog):
    return {i: catalog.load_table(i).metadata_location for i in iw.TABLES}


def test_first_batch_loads_everything(catalog):
    stats = load_day(catalog, 1)
    assert (stats["received"], stats["new"], stats["lines_written"]) == (10, 10, 55)
    assert read(catalog, iw.INVOICES).height == 10
    assert read(catalog, iw.LINE_ITEMS).height == 55
    assert iw.check_consistency(catalog) == []


def test_replay_matches_manifest(catalog):
    for day in range(1, 7):
        load_day(catalog, day)
    inv, lines = read(catalog, iw.INVOICES), read(catalog, iw.LINE_ITEMS)

    last = {}
    for r in manifest():            # manifest is in day order: the last file per invoice wins
        last[r["invoice_id"]] = r["file"]
    assert inv.height == len(last) == inv["invoice_id"].n_unique()

    for invoice_id, rel in last.items():
        header, expected = process_file(DAILY / rel, ts(1), rel)
        row = inv.filter(pl.col("invoice_id") == invoice_id)
        assert row.height == 1 and row["source_path"][0] == rel
        got = lines.filter(pl.col("invoice_id") == invoice_id).sort("line_number")
        want = sorted(expected, key=lambda x: x["line_number"])
        assert got["line_number"].to_list() == [x["line_number"] for x in want]
        assert got["line_total"].to_list() == [x["line_total"] for x in want]
    assert iw.check_consistency(catalog) == []


def test_removed_line_does_not_linger(catalog):
    load_day(catalog, 1)
    before = read(catalog, iw.LINE_ITEMS).filter(pl.col("invoice_id") == LINE_CHANGE)
    for day in (2, 3):
        load_day(catalog, day)
    after = read(catalog, iw.LINE_ITEMS).filter(pl.col("invoice_id") == LINE_CHANGE)

    rel = next(r["file"] for r in manifest() if r["invoice_id"] == LINE_CHANGE and r["day"] == "3")
    _, expected = process_file(DAILY / rel, ts(3), rel)
    key = lambda d: sorted(zip(d["line_number"], d["product_description"], d["line_total"]))  # noqa: E731
    assert key(after) == sorted((x["line_number"], x["product_description"], x["line_total"]) for x in expected)
    assert key(before) != key(after)          # the update really changed the lines
    assert after.height == len(expected)      # and nothing from the old version is left


def test_same_batch_twice_changes_nothing(catalog):
    load_day(catalog, 1)
    snap = meta(catalog)
    again = load_day(catalog, 1)
    assert (again["new"], again["updated"], again["unchanged"]) == (0, 0, 10)
    assert meta(catalog) == snap              # no new snapshot on either table


def test_reparsing_same_files_later_changes_nothing(catalog):
    load_day(catalog, 1)
    snap = meta(catalog)
    again = load_day(catalog, 1, ingested_at="2026-02-01T00:00:00+00:00")
    assert again["unchanged"] == 10 and meta(catalog) == snap


def test_redelivery_in_another_file_moves_lineage_only(catalog):
    load_day(catalog, 1)
    redelivered = next(r for r in manifest() if r["event"] == "redelivery" and r["day"] == "2")
    inv_id, rel = redelivered["invoice_id"], redelivered["file"]
    before = read(catalog, iw.INVOICES).filter(pl.col("invoice_id") == inv_id)
    load_day(catalog, 2)
    inv = read(catalog, iw.INVOICES)
    after = inv.filter(pl.col("invoice_id") == inv_id)

    assert after.height == 1 and inv["invoice_id"].n_unique() == inv.height   # one row, not two
    assert after["source_path"][0] == rel and before["source_path"][0] != rel
    content = [c for c in inv.columns if c not in iw.LINEAGE_COLS]
    assert after.select(content).to_dicts() == before.select(content).to_dicts()


def test_older_delivery_does_not_overwrite_newer(catalog):
    rows = [r for r in manifest() if r["invoice_id"] == TOUCHED]
    d1, d2 = rows[0]["file"], rows[1]["file"]
    h1, _ = process_file(DAILY / d1, ts(1), d1)
    h2, _ = process_file(DAILY / d2, ts(2), d2)
    assert h2["action_datetime"] > h1["action_datetime"]      # precondition: day 2 really is newer

    load_day(catalog, 2)                       # newer version first
    stats = load_day(catalog, 1, ingested_at="2026-03-01T00:00:00+00:00")   # older one arrives later
    assert stats["stale"] >= 1
    row = read(catalog, iw.INVOICES).filter(pl.col("invoice_id") == TOUCHED)
    assert row["source_path"][0] == d2
    assert iw.check_consistency(catalog) == []


def test_two_versions_in_one_batch_keep_the_winner(catalog):
    rows = [r for r in manifest() if r["invoice_id"] == TOUCHED]
    d1, d2 = rows[0]["file"], rows[1]["file"]
    inv_df, line_df = parse_files([d1, d2], ts(2))
    stats = iw.load_batch(catalog, inv_df, line_df)
    assert (stats["received"], stats["new"], stats["superseded"]) == (2, 1, 1)
    assert read(catalog, iw.INVOICES)["source_path"][0] == d2
    _, expected = process_file(DAILY / d2, ts(2), d2)
    assert read(catalog, iw.LINE_ITEMS).height == len(expected)
    assert iw.check_consistency(catalog) == []


def test_schema_evolution_add_column(catalog):
    load_day(catalog, 1)
    from pyiceberg.types import StringType
    t = catalog.load_table(iw.INVOICES)
    with t.update_schema() as u:
        u.add_column("original_system", StringType())
    load_day(catalog, 2)                       # batch does not carry the column: filled with null
    inv = read(catalog, iw.INVOICES)
    assert "original_system" in inv.columns and inv["original_system"].null_count() == inv.height
    assert iw.check_consistency(catalog) == []


def test_bad_values_fail_loudly(catalog):
    inv_df, line_df = parse_files(files_of_day(1), ts(1))
    broken = inv_df.with_columns(pl.when(pl.col("invoice_id") == pl.col("invoice_id").first())
                                 .then(pl.lit("not-a-date")).otherwise(pl.col("document_date")).alias("document_date"))
    with pytest.raises(ValueError, match="document_date"):
        iw.load_batch(catalog, broken, line_df)
    no_id = inv_df.with_columns(pl.lit(None, dtype=pl.Utf8).alias("invoice_id"))
    with pytest.raises(ValueError, match="invoice_id"):
        iw.load_batch(catalog, no_id, line_df)
    assert read(catalog, iw.INVOICES).height == 0      # nothing was written


def test_version_hint_points_at_current_metadata(catalog):
    for day in (1, 2):
        load_day(catalog, day)
    for ident in iw.TABLES:
        t = catalog.load_table(ident)
        meta_dir = Path(t.metadata_location).parent
        stem = (meta_dir / "version-hint.text").read_text()
        assert (meta_dir / f"{stem}.metadata.json").exists()
        assert Path(t.metadata_location).name == f"{stem}.metadata.json"


def test_duckdb_reads_through_the_hint(catalog):
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect()
    try:
        con.execute("INSTALL iceberg; LOAD iceberg;")
    except Exception as exc:                   # offline machines cannot fetch the extension
        pytest.skip(f"DuckDB iceberg extension unavailable: {str(exc)[:80]}")
    for day in range(1, 7):
        load_day(catalog, day)
    for ident, folder in ((iw.INVOICES, "invoices"), (iw.LINE_ITEMS, "line_items")):
        path = Path(catalog.load_table(ident).metadata_location).parent.parent
        got = con.execute(
            f"select count(*) from iceberg_scan('{path}', version_name_format='%s%s.metadata.json', allow_moved_paths=true)"
        ).fetchone()[0]
        assert got == read(catalog, ident).height


def test_iceberg_matches_parquet_output(catalog, tmp_path):
    """What the pipeline writes to Parquet and what Iceberg holds must agree (ADR-0001)."""
    headers, lines = [], []
    for f in sorted((ROOT / "data" / "sample").glob("*.xml")):
        h, ls = process_file(f, ts(1), f.name)
        headers.append(h)
        lines.extend(ls)
    pl.DataFrame(headers, infer_schema_length=len(headers)).write_parquet(tmp_path / "invoices.parquet")
    pl.DataFrame(lines, infer_schema_length=len(lines)).write_parquet(tmp_path / "line_items.parquet")
    inv_pq, line_pq = pl.read_parquet(tmp_path / "invoices.parquet"), pl.read_parquet(tmp_path / "line_items.parquet")

    iw.load_batch(catalog, inv_pq, line_pq)

    tables = {i: catalog.load_table(i) for i in iw.TABLES}
    for ident, parquet, keys in ((iw.INVOICES, inv_pq, ["invoice_id"]), (iw.LINE_ITEMS, line_pq, ["invoice_id", "line_number"])):
        want = iw.conform(parquet, tables[ident].schema(), ident).sort(keys)
        got = read(catalog, ident).sort(keys)
        assert (want.height, got.height) == (parquet.height, parquet.height)
        assert_frame_equal(got, want)
    assert (inv_pq.height, line_pq.height) == (10, 55)
