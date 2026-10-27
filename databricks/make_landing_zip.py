"""Zip data/daily (40 XML files + manifest.csv) for upload to the Databricks volume.

The Databricks web UI uploads single files, not folders, so the folder structure
travels inside a zip. The notebook unzips it inside the volume (setup cell only).

    python databricks/make_landing_zip.py            # writes output/daily_xml.zip
"""
import argparse
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", default=str(ROOT / "data" / "daily"))
    ap.add_argument("--out", default=str(ROOT / "output" / "daily_xml.zip"))
    args = ap.parse_args()

    src, out = Path(args.src), Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as z:
        for f in sorted(src.rglob("*")):
            if f.is_file():
                z.write(f, f.relative_to(src.parent))   # keeps the leading "daily/"
    print(f"{out}: {len(zipfile.ZipFile(out).namelist())} files, {out.stat().st_size} bytes")


if __name__ == "__main__":
    main()
