"""
01b_check_ffiec_nic.py

Report which FR Y-9C quarters should be public but are not on disk.

The FFIEC NIC download page (https://www.ffiec.gov/npw/FinancialReport/FinancialDataDownload)
serves a CAPTCHA to anything that is not a browser, so the 2021Q2+ ZIPs cannot be fetched
by script; they are a manual download. This check makes the gap visible instead of
discovered: it compares the quarters on disk (Chicago Fed CSVs bhcfYYMM.csv and NIC ZIPs
BHCFYYYYMMDD.ZIP in data/raw/, and parquet files in data/processed/y_9c/) with the quarters
whose FR Y-9C data the Federal Reserve has normally published by today.

Publication lag: the FR Y-9C is due 40 days after quarter-end (45 for Q4) and the NIC
bulk file follows within a few weeks, so a quarter is treated as publishable
PUBLISH_LAG_DAYS after its end. That is a calendar rule, not a query of the site; a
quarter it lists as missing may still be a few days from release.

Usage:
    python 01b_check_ffiec_nic.py            # exit 0 if nothing is missing, 1 otherwise
    python 01b_check_ffiec_nic.py --lag 90   # a more conservative publication lag
"""

import argparse
import re
import sys
from datetime import date, timedelta
from pathlib import Path

RAW_DIR = Path("data/raw")
PROCESSED_DIR = Path("data/processed/y_9c")
FIRST_QUARTER = (1986, 3)
PUBLISH_LAG_DAYS = 75
NIC_URL = "https://www.ffiec.gov/npw/FinancialReport/FinancialDataDownload"


def quarter_end(year: int, q: int) -> date:
    return date(year, 3 * q, 31 if q in (1, 4) else 30)


def quarters_since(first, today: date, lag_days: int):
    y, q = first
    while quarter_end(y, q) + timedelta(days=lag_days) <= today:
        yield f"{y}Q{q}"
        y, q = (y + 1, 1) if q == 4 else (y, q + 1)


def on_disk_raw(raw_dir: Path) -> set:
    out = set()
    for p in raw_dir.iterdir():
        m = re.fullmatch(r"bhcf(\d{2})(\d{2})\.csv", p.name.lower())
        if m:
            yy, mm = int(m.group(1)), int(m.group(2))
            out.add(f"{1900 + yy if yy >= 80 else 2000 + yy}Q{(mm - 1) // 3 + 1}")
        m = re.fullmatch(r"bhcf(\d{4})(\d{2})(\d{2})\.zip", p.name.lower())
        if m:
            out.add(f"{m.group(1)}Q{(int(m.group(2)) - 1) // 3 + 1}")
    return out


def on_disk_processed(proc_dir: Path) -> set:
    return {p.stem for p in proc_dir.glob("????Q?.parquet")} if proc_dir.is_dir() else set()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--raw-dir", default=str(RAW_DIR))
    ap.add_argument("--processed-dir", default=str(PROCESSED_DIR))
    ap.add_argument("--lag", type=int, default=PUBLISH_LAG_DAYS, help=f"publication lag in days (default {PUBLISH_LAG_DAYS})")
    args = ap.parse_args()

    expected = list(quarters_since(FIRST_QUARTER, date.today(), args.lag))
    raw, proc = on_disk_raw(Path(args.raw_dir)), on_disk_processed(Path(args.processed_dir))
    missing_raw = [q for q in expected if q not in raw]
    unparsed = sorted(q for q in raw if q not in proc)
    print(f"publishable by {date.today()} (lag {args.lag} days): {expected[0]} .. {expected[-1]}")
    print(f"raw on disk: {len(raw)} quarters, newest {max(raw) if raw else None} | parsed: {len(proc)}, newest {max(proc) if proc else None}")
    print(f"missing raw: {', '.join(missing_raw) if missing_raw else 'none'}")
    if unparsed:
        print(f"raw but not parsed: {', '.join(unparsed)}  -> python 04_parse_data.py")
    if missing_raw:
        print(f"\nDownload the BHCF ZIPs for those quarters from {NIC_URL} into {args.raw_dir}/,\n"
              f"then run python 04_parse_data.py (it extracts the ZIPs itself).")
    return 1 if (missing_raw or unparsed) else 0


if __name__ == "__main__":
    sys.exit(main())
