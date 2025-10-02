from __future__ import annotations
import argparse
import os
import sys
import io
import json
from typing import Dict, Iterable, List, Tuple
from pathlib import Path
from datetime import datetime
import orjson as jsonlib
import tempfile
import zipfile
from contextlib import contextmanager


from sqlalchemy import text, create_engine
from sqlalchemy.engine import Connection
import requests, time
from dotenv import load_dotenv
import os
from typing import cast
from src.sql_scripts.fundamentals import *



# -----------------------------
# Configuration
# -----------------------------


# Canonical metrics we care about and acceptable tag synonyms.
# Map canonical_name -> list of XBRL tag candidates (ordered by preference).
TAG_MAP: Dict[str, List[str]] = {
    "AssetsCurrent": ["AssetsCurrent"],
    "LiabilitiesCurrent": ["LiabilitiesCurrent"],
    "Liabilities": ["Liabilities"],
    "StockholdersEquity": [
    "StockholdersEquity",
    "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
    ],
    "EarningsPerShare": ["EarningsPerShareDiluted", "EarningsPerShareBasic"],
    "NetIncomeLoss": ["NetIncomeLoss", "ProfitLoss"],
    "OperatingCashFlow": ["NetCashProvidedByUsedInOperatingActivities"],
    "DividendsPerShare": ["CommonStockDividendsPerShareDeclared"],
    "DividendsPaidCash": ["PaymentsOfDividendsCommonStock"],
    "SharesOutstanding": ["CommonStockSharesOutstanding"],
    "DebtCurrent": ["DebtCurrent"],
    "DebtNoncurrent": ["DebtNoncurrent"],
}


load_dotenv()

CONTACT_EMAIL : str = cast(str, os.getenv('CONTACT_EMAIL'))

SEC_HEADERS = {
    "User-Agent": f"ValueInvestingDash/0.1 (Michael C. Eaton; {CONTACT_EMAIL})",
    "Accept": "application/json",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
    "Referer": "https://www.sec.gov/search-filings/edgar-application-programming-interfaces",
}

def jloads(b: bytes):
    return jsonlib.loads(b)


CHUNK = 1024 * 1024  # 1 MiB

def _head(url):
    r = requests.head(url, headers=SEC_HEADERS, allow_redirects=True, timeout=30)
    r.raise_for_status()
    return r.headers

def _get_etag_or_mtime(headers):
    return headers.get("ETag") or headers.get("Last-Modified") or headers.get("Content-Length")

def _resume_range(path):
    return os.path.getsize(path) if os.path.exists(path) else 0

# Unit normalization: convert USD variants to plain USD, shares to shares.
# SEC units often appear as 'USD', 'USDm', 'USDth', etc.


def normalize_value_unit(value, unit: str) -> Tuple[float | None, str]:
    if value is None:
        return None, unit
    try:
        v = float(value)
    except Exception:
        return None, unit


    unit_norm = unit
    if unit.upper().startswith("USD"):
        # Scale to USD
        suffix = unit.upper()[3:]
        if suffix == "M":
            v *= 1_000_000
        elif suffix in ("MM", "MN"): 
            v *= 1_000_000
        elif suffix in ("B", "BN"):
            v *= 1_000_000_000
        elif suffix in ("TH", "THS", "THOUSANDS"):
            v *= 1_000
        unit_norm = "USD"
    elif unit.lower() in ("shares", "shrs"):
        unit_norm = "shares"


    return v, unit_norm


def download_zip(url: str, dest_path: str, max_retries: int = 5, sleep_s: float = 2.0) -> str:
    """
    Resumable download to dest_path. Writes to dest_path + ".part" then atomically renames.
    Returns dest_path.
    """
    tmp = dest_path + ".part"
    os.makedirs(os.path.dirname(dest_path) or '.', exist_ok=True)


    # check if file exists and if its new, add etag stamp to test
    hdr = _head(url)
    stamp = _get_etag_or_mtime(hdr)
    stamp_path = dest_path + '.stamp'
    if os.path.exists(dest_path) and os.path.exists(stamp_path):
        with open(stamp_path, 'r') as f:
            if f.read().strip() == str(stamp):
                return dest_path # up to date
            
    
    # resumable get download
    offset = _resume_range(tmp)
    headers = dict(SEC_HEADERS)
    if offset > 0:
        headers['Range'] = f'bytes={offset}-'

    
    for attempt in range(1, max_retries + 1):
        try:
            with requests.get(url, headers=headers, stream=True, timeout=60) as r:
                if r.status_code in (206, 200):
                    mode = 'ab' if r.status_code == 200 and offset > 0 else 'wb'
                    if mode =='wb' and os.path.exists(tmp):
                        os.remove(tmp)
                    with open(tmp, mode) as f:
                        for chunk in r.iter_content(chunk_size=CHUNK):
                            if chunk:
                                f.write(chunk)
                    
                    # done update
                    if os.path.exists(dest_path):
                        os.remove(dest_path)
                    os.rename(tmp, dest_path)
                    with open(stamp_path, 'w') as s:
                        s.write(str(stamp))
                    return dest_path
                
                elif r.status_code == 416:
                    # Range not satisfiable: probably already complete. Rename and move on.
                    if os.path.exists(tmp):
                        if os.path.exists(dest_path):
                            os.remove(dest_path)
                        os.rename(tmp, dest_path)
                    with open(stamp_path, "w") as s:
                        s.write(str(stamp))
                    return dest_path
                else:
                    r.raise_for_status()
        except Exception as e:
            if attempt == max_retries:
                raise
            time.sleep(sleep_s * attempt)  # simple backoff

    return dest_path






def getSECZips():
    COMPANYFACTS = "https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip"
    SUBMISSIONS  = "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip"

    dl_dir = os.getenv("SEC_DL_DIR", "data/sec")
    cf_path = os.path.join(dl_dir, "companyfacts.zip")
    sub_path = os.path.join(dl_dir, "submissions.zip")

    print("Downloading companyfacts...")
    download_zip(COMPANYFACTS, cf_path)
    print("Downloading submissions...")
    download_zip(SUBMISSIONS,  sub_path)
    print('finished downloading')

    return {
        'status' : 200,
        'cf_path' : cf_path,
        'sub_path': sub_path
    }
    # print('counting...')

    # count = 0
    # def _count_handler(name, jsonObj):
    #     nonlocal count
    #     count += 1
    #     if count % 1000 == 0:
    #         print(name)

    # stream_parse_zip_json(cf_path, handler=_count_handler)
    # print(f"companyfacts JSON files seen: {count}")

if __name__ == "__main__":
    getSECZips()
    