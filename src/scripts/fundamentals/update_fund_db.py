import pandas as pd
import zipfile
from contextlib import contextmanager
from typing import Dict, List, Tuple
import orjson as jsonlib
import time
import os
from dotenv import load_dotenv

# -----------------------------
# Configuration
# -----------------------------

FUND_COLS = [
    "cik", "accession_no", "fiscal_year", "fiscal_period",
    "tag", "value", "unit", "frame"
]
fund_master_df = pd.DataFrame(columns=FUND_COLS)
load_dotenv()

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
        elif suffix in ("MM", "MN"): # rarely seen variants
            v *= 1_000_000
        elif suffix in ("B", "BN"):
            v *= 1_000_000_000
        elif suffix in ("TH", "THS", "THOUSANDS"):
            v *= 1_000
        unit_norm = "USD"
    elif unit.lower() in ("shares", "shrs"):
        unit_norm = "shares"


    return v, unit_norm


@contextmanager
def open_zip(path: str):
    zf = zipfile.ZipFile(path, "r")
    try:
        yield zf
    finally:
        zf.close()

skipCount = 0
def stream_parse_zip_json(securities_df: pd.DataFrame, zip_path: str, json_suffix=".json", handler=None, stop_early = 0):
    """
    Iterate files inside ZIP without extracting.
    Call handler(name: str, raw_bytes: bytes) per JSON file.
    """
    if handler is None:
        handler = lambda name, raw: None
    global skipCount
    stop_early_count = 0
    with open_zip(zip_path) as zf:
        for name in zf.namelist():
            if (skipCount + count) % 100 == 0:
                print(f'file: {skipCount + count}')
            if not name.endswith(json_suffix):
                continue

            # Expect filenames like 'CIK0000320193.json'
            try:
                cik_str = name.split('.')[0][3:]
                cik_int = int(cik_str)
            except Exception:
                skipCount += 1
                continue

            if cik_int not in securities_df['cik'].values:
                skipCount += 1
                continue

            stop_early_count += 1
            if stop_early > 0 and stop_early_count >= stop_early:
                return

            with zf.open(name) as fp:
                raw = fp.read()
                # Pass raw bytes; let the handler/extractor decide how to parse
                handler(cik_int, raw)

count = 0

def handleDfInsert(cik_int: int, raw_or_obj):
    """Parse one JSON from the ZIP, extract rows, and append to global fund_master_df."""
    global count, fund_master_df
    count += 1

    
    rows = extract_rows_from_json(cik_int, raw_or_obj)
    if not rows:
        return

    df_add = pd.DataFrame(rows, columns=FUND_COLS)

    # Append to the global master frame
    if fund_master_df.empty:
        fund_master_df = df_add
    else:
        fund_master_df = pd.concat([fund_master_df, df_add], ignore_index=True)

def upsertFundamentals(cf_path : str, sub_path: str, securities_df: pd.DataFrame, stop_early = 0) -> float:

    start_time = time.perf_counter()
    stream_parse_zip_json(
        securities_df=securities_df,
        zip_path=cf_path,
        handler=handleDfInsert,
        stop_early=stop_early)
    print(f'looked at: {count} json files')
    print(f'skipped {skipCount} json files')
    print(fund_master_df.head())
   # print(fund_master_df['tag'].value_counts())
    print(f"rows collected: {len(fund_master_df)}")
    # print(fund_master_df.columns)

    return time.perf_counter() - start_time





def extract_rows_from_json(cik: int, buf_or_obj) -> List[Tuple]:
    """Return rows for fundamentals_raw from one companyfacts JSON, limited to TAG_MAP.
    Accepts either raw bytes (preferred) or a parsed dict."""
    # Parse only if needed
    if isinstance(buf_or_obj, (bytes, bytearray)):
        try:
            j = jsonlib.loads(buf_or_obj)
        except Exception:
            return []
    elif isinstance(buf_or_obj, dict):
        j = buf_or_obj
    else:
        return []

    facts = j.get("facts", {})
    if not isinstance(facts, dict):
        return []

    us_gaap = facts.get("us-gaap", {})
    if not isinstance(us_gaap, dict):
        return []

    rows: List[Tuple] = []

    # Pick first available tag by priority for each canonical metric
    for canon, candidates in TAG_MAP.items():
        tag_payload: Dict | None = None
        tag_found: str | None = None
        for t in candidates:
            tp = us_gaap.get(t)
            if isinstance(tp, dict):
                tag_payload = tp
                tag_found = t
                break
        if not tag_payload or not tag_found:
            continue

        units = tag_payload.get("units", {})
        if not isinstance(units, dict):
            continue

        for unit, entries in units.items():
            if not isinstance(entries, list):
                continue
            for e in entries:
                if not isinstance(e, dict):
                    continue
                # Pull core fields safely
                val_raw = e.get("val")
                accn = e.get("accn")
                fy = e.get("fy")
                fp = e.get("fp")
                frame = e.get("frame")
                if not accn:
                    # Without accession, deduping is messy; skip
                    continue

                val, unit_norm = normalize_value_unit(val_raw, unit if isinstance(unit, str) else str(unit))
                if val is None:
                    continue
                
                rows.append((cik, accn, fy, fp, tag_found, val, unit_norm, frame))

    return rows


if __name__ == '__main__':
    SEC_DL_DIR = os.getenv('SEC_DL_DIR')
    time_taken = upsertFundamentals(
        f'{SEC_DL_DIR}/companyfacts.zip',
        f'{SEC_DL_DIR}/submissions.zip',
        pd.read_csv('data/temp/temp_sec_table.csv'),
        100
    )

    print(f'upsert took: {time_taken:.4f} seconds')
    