import io
import os
import sys
import json
import gzip
import hashlib
from datetime import datetime, timezone
from typing import Tuple, Dict, cast
import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()



NASDAQ_NASDAQ_URL : str = cast(str, os.getenv('NAS_URL'))
NASDAQ_OTHER_URL :str  = cast(str, os.getenv('OTHER_URL'))
SEC_TICKERS_URL : str = cast(str, os.getenv('SEC_TICKERS'))
CONTACT_EMAIL : str = cast(str, os.getenv('CONTACT_EMAIL'))


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT_DIR = os.path.join(ROOT, "data", "listings")
os.makedirs(OUT_DIR, exist_ok=True)


def load_sec_company_tickers() -> pd.DataFrame:
    headers = {f"User-Agent": "ValueInvestingDash/0.1 ({CONTACT_EMAIL})"}


    return pd.DataFrame()


def _get(url: str) -> str:
    # fetch listings
    headers = {
        "User-Agent": "ValueInvestingDash/0.1 (+https://example.com; contact: you@example.com)"
    }
    for attempt in range(3):
        r = requests.get(url, headers=headers, timeout=30)
        if r.ok:
            return r.text
    r.raise_for_status()
    return ""

def _read_pipe_table(text: str) -> pd.DataFrame:
    # convert data from pipe delim text to data frame
    
    return pd.read_csv(
        io.StringIO(text),
        sep='|',
        engine='python',
        skipfooter=1,
        dtype=str
    )

def load_nasdaqlisted() -> pd.DataFrame:
    raw = _get(NASDAQ_NASDAQ_URL)
    df = _read_pipe_table(raw)


    # Expected columns include:
    # Symbol, Security Name, Market Category, Test Issue, Financial Status,
    # Round Lot Size, ETF, NextShares
    

    df = df.rename(columns=str.strip)

    out = pd.DataFrame({
        "symbol": df["Symbol"].str.strip(),
        "security_name": df["Security Name"].str.strip(),
        "exchange": "NASDAQ",
        "etf": _map_yn_bool(df.get("ETF")),
        "test_issue": _map_yn_bool(df.get("Test Issue")),
        "round_lot_size": pd.to_numeric(df.get("Round Lot Size", "100"), errors="coerce"),
        "market_category": cast(pd.Series, df.get("Market Category", "")).fillna("").str.strip(),
        "financial_status": cast(pd.Series, df.get("Financial Status", "")).fillna("").str.strip(),
        "nextshares": _map_yn_bool(df.get("NextShares")),
        "source": "nasdaqlisted",
    })

    return out

def load_otherlisted() -> pd.DataFrame:
    raw = _get(NASDAQ_OTHER_URL)
    df = _read_pipe_table(raw)
    df = df.rename(columns=str.strip)

    # Columns typically include:
    # ACT Symbol, Security Name, Exchange, CQS Symbol, ETF, Round Lot Size, Test Issue, NASDAQ Symbol
    # Exchange codes mapping from Nasdaq Trader docs:
    ex_map = {
        "A": "NYSE American",   # formerly AMEX
        "N": "NYSE",
        "P": "NYSE Arca",
        "Z": "Cboe BZX",        # sometimes Z appears; harmless to keep
        "B": "Cboe BYX",
        "C": "Cboe EDGA",
        "D": "Cboe EDGX",
        # If new codes appear, keep the raw code so you see it in output instead of silently breaking
    }

    out = pd.DataFrame({
        "symbol": df["ACT Symbol"].str.strip(),
        "security_name": df["Security Name"].str.strip(),
        "exchange": df["Exchange"].str.strip().map(ex_map).fillna(df["Exchange"].str.strip()),
        "etf": _map_yn_bool(df.get("ETF")),
        "test_issue": _map_yn_bool(df.get("Test Issue")),
        "round_lot_size": pd.to_numeric(df.get("Round Lot Size", "100"), errors="coerce"),
        # carry a few helpful raw columns
        "cqs_symbol": cast(pd.Series, df.get("CQS Symbol", "")).fillna("").str.strip(),
        "nasdaq_symbol": cast(pd.Series, df.get("NASDAQ Symbol", "")).fillna("").str.strip(),
        "source": "otherlisted",
    })
    return out

def build_security_master() -> pd.DataFrame:
    a = load_nasdaqlisted()
    b = load_otherlisted()

    # Some tickers appear in both feeds with slight differences.
    # Prefer NASDAQ feed for NASDAQ exchange, otherwise prefer otherlisted.
    # Strategy: concat, then keep first occurrence by ('symbol','exchange') priority.
    # Give nasdaqlisted rows a higher priority sort key.

    a['_prio'] = 0
    b['_prio'] = 1

    df = pd.concat([a, b], ignore_index=True)
    df = df.sort_values(by=['symbol', '_prio']).drop_duplicates(subset=['symbol'], keep='first')
    df = df.drop(columns=['_prio'])  # you forgot to assign

    

    df["symbol"] = df["symbol"].str.upper().str.strip()
    df = df[~df["symbol"].str.contains(r"[\s/]", na=False)]  # drop obvious junk

    # attempt to remove ADR
    bad_name_patterns = r"(?i)\bADR\b|\bAmerican Depositary\b|Depositary Share|Preference|Preferred"
    df = df[~df["security_name"].str.contains(bad_name_patterns, na=False)]

    
    df = df[df["test_issue"] == False]         # exclude test issues
    
    # remove etf
    df = df[df["etf"] == False]

    # remove units, rights, warrants
    junk = r"(?i)\bUnit(s)?\b|\bRight(s)?\b|\bWarrant(s)?\b|\bOrdinary Share(s)? \w+? Pref"
    df = df[~df["security_name"].str.contains(junk, na=False)]

    # check for weird ticker lengths
    too_long = ~df["symbol"].str.len().between(1, 7)  
    if too_long.any():
        print(f"Warning: {too_long.sum()} symbols have length outside 1..7; keeping them for now.")
       # print(too_long.head())

    # adjust for y finance grab
    df["symbol_yf"] = df["symbol"].str.replace(".", "-", regex=False)

    # Stable sort for reproducibility
    df = df.sort_values(["exchange", "symbol"]).reset_index(drop=True)

    assert not df["symbol"].duplicated().any(), "Duplicate symbols snuck in"

    return df

def snapshot_path(ts: datetime, ext: str = 'parquet') -> str:
    date_tag = ts.strftime('%Y-%m-%d')
    return os.path.join(OUT_DIR, f'security_master_{date_tag}.{ext}')

def write_snapshot(df: pd.DataFrame, ts:datetime) -> Tuple[str, str]:
    csv_path = snapshot_path(ts, 'csv')
    pq_path = snapshot_path(ts, 'parquet')

    df.to_csv(csv_path, index=False)
    df.to_parquet(pq_path, index=False)

    return csv_path, pq_path

def latest_previous_snapshot(now: datetime) -> str:
    files = sorted(p for p in os.listdir(OUT_DIR) if p.startswith("security_master_") and p.endswith(".parquet"))
    if not files:
        return ""
    # choose the most recent prior to today if multiple exist
    return os.path.join(OUT_DIR, files[-1])

def _map_yn_bool(series: pd.Series | None) -> pd.Series:
    # Robust and quiet: treat anything == 'Y' (case-insensitive) as True, else False
    if series is not None:
        s = series.astype("string")  # keeps NA as <NA>, not 'nan'
        return s.str.upper().eq("Y").fillna(False)
    return pd.Series()

def diff_snapshots(prev_path: str, curr_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    if not prev_path:
        return {"added": curr_df.copy(), "removed": pd.DataFrame(columns=curr_df.columns), "changed": pd.DataFrame()}

    prev = pd.read_parquet(prev_path)
    key = "symbol"

    # adds/removes
    added = curr_df[~curr_df[key].isin(prev[key])]
    removed = prev[~prev[key].isin(curr_df[key])]

    # changed rows: same symbol exists, but any non-key field differs
    merged = prev.merge(curr_df, on=key, how="inner", suffixes=("_prev", "_curr"))
    compare_cols = [c for c in curr_df.columns if c != key]
    changed_mask = pd.Series(False, index=merged.index, dtype=bool)
    for c in compare_cols:
        a = f"{c}_prev"
        b = f"{c}_curr"
        # compare as strings to avoid NaN nuisances
        changed_mask = changed_mask | (merged[a].astype(str).fillna("") != merged[b].astype(str).fillna(""))

    changed = merged.loc[changed_mask, [key] + sum(([f"{c}_prev", f"{c}_curr"] for c in compare_cols), [])]
    return {"added": added, "removed": removed, "changed": changed}

def main():
    now = datetime.now(timezone.utc)
    df = build_security_master()
    csv_path, pq_path = write_snapshot(df, now)

    prev_path = latest_previous_snapshot(now)
    diffs = diff_snapshots(prev_path if prev_path != pq_path else "", df)

    # Human-readable summary to stdout
    print(f"Snapshot written:\n  {csv_path}\n  {pq_path}")
    print(f"Counts: total={len(df)} etf={int(df['etf'].sum())} test_issues={int(df['test_issue'].sum())}")
    if prev_path and os.path.abspath(prev_path) != os.path.abspath(pq_path):
        print(f"\nCompared to previous: {os.path.basename(prev_path)}")
        print(f"  Added:   {len(diffs['added'])}")
        print(f"  Removed: {len(diffs['removed'])}")
        print(f"  Changed: {len(diffs['changed'])}")
        if len(diffs["added"]) > 0:
            print("  Sample additions:", ", ".join(diffs["added"]["symbol"].head(10).tolist()))
        if len(diffs["removed"]) > 0:
            print("  Sample removals: ", ", ".join(diffs["removed"]["symbol"].head(10).tolist()))

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Failed: {e}", file=sys.stderr)
        sys.exit(1)
