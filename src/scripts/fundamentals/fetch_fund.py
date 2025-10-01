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

