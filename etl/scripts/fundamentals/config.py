from dotenv import load_dotenv
import os
from typing import Dict, List


load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL", "") 
SEC_DL_DIR = os.getenv("SEC_DL_DIR", "data/fundamentals/")


# Tune this based on RAM and DB throughput
CHUNK_ROWS = 100_000

FUND_COLS = [
    "cik", "accession_no", "fiscal_year", "fiscal_period",
    "tag", "value", "unit", "frame", "filing_date", "source_file"
]

# Canonical metrics we care about and acceptable tag synonyms.
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