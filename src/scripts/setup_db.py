# scripts/setup_db.py
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
from datetime import date
import pandas as pd
from math import ceil
from src.db_scripts.securities import *

load_dotenv()
DB_URI = os.getenv("DB_URI") 
if not DB_URI:
    raise ValueError("No DB_URI found in environment variables (DB_URI).")

engine = create_engine(DB_URI, pool_pre_ping=True, pool_recycle=1800)



def ensure_schema(conn):
    conn.execute(text(CREATE_SQL))

def _trim_to_limits(df: pd.DataFrame) -> pd.DataFrame:
    limits = {"ticker": 7, "name": 40, "exchange": 15, "company_name": 50, "symbol_yf": 7}
    for col, lim in limits.items():
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.slice(0, lim)
    return df

def _clean_cik_column(df: pd.DataFrame) -> pd.DataFrame:
    # Treat as string first to avoid float nonsense from CSV like "1374310.0"
    if "cik" not in df.columns:
        raise ValueError("CSV missing required column 'cik'")

    s = df["cik"].astype(str).str.strip()

    # Drop trailing ".0" produced by spreadsheets/CSVs and remove non-digits
    s = s.str.replace(r"\.0$", "", regex=True)
    s = s.str.replace(r"\D", "", regex=True)

    # Empty strings become NaN
    s = s.replace("", pd.NA)
    cik = pd.to_numeric(s, errors="coerce")  # pandas nullable Int64 after astype later

    df = df.copy()
    df["cik"] = cik

    # Range filter for sanity (CIK max 10 digits)
    bad_mask = df["cik"].isna() | (df["cik"] < 1) | (df["cik"] > 9_999_999_999)
    if bad_mask.any():
        bad_rows = df.loc[bad_mask, ["cik"] + [c for c in df.columns if c != "cik"]].head(10)
        print(f"⚠️ Skipping {bad_mask.sum()} rows with invalid CIK. First few:\n{bad_rows}")
        df = df.loc[~bad_mask]

    # Convert to Python int so the driver binds as integer, not float
    df["cik"] = df["cik"].astype("Int64").astype(object).apply(lambda x: int(x))

    return df

def _coerce_required_strings(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={
        'symbol':'ticker',
        'security_name':'name'
    })
    required = ["ticker", "name", "exchange", "company_name", "symbol_yf"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"CSV missing required columns: {missing}")
    for c in required:
        df[c] = df[c].astype(str).str.strip()
    return df

def upsert_chunk(conn, records):
    if records:
        conn.execute(text(UPSERT_SQL), records)

def dataframe_upsert(conn, df: pd.DataFrame, chunk_size: int = 2000):
    records = df.to_dict(orient="records")
    n = len(records)
    for start in range(0, n, chunk_size):
        upsert_chunk(conn, records[start:start + chunk_size])

if __name__ == "__main__":
    try:
        today = date.today()
        csv_path = f"data/listings/security_master_{today.strftime('%Y-%m-%d')}.csv"

        # Read CSV; avoid dtype guessing for CIK
        df = pd.read_csv(csv_path, dtype={"cik": "string"})  # other cols infer is fine

        # Clean and validate
        df = _clean_cik_column(df)
        df = _coerce_required_strings(df)
        df = _trim_to_limits(df)

        # Add dates
        df["first_seen"] = today
        df["last_seen"]  = today

        with engine.begin() as conn:
            conn.execute(text("select 1"))
            ensure_schema(conn)
            dataframe_upsert(conn, df, chunk_size=1000)

        print(f"✅ Upserted {len(df)} rows into securities for {today.isoformat()}.")
    except Exception as e:
        print(f"❌ Failed: {e}")
