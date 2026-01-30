# scripts/setup_db.py
from sqlalchemy import create_engine, text
import os, traceback
from dotenv import load_dotenv
from datetime import date
import pandas as pd
from math import ceil
import time
from etl.sql_scripts.securities import *
from etl.sql_scripts.logs import *
from etl.scripts.utilities.upsert import dataframe_upsert
from typing import Tuple
from datetime import datetime



load_dotenv()
DB_URI = os.getenv("DB_URI") 
ADVISORY_LOCK_KEY = os.getenv('ADVISORY_LOCK_KEY')

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

def _clean_cik_column(df: pd.DataFrame) -> Tuple[pd.DataFrame, str]:
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
    note: str = ""
    if bad_mask.any():
       # bad_rows = df.loc[bad_mask, ["cik"] + [c for c in df.columns if c != "cik"]].head(10)
        note = f"{bad_mask.sum()} rows with invalid CIK."
        print()
        df = df.loc[~bad_mask]

    # Convert to Python int so the driver binds as integer, not float
    df["cik"] = df["cik"].astype("Int64").astype(object).apply(lambda x: int(x))

    return df, note

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

        
def acquire_lock(conn) -> bool:
    got = conn.execute(
        text("select pg_try_advisory_lock(:k)"),
        {"k": ADVISORY_LOCK_KEY}
    ).scalar()
    return bool(got)

def release_lock(conn) -> None:
    conn.execute(
        text("select pg_advisory_unlock(:k)"),
        {"k": ADVISORY_LOCK_KEY}
    )

def update_log(error_msg: str,
               t0: datetime,
               t1: datetime,
               status: str,
               notes: str,
               conn):
    conn.execute(
        text(LOG_UPLOAD_ALCH),
        {
            "pipeline_name": "securities_loader",
            "time_start": t0,
            "time_end": t1,
            "status": status,
            "errors": (error_msg or None),
            "notes": (notes or None),
        },
    )
    print("posted daily log")

def db_update(df_in: pd.DataFrame) -> int:
    t0 = datetime.now()
    today = date.today()
    

    # Read CSV; avoid dtype guessing for CIK
    df_raw = df_in  

    # Clean and validate
    df, notes = _clean_cik_column(df_raw)
    df = _coerce_required_strings(df)
    df = _trim_to_limits(df)

    # Add dates
    df["first_seen"] = today
    df["last_seen"]  = today

    with engine.begin() as conn:
        if not acquire_lock(conn):
            print("Another run is holding the lock; exiting.")
            update_log(
            error_msg="Another run is holding the lock",
            t0=t0,
            t1=datetime.now(),
            conn=conn,
            status='409',
            notes="pipeline did not run"
            )

            return 409
        try:
            ensure_schema(conn)
            dataframe_upsert(conn, df, upsertSQL=UPSERT_SQL, chunk_size=1000)
            

        finally:
            t1 = datetime.now()

            update_log(
                    error_msg="",
                    t0=t0,
                    t1=t1,
                    conn=conn,
                    status='ok',
                    notes=notes,
                    )
            release_lock(conn)

    


    return 200

