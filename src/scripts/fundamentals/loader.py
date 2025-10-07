import os
import csv
import time
import tempfile
from typing import Dict, List, Tuple, Iterable
from io import StringIO, BytesIO
import pandas as pd
import psycopg
from psycopg import sql
from psycopg.rows import tuple_row

from src.scripts.utilities.zip import open_zip
from src.sql_scripts.fundamentals import *
from src.scripts.fundamentals.config import FUND_COLS, DATABASE_URL, TAG_MAP, CHUNK_ROWS,SEC_DL_DIR
from src.scripts.fundamentals.json import extract_rows_from_json



def ensure_tables(conn: psycopg.Connection):
    with conn.cursor() as cur:
        cur.execute(DDL_RAW)
        cur.execute(DDL_STAGING)
    conn.commit()


def copy_rows_to_staging(conn: psycopg.Connection, rows: Iterable[Tuple]):
    # Build CSV in memory
    buf = StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerows(rows)
    data = buf.getvalue().encode()

    # Compose a typed COPY statement
    cols = sql.SQL(", ").join(sql.Identifier(c) for c in FUND_COLS)
    copy_stmt = sql.SQL("COPY staging_fundamentals ({cols}) FROM STDIN WITH (FORMAT csv)").format(
        cols=cols
    )

    with conn.cursor() as cur:
        with cur.copy(copy_stmt) as cp:
            cp.write(data)

    conn.commit()


def upsert_from_staging(conn: psycopg.Connection):
    with conn.cursor() as cur:
        cur.execute(UPSERT_FROM_STAGING)
        cur.execute(TRUNCATE_STAGING)
    conn.commit()

# -----------------------------
# ZIP streaming with chunked DB writes
# -----------------------------

def stream_parse_zip_json(
    conn: psycopg.Connection,
    zip_path: str,
    valid_ciks: set[int],
    stop_early: int = 0,
):
    row_buffer: List[Tuple] = []
    looked = skipped = matched = 0

    with open_zip(zip_path) as zf:
        for name in zf.namelist():
            # Only interested in files like 'CIK0000320193.json'
            if not name.endswith(".json") or not name.startswith("CIK"):
                continue

            looked += 1
            if looked % 200 == 0:
                print(f"Scanned {looked} files... matched {matched}, skipped {skipped}")

            try:
                cik_str = name.split(".")[0][3:]
                cik_int = int(cik_str)
            except Exception:
                skipped += 1
                continue

            if cik_int not in valid_ciks:
                skipped += 1
                if stop_early and (looked >= stop_early):
                    break
                continue

            # matched; parse and buffer
            matched += 1
            with zf.open(name) as fp:
                raw = fp.read()
                rows = extract_rows_from_json(cik_int, raw, source_file=name, TAG_MAP=TAG_MAP)
                if not rows:
                    continue

                # filter out rows without filing_date; if you prefer, allow NULL and coalesce later
                filtered = [
                    r for r in rows
                    if r[8] is not None  # filing_date position in FUND_COLS
                ]
                row_buffer.extend(filtered)

                if len(row_buffer) >= CHUNK_ROWS:
                    copy_rows_to_staging(conn, row_buffer)
                    upsert_from_staging(conn)
                    row_buffer.clear()

            if stop_early and (matched >= stop_early):
                break

    # flush tail
    if row_buffer:
        copy_rows_to_staging(conn, row_buffer)
        upsert_from_staging(conn)
        row_buffer.clear()

    print(f"Done. Looked: {looked}, matched: {matched}, skipped: {skipped}")



def upsert_fundamentals(companyfacts_zip: str, securities_df: pd.DataFrame, stop_early: int = 0) -> float:
    # Pre-build a set for O(1) membership tests
    valid_ciks = set(int(x) for x in securities_df["cik"].dropna().astype("int64").tolist())

    t0 = time.perf_counter()
    with psycopg.connect(DATABASE_URL, autocommit=False) as conn:
        ensure_tables(conn)
        stream_parse_zip_json(conn, companyfacts_zip, valid_ciks, stop_early=stop_early)
    return time.perf_counter() - t0


if __name__ == "__main__":
    # 1) for daily runner: pass in today's securities_df
    # 2) for local dev, read a temp CSV produced earlier
    df = pd.read_csv("data/temp/temp_sec_table.csv")

    elapsed = upsert_fundamentals(
        os.path.join(SEC_DL_DIR, "companyfacts.zip"),
        df,
        stop_early=100  # or set to an integer to limit matched CIKs processed
    )
    print(f"ETL completed in {elapsed:.2f}s")