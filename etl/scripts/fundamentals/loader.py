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

from etl.scripts.utilities.zip import open_zip
from etl.sql_scripts.fundamentals import *
from etl.sql_scripts.logs import LOG_UPLOAD_PG
from etl.scripts.fundamentals.config import FUND_COLS, DATABASE_URL, TAG_MAP, CHUNK_ROWS,SEC_DL_DIR
from etl.scripts.fundamentals.json import extract_rows_from_json
from etl.scripts.fundamentals.ledger import *


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

def stream_parse_zip_json(conn: psycopg.Connection, 
                          zip_path: str, 
                          valid_ciks: set[int], 
                          stop_early: int = 0) -> int:
    source_kind = "companyfacts"
    # 0) build meta list for all valid CIK members
    metas = []
    with open_zip(zip_path) as zf:
        for name in zf.namelist():
            if not name.endswith(".json") or not name.startswith("CIK"):
                continue
            cik_str = name.split(".")[0][3:]
            try:
                cik_int = int(cik_str)
            except Exception:
                continue
            if cik_int not in valid_ciks:
                continue
            zi = zf.getinfo(name)
            # zip info → meta
            lm = datetime(*zi.date_time).replace(tzinfo=timezone.utc)
            metas.append({
                "natural_key": cik_str,
                "asset_path": name,
                "byte_size": zi.file_size,
                "crc32": zi.CRC,
                "sha256": None,
                "last_modified": lm,
                "etag": None,
            })
            if stop_early and len(metas) >= stop_early:
                break

    if not metas:
        print("No matching CIKs found.")
        return 0

    # 1) one round-trip to fetch prior ledger state
    prior = ledger_bulk_get(conn, source_kind, [m["natural_key"] for m in metas])

    # 2) decide which changed
    def is_changed(m, p):
        if p is None: return True
        return (
            m["asset_path"] != p.get("asset_path") or
            m["byte_size"]  != p.get("byte_size")  or
            m["crc32"]      != p.get("crc32")      or
            m["last_modified"] != p.get("last_modified")
        )

    changed = [m for m in metas if is_changed(m, prior.get(m["natural_key"]))]
    unchanged = len(metas) - len(changed)

    print(f"Candidates: {len(metas)} | Changed: {len(changed)} | Unchanged: {unchanged}")

    # 3) parse only changed; stage + upsert in chunks
    row_buffer: list[Tuple] = []
    filing_date_idx = FUND_COLS.index("filing_date")

    with open_zip(zip_path) as zf:
        for m in changed:
            name = m["asset_path"]
            cik_int = int(m["natural_key"])
            with zf.open(name) as fp:
                raw = fp.read()
            rows = extract_rows_from_json(cik_int, raw, source_file=name, TAG_MAP=TAG_MAP)
            rows = [r for r in rows if r[filing_date_idx] is not None]
            if rows:
                row_buffer.extend(rows)
            if len(row_buffer) >= CHUNK_ROWS:
                copy_rows_to_staging(conn, row_buffer)
                upsert_from_staging(conn)
                row_buffer.clear()

    if row_buffer:
        copy_rows_to_staging(conn, row_buffer)
        upsert_from_staging(conn)
        row_buffer.clear()

    # 4) one batch upsert to ledger for changed only; single commit after
    ledger_bulk_upsert(conn, source_kind, changed, status="ok")
    conn.commit()

    print(f"Loaded {len(changed)} changed CIKs; skipped parsing {unchanged}.")

    return len(changed)




def upsert_fundamentals(companyfacts_zip: str, securities_df: pd.DataFrame, stop_early: int = 0) -> float:
    # Pre-build a set for O(1) membership tests
    valid_ciks = set(int(x) for x in securities_df["cik"].dropna().astype("int64").tolist())
    t0_dt = datetime.now()
    t0 = time.perf_counter()
    with psycopg.connect(DATABASE_URL, autocommit=False) as conn:
        ensure_tables(conn)
        changed = stream_parse_zip_json(conn, companyfacts_zip, valid_ciks, stop_early=stop_early)
        with conn.cursor() as cur:
            cur.execute(
                LOG_UPLOAD_PG,
                {
                    "pipeline_name": "fundamentals_loader",
                    "time_start": t0_dt,
                    "time_end": datetime.now(),
                    "status": "ok",
                    "errors": None,
                    "notes": f"{changed} records changed"
                }
            )

    return time.perf_counter() - t0


if __name__ == "__main__":
    # 1) for daily runner: pass in today's securities_df
    # 2) for local dev, read a temp CSV produced earlier
    df = pd.read_csv("data/temp/temp_sec_table.csv")

    elapsed = upsert_fundamentals(
        os.path.join(SEC_DL_DIR, "companyfacts.zip"),
        df,
        stop_early=1000  # or set to an integer to limit matched CIKs processed
    )
    print(f"ETL completed in {elapsed:.2f}s")