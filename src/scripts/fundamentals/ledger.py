from datetime import datetime, timezone
from typing import Optional, Dict, Any
import psycopg
from psycopg import sql
import zipfile
from psycopg.rows import tuple_row
from src.sql_scripts.fundamentals import LEDGER_SELECT, LEDGER_UPSERT


def zip_member_meta(zf: zipfile.ZipFile, name: str) -> Dict[str, Any]:
    zi = zf.getinfo(name)
    # Zip stores naive local time tuple; treat as naive then force UTC for consistency
    dt = datetime(*zi.date_time)
    last_modified = dt.replace(tzinfo=timezone.utc)
    return {
        "asset_path": name,
        "byte_size": zi.file_size,
        "crc32": zi.CRC,
        "sha256": None,     # skip hashing the whole blob; CRC+size is plenty for this
        "last_modified": last_modified,
        "etag": None,
    }

def ledger_get(conn: psycopg.Connection, source_kind: str, natural_key: str) -> Optional[dict]:
    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute(LEDGER_SELECT, (source_kind, natural_key))
        row = cur.fetchone()
    if not row:
        return None
    cols = ["source_kind","natural_key","asset_path","byte_size","crc32","sha256","last_modified","etag","processed_at","status"]
    return dict(zip(cols, row))

def ledger_upsert(conn: psycopg.Connection, source_kind: str, natural_key: str, meta: Dict[str, Any], status: str):
    with conn.cursor() as cur:
        cur.execute(
            LEDGER_UPSERT,
            (
                source_kind,
                natural_key,
                meta.get("asset_path"),
                meta.get("byte_size"),
                meta.get("crc32"),
                meta.get("sha256"),
                meta.get("last_modified"),
                meta.get("etag"),
                status,
            ),
        )
    conn.commit()

def should_parse(current: Dict[str, Any], prior: Optional[Dict[str, Any]]) -> bool:
    if prior is None:
        return True
    # If any meaningful fingerprint changed, reparse
    keys = ("asset_path", "byte_size", "crc32", "last_modified")
    return any(current.get(k) != prior.get(k) for k in keys)


def ledger_bulk_get(conn: psycopg.Connection, source_kind: str, natural_keys: list[str]) -> dict[str, dict]:
    if not natural_keys:
        return {}
    placeholders = sql.SQL(",").join(sql.Literal(k) for k in natural_keys)
    q = sql.SQL("""
        SELECT source_kind, natural_key, asset_path, byte_size, crc32, sha256, last_modified, etag, processed_at, status
        FROM etl_source_ledger
        WHERE source_kind = {sk} AND natural_key IN ({keys})
    """).format(sk=sql.Literal(source_kind), keys=placeholders)
    out = {}
    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute(q)
        cols = ["source_kind","natural_key","asset_path","byte_size","crc32","sha256","last_modified","etag","processed_at","status"]
        for row in cur.fetchall():
            d = dict(zip(cols, row))
            out[d["natural_key"]] = d
    return out

def ledger_bulk_upsert(conn: psycopg.Connection, source_kind: str, metas: list[dict], status: str):
    if not metas:
        return
    with conn.cursor() as cur:
        cur.executemany(
            LEDGER_UPSERT,
            [
                (
                    source_kind,
                    m["natural_key"],
                    m["asset_path"],
                    m["byte_size"],
                    m["crc32"],     # BIGINT now, right?
                    m.get("sha256"),
                    m["last_modified"],
                    m.get("etag"),
                    status,
                )
                for m in metas
            ]
        )