import psycopg
from zipfile import ZipFile
import json

def upsert_companyfacts_from_zip(zip_path: str, conn_str: str):
    with psycopg.connect(conn_str, autocommit=True) as conn, ZipFile(zip_path) as zf:
        with conn.cursor() as cur:
            for name in zf.namelist():
                if not name.endswith(".json"): 
                    continue
                with zf.open(name) as fp:
                    data = json.load(fp)