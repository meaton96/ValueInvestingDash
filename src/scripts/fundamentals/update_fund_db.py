import psycopg
from zipfile import ZipFile
import json
import pandas as pd
import zipfile
from contextlib import contextmanager


fund_master_df = pd.DataFrame()

@contextmanager
def open_zip(path: str):
    zf = zipfile.ZipFile(path, "r")
    try:
        yield zf
    finally:
        zf.close()

skipCount = 0
def stream_parse_zip_json(securities_df: pd.DataFrame, zip_path: str, json_suffix=".json", handler=None):
    """
    Iterate files inside ZIP without extracting.
    Call handler(name: str, parsed_json: Any) per JSON file.
    """
    if handler is None:
        handler = lambda name, obj: None
    global skipCount
    with open_zip(zip_path) as zf:
        for name in zf.namelist():
            if not name.endswith(json_suffix):
                continue

            cik = name.split('.')[0][3:]
            if not int(cik) in securities_df['cik'].values:
                skipCount += 1
                continue
            with zf.open(name) as fp:
                # Most SEC JSON is newline-delimited or big JSON. Try both.
                raw = fp.read()
                try:
                    obj = json.loads(raw)
                    handler(name, obj)
                except json.JSONDecodeError:
                    # fallback: NDJSON
                    for line in raw.splitlines():
                        if not line.strip():
                            continue
                        handler(name, json.loads(line))

count = 0

def handleDfInsert(name: str, jsonObj: str):
    global count
    count += 1
    return

def upsertFundamentals(cf_path : str, sub_path: str, securities_df: pd.DataFrame):

   
    stream_parse_zip_json(
        securities_df=securities_df,
        zip_path=cf_path,
        handler=handleDfInsert)
    print(f'looked at: {count} json files')
    print(f'skipped {skipCount} json files')
    return

# build data frame from XRLB data
# parse each json for info and make rows
# upsert to db

# def upsert_companyfacts_from_zip(zip_path: str, conn_str: str):
#     with psycopg.connect(conn_str, autocommit=True) as conn, ZipFile(zip_path) as zf:
#         with conn.cursor() as cur:
#             for name in zf.namelist():
#                 if not name.endswith(".json"): 
#                     continue
#                 with zf.open(name) as fp:
#                     data = json.load(fp)
