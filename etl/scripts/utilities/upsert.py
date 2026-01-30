import pandas as pd
from sqlalchemy import text


def upsert_chunk(conn, records, upsertSQL):
    if records:
        conn.execute(text(upsertSQL), records)

def dataframe_upsert(conn, df: pd.DataFrame, upsertSQL, chunk_size: int = 2000):
    records = df.to_dict(orient="records")
    n = len(records)
    for start in range(0, n, chunk_size):
        upsert_chunk(conn, records[start:start + chunk_size], upsertSQL=upsertSQL)