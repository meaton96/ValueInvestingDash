DDL_RAW = """
CREATE TABLE IF NOT EXISTS fundamentals_raw (
  cik           BIGINT       NOT NULL,
  accession_no  TEXT         NOT NULL,
  fiscal_year   INT,
  fiscal_period TEXT,
  tag           TEXT         NOT NULL,
  value         NUMERIC,
  unit          TEXT,
  frame         TEXT,
  filing_date   DATE         NOT NULL,
  first_seen    DATE         NOT NULL DEFAULT CURRENT_DATE,
  last_seen     DATE         NOT NULL DEFAULT CURRENT_DATE,
  source_file   TEXT,
  PRIMARY KEY (cik, accession_no, tag, frame)
);
"""

DDL_STAGING = """
CREATE TABLE IF NOT EXISTS staging_fundamentals (
  cik           BIGINT,
  accession_no  TEXT,
  fiscal_year   INT,
  fiscal_period TEXT,
  tag           TEXT,
  value         NUMERIC,
  unit          TEXT,
  frame         TEXT,
  filing_date   DATE,
  source_file   TEXT
);
"""

UPSERT_FROM_STAGING = """
INSERT INTO fundamentals_raw
  (cik, accession_no, fiscal_year, fiscal_period,
   tag, value, unit, frame, filing_date, source_file)
SELECT s.cik, s.accession_no, s.fiscal_year, s.fiscal_period,
       s.tag, s.value, s.unit,
       COALESCE(s.frame, '__NOFRAME__') AS frame,
       s.filing_date, s.source_file
FROM staging_fundamentals s
ON CONFLICT (cik, accession_no, tag, frame) DO NOTHING;
"""

TRUNCATE_STAGING = "TRUNCATE staging_fundamentals;"

LEDGER_SELECT = """
SELECT source_kind, natural_key, asset_path, byte_size, crc32, sha256, last_modified, etag, processed_at, status
FROM etl_source_ledger
WHERE source_kind = %s AND natural_key = %s
"""

LEDGER_UPSERT = """
INSERT INTO etl_source_ledger
  (source_kind, natural_key, asset_path, byte_size, crc32, sha256, last_modified, etag, processed_at, status)
VALUES
  (%s, %s, %s, %s, %s, %s, %s, %s, now(), %s)
ON CONFLICT (source_kind, natural_key) DO UPDATE
SET asset_path    = EXCLUDED.asset_path,
    byte_size     = EXCLUDED.byte_size,
    crc32         = EXCLUDED.crc32,
    sha256        = EXCLUDED.sha256,
    last_modified = EXCLUDED.last_modified,
    etag          = EXCLUDED.etag,
    processed_at  = now(),
    status        = EXCLUDED.status
"""