-- Fundamentals staging table
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

-- Historical, append-only-ish, deduped by filing identity
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

CREATE INDEX IF NOT EXISTS idx_fundamentals_raw_cik_tag ON fundamentals_raw (cik, tag);
CREATE INDEX IF NOT EXISTS idx_fundamentals_raw_filing_date ON fundamentals_raw (filing_date);


-- Rebuild as needed after batches, or convert to a materialized view
CREATE TABLE IF NOT EXISTS fundamentals_latest (
  cik          BIGINT NOT NULL,
  tag          TEXT   NOT NULL,
  frame        TEXT,
  unit         TEXT,
  value        NUMERIC,
  filing_date  DATE   NOT NULL,
  accession_no TEXT   NOT NULL,
  PRIMARY KEY (cik, tag, frame)
);

-- Upsert from raw after a batch:
CREATE OR REPLACE FUNCTION refresh_fundamentals_latest() RETURNS VOID AS $$
BEGIN
  CREATE TEMP TABLE _latest AS
  SELECT DISTINCT ON (cik, tag, frame)
         cik, tag, frame, unit, value, filing_date, accession_no
  FROM fundamentals_raw
  ORDER BY cik, tag, frame, filing_date DESC, accession_no DESC;

  INSERT INTO fundamentals_latest AS fl (cik, tag, frame, unit, value, filing_date, accession_no)
  SELECT * FROM _latest
  ON CONFLICT (cik, tag, frame) DO UPDATE
    SET unit = EXCLUDED.unit,
        value = EXCLUDED.value,
        filing_date = EXCLUDED.filing_date,
        accession_no = EXCLUDED.accession_no;

  DROP TABLE _latest;
END;
$$ LANGUAGE plpgsql;


-- One row per source artifact we might parse (e.g., a CIK JSON file)
CREATE TABLE IF NOT EXISTS etl_source_ledger (
  source_kind     TEXT        NOT NULL,   -- 'companyfacts', 'submissions', etc.
  natural_key     TEXT        NOT NULL,   -- e.g., CIK as text: '0000320193'
  asset_path      TEXT        NOT NULL,   -- zip member path or URL-ish identifier
  byte_size       BIGINT,                 -- uncompressed size
  crc32           INTEGER,                -- from ZipInfo if using zip
  sha256          CHAR(64),               -- optional
  last_modified   TIMESTAMPTZ,            -- zip datetime or HTTP Last-Modified
  etag            TEXT,                   -- HTTP ETag if available
  processed_at    TIMESTAMPTZ,            -- when we last parsed it
  status          TEXT        NOT NULL DEFAULT 'ok',  -- 'ok' | 'skipped' | 'error'
  PRIMARY KEY (source_kind, natural_key)
);

CREATE INDEX IF NOT EXISTS idx_etl_source_ledger_status ON etl_source_ledger (status);





CREATE TABLE etl_logs (
    id SERIAL PRIMARY KEY,
    pipeline_name TEXT NOT NULL,
    date DATE NOT NULL DEFAULT CURRENT_DATE,
    time_start TIMESTAMP NOT NULL,
    time_end TIMESTAMP NOT NULL,
    status VARCHAR(10),
    errors TEXT,
    notes TEXT
);

CREATE INDEX idx_etl_logs_pipeline_date ON etl_logs (pipeline_name, date);
