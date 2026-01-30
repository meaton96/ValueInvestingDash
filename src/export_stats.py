import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# 1. Load Environment Variables
load_dotenv()
DB_URI = os.getenv("DB_URI", "")

if not DB_URI or len(DB_URI) < 1:
    raise ValueError("No DB_URI found in .env file.")

# 2. Define the SQL Query (The "Base Dataset" Logic)
# Note: I removed 'form_type' checks since your schema uses accession_no/source_file
EXPORT_QUERY = """
WITH latest_values AS (
    -- 1. Rank filings to find the absolute latest value for each tag per CIK
    SELECT 
        f.cik,
        f.tag,
        f.value,
        -- Rank by filing date (descending) so #1 is the newest data point
        ROW_NUMBER() OVER (PARTITION BY f.cik, f.tag ORDER BY f.filing_date DESC, f.fiscal_year DESC) as rn
    FROM fundamentals_raw f
    WHERE f.tag IN (
        'AssetsCurrent', 
        'StockholdersEquity', 
        'NetIncomeLoss', 
        'EarningsPerShareDiluted', 
        'LiabilitiesCurrent'
    )
),
pivoted AS (
    -- 2. Pivot from Long (Tags) to Wide (Columns)
    SELECT 
        cik,
        MAX(CASE WHEN tag = 'AssetsCurrent' THEN value END) as assets,
        MAX(CASE WHEN tag = 'StockholdersEquity' THEN value END) as equity,
        MAX(CASE WHEN tag = 'NetIncomeLoss' THEN value END) as net_income,
        MAX(CASE WHEN tag = 'EarningsPerShareDiluted' THEN value END) as eps,
        MAX(CASE WHEN tag = 'LiabilitiesCurrent' THEN value END) as liabilities
    FROM latest_values
    WHERE rn = 1 -- Only keep the #1 most recent value
    GROUP BY cik
)
-- 3. Join with Securities to get Tickers and finalize the "Base Dataset"
SELECT 
    s.symbol_yf as ticker,
    s.company_name,
    p.net_income,
    p.equity,
    p.eps,
    p.liabilities,
    -- Calculate Debt-to-Equity safely
    CASE WHEN p.equity > 0 THEN p.liabilities / p.equity ELSE NULL END as debt_to_equity
FROM pivoted p
JOIN securities s ON p.cik = s.cik
WHERE s.symbol_yf IS NOT NULL 
  AND p.assets IS NOT NULL
ORDER BY p.assets DESC
LIMIT 150;
"""

def export_stats_data():
    print("--- Starting Data Export ---")
    
    # Create the engine (using your existing config style)
    engine = create_engine(DB_URI)
    
    try:
        # Use Pandas to read SQL directly into a DataFrame
        # This automatically handles the connection open/close and cursor iteration
        print("Executing SQL Query against Neon DB...")
        df = pd.read_sql(text(EXPORT_QUERY), engine.connect())
        
        row_count = len(df)
        print(f"Query successful. Retrieved {row_count} rows.")

        if row_count > 0:
            # Save to CSV
            output_file = "project_base_data.csv"
            df.to_csv(f"data/{output_file}", index=False)
            print(f"✅ Success! Data saved to: {os.path.abspath(output_file)}")
            print("Preview of data:")
            print(df[['ticker', 'net_income', 'debt_to_equity']].head())
        else:
            print("⚠️ Warning: Query returned 0 rows. Check your fundamentals_raw table.")

    except Exception as e:
        print(f"❌ Error during export: {e}")

if __name__ == "__main__":
    export_stats_data()