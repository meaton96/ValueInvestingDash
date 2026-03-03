import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
DB_URI = os.getenv("DB_URI", "")

# 2. Refined SQL Query with Year Filtering
def get_export_query(year_list):
    years_str = ",".join(map(str, year_list))
    return f"""
    WITH filtered_fundamentals AS (
        -- Filter for specific years and clean tags first
        SELECT 
            cik, tag, value, fiscal_year,
            ROW_NUMBER() OVER (PARTITION BY cik, tag ORDER BY fiscal_year DESC) as rn
        FROM fundamentals_raw
        WHERE fiscal_year IN ({years_str})
          AND tag IN ('StockholdersEquity', 'NetIncomeLoss', 'EarningsPerShareDiluted', 'LiabilitiesCurrent')
          AND value IS NOT NULL
    ),
    pivoted AS (
        SELECT 
            cik,
            MAX(CASE WHEN tag = 'NetIncomeLoss' THEN value END) as net_income,
            MAX(CASE WHEN tag = 'StockholdersEquity' THEN value END) as equity,
            MAX(CASE WHEN tag = 'EarningsPerShareDiluted' THEN value END) as eps,
            MAX(CASE WHEN tag = 'LiabilitiesCurrent' THEN value END) as liabilities
        FROM filtered_fundamentals
        WHERE rn = 1
        GROUP BY cik
    )
    SELECT 
        s.symbol_yf as ticker,
        s.company_name,
        p.net_income,
        p.equity,
        p.eps,
        p.liabilities,
        CASE WHEN p.equity > 0 THEN p.liabilities / p.equity ELSE NULL END as debt_to_equity,
        {year_list[-1]} as era_year -- Tags the data with the final year in the range
    FROM pivoted p
    JOIN securities s ON p.cik = s.cik
    WHERE s.symbol_yf IS NOT NULL 
      AND p.net_income IS NOT NULL
      AND p.eps IS NOT NULL
    ORDER BY p.net_income DESC
    LIMIT 150;
    """

def export_era_data():
    engine = create_engine(DB_URI)
    
    # Define our two time periods
    eras = {
        "crisis_era": [2009, 2010],
        "modern_era": [2021, 2022]
    }
    
    for era_name, years in eras.items():
        print(f"--- Exporting {era_name} ({years}) ---")
        query = get_export_query(years)
        
        try:
            df = pd.read_sql(text(query), engine.connect())
            output_file = f"data/{era_name}_base.csv"
            df.to_csv(output_file, index=False)
            print(f"Saved {len(df)} rows to {output_file}")
        except Exception as e:
            print(f"Error exporting {era_name}: {e}")

if __name__ == "__main__":
    export_era_data()