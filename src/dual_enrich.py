import pandas as pd
import yfinance as yf
import time
import os
from datetime import datetime

# Configuration
DATA_LOC = "data/"
# Maps the era file to the last trading day of that "Recovery" period
ERA_CONFIGS = {
    "crisis_era_base.csv": "2011-12-30",
    "modern_era_base.csv": "2022-12-30"
}

def enrich_era_files():
    if not os.path.exists(DATA_LOC):
        os.makedirs(DATA_LOC)

    for input_filename, target_date in ERA_CONFIGS.items():
        input_path = os.path.join(DATA_LOC, input_filename)
        output_path = os.path.join(DATA_LOC, f"enriched_{input_filename}")
        
        if not os.path.exists(input_path):
            print(f"❌ Skipping {input_filename}: File not found.")
            continue

        print(f"\n🚀 Processing {input_filename} for date {target_date}...")
        df_input = pd.read_csv(input_path)
        enriched_data = []

        for counter, row in enumerate(df_input.to_dict('records'), start=1):
            new_row = row.copy()
            ticker = str(new_row.get('ticker', '')).strip().replace(".", "-")
            
            # Initialize Defaults
            new_row['hist_price'] = pd.NA
            new_row['sector'] = "Unknown"
            new_row['beta'] = pd.NA
            new_row['dividend_yield'] = 0.0

            try:
                # 1. Get Unadjusted Historical Price
                # We pull a 3-day window to ensure we catch a trading day
                end_dt = datetime.strptime(target_date, '%Y-%m-%d')
                start_dt = end_dt.replace(day=end_dt.day - 3)
                
                hist = yf.download(ticker, start=start_dt, end=end_dt, 
                                   auto_adjust=False, progress=False)
                
                if isinstance(hist, pd.DataFrame) and not hist.empty:
                    # Use 'Close' (raw) instead of 'Adj Close' to match historical EPS
                    new_row['hist_price'] = float(hist['Close'].values[-1])
                # 2. Get Metadata (Sector/Beta/Yield)
                stock = yf.Ticker(ticker)
                info = stock.info
                new_row['sector'] = info.get('sector', 'Unknown')
                new_row['beta'] = info.get('beta')
                new_row['dividend_yield'] = info.get('dividendYield', 0)

                print(f"   [{counter}/{len(df_input)}] ✅ {ticker}: ${new_row['hist_price']:.2f} | {new_row['sector']}")

            except Exception as e:
                print(f"   [{counter}/{len(df_input)}] ⚠️ Failed {ticker}: {e}")
            
            enriched_data.append(new_row)
            time.sleep(0.2) # Avoid rate limits

        # Finalize Era DataFrame
        df_final = pd.DataFrame(enriched_data)
        
        # Calculate Metrics
        df_final['hist_price'] = pd.to_numeric(df_final['hist_price'], errors='coerce')
        df_final['eps'] = pd.to_numeric(df_final['eps'], errors='coerce')
        df_final['pe_ratio'] = df_final['hist_price'] / df_final['eps']

        # Clean and Save
        df_clean = df_final.dropna(subset=['hist_price', 'pe_ratio'])
        df_clean.to_csv(output_path, index=False)
        
        print(f"--- Era Complete: {len(df_clean)} valid rows saved to {output_path} ---")

if __name__ == "__main__":
    enrich_era_files()