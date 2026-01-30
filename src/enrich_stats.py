import pandas as pd
import yfinance as yf
import time
import os

# Configuration
DATA_LOC = "data/"
INPUT_FILE = f"{DATA_LOC}project_base_data.csv"
OUTPUT_FILE = f"{DATA_LOC}final_stats_project.csv"

def enrich_with_market_data():
    if not os.path.exists(INPUT_FILE):
        print(f"❌ Error: '{INPUT_FILE}' not found.")
        return

    print(f"📂 Loading {INPUT_FILE}...")
    df_input = pd.read_csv(INPUT_FILE)
    
    # We will collect enriched rows here instead of modifying df in-place
    enriched_data = []
    
    total_rows = len(df_input)
    print(f"🚀 Starting enrichment for {total_rows} tickers...")

    # Use enumerate() to get a guaranteed integer counter for the print statement
    for counter, row in enumerate(df_input.to_dict('records'), start=1):
        
        # Start with the existing data
        new_row = row.copy()
        
        # Initialize defaults
        new_row['price'] = pd.NA
        new_row['sector'] = "Unknown"
        new_row['beta'] = pd.NA
        new_row['dividend_yield'] = 0.0

        ticker = str(new_row.get('ticker', '')).strip()
        
        if "." in ticker:
            ticker = ticker.replace(".", "-")

        try:
            # Fetch data
            stock = yf.Ticker(ticker)
            info = stock.info
            
            # Extract
            current_price = info.get('currentPrice') or info.get('regularMarketPrice')
            sector = info.get('sector')
            beta = info.get('beta')
            div_yield = info.get('dividendYield', 0)

            # Assign to dict (Standard Python, no Pandas typing issues)
            if current_price:
                new_row['price'] = float(current_price)
            if sector:
                new_row['sector'] = sector
            if beta:
                new_row['beta'] = float(beta)
            if div_yield:
                new_row['dividend_yield'] = float(div_yield)

            print(f"   [{counter}/{total_rows}] ✅ {ticker}: ${current_price} | {sector}")

        except Exception as e:
            print(f"   [{counter}/{total_rows}] ⚠️ Failed {ticker}: {e}")
        
        # Append the completed dictionary to our list
        enriched_data.append(new_row)
        
        time.sleep(0.1)

    print("\n🔄 Reassembling DataFrame...")
    
    # Create the final DataFrame all at once (Much faster)
    df_final = pd.DataFrame(enriched_data)

    # Calculate P/E
    df_final['price'] = pd.to_numeric(df_final['price'], errors='coerce')
    df_final['eps'] = pd.to_numeric(df_final['eps'], errors='coerce')
    df_final['pe_ratio'] = df_final['price'] / df_final['eps']

    # Cleanup
    initial_count = len(df_final)
    df_clean = df_final.dropna(subset=['price', 'sector', 'pe_ratio'])
    
    df_clean.to_csv(OUTPUT_FILE, index=False)
    
    print("-" * 30)
    print(f"✅ DONE! Enriched data saved to: {OUTPUT_FILE}")
    print(f"   Original Rows: {initial_count}")
    print(f"   Final Rows:    {len(df_clean)}")
    print("-" * 30)

if __name__ == "__main__":
    enrich_with_market_data()