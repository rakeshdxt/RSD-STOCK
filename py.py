import yfinance as yf
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import os
from datetime import datetime

# --- CONFIGURATION ---
MARKET_CAP_LIMIT = 1000 * 10**7 
ROE_LIMIT = 0.15                
MASTER_LIST = "List_Securities.csv" 
OUTPUT_FILE = "xbrl_fundamental_records.csv"
STATE_FILE = "last_index.txt"
BATCH_SIZE = 50                 # Process 50 stocks per run

def get_last_index():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            return int(f.read().strip())
    return 0

def save_last_index(index):
    with open(STATE_FILE, "w") as f:
        f.write(str(index))

def get_metrics(ticker_symbol):
    try:
        t = yf.Ticker(ticker_symbol)
        info = t.info
        return {"m_cap": info.get('marketCap', 0), "roe": info.get('returnOnEquity', 0)}
    except:
        return {"m_cap": 0, "roe": 0}

def fetch_xbrl_profit(scrip):
    url = f"https://www.bseindia.com/stock-share-price/xbrl/getxbrlfile.aspx?scripcode={scrip}"
    try:
        r = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=15)
        if r.status_code == 200 and len(r.content) > 1000:
            soup = BeautifulSoup(r.content, 'xml')
            return soup.find(['ProfitLossForPeriod', 'NetProfit']).text if soup.find(['ProfitLossForPeriod', 'NetProfit']) else "0"
    except:
        return None

def main():
    df_master = pd.read_csv(MASTER_LIST)
    all_codes = df_master.to_dict('records')
    
    start = get_last_index()
    # Reset if we reached the end of the file
    if start >= len(all_codes):
        start = 0
    
    end = start + BATCH_SIZE
    batch = all_codes[start:end]
    
    results = []
    print(f"--- Starting Batch: {start} to {min(end, len(all_codes))} ---")

    for i, row in enumerate(batch):
        scrip_code = row['Scrip Code']
        ticker = f"{str(row['Scrip Id']).strip()}.BO"

        metrics = get_metrics(ticker)
        if metrics['m_cap'] >= MARKET_CAP_LIMIT and metrics['roe'] >= ROE_LIMIT:
            profit = fetch_xbrl_profit(scrip_code)
            if profit:
                results.append({
                    "Date": datetime.now().strftime("%Y-%m-%d"),
                    "Scrip": scrip_code,
                    "Symbol": ticker,
                    "ROE": f"{metrics['roe']*100:.2f}%",
                    "M-Cap_Cr": round(metrics['m_cap']/10**7, 2),
                    "Profit": profit
                })
                print(f"✅ Match: {ticker}")
        
        time.sleep(1) # Be gentle with servers

    # Update state and save
    save_last_index(end)
    if results:
        pd.DataFrame(results).to_csv(OUTPUT_FILE, mode='a', index=False, header=not os.path.exists(OUTPUT_FILE))
    
    print(f"--- Batch Finished. Next start index: {end} ---")

if __name__ == "__main__":
    main()
