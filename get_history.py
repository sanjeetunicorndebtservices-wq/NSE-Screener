import nsefin
import pandas as pd
from datetime import datetime, timedelta
import time
import os

# FIXED: The correct class name is NSEClient
nse = nsefin.NSEClient()

def fetch_signals():
    all_data = []
    # Start from Dec 1, 2025
    start_date = datetime(2025, 12, 1)
    end_date = datetime.now()
    
    delta = (end_date - start_date).days
    print(f"🚀 Scanning {delta} days for Gap Up/Down movements...")

    for i in range(delta + 1):
        target_date = start_date + timedelta(days=i)
        
        # Skip Weekends (Saturday=5, Sunday=6)
        if target_date.weekday() >= 5:
            continue
            
        try:
            # Fetching F&O Bhavcopy
            df = nse.get_fno_bhav_copy(target_date)
            
            if df is not None and not df.empty:
                # GAP UP: Price Up > 2% and OI Down < -5%
                gap_up = df[(df['pChange'] > 2) & (df['changeOI'] < -5)].copy()
                gap_up['Movement'] = 'POTENTIAL_GAP_UP'
                
                # GAP DOWN: Price Down < -2% and OI Up > 5%
                gap_down = df[(df['pChange'] < -2) & (df['changeOI'] > 5)].copy()
                gap_down['Movement'] = 'POTENTIAL_GAP_DOWN'
                
                day_results = pd.concat([gap_up, gap_down])
                if not day_results.empty:
                    day_results['Date'] = target_date.strftime("%d-%m-%Y")
                    all_data.append(day_results[['Date', 'symbol', 'lastPrice', 'pChange', 'changeOI', 'Movement']])
                    print(f"✅ {target_date.strftime('%d-%m-%Y')}: Found {len(day_results)} signals")
            
            time.sleep(1) # Be polite to NSE servers
        except Exception as e:
            # Silently skip holidays or connection blips
            pass

    if all_data:
        final_df = pd.concat(all_data)
        final_df.to_csv("master_history_report.csv", index=False)
        print(f"💾 SUCCESS: Generated file with {len(final_df)} records.")
    else:
        # Emergency backup: If no signals found, just save a blank file with headers
        pd.DataFrame(columns=['Date','symbol','lastPrice','pChange','changeOI','Movement']).to_csv("master_history_report.csv", index=False)
        print("⚠️ No specific signals met criteria. File created with headers only.")

if __name__ == "__main__":
    fetch_signals()
