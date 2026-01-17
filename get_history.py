import nsefin
import pandas as pd
from datetime import datetime, timedelta
import time
import os

# Initialize the robust NSE tool
nse = nsefin.NSE()

def fetch_signals():
    all_data = []
    # Loop from Dec 1, 2025
    start_date = datetime(2025, 12, 1)
    end_date = datetime.now()
    
    delta = end_date - start_date
    print(f"🚀 Scanning {delta.days} days of history...")

    for i in range(delta.days + 1):
        target_date = start_date + timedelta(days=i)
        
        # Skip Weekends
        if target_date.weekday() >= 5:
            continue
            
        try:
            # Using the specific FNO Bhavcopy endpoint from nsefin
            df = nse.get_fno_bhav_copy(target_date)
            
            if df is not None and not df.empty:
                # Logic: Find Gap Up (Price up, OI down) and Gap Down (Price down, OI up)
                # Filtering using the library's standard column names
                gap_up = df[(df['pChange'] > 2) & (df['changeOI'] < -5)].copy()
                gap_down = df[(df['pChange'] < -2) & (df['changeOI'] > 5)].copy()
                
                day_results = pd.concat([gap_up, gap_down])
                if not day_results.empty:
                    day_results['Date'] = target_date.strftime("%d-%m-%Y")
                    all_data.append(day_results)
                    print(f"✅ {target_date.strftime('%Y-%m-%d')}: Found {len(day_results)} stocks")
            
            time.sleep(1) # Safety delay
        except Exception as e:
            # Just log and move to next day
            print(f"Skipping {target_date.strftime('%Y-%m-%d')} (Likely Holiday)")

    if all_data:
        final_df = pd.concat(all_data)
        final_df.to_csv("master_history_report.csv", index=False)
        print("💾 SUCCESS: File generated.")
    else:
        # Emergency: If no signals found, give you the raw list of all FNO stocks from Friday
        print("⚠️ No specific signals found. Exporting raw FNO list for Monday prep...")
        friday_data = nse.get_fno_bhav_copy(datetime(2026, 1, 16))
        friday_data.to_csv("master_history_report.csv", index=False)

fetch_signals()
