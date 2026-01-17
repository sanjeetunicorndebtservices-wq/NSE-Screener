from nsepython import *
import pandas as pd
from datetime import datetime, timedelta
import time

# Start date fixed as Dec 1, 2025
start_date = datetime(2025, 12, 1)
end_date = datetime.now()
current_date = start_date

all_signals = []

print(f"🚀 Starting Scan from {start_date.strftime('%d-%m-%Y')} to Today...")

while current_date <= end_date:
    date_str = current_date.strftime("%d-%m-%Y")
    
    # Skip Weekends (Saturday=5, Sunday=6)
    if current_date.weekday() >= 5:
        current_date += timedelta(days=1)
        continue

    try:
        # Fetch FNO Bhavcopy
        df = nse_fno_bhavcopy(date_str)
        
        if df is not None and not df.empty:
            # --- SCANNER LOGIC ---
            # 1. Gap Up Candidate (Short Covering): Price Up > 2%, OI Down < -5%
            # 2. Gap Down Candidate (Aggressive Shorts): Price Down < -2%, OI Up > 5%
            
            gap_up = df[(df['pChange'] > 2) & (df['changeOI'] < -5)].copy()
            gap_up['Movement'] = 'Potential GAP UP'
            
            gap_down = df[(df['pChange'] < -2) & (df['changeOI'] > 5)].copy()
            gap_down['Movement'] = 'Potential GAP DOWN'
            
            day_signals = pd.concat([gap_up, gap_down])
            
            if not day_signals.empty:
                day_signals['Date'] = date_str
                # Keep only useful columns
                selected_data = day_signals[['Date', 'symbol', 'lastPrice', 'pChange', 'changeOI', 'Movement']]
                all_signals.append(selected_data)
                print(f"✅ {date_str}: Found {len(day_signals)} stocks")
        
        # Small sleep to avoid getting blocked by NSE
        time.sleep(1)
        
    except Exception as e:
        print(f"⚠️ {date_str}: Market Holiday or Error")
    
    current_date += timedelta(days=1)

# Save the final consolidated report
if all_signals:
    final_report = pd.concat(all_signals)
    final_report.to_csv("master_history_report.csv", index=False)
    print(f"💾 SUCCESS! master_history_report.csv saved with {len(final_report)} records.")
else:
    with open("master_history_report.csv", "w") as f:
        f.write("Date,Symbol,Price,P_Change,OI_Change,Movement\nNo_Data,None,0,0,0,None")
