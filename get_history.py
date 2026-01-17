from nsepython import *
import pandas as pd
from datetime import datetime, timedelta
import time

# START: Dec 1, 2025 | END: Today
start_date = datetime(2025, 12, 1)
end_date = datetime.now()
current_date = start_date

final_results = []

print("📊 STARTING MASTER SCAN (DEC 2025 - JAN 2026)")

while current_date <= end_date:
    # Skip weekends
    if current_date.weekday() < 5:
        date_str = current_date.strftime("%d-%m-%Y")
        try:
            # Using a more robust fetch method
            data = nse_fno_bhavcopy(date_str)
            if data is not None and not data.empty:
                # GAP UP LOGIC: Price Up > 2%, OI Down < -5%
                ups = data[(data['pChange'] > 2) & (data['changeOI'] < -5)].copy()
                ups['Signal'] = 'GAP_UP_CANDIDATE'
                
                # GAP DOWN LOGIC: Price Down < -2%, OI Up > 5%
                downs = data[(data['pChange'] < -2) & (data['changeOI'] > 5)].copy()
                downs['Signal'] = 'GAP_DOWN_CANDIDATE'
                
                combined = pd.concat([ups, downs])
                if not combined.empty:
                    combined['Date'] = date_str
                    final_results.append(combined[['Date', 'symbol', 'lastPrice', 'pChange', 'changeOI', 'Signal']])
                    print(f"✅ {date_str}: Found {len(combined)} stocks")
            time.sleep(0.5) # Avoid NSE blocking
        except:
            pass
    current_date += timedelta(days=1)

# FINAL SAVE
if final_results:
    pd.concat(final_results).to_csv("master_report.csv", index=False)
    print("💾 DONE! master_report.csv is ready.")
else:
    # Create file with headers so it's not empty
    df = pd.DataFrame(columns=['Date', 'symbol', 'lastPrice', 'pChange', 'changeOI', 'Signal'])
    df.to_csv("master_report.csv", index=False)
    print("❌ No signals found in this period.")
