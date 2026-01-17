from nsepython import *
import pandas as pd
from datetime import datetime, timedelta

# List to store our backtest findings
backtest_results = []

print("🚀 Starting Backtest for the last 30 days...")

for i in range(1, 40):  # Check 40 days to ensure we get 30 trading days
    date_str = (datetime.now() - timedelta(days=i)).strftime("%d-%m-%Y")
    
    try:
        # 1. Fetch data for that specific day
        df = nse_fno_bhavcopy(date_str)
        
        # 2. Apply your Scanner Logic: Price Up > 2% and OI Down (Short Covering)
        signals = df[(df['pChange'] > 2) & (df['changeOI'] < -5)]
        
        for _, row in signals.iterrows():
            backtest_results.append({
                'Date': date_str,
                'Symbol': row['symbol'],
                'Price_Change_%': row['pChange'],
                'OI_Change_%': row['changeOI'],
                'Close_Price': row['lastPrice']
            })
        print(f"✅ Processed: {date_str} | Found {len(signals)} stocks")
    except:
        continue # Skip weekends/holidays

# 3. Save the results to a CSV
report = pd.DataFrame(backtest_results)
report.to_csv("accuracy_report_30days.csv", index=False)
print("✨ Done! Download 'accuracy_report_30days.csv' for the full list.")
