from nsepython import *
import pandas as pd
from datetime import datetime, timedelta
import os

results = []
days_collected = 0
days_back = 0

print("🔍 Searching for the last 30 trading days...")

# Loop until we have 30 days of data (skipping weekends/holidays)
while days_collected < 30 and days_back < 60:
    date_to_check = (datetime.now() - timedelta(days=days_back)).strftime("%d-%m-%Y")
    try:
        df = nse_fno_bhavcopy(date_to_check)
        if not df.empty:
            # Logic: Short Covering (Price Up > 2%, OI Down < -5%)
            # This identifies stocks where sellers are panicking
            signals = df[(df['pChange'] > 2) & (df['changeOI'] < -5)]
            
            for _, row in signals.iterrows():
                results.append({
                    'Date': date_to_check,
                    'Symbol': row['symbol'],
                    'Price_Change_%': row['pChange'],
                    'OI_Change_%': row['changeOI'],
                    'Closing_Price': row['lastPrice']
                })
            days_collected += 1
            print(f"✅ {date_to_check}: Found {len(signals)} stocks")
    except:
        pass # Skip if it's a weekend or holiday
    days_back += 1

# Create the file even if 0 results (though history will have data)
final_df = pd.DataFrame(results)
final_df.to_csv("accuracy_report_30days.csv", index=False)
print(f"💾 Saved {len(results)} records to accuracy_report_30days.csv")
