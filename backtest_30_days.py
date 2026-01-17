from nsepython import *
import pandas as pd
from datetime import datetime, timedelta
import os

backtest_results = []
days_to_collect = 30
current_day = 0
found_days = 0

print(f"📂 Current Directory: {os.getcwd()}")

# Loop back up to 45 days to find 30 trading days
while found_days < days_to_collect and current_day < 45:
    date_str = (datetime.now() - timedelta(days=current_day)).strftime("%d-%m-%Y")
    try:
        df = nse_fno_bhavcopy(date_str)
        if not df.empty:
            # Your logic: Price up > 2% and OI down > 5%
            signals = df[(df['pChange'] > 2) & (df['changeOI'] < -5)]
            for _, row in signals.iterrows():
                backtest_results.append({
                    'Date': date_str,
                    'Symbol': row['symbol'],
                    'Price_Change': row['pChange'],
                    'OI_Change': row['changeOI'],
                    'Price': row['lastPrice']
                })
            found_days += 1
            print(f"✅ {date_str}: Found {len(signals)} signals")
    except:
        pass
    current_day += 1

# Force save to the current folder
filename = "accuracy_report_30days.csv"
if backtest_results:
    df_final = pd.DataFrame(backtest_results)
    df_final.to_csv(filename, index=False)
    print(f"💾 SUCCESS: Saved {len(backtest_results)} rows to {filename}")
else:
    # Create a dummy file so the ZIP is never empty
    with open(filename, "w") as f:
        f.write("Date,Symbol,Price\nNo data found,None,0")
    print("⚠️ No signals found, created empty report.")
