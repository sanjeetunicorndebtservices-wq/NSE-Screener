from nsepython import *
import pandas as pd
from datetime import datetime, timedelta

backtest_results = []
days_found = 0
check_day = 0

print("🚀 Starting Professional 30-Day Backtest...")

# We loop back 45 days to ensure we find at least 30 trading days
while days_found < 30 and check_day < 45:
    date_str = (datetime.now() - timedelta(days=check_day)).strftime("%d-%m-%Y")
    
    try:
        # Fetch the bhavcopy for this specific date
        df = nse_fno_bhavcopy(date_str)
        
        if not df.empty:
            # Logic: Short Covering (Price up > 2%, OI down > 5%)
            signals = df[(df['pChange'] > 2) & (df['changeOI'] < -5)]
            
            for _, row in signals.iterrows():
                backtest_results.append({
                    'Trading_Date': date_str,
                    'Symbol': row['symbol'],
                    'Price_Change_%': row['pChange'],
                    'OI_Change_%': row['changeOI'],
                    'Close_Price': row['lastPrice']
                })
            
            print(f"✅ Day {days_found + 1}: Found data for {date_str}")
            days_found += 1
    except:
        # If it fails (weekend/holiday), just skip it
        pass
    
    check_day += 1

# Save the final report
if backtest_results:
    report = pd.DataFrame(backtest_results)
    report.to_csv("accuracy_report_30days.csv", index=False)
    print(f"✨ SUCCESS: Generated report with {len(backtest_results)} signals.")
else:
    print("❌ Critical Error: No records found even in history.")
