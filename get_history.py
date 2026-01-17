from nsepython import *
import pandas as pd
from datetime import datetime, timedelta
import os

# Create a list to store data
final_list = []

# We will check only the last 5 trading days to make it fast and guaranteed
for i in range(1, 10):
    date_str = (datetime.now() - timedelta(days=i)).strftime("%d-%m-%Y")
    try:
        df = nse_fno_bhavcopy(date_str)
        if df is not None and not df.empty:
            # RELAXED LOGIC: Just to make sure we get a file today!
            # We take any stock where Price moved more than 1%
            signals = df[df['pChange'].abs() > 1].copy()
            signals['Date'] = date_str
            final_list.append(signals[['Date', 'symbol', 'lastPrice', 'pChange', 'changeOI']])
            print(f"✅ Found data for {date_str}")
    except:
        continue

# SAVE THE FILE
if final_list:
    pd.concat(final_list).to_csv("master_history_report.csv", index=False)
    print("💾 File created successfully!")
else:
    # EMERGENCY FILE: So the zip is never empty
    pd.DataFrame({"Status": ["No Market Data Found"]}).to_csv("master_history_report.csv", index=False)
