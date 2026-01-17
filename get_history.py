from nsepython import *
import pandas as pd
from datetime import datetime, timedelta

# This will get data for the last 5 trading days
for i in range(1, 6):
    # Format the date for NSE (e.g., 16-01-2026)
    date_to_check = (datetime.now() - timedelta(days=i)).strftime("%d-%m-%Y")
    try:
        df = nse_fno_bhavcopy(date_to_check)
        df.to_csv(f"history_{date_to_check}.csv", index=False)
        print(f"✅ Saved data for {date_to_check}")
    except:
        print(f"❌ No market data for {date_to_check} (Market likely closed)")
