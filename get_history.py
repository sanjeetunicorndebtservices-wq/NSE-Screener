import nsefin
import pandas as pd
from datetime import datetime, timedelta
import time

nse = nsefin.NSEClient()

def fetch_trade_list():
    all_days_data = []
    # Aaj se 30 trading days peeche jaayenge
    end_date = datetime.now()
    start_date = end_date - timedelta(days=45) 
    
    print(f"🔍 Filtering Stocks for Watchlist (Last 30 Days)...")

    curr = start_date
    while curr <= end_date:
        if curr.weekday() < 5: # No Weekends
            try:
                raw_df = nse.get_fno_bhav_copy(curr)
                if raw_df is not None and not raw_df.empty:
                    # Clean Column Names
                    raw_df.columns = [c.upper().strip() for c in raw_df.columns]
                    
                    # --- DHASU FILTER ---
                    # Sirf Futures stocks rakhein, Options ki thousands lines hata dein
                    df = raw_df[raw_df['INSTRUMENT'].isin(['FUTSTK', 'FUTIDX'])].copy()
                    
                    p_col = next((c for c in df.columns if 'PCHANGE' in c or 'P_CHANGE' in c), None)
                    oi_col = next((c for c in df.columns if 'CHANGEOI' in c or 'CHANGE_OI' in c), None)
                    
                    if p_col and oi_col:
                        # Logic for next day watchlist
                        watchlist = df[((df[p_col] > 2) & (df[oi_col] < -5)) | 
                                       ((df[p_col] < -2) & (df[oi_col] > 5))].copy()
                        
                        if not watchlist.empty:
                            watchlist['DATE'] = curr.strftime("%d-%m-%Y")
                            watchlist['ACTION'] = watchlist[p_col].apply(lambda x: 'BUY WATCHLIST' if x > 0 else 'SELL WATCHLIST')
                            
                            # Sirf wahi dikhayenge jo kaam ka hai
                            all_days_data.append(watchlist[['DATE', 'SYMBOL', 'LAST', p_col, oi_col, 'ACTION']])
                            print(f"✅ {curr.date()}: {len(watchlist)} stocks filtered")
                time.sleep(1)
            except:
                pass
        curr += timedelta(days=1)

    if all_days_data:
        final_report = pd.concat(all_days_data)
        final_report.to_csv("watchlist_30_days.csv", index=False)
        print("💾 File Ready: watchlist_30_days.csv")
    else:
        # Emergency: Agar koi signal na mile to poori list save kar dein
        print("⚠️ No signals found. Saving Friday's main stock list.")
        raw_df = nse.get_fno_bhav_copy(datetime(2026, 1, 16))
        raw_df.columns = [c.upper().strip() for c in raw_df.columns]
        raw_df[raw_df['INSTRUMENT'] == 'FUTSTK'].to_csv("watchlist_30_days.csv", index=False)

if __name__ == "__main__":
    fetch_trade_list()
