import nsefin
import pandas as pd
from datetime import datetime, timedelta
import time

nse = nsefin.NSEClient()

def fetch_trade_list():
    all_days_data = []
    # Aaj se peeche 45 din tak scan karenge (taaki 30 trading days mil jayein)
    curr = datetime.now() - timedelta(days=45)
    end_date = datetime.now()
    
    print("🚀 Monday Watchlist ke liye scan shuru ho raha hai...")

    while curr <= end_date:
        if curr.weekday() < 5: # Saturday/Sunday skip
            try:
                # Raw data fetch karein
                raw_df = nse.get_fno_bhav_copy(curr)
                if raw_df is not None and not raw_df.empty:
                    # Saare column names ko uppercase karein aur extra spaces hatayein
                    raw_df.columns = [str(c).upper().strip() for c in raw_df.columns]
                    
                    # 1. Sirf Futures stocks nikaalein (Options ke thousands rows delete)
                    # Library mein column ka naam 'EXPIRY' ya 'INSTRUMENT' ho sakta hai
                    if 'EXPIRY' in raw_df.columns:
                        # Futures mein Strike Price nahi hota, isse filter karein
                        df = raw_df[raw_df['STRIKE'] == 0].copy()
                    else:
                        df = raw_df.copy()

                    # 2. Watchlist Logic (Price > 2% Up AND OI > 5% Down) - Short Covering
                    # Ya (Price > 2% Down AND OI > 5% Up) - Fresh Short
                    p_col = next((c for c in df.columns if 'PCHANGE' in c or 'P_CHANGE' in c), None)
                    oi_col = next((c for c in df.columns if 'CHANGEOI' in c or 'OI_CHG' in c or 'COI' in c), None)
                    sym_col = 'SYMBOL'

                    if p_col and oi_col:
                        watchlist = df[((df[p_col].abs() > 2) & (df[oi_col].abs() > 5))].copy()
                        
                        if not watchlist.empty:
                            watchlist['DATE'] = curr.strftime("%d-%m-%Y")
                            watchlist['ACTION'] = watchlist[p_col].apply(lambda x: 'BUY WATCHLIST' if x > 0 else 'SELL WATCHLIST')
                            
                            # Sirf wahi dikhayenge jo trading ke liye zaroori hai
                            all_days_data.append(watchlist[['DATE', sym_col, 'CLOSE', p_col, oi_col, 'ACTION']])
                            print(f"✅ {curr.strftime('%d-%m-%Y')}: {len(watchlist)} stocks mile")
                
                time.sleep(1) # NSE server safety
            except:
                pass
        curr += timedelta(days=1)

    # FINAL FILE SAVE
    if all_days_data:
        final_report = pd.concat(all_days_data)
        final_report.to_csv("watchlist_30_days.csv", index=False)
        print("💾 SUCCESS! Aapki watchlist_30_days.csv taiyar hai.")
    else:
        # Agar koi stock filter na ho, toh blank file na bhejein
        pd.DataFrame(columns=['DATE','SYMBOL','CLOSE','PCHANGE','OI_CHANGE','ACTION']).to_csv("watchlist_30_days.csv", index=False)
        print("⚠️ No stocks matched criteria.")

if __name__ == "__main__":
    fetch_trade_list()
