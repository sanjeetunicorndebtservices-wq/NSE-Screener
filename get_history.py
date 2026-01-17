import nsefin
import pandas as pd
from datetime import datetime, timedelta
import time

nse = nsefin.NSEClient()

def get_col(df, keywords):
    """Finds the actual column name from a list of possible keywords"""
    for col in df.columns:
        if any(key in col.upper() for key in keywords):
            return col
    return None

def fetch_trade_list():
    all_days_data = []
    end_date = datetime.now()
    start_date = end_date - timedelta(days=45) 
    
    print(f"🔍 Scanning Last 30 Trading Days...")

    curr = start_date
    while curr <= end_date:
        if curr.weekday() < 5: 
            try:
                raw_df = nse.get_fno_bhav_copy(curr)
                if raw_df is not None and not raw_df.empty:
                    # Step 1: Normalize columns (remove spaces)
                    raw_df.columns = [c.strip() for c in raw_df.columns]
                    
                    # Step 2: Dynamically find columns
                    inst_col = get_col(raw_df, ['INSTRUMENT', 'INST'])
                    sym_col = get_col(raw_df, ['SYMBOL', 'SYM'])
                    p_col = get_col(raw_df, ['PCHANGE', 'P_CHANGE', 'PCT_CHG'])
                    oi_col = get_col(raw_df, ['CHANGEOI', 'CHANGE_OI', 'CHG_OI'])
                    last_col = get_col(raw_df, ['LAST', 'CLOSE', 'LTP'])

                    if inst_col:
                        # Step 3: Filter for Futures only
                        df = raw_df[raw_df[inst_col].isin(['FUTSTK', 'FUTIDX'])].copy()
                        
                        if p_col and oi_col:
                            # Final Logic: Gap Up (Price > 2%, OI < -5%) OR Gap Down (Price < -2%, OI > 5%)
                            watchlist = df[((df[p_col] > 2) & (df[oi_col] < -5)) | 
                                           ((df[p_col] < -2) & (df[oi_col] > 5))].copy()
                            
                            if not watchlist.empty:
                                watchlist['DATE'] = curr.strftime("%d-%m-%Y")
                                watchlist['ACTION'] = watchlist[p_col].apply(lambda x: 'BUY' if x > 0 else 'SELL')
                                
                                # Keep only clean data
                                result = watchlist[['DATE', sym_col, last_col, p_col, oi_col, 'ACTION']]
                                all_days_data.append(result)
                                print(f"✅ {curr.date()}: {len(watchlist)} stocks filtered")
                time.sleep(1)
            except Exception as e:
                pass 
        curr += timedelta(days=1)

    if all_days_data:
        final_report = pd.concat(all_days_data)
        final_report.to_csv("watchlist_30_days.csv", index=False)
        print("💾 Success! watchlist_30_days.csv created.")
    else:
        # Emergency: Ensure the file is never empty
        pd.DataFrame(columns=['DATE', 'SYMBOL', 'LAST', 'PCHANGE', 'OI_CHANGE', 'ACTION']).to_csv("watchlist_30_days.csv", index=False)
        print("⚠️ No signals found. Empty file with headers created.")

if __name__ == "__main__":
    fetch_trade_list()
