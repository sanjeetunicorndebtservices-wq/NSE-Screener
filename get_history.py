import nsefin
import pandas as pd
from datetime import datetime, timedelta
import time

# Initialize
nse = nsefin.NSEClient()

def get_clean_df(df):
    """Fixes column names automatically so logic never fails"""
    df.columns = [c.upper().replace(' ', '_').strip() for c in df.columns]
    return df

def fetch_history():
    all_signals = []
    # Start from Dec 1, 2025 to Jan 16, 2026 (Friday)
    start_date = datetime(2025, 12, 1)
    end_date = datetime(2026, 1, 17)
    
    print(f"📡 Scanning NSE Data from {start_date.date()}...")

    curr = start_date
    while curr <= end_date:
        if curr.weekday() < 5: # Skip Sat/Sun
            try:
                raw_df = nse.get_fno_bhav_copy(curr)
                if raw_df is not None and not raw_df.empty:
                    df = get_clean_df(raw_df)
                    
                    # Detect columns dynamically
                    p_col = next((c for c in df.columns if 'PCHANGE' in c or 'P_CHANGE' in c), None)
                    oi_col = next((c for c in df.columns if 'CHANGEOI' in c or 'CHANGE_OI' in c), None)
                    
                    if p_col and oi_col:
                        # Logic: Gap Up (Price > 2%, OI < -5%)
                        signals = df[(df[p_col] > 2) & (df[oi_col] < -5)].copy()
                        if not signals.empty:
                            signals['DATE'] = curr.strftime("%d-%m-%Y")
                            all_signals.append(signals)
                            print(f"✅ {curr.date()}: Found {len(signals)} stocks")
                time.sleep(1)
            except:
                pass
        curr += timedelta(days=1)

    if all_signals:
        pd.concat(all_signals).to_csv("master_history_report.csv", index=False)
        print("💾 File saved with signals!")
    else:
        # EMERGENCY: If no signals, just give us Friday's full list
        print("⚠️ No signals found. Saving Friday's raw data for manual scan.")
        friday_raw = nse.get_fno_bhav_copy(datetime(2026, 1, 16))
        get_clean_df(friday_raw).to_csv("master_history_report.csv", index=False)

if __name__ == "__main__":
    fetch_history()
