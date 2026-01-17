import nsefin
import pandas as pd
from datetime import datetime, timedelta
import time

nse = nsefin.NSEClient()

def fetch_trade_list():
    all_days_data = []
    end_date = datetime.now()
    start_date = end_date - timedelta(days=45)

    print("🔍 Scanning last 30 trading days (F&O)...")

    curr = start_date
    while curr <= end_date:
        if curr.weekday() < 5:  # Skip weekends
            try:
                df = nse.get_fno_bhav_copy(curr)

                if df is None or df.empty:
                    curr += timedelta(days=1)
                    continue

                # Clean column names
                df.columns = df.columns.str.strip()

                # Futures only
                df = df[df['INSTRUMENT'].isin(['FUTSTK', 'FUTIDX'])].copy()

                # Required columns
                required = ['SYMBOL', 'OPEN', 'CLOSE', 'OPEN_INT', 'CHG_IN_OI']
                if not all(col in df.columns for col in required):
                    print(f"⚠️ {curr.date()} → Missing required columns")
                    curr += timedelta(days=1)
                    continue

                # Price % change
                df['PRICE_PCT'] = ((df['CLOSE'] - df['OPEN']) / df['OPEN']) * 100

                # OI % change (safe)
                prev_oi = df['OPEN_INT'] - df['CHG_IN_OI']
                df = df[prev_oi != 0]
                df['OI_PCT'] = (df['CHG_IN_OI'] / prev_oi) * 100

                # 🔥 BTST / STBT Logic
                watchlist = df[
                    ((df['PRICE_PCT'] > 2) & (df['OI_PCT'] < -5)) |
                    ((df['PRICE_PCT'] < -2) & (df['OI_PCT'] > 5))
                ].copy()

                if not watchlist.empty:
                    watchlist['DATE'] = curr.strftime("%d-%m-%Y")
                    watchlist['ACTION'] = watchlist['PRICE_PCT'].apply(
                        lambda x: 'BUY' if x > 0 else 'SELL'
                    )

                    all_days_data.append(
                        watchlist[['DATE', 'SYMBOL', 'CLOSE', 'PRICE_PCT', 'OI_PCT', 'ACTION']]
                    )

                    print(f"✅ {curr.date()} → {len(watchlist)} stocks")

                time.sleep(1)  # NSE rate safety

            except Exception as e:
                print(f"❌ {curr.date()} → {e}")

        curr += timedelta(days=1)

    # 🔒 ALWAYS create CSV (critical for GitHub Actions)
    if all_days_data:
        report = pd.concat(all_days_data)
    else:
        report = pd.DataFrame(
            columns=['DATE', 'SYMBOL', 'CLOSE', 'PRICE_PCT', 'OI_PCT', 'ACTION']
        )

    report.to_csv("watchlist_30_days.csv", index=False)
    print(f"💾 watchlist_30_days.csv created | Rows: {len(report)}")

if __name__ == "__main__":
    fetch_trade_list()
