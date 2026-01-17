import nsefin
import pandas as pd
from datetime import datetime, timedelta
import time

nse = nsefin.NSEClient()

def classify_signal(price_pct, oi_pct):
    if price_pct > 1.5 and oi_pct > 3:
        return "LONG_BUILDUP"
    if price_pct < -1.5 and oi_pct > 3:
        return "SHORT_BUILDUP"
    if price_pct > 1.5 and oi_pct < -3:
        return "SHORT_COVERING"
    if price_pct < -1.5 and oi_pct < -3:
        return "LONG_UNWINDING"
    return None

def confidence_score(price_pct, oi_pct):
    return min(100, round(abs(price_pct) * 12 + abs(oi_pct) * 6))

def run_history_scan():
    rows = []
    end = datetime.now()
    start = end - timedelta(days=45)

    curr = start
    while curr <= end:
        if curr.weekday() < 5:
            try:
                df = nse.get_fno_bhav_copy(curr)
                if df is None or df.empty:
                    curr += timedelta(days=1)
                    continue

                df.columns = df.columns.str.strip()
                df = df[df['INSTRUMENT'].isin(['FUTSTK', 'FUTIDX'])]

                df['PRICE_PCT'] = ((df['CLOSE'] - df['OPEN']) / df['OPEN']) * 100
                prev_oi = df['OPEN_INT'] - df['CHG_IN_OI']
                df = df[prev_oi != 0]
                df['OI_PCT'] = (df['CHG_IN_OI'] / prev_oi) * 100

                for _, r in df.iterrows():
                    signal = classify_signal(r['PRICE_PCT'], r['OI_PCT'])
                    if not signal:
                        continue

                    rows.append({
                        "DATE": curr.strftime("%d-%m-%Y"),
                        "SYMBOL": r['SYMBOL'],
                        "LAST_PRICE": round(r['CLOSE'], 2),
                        "PRICE_CHANGE_PCT": round(r['PRICE_PCT'], 2),
                        "OI_CHANGE_PCT": round(r['OI_PCT'], 2),
                        "SIGNAL_TYPE": signal,
                        "CONFIDENCE_SCORE": confidence_score(r['PRICE_PCT'], r['OI_PCT'])
                    })

                time.sleep(1)

            except Exception as e:
                print(f"{curr.date()} → {e}")

        curr += timedelta(days=1)

    pd.DataFrame(rows).to_csv("watchlist_30_days.csv", index=False)
    print("✅ watchlist_30_days.csv generated")

if __name__ == "__main__":
    run_history_scan()
