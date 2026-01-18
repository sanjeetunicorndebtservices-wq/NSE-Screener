import nsefin
import pandas as pd
from datetime import datetime, timedelta
import time

nse = nsefin.NSEClient()

COLUMNS = [
    "DATE",
    "SYMBOL",
    "LAST_PRICE",
    "PRICE_CHANGE_PCT",
    "OI_CHANGE_PCT",
    "SIGNAL_TYPE",
    "CONFIDENCE_SCORE"
]

# ---------- Institutional v2.1 Logic ----------

def classify_signal(price_pct, oi_pct):
    if price_pct > 0.8 and oi_pct > 1.5:
        return "LONG_BUILDUP"
    if price_pct < -0.8 and oi_pct > 1.5:
        return "SHORT_BUILDUP"
    if price_pct > 0.8 and oi_pct < -1.5:
        return "SHORT_COVERING"
    if price_pct < -0.8 and oi_pct < -1.5:
        return "LONG_UNWINDING"
    return None

def confidence_score(price_pct, oi_pct):
    return min(100, round(abs(price_pct) * 12 + abs(oi_pct) * 6))

# ---------- Core Scanner ----------

def scan_between(start_date, end_date):
    rows = []
    curr = start_date

    while curr <= end_date:
        if curr.weekday() < 5:  # Trading days only
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
                    sig = classify_signal(r['PRICE_PCT'], r['OI_PCT'])
                    if not sig:
                        continue

                    rows.append({
                        "DATE": curr.strftime("%d-%m-%Y"),
                        "SYMBOL": r['SYMBOL'],
                        "LAST_PRICE": round(r['CLOSE'], 2),
                        "PRICE_CHANGE_PCT": round(r['PRICE_PCT'], 2),
                        "OI_CHANGE_PCT": round(r['OI_PCT'], 2),
                        "SIGNAL_TYPE": sig,
                        "CONFIDENCE_SCORE": confidence_score(r['PRICE_PCT'], r['OI_PCT'])
                    })

                time.sleep(1)

            except Exception as e:
                print(curr.date(), e)

        curr += timedelta(days=1)

    return pd.DataFrame(rows, columns=COLUMNS)

# ---------- RUN SCANS ----------

if __name__ == "__main__":

    # 🔹 1. From 01-12-2025 till today
    start_hist = datetime(2025, 12, 1)
    today = datetime.now()

    hist_df = scan_between(start_hist, today)
    hist_df.to_csv("scanned_stocks_from_01_12_2025.csv", index=False)
    print("✅ scanned_stocks_from_01_12_2025.csv | rows:", len(hist_df))

    # 🔹 2. From current Monday till today
    monday = today - timedelta(days=today.weekday())

    week_df = scan_between(monday, today)
    week_df.to_csv("scanned_stocks_current_week.csv", index=False)
    print("✅ scanned_stocks_current_week.csv | rows:", len(week_df))
