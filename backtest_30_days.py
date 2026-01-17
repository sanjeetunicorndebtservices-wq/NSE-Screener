from nsepython import *
import pandas as pd
from datetime import datetime, timedelta

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

rows = []
days = 0
back = 0

while days < 30 and back < 70:
    date = datetime.now() - timedelta(days=back)
    ds = date.strftime("%d-%m-%Y")

    try:
        df = nse_fno_bhavcopy(ds)
        if df is None or df.empty:
            back += 1
            continue

        df.columns = df.columns.str.strip()
        df['PRICE_PCT'] = ((df['closePrice'] - df['openPrice']) / df['openPrice']) * 100
        prev_oi = df['openInterest'] - df['changeinOpenInterest']
        df = df[prev_oi != 0]
        df['OI_PCT'] = (df['changeinOpenInterest'] / prev_oi) * 100

        for _, r in df.iterrows():
            signal = classify_signal(r['PRICE_PCT'], r['OI_PCT'])
            if not signal:
                continue

            rows.append({
                "DATE": ds,
                "SYMBOL": r['symbol'],
                "LAST_PRICE": round(r['closePrice'], 2),
                "PRICE_CHANGE_PCT": round(r['PRICE_PCT'], 2),
                "OI_CHANGE_PCT": round(r['OI_PCT'], 2),
                "SIGNAL_TYPE": signal,
                "CONFIDENCE_SCORE": confidence_score(r['PRICE_PCT'], r['OI_PCT'])
            })

        days += 1

    except:
        pass

    back += 1

pd.DataFrame(rows).to_csv("accuracy_report_30days.csv", index=False)
print("✅ accuracy_report_30days.csv generated")
