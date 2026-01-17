from nsepython import *
import pandas as pd
from datetime import datetime, timedelta

results = []
valid_days = 0
days_back = 0

print("🔍 Backtesting last 30 trading days (F&O Short Covering / Aggressive Shorts)...")

while valid_days < 30 and days_back < 70:
    trade_date = datetime.now() - timedelta(days=days_back)
    date_str = trade_date.strftime("%d-%m-%Y")

    try:
        df = nse_fno_bhavcopy(date_str)

        if df is None or df.empty:
            days_back += 1
            continue

        df.columns = df.columns.str.strip()

        required = [
            'symbol',
            'openPrice',
            'closePrice',
            'openInterest',
            'changeinOpenInterest'
        ]

        if not all(col in df.columns for col in required):
            print(f"⚠️ {date_str} → Missing required columns")
            days_back += 1
            continue

        # Price % change
        df['PRICE_PCT'] = ((df['closePrice'] - df['openPrice']) / df['openPrice']) * 100

        # OI % change (safe)
        prev_oi = df['openInterest'] - df['changeinOpenInterest']
        df = df[prev_oi != 0]
        df['OI_PCT'] = (df['changeinOpenInterest'] / prev_oi) * 100

        # 🔥 SAME LOGIC AS SCANNER
        signals = df[
            ((df['PRICE_PCT'] > 2) & (df['OI_PCT'] < -5)) |   # Short covering
            ((df['PRICE_PCT'] < -2) & (df['OI_PCT'] > 5))    # Aggressive shorting
        ].copy()

        print(f"✅ {date_str} → {len(signals)} signals")

        for _, row in signals.iterrows():
            results.append({
                'Date': date_str,
                'Symbol': row['symbol'],
                'Close': round(row['closePrice'], 2),
                'Price_%': round(row['PRICE_PCT'], 2),
                'OI_%': round(row['OI_PCT'], 2),
                'Signal_Type': (
                    'SHORT_COVERING' if row['PRICE_PCT'] > 0 else 'AGGRESSIVE_SHORT'
                )
            })

        valid_days += 1

    except Exception as e:
        print(f"❌ {date_str} → {e}")

    days_back += 1

# 🔒 ALWAYS create CSV
final_df = pd.DataFrame(results)

if final_df.empty:
    final_df = pd.DataFrame(columns=[
        'Date', 'Symbol', 'Close', 'Price_%', 'OI_%', 'Signal_Type'
    ])

final_df.to_csv("accuracy_report_30days.csv", index=False)
print(f"💾 accuracy_report_30days.csv created | Rows: {len(final_df)}")
