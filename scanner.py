import os
import requests
from datetime import datetime
from nsepython import *

# Telegram credentials (GitHub Secrets)
TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

def send_telegram(message):
    if not TOKEN or not CHAT_ID:
        print("❌ Telegram credentials missing")
        return
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    requests.post(
        url,
        json={
            "chat_id": CHAT_ID,
            "text": message,
            "parse_mode": "Markdown"
        },
        timeout=10
    )

try:
    today = datetime.now().strftime("%d-%m-%Y")
    df = nse_fno_bhavcopy(today)

    if df is None or df.empty:
        raise Exception("Empty F&O bhavcopy")

    df.columns = df.columns.str.strip()

    required = [
        'symbol',
        'openPrice',
        'closePrice',
        'openInterest',
        'changeinOpenInterest'
    ]

    if not all(col in df.columns for col in required):
        raise Exception("Required columns missing in bhavcopy")

    # Price % change
    df['PRICE_PCT'] = ((df['closePrice'] - df['openPrice']) / df['openPrice']) * 100

    # OI % change (safe)
    prev_oi = df['openInterest'] - df['changeinOpenInterest']
    df = df[prev_oi != 0]
    df['OI_PCT'] = (df['changeinOpenInterest'] / prev_oi) * 100

    # 🔥 Scanner logic (same as history + backtest)
    squeeze = df[
        (df['PRICE_PCT'] > 2) &
        (df['OI_PCT'] < -5)
    ]['symbol'].tolist()

    shorting = df[
        (df['PRICE_PCT'] < -2) &
        (df['OI_PCT'] > 5)
    ]['symbol'].tolist()

    report = "📊 *NSE Daily F&O Smart Money Report*\n\n"

    report += "🚀 *BTST – Short Covering (Buy)*\n"
    report += ", ".join(squeeze) if squeeze else "None"

    report += "\n\n📉 *STBT – Aggressive Shorts (Sell)*\n"
    report += ", ".join(shorting) if shorting else "None"

    send_telegram(report)
    print("✅ Telegram alert sent")

except Exception as e:
    send_telegram(f"❌ Scanner Error: `{str(e)}`")
    print(e)
