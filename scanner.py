import os
import requests
from datetime import datetime
from nsepython import *

TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

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

def send(msg):
    if TOKEN and CHAT_ID:
        requests.post(
            f"https://api.telegram.org/bot{TOKEN}/sendMessage",
            json={"chat_id": CHAT_ID, "text": msg}
        )

today = datetime.now().strftime("%d-%m-%Y")
df = nse_fno_bhavcopy(today)
df.columns = df.columns.str.strip()

df['PRICE_PCT'] = ((df['closePrice'] - df['openPrice']) / df['openPrice']) * 100
prev_oi = df['openInterest'] - df['changeinOpenInterest']
df = df[prev_oi != 0]
df['OI_PCT'] = (df['changeinOpenInterest'] / prev_oi) * 100

signals = []

for _, r in df.iterrows():
    sig = classify_signal(r['PRICE_PCT'], r['OI_PCT'])
    if sig:
        signals.append(
            f"{r['symbol']} | {sig} | Conf:{confidence_score(r['PRICE_PCT'], r['OI_PCT'])}"
        )

msg = "📊 Institutional Daily Signals\n\n" + ("\n".join(signals) if signals else "No signals")
send(msg)
