import os
import requests
import pandas as pd
from nsepython import *

# Pull keys from GitHub Secrets
TOKEN = os.getenv('TELEGRAM_TOKEN')
CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

def send_telegram(text):
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    requests.post(url, json={"chat_id": CHAT_ID, "text": text, "parse_mode": "Markdown"})

try:
    # Fetch today's Bhavcopy
    df = df = nse_fno_bhavcopy()
    
    # LOGIC: Short Squeeze (Price rose > 2%, Open Interest fell > 5%)
    squeeze = df[(df['pChange'] > 2) & (df['changeOI'] < -5)]['symbol'].tolist()
    
    # LOGIC: Aggressive Shorting (Price fell > 2%, Open Interest rose > 10%)
    shorting = df[(df['pChange'] < -2) & (df['changeOI'] > 10)]['symbol'].tolist()

    report = "📊 *NSE Daily Hidden Data Report*\n\n"
    report += "🚀 *Short Squeeze (Gap Up Potential):*\n" + (", ".join(squeeze) if squeeze else "None")
    report += "\n\n📉 *Aggressive Shorts (Gap Down Potential):*\n" + (", ".join(shorting) if shorting else "None")
    
    send_telegram(report)
except Exception as e:
    send_telegram(f"❌ Error during scan: {str(e)}")
