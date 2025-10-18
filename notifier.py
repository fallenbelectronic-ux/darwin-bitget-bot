import os
import requests

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

def tg_send(text: str):
    """Envoie un message Markdown à Telegram (ne fait rien si non configuré)."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("[NOTIFIER] Telegram not configured.")
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": "Markdown"
        }
        r = requests.post(url, data=payload, timeout=10)
        if r.status_code != 200:
            print("[NOTIFIER] Telegram send failed:", r.status_code, r.text)
    except Exception as e:
        print("[NOTIFIER] Exception sending telegram:", e)
