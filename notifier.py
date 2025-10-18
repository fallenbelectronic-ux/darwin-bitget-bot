import os, requests

TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

def tg_send(msg):
    """Envoi sécurisé vers Telegram (Markdown activé)."""
    if not TG_TOKEN or not TG_CHAT_ID:
        print("[TG] message ignoré (token/chat non définis)")
        return
    try:
        url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
        requests.post(url, data={"chat_id": TG_CHAT_ID, "text": msg, "parse_mode": "Markdown"})
    except Exception as e:
        print("[TG ERROR]", e)
