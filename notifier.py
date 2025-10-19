# Fichier: notifier.py
import os, time, html, requests, io
from typing import Dict, Any

TG_TOKEN   = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
TELEGRAM_API = f"https://api.telegram.org/bot{TG_TOKEN}"

def _escape(text: str) -> str:
    return html.escape(str(text))

def tg_send(text: str):
    """Envoie un message texte simple."""
    if not TG_TOKEN or not TG_CHAT_ID:
        return
    try:
        payload = {"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML"}
        requests.post(f"{TELEGRAM_API}/sendMessage", json=payload, timeout=10)
    except Exception as e:
        print(f"Erreur d'envoi Telegram: {e}")

def tg_send_with_photo(photo_buffer: io.BytesIO, caption: str):
    """Envoie une photo avec une légende."""
    if not photo_buffer:
        return tg_send(caption)
    try:
        files = {'photo': ('trade_setup.png', photo_buffer, 'image/png')}
        payload = {"chat_id": TG_CHAT_ID, "caption": caption, "parse_mode": "HTML"}
        requests.post(f"{TELEGRAM_API}/sendPhoto", data=payload, files=files, timeout=20)
    except Exception as e:
        tg_send(f"⚠️ Erreur de graphique\n{caption}")

def format_start_message(platform: str, trading: str, risk: float):
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    msg = (
        f"<b>🔔 Darwin Bot Démarré</b>\n\n"
        f" plateforme: <code>{_escape(platform)}</code>\n"
        f" Mode: <b>{_escape(trading)}</b>\n"
        f" Risque: <code>{risk}%</code>\n\n"
        f"<i>{now}</i>"
    )
    tg_send(msg)

def format_trade_message(symbol: str, signal: Dict[str, Any], quantity: float, mode: str, risk: float) -> str:
    """Formate le message de notification de trade."""
    side_icon = "📈" if signal['side'] == 'buy' else "📉"
    mode_icon = "📝" if mode == 'PAPIER' else "✅"
    return (
        f"{mode_icon} <b>{mode} | Nouveau Trade {side_icon}</b>\n\n"
        f" paire: <code>{_escape(symbol)}</code>\n"
        f" Type: <b>{_escape(signal['regime'].capitalize())}</b>\n\n"
        f" Entrée: <code>{signal['entry']:.5f}</code>\n"
        f" SL: <code>{signal['sl']:.5f}</code>\n"
        f" TP: <code>{signal['tp']:.5f}</code>\n\n"
        f" Quantité: <code>{quantity:.4f}</code>\n"
        f" Risque: <code>{risk:.2f}%</code> | RR: <b>x{signal['rr']:.2f}</b>"
    )

def tg_send_error(title: str, error: Any):
    """Envoie un message d'erreur formaté."""
    tg_send(f"❌ <b>Erreur: {_escape(title)}</b>\n<code>{_escape(error)}</code>")
