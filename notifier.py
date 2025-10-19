import os
import time
import html
import requests
from typing import Dict, Any, Optional

# =========================
# ENV – Telegram
# =========================
TG_TOKEN   = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
TELEGRAM_API = f"https://api.telegram.org/bot{TG_TOKEN}"

# =========================
# FONCTIONS DE BASE (Corrigées)
# =========================

def _escape_html(text: str) -> str:
    return html.escape(str(text))

def tg_send(text: str, **kwargs) -> bool:
    """
    Envoie un message à Telegram. Gère les erreurs.
    Le texte doit déjà être formaté en HTML si nécessaire.
    """
    if not TG_TOKEN or not TG_CHAT_ID:
        print("Erreur: Token ou Chat ID Telegram manquant.")
        return False
    try:
        payload = {
            "chat_id": TG_CHAT_ID,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
            **kwargs
        }
        r = requests.post(f"{TELEGRAM_API}/sendMessage", json=payload, timeout=10)
        return r.json().get("ok", False)
    except Exception as e:
        print(f"Erreur d'envoi Telegram: {e}")
        return False

# =========================
# NOUVELLES NOTIFICATIONS FORMATÉES
# =========================

def tg_send_start_banner(platform_mode: str, trading_mode: str, risk_percent: float):
    """
    Envoie une belle bannière de démarrage.
    """
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    message = (
        f"<b>🔔 Darwin Bot Démarré</b>\n\n"
        f" plateforme: <code>{_escape_html(platform_mode)}</code>\n"
        f" Mode de trading: <b>{_escape_html(trading_mode)}</b>\n"
        f" Risque par trade: <code>{risk_percent}%</code>\n\n"
        f"<i>{now}</i>"
    )
    tg_send(message)


def tg_format_trade(symbol: str, signal: Dict[str, Any], quantity: float, mode: str) -> str:
    """
    Formate un message de trade (réel ou papier) avec des émojis.
    """
    side_icon = "📈" if signal['side'] == 'buy' else "📉"
    side_text = "LONG" if signal['side'] == 'buy' else "SHORT"
    mode_icon = "📝" if mode == 'PAPIER' else "✅"

    return (
        f"{mode_icon} <b>{mode} | Nouveau Trade {side_icon}</b>\n\n"
        f" paire: <code>{_escape_html(symbol)}</code>\n"
        f" Type: <b>{_escape_html(signal['regime'].capitalize())}</b>\n\n"
        f" Entrée: <code>{signal['entry']:.5f}</code>\n"
        f" SL: <code>{signal['sl']:.5f}</code>\n"
        f" TP: <code>{signal['tp']:.5f}</code>\n\n"
        f" Quantité: <code>{quantity:.4f}</code>\n"
        f" RR: <b>x{signal['rr']:.2f}</b>"
    )

def tg_send_error(title: str, error_message: Any):
    """
    Envoie un message d'erreur formaté.
    """
    message = (
        f"❌ <b>Erreur: {_escape_html(title)}</b>\n\n"
        f"<code>{_escape_html(error_message)}</code>"
    )
    tg_send(message)
