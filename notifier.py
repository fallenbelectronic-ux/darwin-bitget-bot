import os
import time
import html
import requests
from typing import List, Dict, Any, Optional

TG_TOKEN   = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
TELEGRAM_API = f"https://api.telegram.org/bot{TG_TOKEN}"

_SIGNALS_BUFFER: List[Dict[str, Any]] = []
_BUFFER_HORIZON_SEC = 3600

def _escape(text: str) -> str: return html.escape(str(text))

def tg_send(text: str, **kwargs) -> bool:
    if not TG_TOKEN or not TG_CHAT_ID: return False
    try:
        payload = {"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML", **kwargs}
        r = requests.post(f"{TELEGRAM_API}/sendMessage", json=payload, timeout=10)
        return r.json().get("ok", False)
    except Exception: return False

def tg_get_updates(offset: Optional[int] = None) -> List[Dict[str, Any]]:
    params = {"timeout": 1}
    if offset: params["offset"] = offset
    try:
        r = requests.get(f"{TELEGRAM_API}/getUpdates", params=params, timeout=5)
        data = r.json()
        return data.get("result", []) if data.get("ok") else []
    except Exception: return []

def format_start_message(platform: str, trading: str, risk: float):
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    tg_send(
        f"<b>🔔 Darwin Bot Démarré</b>\n\n"
        f" plateforme: <code>{_escape(platform)}</code>\n"
        f" Mode: <b>{_escape(trading)}</b>\n"
        f" Risque: <code>{risk}%</code>\n\n"
        f"<i>{now}</i>"
    )

def format_config_message(params: Dict[str, Any]) -> str:
    lines = ["<b>⚙️ Configuration Active</b>"]
    for key, value in params.items():
        lines.append(f"• {_escape(key)}: <code>{_escape(value)}</code>")
    return "\n".join(lines)

def format_mode_message(platform: str, trading: str) -> str:
    return f"🧭 <b>Mode Actuel</b>\n• Plateforme: <code>{_escape(platform)}</code>\n• Trading: <b>{_escape(trading)}</b>"

def format_stats_message() -> str:
    return "📈 Les statistiques de performance ne sont pas encore implémentées."

def remember_signal_message(symbol: str, side: str, rr: float):
    now = int(time.time())
    _SIGNALS_BUFFER.append({"ts": now, "symbol": symbol, "side": side, "rr": rr})
    while _SIGNALS_BUFFER and _SIGNALS_BUFFER[0]["ts"] < (now - _BUFFER_HORIZON_SEC):
        _SIGNALS_BUFFER.pop(0)

def signals_last_hour_text() -> str:
    if not _SIGNALS_BUFFER: return "🚀 Aucun signal détecté dans la dernière heure."
    lines = ["<b>🚀 Signaux de la dernière heure</b>"]
    for s in _SIGNALS_BUFFER:
        lines.append(f"• <code>{_escape(s['symbol'])}</code> {s['side'].upper()} (RR x{s['rr']:.1f})")
    return "\n".join(lines)

def format_trade_message(symbol: str, signal: Dict[str, Any], quantity: float, mode: str) -> str:
    side_icon = "📈" if signal['side'] == 'buy' else "📉"
    mode_icon = "📝" if mode == 'PAPIER' else "✅"
    return (
        f"{mode_icon} <b>{mode} | Nouveau Trade {side_icon}</b>\n\n"
        f" paire: <code>{_escape(symbol)}</code>\n"
        f" Type: <b>{_escape(signal['regime'].capitalize())}</b>\n\n"
        f" Entrée: <code>{signal['entry']:.5f}</code>\n"
        f" SL: <code>{signal['sl']:.5f}</code>\n"
        f" TP: <code>{signal['tp']:.5f}</code>\n\n"
        f" Quantité: <code>{quantity:.4f}</code> | RR: <b>x{signal['rr']:.2f}</b>"
    )

def tg_send_error(title: str, error: Any):
    tg_send(f"❌ <b>Erreur: {_escape(title)}</b>\n<code>{_escape(error)}</code>")
