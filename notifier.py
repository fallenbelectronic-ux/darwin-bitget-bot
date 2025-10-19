# Fichier: notifier.py
import os
import time
import html
import requests
import io
import reporting
from typing import List, Dict, Any, Optional

TG_TOKEN   = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
TELEGRAM_API = f"https://api.telegram.org/bot{TG_TOKEN}"

def _escape(text: str) -> str:
    return html.escape(str(text))

def tg_send(text: str, reply_markup: Optional[Dict] = None):
    if not TG_TOKEN or not TG_CHAT_ID: return
    try:
        payload = {"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML"}
        if reply_markup: payload['reply_markup'] = reply_markup
        requests.post(f"{TELEGRAM_API}/sendMessage", json=payload, timeout=10)
    except Exception as e: print(f"Erreur d'envoi Telegram: {e}")

def tg_send_with_photo(photo_buffer: io.BytesIO, caption: str):
    if not photo_buffer: return tg_send(caption)
    try:
        files = {'photo': ('trade_setup.png', photo_buffer, 'image/png')}
        payload = {"chat_id": TG_CHAT_ID, "caption": caption, "parse_mode": "HTML"}
        requests.post(f"{TELEGRAM_API}/sendPhoto", data=payload, files=files, timeout=20)
    except Exception as e: tg_send(f"⚠️ Erreur de graphique\n{caption}")

def tg_get_updates(offset: Optional[int] = None) -> List[Dict[str, Any]]:
    params = {"timeout": 1}
    if offset: params["offset"] = offset
    try:
        r = requests.get(f"{TELEGRAM_API}/getUpdates", params=params, timeout=5)
        data = r.json()
        return data.get("result", []) if data.get("ok") else []
    except Exception: return []

def get_main_menu_keyboard(is_paused: bool) -> Dict:
    pause_resume_btn = {"text": "▶️ Relancer", "callback_data": "resume"} if is_paused else {"text": "⏸️ Pauser", "callback_data": "pause"}
    return {"inline_keyboard": [[pause_resume_btn, {"text": "📊 Positions", "callback_data": "list_positions"}]]}

def get_positions_keyboard(positions: List[Dict[str, Any]]) -> Optional[Dict]:
    if not positions: return None
    keyboard = []
    for pos in positions:
        trade_id = pos.get('id', 0)
        keyboard.append([{"text": f"❌ Clôturer Trade #{trade_id}", "callback_data": f"close_trade_{trade_id}"}])
    return {"inline_keyboard": keyboard}

def send_start_banner(platform: str, trading: str, risk: float):
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    msg = (
        f"<b>🔔 Darwin Bot Démarré</b>\n\n"
        f" plateforme: <code>{_escape(platform)}</code>\n"
        f" Mode: <b>{_escape(trading)}</b>\n"
        f" Risque: <code>{risk}%</code>"
    )
    tg_send(msg)

def send_main_menu(is_paused: bool):
    tg_send("🤖 **Panneau de Contrôle**", reply_markup=get_main_menu_keyboard(is_paused))

def format_open_positions(positions: List[Dict[str, Any]]):
    if not positions:
        return tg_send("📊 Aucune position n'est actuellement ouverte.")
    lines = ["<b>📊 Positions Ouvertes</b>\n"]
    for pos in positions:
        side_icon = "📈" if pos.get('side') == 'buy' else "📉"
        lines.append(
            f"<b>{pos.get('id')}. {side_icon} {_escape(pos.get('symbol', 'N/A'))}</b>\n"
            f"   Entrée: <code>{pos.get('entry_price', 0.0):.4f}</code>\n"
            f"   SL: <code>{pos.get('sl_price', 0.0):.4f}</code> | TP: <code>{pos.get('tp_price', 0.0):.4f}</code>\n"
        )
    message = "\n".join(lines)
    keyboard = get_positions_keyboard(positions)
    tg_send(message, reply_markup=keyboard)

def tg_send_error(title: str, error: Any):
    tg_send(f"❌ <b>Erreur: {_escape(title)}</b>\n<code>{_escape(error)}</code>")

def format_trade_message(symbol, signal, quantity, mode, risk) -> str:
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

def send_report(title: str, trades: List[Dict[str, Any]]):
    """Calcule les stats et envoie un rapport formaté sur Telegram."""
    stats = reporting.get_report_stats(trades)
    message = reporting.format_report_message(title, stats)
    tg_send(message)
        
    )
