# Fichier: notifier.py
import os, time, html, requests, io
from typing import List, Dict, Any, Optional

TG_TOKEN   = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
TELEGRAM_API = f"https://api.telegram.org/bot{TG_TOKEN}"

def _escape(text: str) -> str: return html.escape(str(text))

def tg_send(text: str, reply_markup: Optional[Dict] = None):
    if not TG_TOKEN or not TG_CHAT_ID: return
    payload = {"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML"}
    if reply_markup: payload['reply_markup'] = reply_markup
    try:
        requests.post(f"{TELEGRAM_API}/sendMessage", json=payload, timeout=10)
    except Exception: pass

def tg_send_with_photo(photo_buffer: io.BytesIO, caption: str):
    if not photo_buffer: return tg_send(caption)
    try:
        files = {'photo': ('trade_setup.png', photo_buffer, 'image/png')}
        payload = {"chat_id": TG_CHAT_ID, "caption": caption, "parse_mode": "HTML"}
        requests.post(f"{TELEGRAM_API}/sendPhoto", data=payload, files=files, timeout=20)
    except Exception:
        tg_send(f"⚠️ Erreur de graphique\n{caption}")

def tg_get_updates(offset: Optional[int] = None) -> List[Dict[str, Any]]:
    # ... (inchangé) ...

# --- CLAVIERS INTERACTIFS ---
def get_main_menu_keyboard() -> Dict:
    return {"inline_keyboard": [
        [{"text": "▶️ Relancer", "callback_data": "resume"}, {"text": "⏸️ Pauser", "callback_data": "pause"}],
        [{"text": "⚙️ Paramètres", "callback_data": "settings_menu"}, {"text": "📊 Positions", "callback_data": "list_positions"}]
    ]}

def get_settings_keyboard(settings: Dict[str, Any]) -> Dict:
    # ... (logique pour afficher les boutons de configuration avec leur état actuel ON/OFF) ...

# --- FORMATAGE DES MESSAGES ---
def format_trade_message(symbol, signal, quantity, mode, risk) -> str:
    # ... (inchangé, mais plus joli) ...

# ... (toutes les autres fonctions de formatage) ...
