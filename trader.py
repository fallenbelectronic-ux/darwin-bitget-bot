# Fichier: notifier.py
import os, time, html, requests, io
from typing import List, Dict, Any, Optional
import reporting

# --- PARAMÈTRES TELEGRAM ---
TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
TG_ALERTS_CHAT_ID = os.getenv("TELEGRAM_ALERTS_CHAT_ID", "")
TELEGRAM_API = f"https://api.telegram.org/bot{TG_TOKEN}"

def tg_send(text: str, reply_markup: Optional[Dict] = None, chat_id: Optional[str] = None):
    """Envoie un message texte. Peut cibler un chat_id spécifique."""
    target_chat_id = chat_id if chat_id else TG_CHAT_ID
    if not TG_TOKEN or not target_chat_id: return
    try:
        payload = {"chat_id": target_chat_id, "text": text, "parse_mode": "HTML"}
        if reply_markup: payload['reply_markup'] = reply_markup
        requests.post(f"{TELEGRAM_API}/sendMessage", json=payload, timeout=10)
    except Exception as e: print(f"Erreur d'envoi Telegram: {e}")

def send_confirmed_signal_notification(symbol: str, signal: Dict):
    """Envoie une notification pour un signal 100% confirmé, avant la sélection."""
    side_icon = "📈" if signal['side'] == 'buy' else "📉"
    side_text = "LONG" if signal['side'] == 'buy' else "SHORT"
    message = (
        f"🎯 <b>Signal Confirmé {side_icon} {side_text}</b>\n\n"
        f" paire: <code>{html.escape(symbol)}</code>\n"
        f" Type: <b>{html.escape(signal['regime'].capitalize())}</b>\n"
        f" RR à l'ouverture: <b>x{signal['rr']:.2f}</b>"
    )
    tg_send(message, chat_id=TG_ALERTS_CHAT_ID or TG_CHAT_ID)

def send_validated_signal_report(symbol: str, signal: Dict, is_taken: bool, reason: str):
    """Envoie un rapport de signal (utilisé pour les rejets finaux)."""
    side_icon = "📈" if signal['side'] == 'buy' else "📉"
    side_text = "LONG" if signal['side'] == 'buy' else "SHORT"
    status_icon = "❌"
    status_text = f"<b>Position NON Ouverte</b>\n   - Raison: <i>{html.escape(reason)}</i>"
    message = (
        f"<b>{status_icon} Signal {side_icon} {side_text} Rejeté</b>\n\n"
        f" paire: <code>{html.escape(symbol)}</code>\n"
        f" Type: <b>{html.escape(signal['regime'].capitalize())}</b>\n"
        f" RR calculé: <b>x{signal['rr']:.2f}</b>\n\n"
        f"{status_text}"
    )
    # Les rejets sont envoyés sur le canal de contrôle principal
    tg_send(message, chat_id=TG_CHAT_ID)

def format_trade_message(symbol, signal, quantity, mode, risk) -> str:
    """Formate le message pour un trade réellement ouvert."""
    side_icon = "📈" if signal['side'] == 'buy' else "📉"
    mode_icon = "📝" if mode == 'PAPIER' else "✅"
    side_text = "LONG" if signal['side'] == 'buy' else "SHORT"
    return (
        f"{mode_icon} <b>{mode} | Nouveau Trade {side_icon} {side_text}</b>\n\n"
        f" paire: <code>{html.escape(symbol)}</code>\n"
        f" Type: <b>{html.escape(signal['regime'].capitalize())}</b>\n\n"
        f" Entrée: <code>{signal['entry']:.5f}</code>\n"
        f" SL: <code>{signal['sl']:.5f}</code>\n"
        f" TP: <code>{signal['tp']:.5f}</code>\n\n"
        f" Quantité: <code>{quantity:.4f}</code>\n"
        f" Risque: <code>{risk:.2f}%</code> | RR: <b>x{signal['rr']:.2f}</b>"
    )

# --- Le reste du fichier est identique et complet ---
def send_config_message(min_rr: float, risk: float, max_pos: int, leverage: int): #...
def send_mode_message(is_testnet: bool, is_paper: bool): #...
def get_trading_mode_keyboard(is_paper: bool) -> Dict: #...
def get_main_menu_keyboard(is_paused: bool) -> Dict: #...
def send_breakeven_notification(symbol: str, pnl_realised: float, remaining_qty: float): #...
def tg_send_with_photo(photo_buffer: io.BytesIO, caption: str, chat_id: Optional[str] = None): #...
def tg_get_updates(offset: Optional[int] = None) -> List[Dict[str, Any]]: #...
def get_strategy_menu_keyboard(current_strategy: str) -> Dict: #...
def get_positions_keyboard(positions: List[Dict[str, Any]]) -> Optional[Dict]: #...
def send_start_banner(platform: str, trading: str, risk: float): #...
def send_main_menu(is_paused: bool): #...
def send_strategy_menu(current_strategy: str): #...
def format_open_positions(positions: List[Dict[str, Any]]): #...
def tg_send_error(title: str, error: Any): #...
def send_report(title: str, trades: List[Dict[str, Any]], balance: Optional[float]): #...
