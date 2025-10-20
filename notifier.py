# Fichier: notifier.py
import os
import time
import html
import requests
import io
from typing import List, Dict, Any, Optional

import reporting

TG_TOKEN   = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
TELEGRAM_API = f"https://api.telegram.org/bot{TG_TOKEN}"

def tg_send(text: str, reply_markup: Optional[Dict] = None):
    """Envoie un message texte simple sur Telegram."""
    if not TG_TOKEN or not TG_CHAT_ID:
        return
    try:
        payload = {"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML"}
        if reply_markup:
            payload['reply_markup'] = reply_markup
        requests.post(f"{TELEGRAM_API}/sendMessage", json=payload, timeout=10)
    except Exception as e:
        print(f"Erreur d'envoi Telegram: {e}")

# --- CORRECTION DU BUG DE NOTIFICATION D'ERREUR ---
def tg_send_error(title: str, error: Any):
    """Envoie un message d'erreur formaté, en convertissant l'erreur en string."""
    # On convertit l'objet 'error' en string avec str() avant de l'échapper.
    error_text = str(error)
    tg_send(f"❌ <b>Erreur: {html.escape(title)}</b>\n<code>{html.escape(error_text)}</code>")

# --- Le reste du fichier est identique à la version stable précédente ---
def send_validated_signal_report(symbol: str, signal: Dict, is_taken: bool, reason: str):
    side_icon = "📈" if signal['side'] == 'buy' else "📉"; status_icon = "✅" if is_taken else "❌"
    status_text = "<b>Position Ouverte</b>" if is_taken else f"<b>Position NON Ouverte</b>\n   - Raison: <i>{html.escape(reason)}</i>"
    message = ( f"<b>{status_icon} Signal Validé {side_icon}</b>\n\n"
               f" paire: <code>{html.escape(symbol)}</code>\n Type: <b>{html.escape(signal['regime'].capitalize())}</b>\n\n"
               f" Entrée: <code>{signal['entry']:.5f}</code>\n SL: <code>{signal['sl']:.5f}</code>\n TP: <code>{signal['tp']:.5f}</code>\n"
               f" RR: <b>x{signal['rr']:.2f}</b>\n\n{status_text}"); tg_send(message)
def send_config_message(min_rr: float, risk: float, max_pos: int, leverage: int):
    message = ( f"<b>⚙️ Configuration Actuelle</b>\n\n"
               f" - Ratio Risque/Récompense Min: <code>{min_rr}</code>\n - Risque par Trade: <code>{risk}%</code>\n"
               f" - Positions Max Ouvertes: <code>{max_pos}</code>\n - Levier: <code>x{leverage}</code>"); tg_send(message)
def send_mode_message(is_testnet: bool, is_paper: bool):
    platform_mode = "TESTNET" if is_testnet else "LIVE"; trading_mode = "PAPIER" if is_paper else "RÉEL"
    message = ( f"<b>🖥️ Modes de Fonctionnement</b>\n\n"
               f" - Plateforme: <b>{platform_mode}</b>\n - Trading: <b>{trading_mode}</b>"); tg_send(message)
def get_main_menu_keyboard(is_paused: bool) -> Dict:
    pause_resume_btn = {"text": "▶️ Relancer", "callback_data": "resume"} if is_paused else {"text": "⏸️ Pauser", "callback_data": "pause"}
    return {"inline_keyboard": [ [pause_resume_btn, {"text": "📊 Positions", "callback_data": "list_positions"}], [{"text": "⚙️ Stratégie", "callback_data": "manage_strategy"}, {"text": "📈 Stats", "callback_data": "get_stats"}], [{"text": "⏱️ Signaux Récents (6h)", "callback_data": "get_recent_signals"}]]}
def send_breakeven_notification(symbol: str, pnl_realised: float, remaining_qty: float):
    message = ( f"<b>⚙️ Gestion de Trade sur {html.escape(symbol)}</b>\n\n"
               f"✅ <b>MM20 atteinte !</b> Prise de profit partielle.\n   - Gain réalisé: <code>{pnl_realised:.2f} USDT</code>\n\n"
               f"🛡️ <b>Trade sécurisé à Breakeven.</b>\n   - Quantité restante: <code>{remaining_qty:.4f}</code>"); tg_send(message)
def tg_send_with_photo(photo_buffer: io.BytesIO, caption: str):
    if not photo_buffer: return tg_send(caption)
    try:
        files = {'photo': ('trade_setup.png', photo_buffer, 'image/png')}; payload = {"chat_id": TG_CHAT_ID, "caption": caption, "parse_mode": "HTML"}; requests.post(f"{TELEGRAM_API}/sendPhoto", data=payload, files=files, timeout=20)
    except Exception: tg_send(f"⚠️ Erreur de graphique\n{caption}")
def tg_get_updates(offset: Optional[int] = None) -> List[Dict[str, Any]]:
    params = {"timeout": 1};
    if offset: params["offset"] = offset
    try: r = requests.get(f"{TELEGRAM_API}/getUpdates", params=params, timeout=5); data = r.json(); return data.get("result", []) if data.get("ok") else []
    except Exception: return []
def get_strategy_menu_keyboard(current_strategy: str) -> Dict:
    buttons = [];
    if current_strategy == 'NORMAL': buttons.append([{"text": "✅ NORMAL", "callback_data": "no_change"}, {"text": "Activer: SPLIT", "callback_data": "switch_to_SPLIT"}])
    else: buttons.append([{"text": "Activer: NORMAL", "callback_data": "switch_to_NORMAL"}, {"text": "✅ SPLIT", "callback_data": "no_change"}]);
    buttons.append([{"text": "⬅️ Retour", "callback_data": "back_to_main"}]); return {"inline_keyboard": buttons}
def get_positions_keyboard(positions: List[Dict[str, Any]]) -> Optional[Dict]:
    if not positions: return None; keyboard = []
    for pos in positions: keyboard.append([{"text": f"❌ Clôturer Trade #{pos.get('id', 0)}", "callback_data": f"close_trade_{pos.get('id', 0)}"}]);
    return {"inline_keyboard": keyboard}
def send_start_banner(platform: str, trading: str, risk: float):
    tg_send(f"<b>🔔 Darwin Bot Démarré</b>\n\n plateforme: <code>{html.escape(platform)}</code>\n Mode: <b>{html.escape(trading)}</b>\n Risque: <code>{risk}%</code>")
def send_main_menu(is_paused: bool):
    tg_send("🤖 <b>Panneau de Contrôle</b>\nUtilisez les boutons ou /start.", reply_markup=get_main_menu_keyboard(is_paused))
def send_strategy_menu(current_strategy: str):
    message = (f"<b>⚙️ Gestion de la Stratégie</b>\n\nDéfinit comment les trades de <b>contre-tendance</b> sont gérés.\n\nStratégie Actuelle: <b><code>{current_strategy}</code></b>")
    tg_send(message, reply_markup=get_strategy_menu_keyboard(current_strategy))
def format_open_positions(positions: List[Dict[str, Any]]):
    if not positions: return tg_send("📊 Aucune position n'est actuellement ouverte.")
    lines = ["<b>📊 Positions Ouvertes</b>\n"];
    for pos in positions:
        side_icon = "📈" if pos.get('side') == 'buy' else "📉"; lines.append(f"<b>{pos.get('id')}. {side_icon} {html.escape(pos.get('symbol', 'N/A'))}</b>\n   Entrée: <code>{pos.get('entry_price', 0.0):.4f}</code>\n   SL: <code>{pos.get('sl_price', 0.0):.4f}</code> | TP: <code>{pos.get('tp_price', 0.0):.4f}</code>\n")
    tg_send("\n".join(lines), reply_markup=get_positions_keyboard(positions))
def send_report(title: str, trades: List[Dict[str, Any]]):
    stats = reporting.get_report_stats(trades); message = reporting.format_report_message(title, stats); tg_send(message)
def format_trade_message(symbol, signal, quantity, mode, risk) -> str:
    side_icon = "📈" if signal['side'] == 'buy' else "📉"; mode_icon = "📝" if mode == 'PAPIER' else "✅"
    return ( f"{mode_icon} <b>{mode} | Nouveau Trade {side_icon}</b>\n\n"
            f" paire: <code>{html.escape(symbol)}</code>\n Type: <b>{html.escape(signal['regime'].capitalize())}</b>\n\n"
            f" Entrée: <code>{signal['entry']:.5f}</code>\n SL: <code>{signal['sl']:.5f}</code>\n TP: <code>{signal['tp']:.5f}</code>\n\n"
            f" Quantité: <code>{quantity:.4f}</code>\n Risque: <code>{risk:.2f}%</code> | RR: <b>x{signal['rr']:.2f}</b>" )
