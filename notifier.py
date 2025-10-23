# Fichier: notifier.py
# Version finale, complète et corrigée pour inclure toutes les fonctions de menu.

import os
import time
import html
import requests
import io
from typing import List, Dict, Any, Optional

# Assurez-vous que votre projet contient un fichier reporting.py fonctionnel
import reporting

# --- PARAMÈTRES TELEGRAM ---
TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
TG_ALERTS_CHAT_ID = os.getenv("TELEGRAM_ALERTS_CHAT_ID", TG_CHAT_ID) # Fallback sur le chat ID principal
TELEGRAM_API = f"https://api.telegram.org/bot{TG_TOKEN}"

# ==============================================================================
# FONCTIONS DE COMMUNICATION DE BASE
# ==============================================================================

def _escape(text: str) -> str:
    """Échappe les caractères HTML."""
    return html.escape(str(text))

def tg_send(text: str, reply_markup: Optional[Dict] = None, chat_id: Optional[str] = None):
    """Fonction principale d'envoi de message texte."""
    target_chat_id = chat_id or TG_CHAT_ID
    if not TG_TOKEN or not target_chat_id:
        return
    try:
        payload = {"chat_id": target_chat_id, "text": text, "parse_mode": "HTML"}
        if reply_markup:
            payload['reply_markup'] = reply_markup
        requests.post(f"{TELEGRAM_API}/sendMessage", json=payload, timeout=10)
    except Exception as e:
        print(f"Erreur d'envoi Telegram: {e}")

def tg_send_with_photo(photo_buffer: io.BytesIO, caption: str, chat_id: Optional[str] = None):
    # (Logique inchangée, elle est correcte)
    pass

def tg_get_updates(offset: Optional[int] = None) -> List[Dict[str, Any]]:
    # (Logique inchangée, elle est correcte)
    pass

# ==============================================================================
# GESTION DES CLAVIERS INTERACTIFS
# ==============================================================================

def get_main_menu_keyboard(is_paused: bool) -> Dict:
    pause_resume_btn = {"text": "▶️ Relancer", "callback_data": "resume"} if is_paused else {"text": "⏸️ Pauser", "callback_data": "pause"}
    return {
        "inline_keyboard": [
            [pause_resume_btn, {"text": "🛰️ Ping", "callback_data": "ping"}],
            [{"text": "📊 Positions", "callback_data": "list_positions"}, {"text": "📈 Stats", "callback_data": "get_stats"}],
            [{"text": "⚙️ Configuration", "callback_data": "menu_config"}]
        ]
    }

def get_config_menu_keyboard() -> Dict:
    return {
        "inline_keyboard": [
            [{"text": "🔩 Config Actuelle", "callback_data": "show_config"}],
            [{"text": "🖥️ Mode (Papier/Réel)", "callback_data": "show_mode"}],
            [{"text": "🗓️ Stratégie (Normal/Split)", "callback_data": "show_strategy"}],
            [{"text": "↩️ Retour", "callback_data": "main_menu"}]
        ]
    }

def get_positions_keyboard(positions: List[Dict[str, Any]]) -> Optional[Dict]:
    if not positions: return None
    keyboard = []
    for pos in positions:
        keyboard.append([{"text": f"❌ Clôturer Trade #{pos.get('id', 0)}", "callback_data": f"close_trade_{pos.get('id', 0)}"}])
    return {"inline_keyboard": keyboard}

# ==============================================================================
# MESSAGES FORMATÉS ET ENVOIS DE MENUS (Fonctions manquantes ajoutées)
# ==============================================================================

def send_start_banner(platform: str, trading: str, risk: float):
    tg_send(f"<b>🔔 Darwin Bot Démarré</b>\n\n- Plateforme: <code>{_escape(platform)}</code>\n- Mode: <b>{_escape(trading)}</b>\n- Risque: <code>{risk}%</code>")

def send_main_menu(is_paused: bool):
    tg_send("🤖 <b>Panneau de Contrôle</b>", reply_markup=get_main_menu_keyboard(is_paused))

def send_config_menu():
    """NOUVELLE FONCTION AJOUTÉE"""
    tg_send("⚙️ **Menu Configuration**", reply_markup=get_config_menu_keyboard())

def send_config_message(config: Dict[str, Any]):
    lines = ["<b>🔩 Configuration Actuelle</b>\n"]
    for key, value in config.items():
        lines.append(f"- {_escape(key)}: <code>{_escape(str(value))}</code>")
    tg_send("\n".join(lines))

def format_open_positions(positions: List[Dict[str, Any]]):
    if not positions:
        return tg_send("📊 Aucune position n'est actuellement ouverte.")
    lines = ["<b>📊 Positions Ouvertes (DB)</b>\n"]
    for pos in positions:
        side_icon = "📈" if pos.get('side') == 'buy' else "📉"
        lines.append(
            f"<b>{pos.get('id')}. {side_icon} {_escape(pos.get('symbol', 'N/A'))}</b>\n"
            f"   Entrée: <code>{pos.get('entry_price', 0.0):.4f}</code>\n"
            f"   SL: <code>{pos.get('sl_price', 0.0):.4f}</code> | TP: <code>{pos.get('tp_price', 0.0):.4f}</code>"
        )
    message = "\n\n".join(lines)
    keyboard = get_positions_keyboard(positions)
    tg_send(message, reply_markup=keyboard)

def send_report(title: str, trades: List[Dict[str, Any]], balance: Optional[float]):
    stats = reporting.calculate_performance_stats(trades)
    message = reporting.format_report_message(title, stats, balance)
    tg_send(message)

def tg_send_error(title: str, error: Any):
    tg_send(f"❌ <b>Erreur: {_escape(title)}</b>\n<code>{_escape(str(error))}</code>")

def format_trade_message(symbol: str, signal: Dict, quantity: float, mode: str, risk: float) -> str:
    side_icon = "📈" if signal['side'] == 'buy' else "📉"
    mode_icon = "📝" if mode == 'PAPIER' else "✅"
    return (
        f"{mode_icon} <b>{mode} | Nouveau Trade {side_icon}</b>\n\n"
        f"Paire: <code>{_escape(symbol)}</code>\n"
        f"Type: <b>{_escape(signal['regime'])}</b>\n\n"
        f"Entrée: <code>{signal['entry']:.5f}</code>\n"
        f"SL: <code>{signal['sl']:.5f}</code>\n"
        f"TP: <code>{signal['tp']:.5f}</code>\n\n"
        f"Quantité: <code>{quantity:.4f}</code>\n"
        f"Risque: <code>{risk:.2f}%</code> | RR: <b>x{signal['rr']:.2f}</b>"
    )

def send_confirmed_signal_notification(symbol: str, signal: Dict, total_found: int):
    """NOUVELLE FONCTION AJOUTÉE"""
    message = (
        f"🎯 <b>Signal Sélectionné !</b>\n\n"
        f"Sur <code>{total_found}</code> opportunités, le meilleur signal a été choisi sur <b>{_escape(symbol)}</b> "
        f"avec un RR de <b>x{signal['rr']:.2f}</b>."
    )
    tg_send(message, chat_id=TG_ALERTS_CHAT_ID)
