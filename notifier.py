# Fichier: notifier.py
# Version finale, fusionnée et corrigée, incluant toutes les fonctionnalités avancées.

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
    """Envoie une photo avec une légende."""
    target_chat_id = chat_id or TG_CHAT_ID
    if not target_chat_id: return
    if not photo_buffer:
        return tg_send(caption, chat_id=target_chat_id)
    try:
        files = {'photo': ('trade_setup.png', photo_buffer, 'image/png')}
        payload = {"chat_id": target_chat_id, "caption": caption, "parse_mode": "HTML"}
        requests.post(f"{TELEGRAM_API}/sendPhoto", data=payload, files=files, timeout=20)
    except Exception:
        tg_send(f"⚠️ Erreur de graphique\n{caption}", chat_id=target_chat_id)

def tg_get_updates(offset: Optional[int] = None) -> List[Dict[str, Any]]:
    """Récupère les mises à jour de Telegram."""
    params = {"timeout": 1}
    if offset:
        params["offset"] = offset
    try:
        r = requests.get(f"{TELEGRAM_API}/getUpdates", params=params, timeout=5)
        if r.status_code == 200:
            data = r.json()
            return data.get("result", []) if data.get("ok") else []
    except Exception:
        pass
    return []

# ==============================================================================
# GESTION DES CLAVIERS INTERACTIFS
# ==============================================================================

def get_main_menu_keyboard(is_paused: bool) -> Dict:
    pause_resume_btn = {"text": "▶️ Relancer", "callback_data": "resume"} if is_paused else {"text": "⏸️ Pauser", "callback_data": "pause"}
    return {"inline_keyboard": [
        [pause_resume_btn, {"text": "🛰️ Ping", "callback_data": "ping"}],
        [{"text": "📊 Positions", "callback_data": "list_positions"}, {"text": "📈 Stats", "callback_data": "get_stats"}],
        [{"text": "⚙️ Configuration", "callback_data": "menu_config"}]
    ]}

def get_positions_keyboard(positions: List[Dict[str, Any]]) -> Optional[Dict]:
    if not positions: return None
    keyboard = []
    for pos in positions:
        keyboard.append([{"text": f"❌ Clôturer Trade #{pos.get('id', 0)}", "callback_data": f"close_trade_{pos.get('id', 0)}"}])
    return {"inline_keyboard": keyboard}

# ==============================================================================
# MESSAGES FORMATÉS
# ==============================================================================

def send_start_banner(platform: str, trading: str, risk: float):
    tg_send(f"<b>🔔 Darwin Bot Démarré</b>\n\n- Plateforme: <code>{_escape(platform)}</code>\n- Mode: <b>{_escape(trading)}</b>\n- Risque: <code>{risk}%</code>")

def send_main_menu(is_paused: bool):
    tg_send("🤖 <b>Panneau de Contrôle</b>", reply_markup=get_main_menu_keyboard(is_paused))

def format_open_positions(positions: List[Dict[str, Any]]):
    """Formate et envoie la liste des positions ouvertes depuis la DB."""
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
    """Calcule et envoie un rapport de performance."""
    stats = reporting.calculate_performance_stats(trades)
    message = reporting.format_report_message(title, stats, balance)
    tg_send(message)

def tg_send_error(title: str, error: Any):
    tg_send(f"❌ <b>Erreur: {_escape(title)}</b>\n<code>{_escape(str(error))}</code>")

def format_trade_message(symbol: str, signal: Dict, quantity: float, mode: str, risk: float) -> str:
    """Construit le message pour un trade qui vient d'être ouvert."""
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
    """Notifie l'utilisateur que le bot a choisi le meilleur signal parmi plusieurs."""
    message = (
        f"🎯 <b>Signal Sélectionné !</b>\n\n"
        f"Sur <code>{total_found}</code> opportunités, le meilleur signal a été choisi pour exécution sur <b>{_escape(symbol)}</b> "
        f"avec un RR de <b>x{signal['rr']:.2f}</b>."
    )
    tg_send(message, chat_id=TG_ALERTS_CHAT_ID)

def send_pending_signal_notification(symbol: str, signal: Dict):
    """Notifie qu'un signal a été détecté et est en attente."""
    side_icon = "📈" if signal['side'] == 'buy' else "📉"
    message = (
        f"⏱️ <b>Signal en attente {side_icon}</b>\n\n"
        f"Paire: <code>{_escape(symbol)}</code>\n"
        f"Type: {_escape(signal['regime'])}\n"
        f"RR Potentiel: x{signal['rr']:.2f}\n\n"
        f"<i>En attente de la clôture de la bougie pour validation finale.</i>"
    )
    tg_send(message, chat_id=TG_ALERTS_CHAT_ID)

def send_breakeven_notification(symbol: str, pnl_realised: float, remaining_qty: float):
    """Envoie une notification de mise à breakeven."""
    message = (
        f"🛡️ <b>Trade Sécurisé sur {_escape(symbol)} !</b>\n\n"
        f"Prise de profit partielle à la MM20 avec un gain de <code>{pnl_realised:.2f} USDT</code>.\n"
        f"Le Stop Loss a été remonté au point d'entrée pour le reste de la position (<code>{remaining_qty:.4f}</code>)."
    )
    tg_send(message, chat_id=TG_ALERTS_CHAT_ID)
