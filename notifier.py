# Fichier: notifier.py
import os
import time
import html
import requests
import io
from typing import List, Dict, Any, Optional

# --- Constantes API ---
TG_TOKEN   = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
TELEGRAM_API = f"https://api.telegram.org/bot{TG_TOKEN}"

# --- Buffer pour la commande /signals ---
_SIGNALS_BUFFER: List[Dict[str, Any]] = []
_BUFFER_HORIZON_SEC = 3600

# =========================================
# Fonctions de Communication de Base
# =========================================

def _escape(text: str) -> str:
    """Échappe les caractères HTML pour éviter les erreurs de parsing."""
    return html.escape(str(text))

def tg_send(text: str, reply_markup: Optional[Dict] = None):
    """Envoie un message texte, potentiellement avec un clavier de boutons."""
    if not TG_TOKEN or not TG_CHAT_ID:
        return
    try:
        payload = {"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML"}
        if reply_markup:
            payload['reply_markup'] = reply_markup
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

def tg_get_updates(offset: Optional[int] = None) -> List[Dict[str, Any]]:
    """Récupère les mises à jour (messages, clics de bouton) de Telegram."""
    params = {"timeout": 1}
    if offset:
        params["offset"] = offset
    try:
        r = requests.get(f"{TELEGRAM_API}/getUpdates", params=params, timeout=5)
        data = r.json()
        return data.get("result", []) if data.get("ok") else []
    except Exception:
        return []

# =========================================
# Génération des Claviers Interactifs
# =========================================

def get_main_menu_keyboard(is_paused: bool) -> Dict:
    """Génère le clavier du menu principal, avec un bouton Pause/Resume dynamique."""
    pause_resume_button = {"text": "▶️ Relancer", "callback_data": "resume"} if is_paused else {"text": "⏸️ Pauser", "callback_data": "pause"}
    return {
        "inline_keyboard": [
            [pause_resume_button, {"text": "📊 Positions", "callback_data": "list_positions"}],
            [{"text": "⚙️ Paramètres", "callback_data": "menu_settings"}, {"text": "ℹ️ Statut", "callback_data": "show_status"}]
        ]
    }

def get_settings_keyboard(settings: Dict[str, Any]) -> Dict:
    """Génère le clavier des paramètres avec des boutons ON/OFF."""
    dynamic_risk_status = "🟢 ON" if settings.get('DYNAMIC_RISK_ENABLED') else "🔵 OFF"
    trend_filter_status = "🟢 ON" if settings.get('TREND_FILTER_ENABLED') else "🔵 OFF"
    return {
        "inline_keyboard": [
            [{"text": f"Risque Dynamique: {dynamic_risk_status}", "callback_data": "toggle_dynamic_risk"}],
            [{"text": f"Filtre de Tendance: {trend_filter_status}", "callback_data": "toggle_trend_filter"}],
            [{"text": "🛡️ Gérer Blacklist", "callback_data": "menu_blacklist"}],
            [{"text": "↩️ Retour", "callback_data": "main_menu"}]
        ]
    }

def get_positions_keyboard(positions: List[Dict[str, Any]]) -> Optional[Dict]:
    """Génère des boutons pour chaque position ouverte."""
    if not positions:
        return None
    keyboard = []
    for pos in positions:
        trade_id = pos.get('id', 0)
        keyboard.append([
            {"text": f"❌ Clôturer Trade #{trade_id}", "callback_data": f"close_trade_{trade_id}"},
            {"text": f"🛡️ BE Trade #{trade_id}", "callback_data": f"breakeven_trade_{trade_id}"}
        ])
    return {"inline_keyboard": keyboard}

# =========================================
# Formatage des Messages
# =========================================

def tg_send_start_banner(platform: str, trading: str, risk: float):
    """Envoie la bannière de démarrage."""
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    msg = (
        f"<b>🔔 Darwin Bot Démarré</b>\n\n"
        f" plateforme: <code>{_escape(platform)}</code>\n"
        f" Mode: <b>{_escape(trading)}</b>\n"
        f" Risque par défaut: <code>{risk}%</code>\n\n"
        f"<i>{now}</i>"
    )
    tg_send(msg)

def format_trade_message(symbol: str, signal: Dict[str, Any], quantity: float, mode: str, risk: float) -> str:
    """Formate le message de notification pour un trade."""
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

def format_open_positions(positions: List[Dict[str, Any]]) -> str:
    """Formate la liste des positions ouvertes, incluant le TP."""
    if not positions:
        return "📊 Aucune position n'est actuellement ouverte."
    lines = ["<b>📊 Positions Ouvertes</b>\n"]
    for pos in positions:
        side_icon = "📈" if pos.get('side') == 'buy' else "📉"
        lines.append(
            f"<b>{pos.get('id')}. {side_icon} {_escape(pos.get('symbol', 'N/A'))}</b>\n"
            f"   Entrée: <code>{pos.get('entry_price', 0.0):.4f}</code>\n"
            f"   SL: <code>{pos.get('sl_price', 0.0):.4f}</code> | TP: <code>{pos.get('tp_price', 0.0):.4f}</code>\n"
        )
    return "\n".join(lines)

def tg_send_error(title: str, error: Any):
    """Envoie un message d'erreur formaté."""
    tg_send(f"❌ <b>Erreur: {_escape(title)}</b>\n<code>{_escape(error)}</code>")

def remember_signal(symbol: str, side: str, rr: float):
    """Mémorise un signal pour la commande /sig."""
    now = int(time.time())
    _SIGNALS_BUFFER.append({"ts": now, "symbol": symbol, "side": side, "rr": rr})
    while _SIGNALS_BUFFER and _SIGNALS_BUFFER[0]["ts"] < (now - _BUFFER_HORIZON_SEC):
        _SIGNALS_BUFFER.pop(0)

def get_signals_text() -> str:
    """Construit le texte pour la commande /sig."""
    if not _SIGNALS_BUFFER: return "🚀 Aucun signal détecté dans la dernière heure."
    lines = ["<b>🚀 Signaux de la dernière heure</b>"]
    for s in _SIGNALS_BUFFER:
        lines.append(f"• <code>{_escape(s['symbol'])}</code> {s['side'].upper()} (RR x{s['rr']:.1f})")
    return "\n".join(lines)
