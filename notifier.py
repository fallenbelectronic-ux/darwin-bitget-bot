# Fichier: notifier.py
import os
import time
import html
import requests
import io
from typing import List, Dict, Any, Optional

import reporting

# --- PARAMÈTRES TELEGRAM ---
TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
# ID du canal principal de contrôle
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
# NOUVEAU : ID du canal dédié aux alertes de signaux
TG_ALERTS_CHAT_ID = os.getenv("TELEGRAM_ALERTS_CHAT_ID", "")
TELEGRAM_API = f"https://api.telegram.org/bot{TG_TOKEN}"

def tg_send(text: str, reply_markup: Optional[Dict] = None, chat_id: Optional[str] = None):
    """Envoie un message texte. Peut cibler un chat_id spécifique."""
    target_chat_id = chat_id if chat_id else TG_CHAT_ID
    if not TG_TOKEN or not target_chat_id:
        return
    try:
        payload = {"chat_id": target_chat_id, "text": text, "parse_mode": "HTML"}
        if reply_markup:
                     payload['reply_markup'] = reply_markup
        requests.post(f"{TELEGRAM_API}/sendMessage", json=payload, timeout=10)
    except Exception as e:
        print(f"Erreur d'envoi Telegram: {e}")

def send_validated_signal_report(symbol: str, signal: Dict, is_taken: bool, reason: str):
    """Envoie un rapport de signal validé, avec le statut d'exécution."""
    side_icon = "📈" if signal['side'] == 'buy' else "📉"
    side_text = "LONG" if signal['side'] == 'buy' else "SHORT"
    status_icon = "✅" if is_taken else ("⏳" if "En attente" in reason else "❌") # Icône d'attente
    status_text = "<b>Position Ouverte</b>" if is_taken else f"<b>Position NON Ouverte</b>\n   - Raison: <i>{html.escape(reason)}</i>"
    
    message = (
        f"<b>{status_icon} Signal Validé {side_icon} {side_text}</b>\n\n"
        f" paire: <code>{html.escape(symbol)}</code>\n"
        f" Type: <b>{html.escape(signal['regime'].capitalize())}</b>\n\n"
        f" Entrée: <code>{signal['entry']:.5f}</code>\n"
        f" SL: <code>{signal['sl']:.5f}</code>\n"
        f" TP: <code>{signal['tp']:.5f}</code>\n"
        f" RR: <b>x{signal['rr']:.2f}</b>\n\n"
        f"{status_text}"
    )
    # On envoie ce message spécifiquement sur le canal d'alertes
    tg_send(message, chat_id=TG_ALERTS_CHAT_ID or TG_CHAT_ID)


def send_confirmed_signal_notification(symbol: str, signal: Dict[str, Any]):
    """Informe Telegram qu'un signal en attente vient d'être confirmé."""

    if not signal:
        return

    side = signal.get('side', '').lower()
    side_icon = "📈" if side == 'buy' else "📉"
    side_text = "LONG" if side == 'buy' else "SHORT"

    entry = signal.get('entry')
    sl = signal.get('sl')
    tp = signal.get('tp')
    rr = signal.get('rr')
    regime = signal.get('regime', 'Inconnu')

    def fmt_price(value: Optional[float]) -> str:
        return "N/A" if value is None else f"{value:.5f}"

    message = (
        f"✅ <b>Signal Confirmé</b> {side_icon} {side_text}\n\n"
        f"Paire: <code>{html.escape(symbol)}</code>\n"
        f"Type: <b>{html.escape(regime)}</b>\n\n"
        f"Entrée: <code>{fmt_price(entry)}</code>\n"
        f"SL: <code>{fmt_price(sl)}</code>\n"
        f"TP: <code>{fmt_price(tp)}</code>\n"
    )

    if rr is not None:
        message += f"RR Actualisé: <b>x{rr:.2f}</b>"

    tg_send(message, chat_id=TG_ALERTS_CHAT_ID or TG_CHAT_ID)

def send_confirmed_signal_notification(symbol: str, signal: Dict[str, Any], total_found: int):
    """
    Notifie l'utilisateur que le bot a choisi le meilleur signal parmi plusieurs.
    """
    message = (
        f"🎯 **Signal Sélectionné**\n\n"
        f"Parmi <code>{total_found}</code> opportunités détectées, le signal avec le plus haut RR a été choisi pour exécution :\n\n"
        f" paire: <b>{_escape(symbol)}</b>\n"
        f" Côté: <b>{signal['side'].upper()}</b>\n"
        f" RR: <b>x{signal['rr']:.2f}</b>"
    )
    tg_send(message)

def tg_send_with_photo(photo_buffer: io.BytesIO, caption: str, chat_id: Optional[str] = None):
    """Envoie un message avec photo. Peut cibler un chat_id spécifique."""
    target_chat_id = chat_id if chat_id else TG_CHAT_ID
    if not target_chat_id: return

    if not photo_buffer:
        return tg_send(caption, chat_id=target_chat_id)
    try:
        files = {'photo': ('trade_setup.png', photo_buffer, 'image/png')}
        payload = {"chat_id": target_chat_id, "caption": caption, "parse_mode": "HTML"}
        requests.post(f"{TELEGRAM_API}/sendPhoto", data=payload, files=files, timeout=20)
    except Exception:
        tg_send(f"⚠️ Erreur de graphique\n{caption}", chat_id=target_chat_id)

def format_trade_message(symbol, signal, quantity, mode, risk) -> str:
    """Formate le message d'un nouveau trade."""
    side_icon = "📈" if signal['side'] == 'buy' else "📉"
    mode_icon = "📝" if mode == 'PAPIER' else "✅"
    side_text = "LONG" if signal['side'] == 'buy' else "SHORT"
    return (
        f"{mode_icon} <b>{mode} | Nouveau Trade {side_icon} {side_text}</b>\n\n"
        f" paire: <code>{html.escape(symbol)}</code>\n"
        f" Type: <b>{html.escape(signal['regime'].capitalize())}</b>\n\n"
        f" Entrée: <code>{signal['entry']:.5f}</code>\n"
        f" SL: <code>{signal['sl']:.5f}</code>\n"
@@ -171,29 +205,43 @@ def format_synced_open_positions(exchange_positions: List[Dict], db_positions: L
    lines = ["<b>📊 Positions Ouvertes (Synchronisé)</b>\n"]
    
    if synced_symbols:
        lines.append("--- POSITIONS SYNCHRONISÉES ---")
        synced_db_pos = [p for p in db_positions if p['symbol'].replace('/', '') in synced_symbols]
        for pos in synced_db_pos:
            side_icon = "📈" if pos.get('side') == 'buy' else "📉"
            lines.append(f"<b>{pos.get('id')}. {side_icon} {html.escape(pos.get('symbol', 'N/A'))}</b>")
    
    if ghost_symbols:
        lines.append("\n⚠️ <b>Positions FANTÔMES</b> (sur l'exchange, pas dans la DB):")
        for symbol in ghost_symbols:
            lines.append(f"- <code>{symbol}</code>")
    
    if zombie_symbols:
        lines.append("\n🔍 <b>Positions DÉSYNCHRONISÉES</b> (dans la DB, pas sur l'exchange):")
        for symbol in zombie_symbols:
            lines.append(f"- <code>{symbol.replace('USDT', '/USDT')}</code>")

    tg_send("\n".join(lines), reply_markup=get_positions_keyboard(db_positions))

def tg_send_error(title: str, error: Any):
    error_text = str(error)
    tg_send(f"❌ <b>Erreur: {html.escape(title)}</b>\n<code>{html.escape(error_text)}</code>")

def send_report(title: str, trades: List[Dict[str, Any]], balance: Optional[float], days: Optional[int] = None):
    filtered_trades = trades

    if days is not None and days > 0:
        cutoff = time.time() - days * 24 * 60 * 60
        filtered_trades = []
        for trade in trades:
            close_ts = trade.get('close_timestamp') or 0
            open_ts = trade.get('open_timestamp') or 0
            trade_ts = close_ts or open_ts
            if trade_ts >= cutoff:
                filtered_trades.append(trade)

    stats = reporting.get_report_stats(filtered_trades)
    message = reporting.format_report_message(title, stats, balance)
    if days is not None and days > 0 and stats.get('total_trades', 0) == 0:
        message += f"\n\n<i>Aucun trade sur les {days} derniers jours.</i>"
    tg_send(message)
