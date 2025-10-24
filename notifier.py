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
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
TELEGRAM_API = f"https://api.telegram.org/bot{TG_TOKEN}"

def _escape(text: str) -> str: return html.escape(str(text))

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

def send_validated_signal_report(symbol: str, signal: Dict, is_taken: bool, reason: str, is_control_only: bool = False):
    """Envoie un rapport de signal validé, avec le statut d'exécution."""
    side_icon = "📈" if signal['side'] == 'buy' else "📉"
    side_text = "LONG" if signal['side'] == 'buy' else "SHORT"
    status_icon = "✅" if is_taken else "❌"
    status_text = "<b>Position Ouverte</b>" if is_taken else f"<b>Position NON Ouverte</b>\n   - Raison: <i>{html.escape(reason)}</i>"

    message = (
        f"<b>{status_icon} Signal {side_icon} {side_text} {'Pris' if is_taken else 'Rejeté'}</b>\n\n"
        f" paire: <code>{html.escape(symbol)}</code>\n"
        f" Type: <b>{html.escape(signal['regime'].capitalize())}</b>\n\n"
        f" Entrée: <code>{signal['entry']:.5f}</code>\n"
        f" SL: <code>{signal['sl']:.5f}</code>\n"
        f" TP: <code>{signal['tp']:.5f}</code>\n"
        f" RR: <b>x{signal['rr']:.2f}</b>\n\n"
        f"{status_text}"
    )

    target_chat_id = TG_CHAT_ID if is_control_only else (TG_ALERTS_CHAT_ID or TG_CHAT_ID)
    tg_send(message, chat_id=target_chat_id)

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
# ==============================================================================
# FONCTIONS DE COMMUNICATION DE BASE
# ==============================================================================

def send_config_message(min_rr: float, risk: float, max_pos: int, leverage: int):
    """Envoie un message affichant la configuration actuelle du bot."""
    message = ( f"<b>⚙️ Configuration Actuelle</b>\n\n"
               f" - RR Min: <code>{min_rr}</code>\n - Risque: <code>{risk}%</code>\n"
               f" - Positions Max: <code>{max_pos}</code>\n - Levier: <code>x{leverage}</code>" )
    tg_send(message)
    
def _escape(text: str) -> str:
    """Échappe les caractères HTML."""
    return html.escape(str(text))

def send_mode_message(is_testnet: bool, is_paper: bool):
    """Envoie un message affichant les modes de fonctionnement avec des boutons."""
    platform_mode = "TESTNET" if is_testnet else "LIVE"
    trading_mode = "PAPIER" if is_paper else "RÉEL"
    message = ( f"<b>🖥️ Modes de Fonctionnement</b>\n\n"
               f"<b>Plateforme :</b> {platform_mode}\n<i>(Défini au démarrage)</i>\n\n"
               f"<b>Trading :</b> {trading_mode}\n<i>(Changez ci-dessous)</i>" )
    tg_send(message, reply_markup=get_trading_mode_keyboard(is_paper))

def tg_send(text: str, reply_markup: Optional[Dict] = None):
    if not TG_TOKEN or not TG_CHAT_ID: return
        
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
        
def send_breakeven_notification(symbol: str, pnl_realised: float, remaining_qty: float):
    """Envoie une notification de mise à breakeven."""
    message = (
        f"<b>⚙️ Gestion de Trade sur {_escape(symbol)}</b>\n\n"
        f"✅ <b>MM20 atteinte !</b> Prise de profit partielle.\n"
        f"   - Gain réalisé: <code>{pnl_realised:.2f} USDT</code>\n\n"
        f"🛡️ <b>Trade sécurisé à Breakeven.</b>\n"
        f"   - Quantité restante: <code>{remaining_qty:.4f}</code>"
    )
    tg_send(message, chat_id=TG_ALERTS_CHAT_ID or TG_CHAT_ID)

def tg_send_with_photo(photo_buffer: io.BytesIO, caption: str, chat_id: Optional[str] = None):
    """Envoie un message avec photo. Peut cibler un chat_id spécifique."""
    target_chat_id = chat_id if chat_id else TG_CHAT_ID
    if not target_chat_id:
        return
        
    if not photo_buffer:
        return tg_send(caption, chat_id=target_chat_id)
        
    try:
        files = {'photo': ('trade_setup.png', photo_buffer, 'image/png')}
        payload = {"chat_id": target_chat_id, "caption": caption, "parse_mode": "HTML"}
        requests.post(f"{TELEGRAM_API}/sendPhoto", data=payload, files=files, timeout=20)
    except Exception as e:
        # CORRECTION : Un bloc 'except' doit contenir du code.
        print(f"Erreur d'envoi de photo Telegram: {e}")
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
            if data.get("ok"):
                return data.get("result", [])
    except Exception as e:
        print(f"Erreur lors de la récupération des updates Telegram: {e}")
        
    return []

# ==============================================================================
# GESTION DES CLAVIERS INTERACTIFS
# ==============================================================================

def get_config_menu_keyboard() -> Dict:
    """Crée le clavier pour le menu de configuration."""
    return {
        "inline_keyboard": [
            [{"text": "🔩 Afficher Config Actuelle", "callback_data": "show_config"}],
            [{"text": "🖥️ Changer Mode (Papier/Réel)", "callback_data": "show_mode"}],
            [{"text": "🗓️ Changer Stratégie", "callback_data": "manage_strategy"}],
            [{"text": "↩️ Retour au Menu Principal", "callback_data": "main_menu"}]
        ]
    }

def get_main_menu_keyboard(is_paused: bool) -> Dict:
    pause_resume_btn = {"text": "▶️ Relancer", "callback_data": "resume"} if is_paused else {"text": "⏸️ Pauser", "callback_data": "pause"}
    return {"inline_keyboard": [
        [pause_resume_btn, {"text": "🛰️ Ping", "callback_data": "ping"}],
        [{"text": "📊 Positions", "callback_data": "list_positions"}, {"text": "📈 Stats", "callback_data": "get_stats"}],
        [{"text": "⚙️ Configuration", "callback_data": "menu_config"}, {"text": "🚀 Signaux", "callback_data": "menu_signals"}]
    ]}

def get_signals_menu_keyboard() -> Dict:
    """Crée le clavier pour le menu des signaux."""
    return {
        "inline_keyboard": [
            [{"text": "🚀 Signaux (Dernière Heure)", "callback_data": "signals_1h"}],
            [{"text": "⏱️ Signaux (6 Dernières Heures)", "callback_data": "signals_6h"}],
            [{"text": "↩️ Retour", "callback_data": "main_menu"}]
        ]
    }

def get_positions_keyboard(positions: List[Dict[str, Any]]) -> Optional[Dict]:
    """Retourne le clavier pour la gestion des positions ouvertes."""
    if not positions: return None
    keyboard = []
    for pos in positions:
        keyboard.append([{"text": f"❌ Clôturer Trade #{pos.get('id', 0)}", "callback_data": f"close_trade_{pos.get('id', 0)}"}])
    return {"inline_keyboard": keyboard}
    
def get_strategy_menu_keyboard(current_strategy: str) -> Dict:
    """Retourne le clavier du menu de stratégie."""
    buttons = []
    if current_strategy == 'NORMAL':
        buttons.append([
            {"text": "✅ NORMAL (Actuel)", "callback_data": "no_change"}, 
            {"text": "➡️ Passer en SPLIT", "callback_data": "switch_to_SPLIT"}
        ])
    else: # Si c'est SPLIT
        buttons.append([
            {"text": "➡️ Passer en NORMAL", "callback_data": "switch_to_NORMAL"}, 
            {"text": "✅ SPLIT (Actuel)", "callback_data": "no_change"}
        ])
    buttons.append([{"text": "↩️ Retour au Menu Principal", "callback_data": "main_menu"}])
    return {"inline_keyboard": buttons}

def get_trading_mode_keyboard(is_paper: bool) -> Dict:
    """Crée le clavier pour changer de mode de trading."""
    buttons = []
    if is_paper:
        buttons.append([
            {"text": "✅ PAPIER (Actuel)", "callback_data": "no_change"},
            {"text": "➡️ Passer en RÉEL", "callback_data": "switch_to_REAL"}
        ])
    else: # Si c'est RÉEL
        buttons.append([
            {"text": "➡️ Passer en PAPIER", "callback_data": "switch_to_PAPER"},
            {"text": "✅ RÉEL (Actuel)", "callback_data": "no_change"}
        ])
    
    buttons.append([{"text": "↩️ Retour", "callback_data": "main_menu"}])
    return {"inline_keyboard": buttons}
    
# ==============================================================================
# MESSAGES FORMATÉS
# ==============================================================================

def send_start_banner(platform: str, trading: str, risk: float):
    """Envoie la bannière de démarrage."""
    tg_send(f"<b>🔔 Darwin Bot Démarré</b>\n\n- Plateforme: <code>{_escape(platform)}</code>\n- Mode: <b>{_escape(trading)}</b>\n- Risque: <code>{risk}%</code>")
    
def send_main_menu(is_paused: bool):
    tg_send("🤖 **Panneau de Contrôle**", reply_markup=get_main_menu_keyboard(is_paused))

def send_config_menu():
    tg_send("⚙️ **Menu Configuration**", reply_markup=get_config_menu_keyboard())
    
def send_signals_menu():
    tg_send("🚀 **Menu Signaux**", reply_markup=get_signals_menu_keyboard())

def send_strategy_menu(current_strategy: str):
    """Envoie le menu de sélection de stratégie."""
    message = (
        f"<b>⚙️ Gestion de la Stratégie</b>\n\n"
        f"Définit comment les trades de <b>contre-tendance</b> sont gérés.\n\n"
        f"Stratégie Actuelle: <b><code>{current_strategy}</code></b>"
    )
    tg_send(message, reply_markup=get_strategy_menu_keyboard(current_strategy))

def send_mode_message(is_testnet: bool, is_paper: bool):
    """Envoie un message affichant les modes de fonctionnement avec des boutons."""
    platform_mode = "TESTNET" if is_testnet else "LIVE"
    trading_mode = "PAPIER" if is_paper else "RÉEL"
    
    message = (
        f"<b>🖥️ Modes de Fonctionnement</b>\n\n"
        f"<b>Plateforme :</b> {platform_mode}\n"
        f"<i>(Défini au démarrage du bot)</i>\n\n"
        f"<b>Trading :</b> {trading_mode}\n"
        f"<i>(Vous pouvez changer le mode de trading ci-dessous)</i>"
    )
    
    tg_send(message, reply_markup=get_trading_mode_keyboard(is_paper))

def send_config_message(config: Dict):
    lines = ["<b>🔩 Configuration Actuelle</b>\n"]
    for key, value in config.items():
        lines.append(f"- {_escape(key)}: <code>{_escape(value)}</code>")
    tg_send("\n".join(lines))
    
def send_report(title: str, trades: List[Dict[str, Any]], balance: Optional[float]):
    """Calcule les statistiques et envoie un rapport."""
    stats = reporting.calculate_performance_stats(trades)
    message = reporting.format_report_message(title, stats, balance)
    tg_send(message)

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

def format_synced_open_positions(exchange_positions: List[Dict], db_positions: List[Dict]):
    """Formate et envoie un rapport complet des positions ouvertes, synchronisé avec l'exchange."""
    open_exchange_symbols = {p['info']['symbol'] for p in exchange_positions if p.get('contracts') and float(p['contracts']) > 0}
    open_db_symbols = {p['symbol'].replace('/', '') for p in db_positions}
    
    synced_symbols = open_exchange_symbols.intersection(open_db_symbols)
    ghost_symbols = open_exchange_symbols - open_db_symbols
    zombie_symbols = open_db_symbols - open_exchange_symbols

    if not open_exchange_symbols and not open_db_symbols:
        return tg_send("✅ Aucune position ouverte (vérifié sur l'exchange et dans la DB).")

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
    """Envoie un message d'erreur formaté."""
    error_text = str(error)
    tg_send(f"❌ <b>Erreur: {html.escape(title)}</b>\n<code>{html.escape(error_text)}</code>")

def send_report(title: str, trades: List[Dict[str, Any]], balance: Optional[float]):
    """Calcule les stats et envoie un rapport."""
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
        f"🛡️ **Trade Sécurisé sur {_escape(symbol)} !**\n\n"
        f"Prise de profit partielle à la MM20 avec un gain de <code>{pnl_realised:.2f} USDT</code>.\n"
        f"Le reste de la position est maintenant à breakeven (risque zéro)."
    )
    tg_send(message, chat_id=TG_ALERTS_CHAT_ID)
