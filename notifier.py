# Fichier: notifier.py
import os
import time
import html
import requests
import io
from typing import List, Dict, Any, Optional
import reporting
import database
import trader


# --- PARAMÈTRES TELEGRAM ---
TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
TELEGRAM_API = f"https://api.telegram.org/bot{TG_TOKEN}"
TG_ALERTS_CHAT_ID = os.getenv("TELEGRAM_ALERTS_CHAT_ID", "")

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

def tg_edit_message_text(text: str, chat_id: str, message_id: int, reply_markup: Optional[Dict] = None):
    if not TG_TOKEN or not chat_id or not message_id:
        return
    try:
        payload = {
            "chat_id": chat_id,
            "message_id": int(message_id),
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True
        }
        if reply_markup:
            payload["reply_markup"] = reply_markup
        requests.post(f"{TELEGRAM_API}/editMessageText", json=payload, timeout=10)
    except Exception as e:
        print(f"Erreur editMessageText: {e}")
        
def tg_answer_callback_query(callback_query_id: str, text: str = ""):
    """Accuse réception d'un clic sur un bouton inline Telegram (évite l'impression que rien ne se passe)."""
    if not TG_TOKEN or not callback_query_id:
        return
    try:
        payload = {"callback_query_id": callback_query_id}
        if text:
            payload["text"] = text
        requests.post(f"{TELEGRAM_API}/answerCallbackQuery", json=payload, timeout=10)
    except Exception as e:
        print(f"Erreur answerCallbackQuery: {e}")

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

# ==============================================================================
# FONCTIONS DE COMMUNICATION DE BASE
# ==============================================================================

def send_mode_message(is_testnet: bool, is_paper: bool):
    platform_mode = "TESTNET" if is_testnet else "LIVE"
    trading_mode = "PAPIER" if is_paper else "RÉEL"
    text = (
        f"<b>🖥️ Modes de Fonctionnement</b>\n\n"
        f"<b>Plateforme :</b> {platform_mode}\n"
        f"<i>(Défini au démarrage du bot)</i>\n\n"
        f"<b>Trading :</b> {trading_mode}\n"
        f"<i>(Vous pouvez changer le mode de trading ci-dessous)</i>"
    )
    keyboard = get_trading_mode_keyboard(is_paper)
    msg_id = database.get_setting('MAIN_MENU_MESSAGE_ID', None)

    if TG_TOKEN and TG_CHAT_ID and msg_id:
        try:
            payload_edit = {
                "chat_id": TG_CHAT_ID,
                "message_id": int(msg_id),
                "text": text,
                "parse_mode": "HTML",
                "reply_markup": keyboard
            }
            r = requests.post(f"{TELEGRAM_API}/editMessageText", json=payload_edit, timeout=10)
            data = r.json()
            if data.get("ok"):
                return
        except Exception as e:
            print(f"Erreur editMessageText (mode): {e}")

    try:
        payload_send = {"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML", "reply_markup": keyboard}
        r = requests.post(f"{TELEGRAM_API}/sendMessage", json=payload_send, timeout=10)
        data = r.json()
        if data.get("ok"):
            database.set_setting('MAIN_MENU_MESSAGE_ID', str(data["result"]["message_id"]))
    except Exception as e:
        print(f"Erreur sendMessage (mode): {e}")

def open_offset_root_panel(chat_id: Optional[str] = None, message_id: Optional[int] = None):
    """Menu racine Offset : choix TP ou SL. Édite en place si message_id fourni, sinon envoie un nouveau message."""
    kb = {
        "inline_keyboard": [
            [
                {"text": "⚙️ Offset TP (Bollinger)", "callback_data": "OFS:ROOT:TP"},
                {"text": "🛡️ Offset SL", "callback_data": "OFS:ROOT:SL"},
            ],
            [
                {"text": "↩︎ Menu principal", "callback_data": "OFS:BACK"},
            ],
        ]
    }
    txt = "Choisis le paramètre à ajuster :"
    if chat_id and message_id:
        tg_edit_message_text(txt, str(chat_id), int(message_id), kb)
    else:
        tg_send(txt, reply_markup=kb, chat_id=chat_id)


def open_offset_tp_panel(chat_id: Optional[str] = None, message_id: Optional[int] = None):
    """Rafraîchit le panneau d’offset TP (Bollinger) EN PLACE (aucun nouvel envoi)."""
    if not chat_id or not message_id:
        return
    try:
        cur = float(database.get_setting('TP_BB_OFFSET_PCT', '0.0100'))
    except Exception:
        cur = 0.01
    txt = (
        f"⚙️ <b>Offset TP (Bollinger)</b>\n\n"
        f"<b>Valeur actuelle : {cur*100:.2f} %</b>\n\n"
        f"• Plage autorisée : 0,05 % ↔ 10,00 %\n"
        f"• Pas via ± : 0,01 %\n"
        f"• Presets rapides :  0,50 % / 1,00 % / 2,00 % / 5,00 %"
    )
    kb = {
        "inline_keyboard": [
            [
                {"text": "−", "callback_data": "OFS:TP:INC:-"},
                {"text": "+", "callback_data": "OFS:TP:INC:+"}
            ],
            [
                {"text": "0,50 %", "callback_data": "OFS:TP:SET:0.0050"},
                {"text": "1,00 %", "callback_data": "OFS:TP:SET:0.0100"},
                {"text": "2,00 %", "callback_data": "OFS:TP:SET:0.0200"}
            ],
            [
                {"text": "5,00 %", "callback_data": "OFS:TP:SET:0.0500"},
                {"text": "⟲ Défaut (1,00 %)", "callback_data": "OFS:TP:DEF"},
                {"text": "⬅︎ Retour", "callback_data": "OFS:ROOT"}
            ]
        ]
    }
    tg_edit_message_text(txt, str(chat_id), int(message_id), kb)

def add_main_menu_return_button(reply_markup: Optional[Dict]) -> Dict:
    """
    Ajoute un bouton '↩️ Retour au Menu Principal' (callback_data='main_menu')
    en dernière ligne d'un inline keyboard Telegram.
    """
    try:
        btn = {"text": "↩️ Retour au Menu Principal", "callback_data": "main_menu"}
        if not reply_markup or not isinstance(reply_markup, dict):
            return {"inline_keyboard": [[btn]]}

        inline_kb = reply_markup.get("inline_keyboard")
        if not isinstance(inline_kb, list):
            reply_markup["inline_keyboard"] = [[btn]]
            return reply_markup

        # Évite les doublons si déjà présent
        for row in inline_kb:
            if isinstance(row, list):
                for b in row:
                    if isinstance(b, dict) and b.get("callback_data") == "main_menu":
                        return reply_markup

        inline_kb.append([btn])
        return reply_markup
    except Exception:
        # Fallback ultra-sûr
        return {"inline_keyboard": [[{"text": "↩️ Retour au Menu Principal", "callback_data": "main_menu"}]]}



def open_offset_sl_panel(chat_id: Optional[str] = None, message_id: Optional[int] = None):
    """Rafraîchit le panneau d’offset SL EN PLACE (aucun nouvel envoi)."""
    if not chat_id or not message_id:
        return
    try:
        cur = float(database.get_setting('SL_OFFSET_PCT', '0.0100'))
    except Exception:
        cur = 0.01
    txt = (
        f"🛡️ <b>Offset SL (padding)</b>\n\n"
        f"<b>Valeur actuelle : {cur*100:.2f} %</b>\n\n"
        f"• Plage autorisée : 0,05 % ↔ 10,00 %\n"
        f"• Pas via ± : 0,01 %\n"
        f"• Presets rapides :  0,50 % / 1,00 % / 2,00 % / 5,00 %"
    )
    kb = {
        "inline_keyboard": [
            [
                {"text": "−", "callback_data": "OFS:SL:INC:-"},
                {"text": "+", "callback_data": "OFS:SL:INC:+"}
            ],
            [
                {"text": "0,50 %", "callback_data": "OFS:SL:SET:0.0050"},
                {"text": "1,00 %", "callback_data": "OFS:SL:SET:0.0100"},
                {"text": "2,00 %", "callback_data": "OFS:SL:SET:0.0200"}
            ],
            [
                {"text": "5,00 %", "callback_data": "OFS:SL:SET:0.0500"},
                {"text": "⟲ Défaut (1,00 %)", "callback_data": "OFS:SL:DEF"},
                {"text": "⬅︎ Retour", "callback_data": "OFS:ROOT"}
            ]
        ]
    }
    tg_edit_message_text(txt, str(chat_id), int(message_id), kb)



def handle_offset_callback(cb_data: str, chat_id: Optional[str] = None, message_id: Optional[int] = None, callback_query_id: Optional[str] = None):
    """
    Callbacks Offset: on édite le message existant (pas de tg_send).
    """
    if not cb_data or not cb_data.startswith("OFS:"):
        return

    # NEW: afficher le menu racine quand on clique depuis la config (OFS:ROOT)
    if cb_data == "OFS:ROOT":
        kb = {
            "inline_keyboard": [
                [
                    {"text": "⚙️ Offset TP (Bollinger)", "callback_data": "OFS:ROOT:TP"},
                    {"text": "🛡️ Offset SL", "callback_data": "OFS:ROOT:SL"},
                ],
                [
                    {"text": "↩︎ Menu principal", "callback_data": "main_menu"},
                ],
            ]
        }
        if chat_id and message_id:
            tg_edit_message_text("Choisis le paramètre à ajuster :", str(chat_id), int(message_id), kb)
        if callback_query_id:
            tg_answer_callback_query(callback_query_id)
        return

    MIN_V, MAX_V, STEP = 0.0005, 0.1, 0.0001
    def _clamp(v: float) -> float: return max(MIN_V, min(MAX_V, v))

    parts = cb_data.split(":")
    if len(parts) < 3:
        if callback_query_id: tg_answer_callback_query(callback_query_id)
        return

    scope = parts[1]                  # "TP" ou "SL"
    action = parts[2]
    key = 'TP_BB_OFFSET_PCT' if scope == "TP" else 'SL_OFFSET_PCT'
    default_val = 0.01

    try:
        cur = float(database.get_setting(key, f"{default_val}"))
    except Exception:
        cur = default_val

    if action == "INC" and len(parts) == 4:
        sign = parts[3]
        cur = _clamp(cur + STEP) if sign == "+" else _clamp(cur - STEP)
        database.set_setting(key, f"{cur:.6f}")
        (open_offset_tp_panel if scope == "TP" else open_offset_sl_panel)(chat_id=chat_id, message_id=message_id)
        if callback_query_id: tg_answer_callback_query(callback_query_id)
        return

    if action == "SET" and len(parts) == 4:
        try:
            val = _clamp(float(parts[3]))
        except Exception:
            val = cur
        database.set_setting(key, f"{val:.6f}")
        (open_offset_tp_panel if scope == "TP" else open_offset_sl_panel)(chat_id=chat_id, message_id=message_id)
        if callback_query_id: tg_answer_callback_query(callback_query_id)
        return

    if action == "DEF":
        val = default_val
        database.set_setting(key, f"{val:.6f}")
        (open_offset_tp_panel if scope == "TP" else open_offset_sl_panel)(chat_id=chat_id, message_id=message_id)
        if callback_query_id: tg_answer_callback_query(callback_query_id)
        return

    if parts[1] == "ROOT" and len(parts) >= 3:
        if parts[2] == "TP":
            open_offset_tp_panel(chat_id=chat_id, message_id=message_id)
        elif parts[2] == "SL":
            open_offset_sl_panel(chat_id=chat_id, message_id=message_id)
        if callback_query_id: tg_answer_callback_query(callback_query_id)
        return



def offset_command(chat_id: Optional[str] = None):
    """Commande /offset : ouvre le menu racine (choix TP/SL)."""
    open_offset_root_panel(chat_id=chat_id)


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
    """Crée le clavier pour le menu de configuration (avec bouton Redémarrer, protégé dans ce menu)."""
    cw = str(database.get_setting('CUT_WICK_FOR_RR', 'false')).lower() == 'true'
    cw_label = f"{'🟢' if cw else '⚪️'} Cut-wick RR≥2.8 : {'ON' if cw else 'OFF'}"
    return {
        "inline_keyboard": [
            [{"text": "📝 Changer Mode (Papier/Réel)", "callback_data": "show_mode"}],
            [{"text": "🔩 Afficher Config Actuelle", "callback_data": "show_config"}],
            [{"text": cw_label, "callback_data": "toggle_cutwick"}],
            [{"text": "💹 Offset TP/SL", "callback_data": "OFS:ROOT"}],
            [{"text": "🗓️ Changer Stratégie", "callback_data": "manage_strategy"}],
            [{"text": "🛑 Redémarrer le bot", "callback_data": "restart_bot"}],
            [{"text": "↩️ Retour au Menu Principal", "callback_data": "main_menu"}]
        ]
    }


def get_main_menu_keyboard(is_paused: bool) -> Dict:
    pause_resume_btn = {"text": "💹 Relancer", "callback_data": "resume"} if is_paused else {"text": "⏸️ Pause", "callback_data": "pause"}
    return {"inline_keyboard": [
        [pause_resume_btn, {"text": "🛰️ Ping", "callback_data": "ping"}],
        [{"text": "🚀 Signaux", "callback_data": "menu_signals"}, {"text": "📈 Stats", "callback_data": "get_stats"}],
        [{"text": "📊 Positions", "callback_data": "list_positions"}, {"text": "⚙️ Configuration", "callback_data": "menu_config"}]
    ]}

def get_signals_menu_keyboard() -> Dict:
    """Crée le clavier pour le menu des signaux."""
    return {
        "inline_keyboard": [
            [{"text": "⏱️ Signal(s) en attente", "callback_data": "signals_pending"}],
            [{"text": "🚀 Signaux (Dernière Heure)", "callback_data": "signals_1h"}],
            [{"text": "⏱️ Signaux (6 Dernières Heures)", "callback_data": "signals_6h"}],
            [{"text": "↩️ Retour", "callback_data": "main_menu"}]
        ]
    }

def get_positions_keyboard(positions: List[Dict[str, Any]]) -> Optional[Dict]:
    """Retourne le clavier pour la gestion des positions ouvertes + bouton retour menu principal."""
    if not positions:
        return {"inline_keyboard": [[{"text": "↩️ Retour au Menu Principal", "callback_data": "main_menu"}]]}
    keyboard = []
    for pos in positions:
        keyboard.append([{"text": f"❌ Clôturer Trade #{pos.get('id', 0)}", "callback_data": f"close_trade_{pos.get('id', 0)}"}])
    # Ligne retour (toujours visible)
    keyboard.append([{"text": "↩️ Retour au Menu Principal", "callback_data": "main_menu"}])
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

def _restart_confirm_keyboard() -> Dict:
    """Clavier de confirmation pour le redémarrage (2 étapes)."""
    return {
        "inline_keyboard": [
            [
                {"text": "✅ Oui, redémarrer", "callback_data": "confirm_restart_bot"},
                {"text": "❌ Annuler", "callback_data": "cancel_restart_bot"}
            ]
        ]
    }


def handle_restart_callback(callback_query: Dict[str, Any]) -> None:
    """
    Étape 1 : clic sur '🔁 Redémarrer le bot' dans le menu Configuration.
    Affiche une confirmation en 2 boutons (Oui/Annuler), avec cooldown.
    """
    try:
        cq_id = str(callback_query.get("id", "")) if callback_query else ""
        if cq_id:
            tg_answer_callback_query(cq_id, "Confirmez le redémarrage…")

        # Cooldown anti-spam (paramétrable)
        try:
            cooldown = int(database.get_setting('RESTART_COOLDOWN_SEC', 60))
        except Exception:
            cooldown = 60
        now = int(time.time())
        try:
            last = int(database.get_setting('LAST_RESTART_TS', 0))
        except Exception:
            last = 0
        remaining = cooldown - (now - last)
        if remaining > 0:
            if cq_id:
                tg_answer_callback_query(cq_id, f"Cooldown actif ({remaining}s)…")
            tg_send(f"⏱️ Redémarrage refusé (cooldown {remaining}s).")
            return

        # Éditer le message courant pour afficher la confirmation
        message = (callback_query or {}).get("message") or {}
        chat_id = (message.get("chat") or {}).get("id")
        message_id = message.get("message_id")
        if chat_id and message_id:
            try:
                requests.post(
                    f"{TELEGRAM_API}/editMessageReplyMarkup",
                    json={
                        "chat_id": chat_id,
                        "message_id": message_id,
                        "reply_markup": _restart_confirm_keyboard()
                    },
                    timeout=10
                )
            except Exception as e:
                print(f"Erreur editMessageReplyMarkup (restart confirm): {e}")
    except Exception as e:
        tg_send_error("Redémarrage (étape 1)", e)

def handle_restart_confirm(callback_query: Dict[str, Any]) -> None:
    """
    Étape 2 : confirmation '✅ Oui, redémarrer'.
    Pose le timestamp de cooldown, notifie, puis quitte le process (superviseur relance).
    """
    try:
        cq_id = str(callback_query.get("id", "")) if callback_query else ""
        if cq_id:
            tg_answer_callback_query(cq_id, "Redémarrage…")

        # Poser le timestamp pour le cooldown
        try:
            now = int(time.time())
            database.set_setting('LAST_RESTART_TS', str(now))
        except Exception:
            pass

        from_obj = (callback_query or {}).get("from", {}) or {}
        user_label = from_obj.get("username") or from_obj.get("first_name") or "inconnu"

        msg_chat_id = (((callback_query or {}).get("message", {}) or {}).get("chat", {}) or {}).get("id")
        tg_send(f"♻️ Redémarrage demandé par <b>{_escape(user_label)}</b>. Le bot va redémarrer…", chat_id=msg_chat_id or TG_CHAT_ID)

        time.sleep(1.0)
        os._exit(0)
    except Exception as e:
        tg_send_error("Redémarrage (étape 2)", e)
        
def handle_restart_cancel(callback_query: Dict[str, Any]) -> None:
    """Annule la confirmation et ré-affiche le menu Configuration."""
    try:
        cq_id = str(callback_query.get("id", "")) if callback_query else ""
        if cq_id:
            tg_answer_callback_query(cq_id, "Annulé.")
        # Réafficher le menu configuration
        try:
            text = "⚙️ <b>Menu Configuration</b>"
            keyboard = get_config_menu_keyboard()
            msg = (callback_query or {}).get("message") or {}
            chat_id = (msg.get("chat") or {}).get("id")
            message_id = msg.get("message_id")
            if chat_id and message_id:
                requests.post(
                    f"{TELEGRAM_API}/editMessageText",
                    json={
                        "chat_id": chat_id,
                        "message_id": message_id,
                        "text": text,
                        "parse_mode": "HTML",
                        "reply_markup": keyboard
                    },
                    timeout=10
                )
        except Exception as e:
            print(f"Erreur retour menu config: {e}")
    except Exception as e:
        tg_send_error("Redémarrage (annulation)", e)

def try_handle_inline_callback(data: Dict[str, Any]) -> bool:
    """
    Route Offset: transmet message_id + callback_query_id pour l’édition en place.
    """
    try:
        if not data: return False
        cmd = data.get("data")
        if not cmd: return False

        if cmd.startswith("OFS:"):
            msg = data.get("message") or {}
            chat = msg.get("chat") or {}
            chat_id = chat.get("id")
            message_id = msg.get("message_id")
            cq_id = data.get("id")
            handle_offset_callback(cmd, chat_id=chat_id, message_id=message_id, callback_query_id=cq_id)
            return True

        if cmd == "restart_bot": handle_restart_callback(data); return True
        if cmd == "confirm_restart_bot": handle_restart_confirm(data); return True
        if cmd == "cancel_restart_bot": handle_restart_cancel(data); return True
        return False
    except Exception as e:
        tg_send_error("Callback routing", e)
        return False

def send_commands_help(chat_id: Optional[str] = None):
    """
    Affiche le bloc 'Commandes' dans Telegram, même format/emoji que setuniverse & setmaxpos,
    avec ajout de /offset pour ouvrir le panneau TP/SL.
    """
    lines = [
        "🛠️ <b>Commandes</b>",
        "",
        "/setuniverse &lt;nombre&gt;  — 🌐 Taille du scan",
        "/setmaxpos &lt;nombre&gt;    — 🧮 Nb max de trades",
        "/offset                     — ⚙️ Offset TP/SL (panneau)"
    ]
    tg_send("\n".join(lines), chat_id=chat_id)


# ==============================================================================
# MESSAGES FORMATÉS
# ==============================================================================
    
def send_main_menu(is_paused: bool):
    mode_raw = database.get_setting('PAPER_TRADING_MODE', 'true')
    is_paper = str(mode_raw).lower() == 'true'
    mode_text = "PAPIER" if is_paper else "RÉEL"
    etat_text = "PAUSE" if is_paused else "ACTIF"

    # Chips d’état
    mode_chip = "🟦 Mode: <b>PAPIER</b>" if is_paper else "🟩 Mode: <b>RÉEL</b>"
    status_chip = "🟠 État: <b>PAUSE</b>" if is_paused else "🟢 État: <b>ACTIF</b>"

    # Config actuelle
    try:
        min_rr = float(database.get_setting('MIN_RR', os.getenv("MIN_RR", "3.0")))
    except Exception:
        min_rr = 3.0
    try:
        max_pos = int(database.get_setting('MAX_OPEN_POSITIONS', os.getenv("MAX_OPEN_POSITIONS", "3")))
    except Exception:
        max_pos = 3
    risk = getattr(trader, "RISK_PER_TRADE_PERCENT", 1.0)
    leverage = getattr(trader, "LEVERAGE", 1)

    # Stratégie actuelle
    current_strategy = str(database.get_setting('STRATEGY_MODE', 'NORMAL')).upper()
    cw = str(database.get_setting('CUT_WICK_FOR_RR', 'false')).lower() == 'true'
    cw_chip = f"✂️ Couper mèches      : <code>{'ON' if cw else 'OFF'}</code>\n"

    text = (
        f"<b>💹🤖 Darwin Bot</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"{mode_chip} • {status_chip}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>🔧 Configuration</b>\n"
        f"🟩 Risque/Trade : <code>{risk:.1f}%</code>\n"
        f"🟦 Levier       : <code>x{leverage}</code>\n"
        f"🎯 RR Minimum   : <code>{min_rr:.1f}</code>\n"
        f"📊 Positions Max: <code>{max_pos}</code>\n"
        f"🧭 Stratégie    : <code>{_escape(current_strategy)}</code>\n"
        f"{cw_chip}"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>🛠️ Commandes</b>\n"
        f"🌐 <code>/setuniverse &lt;nombre&gt;</code> — Taille du scan\n"
        f"🔢 <code>/setmaxpos &lt;nombre&gt;</code> — Nb max de trades"
    )

    keyboard = get_main_menu_keyboard(is_paused)
    msg_id = database.get_setting('MAIN_MENU_MESSAGE_ID', None)

    # Essayer d'éditer pour éviter le spam
    if TG_TOKEN and TG_CHAT_ID and msg_id:
        try:
            payload_edit = {
                "chat_id": TG_CHAT_ID,
                "message_id": int(msg_id),
                "text": text,
                "parse_mode": "HTML",
                "reply_markup": keyboard
            }
            r = requests.post(f"{TELEGRAM_API}/editMessageText", json=payload_edit, timeout=10)
            data = r.json()
            if data.get("ok"):
                return
        except Exception as e:
            print(f"Erreur editMessageText: {e}")

    # Sinon envoyer et mémoriser l'id (⚠️ pas d’épinglage)
    try:
        payload_send = {"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML", "reply_markup": keyboard}
        r = requests.post(f"{TELEGRAM_API}/sendMessage", json=payload_send, timeout=10)
        data = r.json()
        if data.get("ok"):
            database.set_setting('MAIN_MENU_MESSAGE_ID', str(data["result"]["message_id"]))
    except Exception as e:
        print(f"Erreur sendMessage (menu): {e}")

def send_config_menu():
    text = "⚙️ <b>Menu Configuration</b>"
    keyboard = get_config_menu_keyboard()
    msg_id = database.get_setting('MAIN_MENU_MESSAGE_ID', None)

    # Essayer d'éditer le message de menu existant
    if TG_TOKEN and TG_CHAT_ID and msg_id:
        try:
            payload_edit = {
                "chat_id": TG_CHAT_ID,
                "message_id": int(msg_id),
                "text": text,
                "parse_mode": "HTML",
                "reply_markup": keyboard
            }
            r = requests.post(f"{TELEGRAM_API}/editMessageText", json=payload_edit, timeout=10)
            data = r.json()
            if data.get("ok"):
                return
        except Exception as e:
            print(f"Erreur editMessageText (config): {e}")

    # Sinon, envoyer puis mémoriser l'id (premier lancement)
    try:
        payload_send = {"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML", "reply_markup": keyboard}
        r = requests.post(f"{TELEGRAM_API}/sendMessage", json=payload_send, timeout=10)
        data = r.json()
        if data.get("ok"):
            database.set_setting('MAIN_MENU_MESSAGE_ID', str(data["result"]["message_id"]))
    except Exception as e:
        print(f"Erreur sendMessage (config): {e}")

def send_signals_menu():
    text = "🚀 <b>Menu Signaux</b>"
    keyboard = get_signals_menu_keyboard()
    msg_id = database.get_setting('MAIN_MENU_MESSAGE_ID', None)

    if TG_TOKEN and TG_CHAT_ID and msg_id:
        try:
            payload_edit = {
                "chat_id": TG_CHAT_ID,
                "message_id": int(msg_id),
                "text": text,
                "parse_mode": "HTML",
                "reply_markup": keyboard
            }
            r = requests.post(f"{TELEGRAM_API}/editMessageText", json=payload_edit, timeout=10)
            data = r.json()
            if data.get("ok"):
                return
        except Exception as e:
            print(f"Erreur editMessageText (signaux): {e}")

    try:
        payload_send = {"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML", "reply_markup": keyboard}
        r = requests.post(f"{TELEGRAM_API}/sendMessage", json=payload_send, timeout=10)
        data = r.json()
        if data.get("ok"):
            database.set_setting('MAIN_MENU_MESSAGE_ID', str(data["result"]["message_id"]))
    except Exception as e:
        print(f"Erreur sendMessage (signaux): {e}")

def send_strategy_menu(current_strategy: str):
    text = (
        f"<b>⚙️ Gestion de la Stratégie</b>\n\n"
        f"Définit comment les trades de <b>contre-tendance</b> sont gérés.\n\n"
        f"Stratégie Actuelle: <b><code>{_escape(current_strategy)}</code></b>"
    )
    keyboard = get_strategy_menu_keyboard(current_strategy)
    msg_id = database.get_setting('MAIN_MENU_MESSAGE_ID', None)

    if TG_TOKEN and TG_CHAT_ID and msg_id:
        try:
            payload_edit = {
                "chat_id": TG_CHAT_ID,
                "message_id": int(msg_id),
                "text": text,
                "parse_mode": "HTML",
                "reply_markup": keyboard
            }
            r = requests.post(f"{TELEGRAM_API}/editMessageText", json=payload_edit, timeout=10)
            data = r.json()
            if data.get("ok"):
                return
        except Exception as e:
            print(f"Erreur editMessageText (stratégie): {e}")

    try:
        payload_send = {"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML", "reply_markup": keyboard}
        r = requests.post(f"{TELEGRAM_API}/sendMessage", json=payload_send, timeout=10)
        data = r.json()
        if data.get("ok"):
            database.set_setting('MAIN_MENU_MESSAGE_ID', str(data["result"]["message_id"]))
    except Exception as e:
        print(f"Erreur sendMessage (stratégie): {e}")

def send_config_message(config: Dict):
    lines = ["<b>🔩 Configuration Actuelle</b>\n"]
    for key, value in config.items():
        lines.append(f"- {_escape(key)}: <code>{_escape(value)}</code>")
    cw = str(database.get_setting('CUT_WICK_FOR_RR', 'false')).lower() == 'true'
    lines.append(f"- Cut-wick RR≥2.8: <code>{'ON' if cw else 'OFF'}</code>")  
    tg_send("\n".join(lines))

def send_report(title: str, trades: List[Dict[str, Any]], balance: Optional[float]):
    """Calcule les stats et affiche le rapport dans le même message épinglé (pas de spam)."""
    stats = reporting.calculate_performance_stats(trades)
    text = reporting.format_report_message(title, stats, balance)

    # petit clavier avec un bouton retour vers le menu principal
    keyboard = {"inline_keyboard": [[{"text": "↩️ Retour", "callback_data": "main_menu"}]]}

    msg_id = database.get_setting('MAIN_MENU_MESSAGE_ID', None)

    # 1) Essayer d'éditer le message existant
    if TG_TOKEN and TG_CHAT_ID and msg_id:
        try:
            payload_edit = {
                "chat_id": TG_CHAT_ID,
                "message_id": int(msg_id),
                "text": text,
                "parse_mode": "HTML",
                "reply_markup": keyboard
            }
            r = requests.post(f"{TELEGRAM_API}/editMessageText", json=payload_edit, timeout=10)
            data = r.json()
            if data.get("ok"):
                return
        except Exception as e:
            print(f"Erreur editMessageText (report): {e}")

    # 2) Sinon, envoyer puis mémoriser le nouvel id (premier lancement)
    try:
        payload_send = {
            "chat_id": TG_CHAT_ID,
            "text": text,
            "parse_mode": "HTML",
            "reply_markup": keyboard
        }
        r = requests.post(f"{TELEGRAM_API}/sendMessage", json=payload_send, timeout=10)
        data = r.json()
        if data.get("ok"):
            database.set_setting('MAIN_MENU_MESSAGE_ID', str(data["result"]["message_id"]))
    except Exception as e:
        print(f"Erreur sendMessage (report): {e}")

def format_open_positions(positions: List[Dict[str, Any]]):
    """Affiche les positions ouvertes dans le message principal (pas de spam)."""
    keyboard = get_positions_keyboard(positions) or {"inline_keyboard": [[{"text": "↩️ Retour", "callback_data": "main_menu"}]]}

    if not positions:
        notifier_text = "📊 Aucune position n'est actuellement ouverte."
        edit_main(notifier_text, keyboard)
        return

    lines = ["<b>📊 Positions Ouvertes (DB)</b>\n"]
    for pos in positions:
        side_icon = "📈" if pos.get('side') == 'buy' else "📉"
        lines.append(
            f"<b>{pos.get('id')}. {side_icon} {_escape(pos.get('symbol', 'N/A'))}</b>\n"
            f"   Entrée: <code>{pos.get('entry_price', 0.0):.4f}</code>\n"
            f"   SL: <code>{pos.get('sl_price', 0.0):.4f}</code> | TP: <code>{pos.get('tp_price', 0.0):.4f}</code>"
        )
    message = "\n\n".join(lines)
    edit_main(message, keyboard)

def format_synced_open_positions(exchange_positions: List[Dict], db_positions: List[Dict]):
    """Formate et envoie un rapport complet des positions ouvertes, synchronisé avec l'exchange."""
    # Extraction robuste des symboles exchange
    open_exchange_symbols = set()
    for p in exchange_positions or []:
        try:
            contracts = float(p.get('contracts') or 0.0)
        except Exception:
            contracts = 0.0
        if contracts > 0:
            info = p.get('info') or {}
            sym = info.get('symbol') or p.get('symbol') or ""
            if sym:
                open_exchange_symbols.add(sym)

    open_db_symbols = { (p.get('symbol') or '').replace('/', '') for p in (db_positions or []) }

    synced_symbols = open_exchange_symbols.intersection(open_db_symbols)
    ghost_symbols = open_exchange_symbols - open_db_symbols
    zombie_symbols = open_db_symbols - open_exchange_symbols

    if not open_exchange_symbols and not open_db_symbols:
        return tg_send("✅ Aucune position ouverte (vérifié sur l'exchange et dans la DB).")

    lines = ["<b>📊 Positions Ouvertes (Synchronisé)</b>\n"]

    if synced_symbols:
        lines.append("--- POSITIONS SYNCHRONISÉES ---")
        synced_db_pos = [p for p in db_positions if (p.get('symbol') or '').replace('/', '') in synced_symbols]
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


def format_trade_message(symbol: str, signal: Dict, quantity: float, mode: str, risk: float) -> str:
    """Construit le message pour un trade qui vient d'être ouvert."""
    is_long = (signal['side'] == 'buy')
    side_text = "Long" if is_long else "Short"
    side_emoji = "📈" if is_long else "📉"
    mode_icon = "📝" if mode == 'PAPIER' else "✅"

    return (
        f"{mode_icon} <b>{mode} | Nouveau Trade - {side_text} {side_emoji}</b>\n\n"
        f"Paire: <code>{html.escape(symbol)}</code>\n"
        f"Type: <b>{html.escape(signal['regime'])}</b>\n\n"
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

def edit_main(text: str, reply_markup: Optional[Dict] = None) -> bool:
    """Édite le message principal (épinglé). Envoie un nouveau message si l'id n'existe pas encore."""
    msg_id = database.get_setting('MAIN_MENU_MESSAGE_ID', None)

    # 1) Essayer d'éditer
    if TG_TOKEN and TG_CHAT_ID and msg_id:
        try:
            payload = {
                "chat_id": TG_CHAT_ID,
                "message_id": int(msg_id),
                "text": text,
                "parse_mode": "HTML"
            }
            if reply_markup:
                payload["reply_markup"] = reply_markup
            r = requests.post(f"{TELEGRAM_API}/editMessageText", json=payload, timeout=10)
            data = r.json()
            if data.get("ok"):
                return True
        except Exception as e:
            print(f"Erreur edit_main (edit): {e}")

    # 2) Sinon, envoyer et mémoriser l'id
    try:
        payload_send = {"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML"}
        if reply_markup:
            payload_send["reply_markup"] = reply_markup
        r = requests.post(f"{TELEGRAM_API}/sendMessage", json=payload_send, timeout=10)
        data = r.json()
        if data.get("ok"):
            database.set_setting('MAIN_MENU_MESSAGE_ID', str(data["result"]["message_id"]))
            return True
    except Exception as e:
        print(f"Erreur edit_main (send): {e}")

    return False

def tg_send_error(title: str, error: Any):
    """Envoie un message d'erreur formaté sur Telegram (canal principal)."""
    try:
        err_txt = str(error)
    except Exception:
        err_txt = repr(error)
    tg_send(f"❌ <b>Erreur: {_escape(title)}</b>\n<code>{_escape(err_txt)}</code>")


