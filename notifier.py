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
import charting


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

    # --- Filtre des alertes bruyantes à masquer ---
    # (ex: "⚠️ Exécution du meilleur signal non aboutie: Rejeté: séquence contre-tendance BB20+BB80 non conforme.")
    try:
        txt = str(text) if text is not None else ""
    except Exception:
        txt = "" if text is None else f"{text}"

    blocked_substrings = (
        "Exécution du meilleur signal non aboutie",
        "séquence contre-tendance BB20+BB80 non conforme",
    )
    for pattern in blocked_substrings:
        if pattern in txt:
            # On ignore complètement ce message
            return

    try:
        payload = {"chat_id": target_chat_id, "text": txt, "parse_mode": "HTML"}
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
    """Rapport de validation + mise à jour DB (VALID_TAKEN / VALID_SKIPPED) avec persistance de la raison."""
    try:
        side = str(signal.get('side', '')).lower()
        regime = str(signal.get('regime', '') or '')
        rr = float(signal.get('rr', 0.0) or 0.0)
        entry = float(signal.get('entry', 0.0) or 0.0)
        sl = float(signal.get('sl', 0.0) or 0.0)
        tp = float(signal.get('tp', 0.0) or 0.0)
        timeframe = str(signal.get('timeframe') or database.get_setting('TIMEFRAME', '1h'))
        ts = int(signal.get('ts') or int(time.time() * 1000))

        # --- Persist VALID_* state (+ reason)
        try:
            payload = {
                "side": side,
                "regime": regime,
                "rr": rr,
                "entry": entry,
                "sl": sl,
                "tp": tp,
                "timeframe": timeframe,
                "signal": dict(signal or {}),
                "reason": str(reason or "")
            }
            database.mark_signal_validated(symbol, ts, payload, taken=is_taken)
        except Exception:
            pass

        side_icon = "📈" if side == 'buy' else "📉"
        side_text = "LONG" if side == 'buy' else "SHORT"
        status_icon = "✅" if is_taken else "❌"
        status_text = "Pris" if is_taken else "Rejeté"

        # Détails + raison quand rejeté
        detail_lines = [
            f"paire: <code>{html.escape(symbol)}</code>",
            f"Type: <b>{html.escape(regime.capitalize())}</b>",
            f"Entrée: <code>{entry:.5f}</code>",
            f"SL: <code>{sl:.5f}</code>",
            f"TP: <code>{tp:.5f}</code>",
            f"RR: <b>x{rr:.2f}</b>"
        ]
        if not is_taken and reason:
            detail_lines.append(f"Raison: <i>{html.escape(reason)}</i>")

        message = (
            f"<b>{status_icon} Signal {side_icon} {side_text} {status_text}</b>\n\n"
            + "\n".join(detail_lines)
        )

        target_chat_id = TG_CHAT_ID if is_control_only else (TG_ALERTS_CHAT_ID or TG_CHAT_ID)
        tg_send(message, chat_id=target_chat_id)
    except Exception:
        pass


    
def send_signal_notification(symbol: str, timeframe: str, signal: Dict[str, Any]) -> None:
    """
    Message Telegram propre pour un signal détecté (sans impacter le reste) + UPSERT en DB (state=PENDING).
    Envoie une image si charting renvoie un buffer, sinon texte seul.
    """
    try:
        side = str(signal.get("side", "")).lower()
        regime = signal.get("regime", "-")
        entry = signal.get("entry")
        tp = signal.get("tp")
        sl = signal.get("sl")
        rr = signal.get("rr")
        ts = int(signal.get("ts") or int(time.time() * 1000))

        # --- Persist PENDING in DB (clé logique: symbol/side/timeframe/ts)
        try:
            if side in ("buy", "sell"):
                database.upsert_signal_pending(
                    symbol=symbol,
                    timeframe=str(timeframe or signal.get("timeframe") or database.get_setting('TIMEFRAME', '1h')),
                    ts=ts,
                    side=side,
                    regime=str(regime or ""),
                    rr=float(rr or 0.0),
                    entry=float(entry or 0.0),
                    sl=float(sl or 0.0),
                    tp=float(tp or 0.0),
                )
        except Exception:
            pass

        lines = [
            f"🔔 <b>SIGNAL</b> {symbol} • {timeframe}",
            f"• Sens: <b>{side.upper()}</b> • Régime: <b>{regime}</b>",
        ]
        if entry is not None: lines.append(f"• Entrée: <code>{float(entry):.6f}</code>")
        if tp is not None:    lines.append(f"• TP: <code>{float(tp):.6f}</code>")
        if sl is not None:    lines.append(f"• SL: <code>{float(sl):.6f}</code>")
        if rr is not None:    lines.append(f"• RR: <b>x{float(rr):.2f}</b>")

        msg = "\n".join(lines)

        try:
            img = charting.generate_trade_chart(symbol, None, signal)
        except Exception:
            img = None

        if img is not None:
            tg_send_with_photo(photo_buffer=img, caption=msg)
        else:
            tg_send(msg)
    except Exception:
        pass


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
                {"text": "↩︎ Menu principal", "callback_data": "main_menu"},
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

def set_universe_command(message: Dict[str, Any]):
    """Commande texte: /setuniverse <nombre> — met à jour UNIVERSE_SIZE (impact IMMÉDIAT + rafraîchit le menu)."""
    try:
        text = (message or {}).get("text", "") or ""
        parts = text.strip().split()
        if len(parts) < 2:
            tg_send("❌ Utilisation: /setuniverse <nombre>")
            return

        size = int(parts[1])
        if size <= 0:
            tg_send("❌ Le nombre doit être > 0.")
            return

        # Enregistre et confirme
        database.set_setting('UNIVERSE_SIZE', str(size))
        tg_send(f"✅ Taille de l'univers mise à <b>{size}</b>.")

        # Rafraîchit immédiatement le menu principal pour refléter la valeur
        try:
            is_paused = str(database.get_setting('PAUSED', 'false')).lower() == 'true'
        except Exception:
            is_paused = False
        send_main_menu(is_paused)
    except Exception as e:
        tg_send(f"❌ Erreur /setuniverse: <code>{_escape(e)}</code>")


def set_maxpos_command(message: Dict[str, Any]):
    """Commande texte: /setmaxpos <nombre> — met à jour MAX_OPEN_POSITIONS (immédiat pour la logique qui lit la DB)."""
    try:
        text = (message or {}).get("text", "") or ""
        parts = text.strip().split()
        if len(parts) < 2:
            tg_send("❌ Utilisation: /setmaxpos <nombre>")
            return
        max_p = int(parts[1])
        if max_p < 0:
            tg_send("❌ Le nombre doit être ≥ 0.")
            return
        database.set_setting('MAX_OPEN_POSITIONS', max_p)
        tg_send(f"✅ Positions max mises à <b>{max_p}</b>.")
    except Exception as e:
        tg_send(f"❌ Erreur /setmaxpos: <code>{_escape(e)}</code>")


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
            [{"text": "🚀 Signaux valides (6 Dernières Heures)", "callback_data": "signals_6h"}],
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
    Passe en mode 'redémarrage par drapeau' (RESTART_REQUESTED=true) et NOTIFIE,
    sans quitter le process immédiatement (évitant les boucles).
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

        # Drapeau de redémarrage (lu par main.check_restart_request)
        try:
            database.set_setting('RESTART_REQUESTED', 'true')
        except Exception:
            pass

        from_obj = (callback_query or {}).get("from", {}) or {}
        user_label = from_obj.get("username") or from_obj.get("first_name") or "inconnu"

        msg_chat_id = (((callback_query or {}).get("message", {}) or {}).get("chat", {}) or {}).get("id")
        tg_send(
            f"♻️ Redémarrage demandé par <b>{_escape(user_label)}</b>. "
            f"Le bot va se relancer proprement…",
            chat_id=msg_chat_id or TG_CHAT_ID
        )

        # ⚠️ Ne PAS quitter ici (pas de os._exit) : la boucle principale détectera le drapeau.
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

def try_handle_inline_callback(event: Any) -> bool:
    """
    Route Offset/Signaux/Restart/Stats.
    Accepte soit un objet 'callback_query', soit un update complet, soit une LISTE d'updates.
    """
    if isinstance(event, list):
        handled = False
        for it in event:
            if try_handle_inline_callback(it):
                handled = True
        return handled

    if not isinstance(event, dict):
        return False

    data = event.get("callback_query") if "callback_query" in event else event
    if not isinstance(data, dict):
        return False

    try:
        cmd = data.get("data")
        if not cmd:
            return False

        # ----- MENU PRINCIPAL -----
        if cmd == "main_menu":
            try:
                is_paused = str(database.get_setting('PAUSED', 'false')).lower() == 'true'
            except Exception:
                is_paused = False
            send_main_menu(is_paused)
            cq_id = data.get("id")
            if cq_id: tg_answer_callback_query(cq_id)
            return True

        # ----- MENU CONFIGURATION -----
        if cmd == "menu_config":
            send_config_menu()
            cq_id = data.get("id")
            if cq_id: tg_answer_callback_query(cq_id)
            return True

        # ----- MENUS SIGNAUX -----
        if cmd == "menu_signals":
            send_signals_menu()
            cq_id = data.get("id")
            if cq_id: tg_answer_callback_query(cq_id)
            return True

        if cmd == "signals_pending":
            tg_show_signals_pending()
            cq_id = data.get("id")
            if cq_id: tg_answer_callback_query(cq_id)
            return True

        if cmd == "signals_6h":
            tg_show_signals_6h()
            cq_id = data.get("id")
            if cq_id: tg_answer_callback_query(cq_id)
            return True

        # ----- POSITIONS -----
        if cmd == "list_positions":
            tg_show_positions()
            cq_id = data.get("id")
            if cq_id: tg_answer_callback_query(cq_id)
            return True

        # ----- OFFSETS -----
        if isinstance(cmd, str) and cmd.startswith("OFS:"):
            msg = data.get("message") or {}
            chat = msg.get("chat") or {}
            chat_id = chat.get("id")
            message_id = msg.get("message_id")
            cq_id = data.get("id")
            handle_offset_callback(cmd, chat_id=chat_id, message_id=message_id, callback_query_id=cq_id)
            return True

        # ----- STATS -----
        if cmd == "get_stats":
            tg_show_stats("24h")
            cq_id = data.get("id")
            if cq_id: tg_answer_callback_query(cq_id)
            return True

        if isinstance(cmd, str) and cmd.startswith("stats:"):
            period = cmd.split(":", 1)[1] if ":" in cmd else "24h"
            tg_show_stats(period)
            cq_id = data.get("id")
            if cq_id: tg_answer_callback_query(cq_id)
            return True

        # ----- RESTART -----
        if cmd == "restart_bot":
            handle_restart_callback(data); return True
        if cmd == "confirm_restart_bot":
            handle_restart_confirm(data); return True
        if cmd == "cancel_restart_bot":
            handle_restart_cancel(data); return True

        return False
    except Exception as e:
        tg_send_error("Callback routing", e)
        return False
        

def _format_signal_row(sig: dict) -> str:
    """Format compact d'un signal (sans la raison), horodaté Europe/Lisbon.
    Gère automatiquement ts en secondes ou millisecondes.
    """
    from datetime import datetime
    import pytz

    tz = pytz.timezone("Europe/Lisbon")
    try:
        ts_raw = float(sig.get("ts", 0)) or 0.0
    except Exception:
        ts_raw = 0.0
    # Milliseconds -> seconds si nécessaire
    ts = ts_raw / 1000.0 if ts_raw > 10_000_000_000 else ts_raw
    dt = datetime.fromtimestamp(ts, tz).strftime("%Y-%m-%d %H:%M")

    symbol = str(sig.get("symbol", "?"))
    tf = str(sig.get("timeframe", "?"))
    side = str(sig.get("side", "?")).upper()

    def _fmt(x, n=5):
        try:
            return f"{float(x):.{n}f}"
        except Exception:
            return str(x)

    entry = sig.get("entry", None)
    sl = sig.get("sl", None)
    tp = sig.get("tp", None)
    rr = sig.get("rr", None)

    parts = [f"• {dt} — <b>{_escape(symbol)}</b> ({_escape(tf)}) <code>{_escape(side)}</code>"]
    if entry is not None: parts.append(f"Entry: <code>{_fmt(entry, 5)}</code>")
    if sl is not None:    parts.append(f"SL: <code>{_fmt(sl, 5)}</code>")
    if tp is not None:    parts.append(f"TP: <code>{_fmt(tp, 5)}</code>")
    if rr is not None:
        try:
            parts.append(f"RR: <b>x{float(rr):.2f}</b>")
        except Exception:
            parts.append(f"RR: <b>x{_escape(rr)}</b>")
    return "  ".join(parts)


def tg_show_signals_pending(limit: int = 50):
    """
    Affiche 'Signaux en attente' : state=PENDING, sans fenêtre de temps.
    Tri décroissant par ts (les plus récents en premier).
    Met à jour le message principal avec un bouton Retour.
    """
    keyboard = {"inline_keyboard": [[{"text": "↩️ Retour", "callback_data": "main_menu"}]]}
    try:
        signals = database.get_signals(state="PENDING", since_minutes=None, limit=limit)
    except Exception as e:
        edit_main(f"⚠️ Erreur lecture signaux en attente : <code>{_escape(e)}</code>", keyboard)
        return

    signals = sorted(signals or [], key=lambda s: int(s.get("ts", 0)), reverse=True)

    if not signals:
        edit_main("<b>📟 Signaux en attente</b>\n\nAucun signal en attente pour le moment.", keyboard)
        return

    lines = ["<b>📟 Signaux en attente</b>", ""]
    lines.extend(_format_signal_row(s) for s in signals)
    edit_main("\n".join(lines), keyboard)


def tg_show_signals_6h(limit: int = 50):
    """
    Affiche 'Signaux des 6 dernières heures' :
    inclut VALID_SKIPPED et VALID_TAKEN sur une fenêtre = 360 min.
    Tri décroissant par ts (les plus récents en premier).

    Règles d'affichage:
      - CUT_WICK_FOR_RR = false  -> masquer VALID_TAKEN si RR < MIN_RR
      - CUT_WICK_FOR_RR = true   -> masquer VALID_TAKEN si RR < 2.8
    """
    keyboard = {"inline_keyboard": [[{"text": "↩️ Retour", "callback_data": "main_menu"}]]}

    # Paramètres
    try:
        min_rr = float(database.get_setting('MIN_RR', os.getenv("MIN_RR", "3.0")))
    except Exception:
        min_rr = 3.0
    try:
        cut_wick = str(database.get_setting('CUT_WICK_FOR_RR', 'false')).lower() == 'true'
    except Exception:
        cut_wick = False
    # Seuil spécifique cut-wick (configurable sinon défaut 2.8)
    try:
        cw_min_rr = float(database.get_setting('CUT_WICK_MIN_RR', '2.8'))
    except Exception:
        cw_min_rr = 2.8

    # Lecture DB
    try:
        s_skipped = database.get_signals(state="VALID_SKIPPED", since_minutes=360, limit=limit) or []
        s_taken   = database.get_signals(state="VALID_TAKEN",   since_minutes=360, limit=limit) or []
        signals = s_skipped + s_taken
    except Exception as e:
        edit_main(f"⚠️ Erreur lecture signaux (6h) : <code>{_escape(e)}</code>", keyboard)
        return

    # Filtrage selon la règle
    filtered = []
    for s in signals:
        try:
            st = str(s.get("state", "")).upper()
            rr_val = float(s.get("rr", 0) or 0.0)
            if st == "VALID_TAKEN":
                if cut_wick:
                    # cut-wick actif -> masquer si RR < 2.8
                    if rr_val < cw_min_rr:
                        continue
                else:
                    # cut-wick inactif -> masquer si RR < MIN_RR
                    if rr_val < min_rr:
                        continue
        except Exception:
            pass
        filtered.append(s)
    signals = filtered

    signals = sorted(signals, key=lambda s: int(s.get("ts", 0)), reverse=True)

    if not signals:
        edit_main("<b>⏱️ Signaux validés (6h)</b>\n\nAucun signal validé sur les 6 dernières heures.", keyboard)
        return

    def _badge(s: dict) -> str:
        st = str(s.get('state', '')).upper()
        return "✅ Pris" if st == "VALID_TAKEN" else "❌ Non pris"

    def _reason(s: dict) -> str:
        payload = s.get("payload") or s.get("data") or {}
        reason = s.get("reason") or (payload.get("reason") if isinstance(payload, dict) else "")
        return f"  (raison: {html.escape(str(reason))})" if reason else ""

    lines = ["<b>⏱️ Signaux validés (6h)</b>", ""]
    for s in signals:
        row = _format_signal_row(s)
        note = ""
        try:
            rr_val = float(s.get("rr", 0) or 0.0)
            # Si cut-wick ON et RR entre [2.8 ; MIN_RR[, on l’affiche mais on tag l’écart vs MIN_RR
            if cut_wick and rr_val < min_rr and rr_val >= cw_min_rr:
                note = "  ⚠ RR<MIN_RR (cut-wick)"
        except Exception:
            pass
        lines.append(f"{row}  —  {_badge(s)}{note}{_reason(s)}")

    edit_main("\n".join(lines), keyboard)



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

    # Univers scanné (fallback 500)
    try:
        universe_size = int(database.get_setting('UNIVERSE_SIZE', os.getenv("UNIVERSE_SIZE", "500")))
    except Exception:
        universe_size = int(os.getenv("UNIVERSE_SIZE", "500"))

    # 🔹 Solde USDT (live si possible, sinon cache)
    balance_line = ""
    try:
        ex = getattr(trader, "create_exchange", None) and trader.create_exchange()
        if ex and hasattr(trader, "get_account_balance_usdt"):
            bal = trader.get_account_balance_usdt(ex)
        else:
            bal = None
        if bal is None:
            cached = database.get_setting('CURRENT_BALANCE_USDT', None)
            bal = float(cached) if cached not in (None, "", "None") else None
        if isinstance(bal, (int, float)):
            balance_line = f"💼 Solde USDT : <code>{bal:.2f}</code>\n"
    except Exception:
        pass

    # Stratégie actuelle
    current_strategy = str(database.get_setting('STRATEGY_MODE', 'NORMAL')).upper()
    cw = str(database.get_setting('CUT_WICK_FOR_RR', 'false')).lower() == 'true'
    cw_chip = f"✂️ Couper mèches      : <code>{'ON' if cw else 'OFF'}</code>\n"

    text = (
        f"<b>💹🤖 Darwin Bot</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"{mode_chip} • {status_chip}\n"
        f"{balance_line}"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>🔧 Configuration</b>\n"
        f"🌐 Univers scanné : <code>{universe_size}</code>\n"
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
    """Calcule les stats et affiche le rapport dans le même message épinglé (pas de spam).
    Ajout: résolution robuste du solde si `balance` est None (fallback DB CURRENT_BALANCE_USDT).
    """
    # 1) Résoudre le solde actuel
    resolved_balance: Optional[float] = None
    if isinstance(balance, (int, float)):
        try:
            resolved_balance = float(balance)
        except Exception:
            resolved_balance = None
    if resolved_balance is None:
        try:
            raw = database.get_setting('CURRENT_BALANCE_USDT', None)
            if raw is not None and str(raw).strip() != "":
                resolved_balance = float(raw)
        except Exception:
            resolved_balance = None

    # 2) Construire le texte du rapport via reporting (inchangé)
    stats = reporting.calculate_performance_stats(trades)
    text = reporting.format_report_message(title, stats, resolved_balance)

    # 3) Clavier (inchangé)
    keyboard = {"inline_keyboard": [[{"text": "↩️ Retour", "callback_data": "main_menu"}]]}

    msg_id = database.get_setting('MAIN_MENU_MESSAGE_ID', None)

    # 4) Essayer d'éditer le message existant
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

    # 5) Sinon, envoyer puis mémoriser le nouvel id
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


# ===== Stats persistées (depuis database.trades) =====

def _fmt_pf(x):
    try:
        return "–" if (x is None) else f"{float(x):.2f}"
    except Exception:
        return "–"

def _load_balance_optional() -> Optional[float]:
    """
    Tente d'abord un solde LIVE via trader (portfolio equity ou balance USDT),
    puis retombe sur le cache en base (CURRENT_BALANCE_USDT / LAST_BALANCE_USDT).
    Met à jour le cache si une valeur live est trouvée.
    """
    # 1) Tentative LIVE via trader
    try:
        ex = getattr(trader, "create_exchange", None) and trader.create_exchange()
        if ex:
            bal = None
            get_pf = getattr(trader, "get_portfolio_equity_usdt", None)
            if callable(get_pf):
                try:
                    bal = float(get_pf(ex))
                except Exception:
                    bal = None
            if bal is None:
                get_bal = getattr(trader, "get_account_balance_usdt", None)
                if callable(get_bal):
                    try:
                        bal = float(get_bal(ex))
                    except Exception:
                        bal = None
            if isinstance(bal, (int, float)):
                try:
                    database.set_setting('CURRENT_BALANCE_USDT', f"{bal:.2f}")
                except Exception:
                    pass
                return float(bal)
    except Exception:
        pass

    # 2) Fallback: cache DB
    try:
        b = database.get_setting('CURRENT_BALANCE_USDT', None)
        if b in (None, "", "None"):
            b = database.get_setting('LAST_BALANCE_USDT', None)
        return float(b) if b not in (None, "", "None") else None
    except Exception:
        return None


def _render_stats_period(period: str) -> str:
    """
    Construit le message Stats pour 24h / 7j / 30j / all à partir des TRADES fermés.
    Tolère DB en secondes OU millisecondes pour les timestamps.
    Pas de fallback compliqué : si la table trades ne contient rien,
    on laisse reporting indiquer qu'il n'y a pas assez de données.
    """
    period = (period or "24h").lower()

    if period == "7d":
        seconds = 7 * 24 * 60 * 60
        title = "Bilan Hebdomadaire (7 jours)"
    elif period == "30d":
        seconds = 30 * 24 * 60 * 60
        title = "Bilan 30 jours"
    elif period == "all":
        seconds = None
        title = "Bilan Global"
    else:
        # défaut : 24h
        seconds = 24 * 60 * 60
        title = "Bilan Quotidien (24h)"

    # Fenêtre temporelle
    try:
        since_ts = 0 if seconds is None else int(time.time()) - int(seconds)
    except Exception:
        since_ts = 0

    # Lecture DB robuste: tente en secondes puis en millisecondes si vide
    try:
        trades = database.get_closed_trades_since(since_ts)
        if seconds is not None and not trades:
            # si les timestamps sont stockés en ms
            trades = database.get_closed_trades_since(since_ts * 1000)
    except Exception:
        trades = []

    # Calcul des stats uniquement à partir de la table trades
    stats = reporting.calculate_performance_stats(trades)
    balance = _load_balance_optional()

    return reporting.format_report_message(title, stats, balance)



def _stats_keyboard(active: str = "24h") -> Dict:
    active = (active or "24h").lower()
    def tag(lbl, key):
        prefix = "✅ " if key == active else ""
        return {"text": f"{prefix}{lbl}", "callback_data": f"stats:{key}"}
    return {
        "inline_keyboard": [
            [tag("24h", "24h"), tag("7j", "7d"), tag("30j", "30d"), tag("Tout", "all")],
            [{"text": "↩️ Retour", "callback_data": "main_menu"}]
        ]
    }

def tg_show_stats(period: str = "24h"):
    """
    Affiche les stats + envoie un graphique equity automatiquement.
    """
    # 1) texte des stats
    text = _render_stats_period(period)
    kb = _stats_keyboard(period)
    edit_main(text, kb)

    # 2) Récupérer les trades fermés depuis la DB
    try:
        if period == "7d":
            since = int(time.time()) - 7 * 24 * 3600
        elif period == "30d":
            since = int(time.time()) - 30 * 24 * 3600
        elif period == "all":
            since = 0
        else:
            since = int(time.time()) - 24 * 3600

        trades = database.get_closed_trades_since(since)
        if not trades and since > 0:
            trades = database.get_closed_trades_since(since * 1000)
    except Exception:
        trades = []

    # 3) Construire historique equity
    history = reporting.build_equity_history(trades)

    # 4) Générer graphique
    img = reporting.generate_equity_chart(history)

    # 5) Envoyer image si dispo
    if img is not None:
        tg_send_with_photo(img, caption="📈 Évolution du Portefeuille")


def tg_show_positions():
    """
    Affiche les positions ouvertes depuis la DB dans le message principal.
    (Routé par le bouton '📊 Positions') — tente d'abord une sync avec l'exchange.
    """
    try:
        # 🔄 Sync on-demand avec l'exchange pour remonter une position ouverte ailleurs (ex: Bybit)
        ex = None
        try:
            ex = getattr(trader, "create_exchange", None) and trader.create_exchange()
        except Exception:
            ex = None
        try:
            if ex and hasattr(trader, "sync_positions_with_exchange"):
                trader.sync_positions_with_exchange(ex)
        except Exception as _sync_err:
            # On ne bloque pas l’affichage si la sync échoue
            print(f"[notifier.tg_show_positions] Sync positions warning: {_sync_err}")

        positions = database.get_open_positions() or []
    except Exception as e:
        edit_main(f"⚠️ Erreur lecture positions : <code>{_escape(e)}</code>",
                  {"inline_keyboard": [[{"text": "↩️ Retour", "callback_data": "main_menu"}]]})
        return

    format_open_positions(positions)

def format_open_positions(positions: List[Dict[str, Any]]):
    """Affiche les positions ouvertes dans le message principal (pas de spam) avec PNL si prix courant dispo
    ou, à défaut, récupéré via API publique CCXT.
    PNL = (price - entry) * qty pour un LONG, (entry - price) * qty pour un SHORT.

    Recherche du prix courant dans: current_price, last_price, mark_price, ticker_last, price, last.
    Si absent: tentative fetch_ticker(symbol) (Bybit puis Bitget, type=swap).
    Recherche de la qty dans: quantity, qty, contracts, size, amount. Sinon 'N/A'.
    """
    keyboard = get_positions_keyboard(positions) or {"inline_keyboard": [[{"text": "↩️ Retour", "callback_data": "main_menu"}]]}

    if not positions:
        notifier_text = "📊 Aucune position n'est actuellement ouverte."
        edit_main(notifier_text, keyboard)
        return

    # --- Helpers locaux (aucun impact global) ---
    def _to_float(x, default=None):
        try:
            if x is None:
                return default
            return float(x)
        except Exception:
            return default

    def _first(pos: Dict[str, Any], keys: List[str], default=None):
        for k in keys:
            if k in pos and pos[k] is not None:
                return pos[k]
        return default

    _price_cache: Dict[str, Optional[float]] = {}

    def _normalize_candidates(sym: str) -> List[str]:
        """Génère plusieurs variantes de symbole compatibles CCXT."""
        if not sym:
            return []
        sym = sym.strip()
        cands = [sym]
        # extraire 'BASE' si possible
        base = ""
        if "/" in sym:
            base = sym.split("/")[0]
        elif sym.endswith("USDT"):
            base = sym[:-4]
        base = base or sym

        # Variantes futures/spot courantes
        cands.append(f"{base}/USDT:USDT")
        cands.append(f"{base}/USDT")
        # dédoublonner en conservant l'ordre
        seen, out = set(), []
        for s in cands:
            if s and s not in seen:
                seen.add(s)
                out.append(s)
        return out

    def _fetch_public_price(sym: str) -> Optional[float]:
        """Tentative de récupération d’un prix 'last' public (cache par appel)."""
        try:
            from importlib import import_module
            if sym in _price_cache:
                return _price_cache[sym]
            # Import CCXT seulement si nécessaire
            ccxt = import_module("ccxt")
            exchs = []
            # Instances publiques, rate-limit ON, type swap par défaut
            try:
                exchs.append(ccxt.bybit({"enableRateLimit": True, "options": {"defaultType": "swap"}}))
            except Exception:
                pass
            try:
                exchs.append(ccxt.bitget({"enableRateLimit": True, "options": {"defaultType": "swap"}}))
            except Exception:
                pass
            if not exchs:
                _price_cache[sym] = None
                return None

            for candidate in _normalize_candidates(sym):
                for ex in exchs:
                    try:
                        t = ex.fetch_ticker(candidate)
                        last = t.get("last") or t.get("close") or t.get("info", {}).get("lastPrice")
                        last_f = _to_float(last, None)
                        if last_f is not None:
                            _price_cache[sym] = last_f
                            return last_f
                    except Exception:
                        continue
            _price_cache[sym] = None
            return None
        except Exception:
            _price_cache[sym] = None
            return None

    # ⚠️ pas d'annotation Tuple ici pour éviter des imports en plus
    def _pnl_tuple(pos: Dict[str, Any]):
        try:
            entry = _to_float(pos.get('entry_price'), 0.0)
            qty   = _to_float(_first(pos, ['quantity', 'qty', 'contracts', 'size', 'amount'], 0.0), 0.0)
            side  = (pos.get('side') or '').lower()
            sym   = (pos.get('symbol') or '').strip()

            # Prix courant: on tente plusieurs clés, sinon API publique
            cur_raw = _first(pos, ['current_price', 'last_price', 'mark_price', 'ticker_last', 'price', 'last'], None)
            cur     = _to_float(cur_raw, None)
            if cur is None and sym:
                cur = _fetch_public_price(sym)

            if cur is None or qty <= 0 or entry <= 0:
                return None, None

            if side == 'buy':
                pnl = (cur - entry) * qty
                pnl_pct = (cur - entry) / entry * 100.0
            else:
                pnl = (entry - cur) * qty
                pnl_pct = (entry - cur) / entry * 100.0
            return pnl, pnl_pct
        except Exception:
            return None, None

    # --- Construction du message ---
    lines = ["<b>📊 Positions Ouvertes (DB)</b>\n"]
    for pos in positions:
        side_icon = "📈" if (pos.get('side') or '').lower() == 'buy' else "📉"
        pnl_val, pnl_pct = _pnl_tuple(pos)
        emoji = "💰"
        if pnl_val is None or pnl_pct is None:
            pnl_str = f"{emoji} PNL: <i>N/A</i>"
        else:
            sign = "+" if pnl_val >= 0 else "−"
            pnl_abs = abs(pnl_val)
            pct_abs = abs(pnl_pct)
            pnl_str = f"💰: <b>{sign}{pnl_abs:.2f} USDT</b> ({sign}{pct_abs:.2f}%)"

        # Affichages robustes (évite crash si None)
        def _fmt(x, d=0.0):
            v = _to_float(x, d)
            return f"{v:.4f}"

        lines.append(
            f"<b>{pos.get('id')}. {side_icon} {_escape(pos.get('symbol', 'N/A'))}</b>\n"
            f"   Entrée: <code>{_fmt(pos.get('entry_price'), 0.0)}</code>\n"
            f"   SL: <code>{_fmt(pos.get('sl_price'), 0.0)}</code> | TP: <code>{_fmt(pos.get('tp_price'), 0.0)}</code>\n"
            f"   {pnl_str}"
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
    """Notifie qu'un signal est en attente ET l’enregistre en DB (state=PENDING)."""
    try:
        side = str(signal.get('side', '')).lower()
        regime = str(signal.get('regime', '') or '')
        rr = float(signal.get('rr', 0.0) or 0.0)
        entry = float(signal.get('entry', 0.0) or 0.0)
        sl = float(signal.get('sl', 0.0) or 0.0)
        tp = float(signal.get('tp', 0.0) or 0.0)
        timeframe = str(signal.get('timeframe') or database.get_setting('TIMEFRAME', '1h'))
        ts = int(signal.get('ts') or int(time.time() * 1000))

        # --- Persist PENDING
        try:
            if side in ("buy", "sell"):
                database.upsert_signal_pending(symbol, timeframe, ts, side, regime, rr, entry, sl, tp)
        except Exception:
            pass

        side_icon = "📈" if side == 'buy' else "📉"
        message = (
            f"⏱️ <b>Signal en attente {side_icon}</b>\n\n"
            f"Paire: <code>{_escape(symbol)}</code>\n"
            f"Type: {_escape(regime)}\n"
            f"RR Potentiel: x{rr:.2f}\n\n"
            f"<i>En attente de la clôture de la bougie pour validation finale.</i>"
        )
        tg_send(message, chat_id=TG_ALERTS_CHAT_ID or TG_CHAT_ID)
    except Exception:
        pass

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

def tg_send_error(text: str, ex: Optional[BaseException] = None, chat_id: Optional[str] = None) -> None:
    """Envoie un message d'erreur vers le canal d’alertes s’il est configuré, sinon vers le chat par défaut."""
    prefix = "⚠️ Erreur"
    details = f" ({type(ex).__name__}: {ex})" if ex else ""
    target_chat = (os.getenv("TELEGRAM_ALERTS_CHAT_ID", "") or chat_id or TG_ALERTS_CHAT_ID or TG_CHAT_ID)
    if not target_chat:
        return
    tg_send(f"{prefix} : {_escape(text)}{_escape(details)}", chat_id=target_chat)


