# /app/notifier.py
import os
import time
import json
import requests
from typing import Any, Dict, List, Optional, Tuple

# --- ENV ----------------------------------------------------------------------
TG_TOKEN   = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

TELEGRAM_API = f"https://api.telegram.org/bot{TG_TOKEN}"

# Sécurité basique : ne fait rien si token/chat_id manquants
def _enabled() -> bool:
    return bool(TG_TOKEN) and bool(TG_CHAT_ID)

# --- ENVOI DE MESSAGES --------------------------------------------------------
def tg_send(
    text: str,
    parse_mode: Optional[str] = "Markdown",
    disable_web_page_preview: bool = True,
    silent: bool = False,
) -> Optional[int]:
    """
    Envoie un message au chat configuré.
    Retourne le message_id Telegram si succès, sinon None.
    """
    if not _enabled():
        # Pas de crash si on a pas encore configuré Telegram
        print("[NOTIFIER] tg_send ignoré (TG_TOKEN ou TG_CHAT_ID manquant).")
        return None

    url = f"{TELEGRAM_API}/sendMessage"
    payload = {
        "chat_id": TG_CHAT_ID,
        "text": text,
        "disable_web_page_preview": disable_web_page_preview,
        "disable_notification": silent,
    }
    if parse_mode:
        payload["parse_mode"] = parse_mode

    try:
        r = requests.post(url, json=payload, timeout=10)
        data = r.json()
        if not data.get("ok"):
            print("[NOTIFIER] sendMessage error:", data)
            return None
        return data["result"]["message_id"]
    except Exception as e:
        print("[NOTIFIER] sendMessage exception:", e)
        return None

# --- SUPPRESSION / GESTION DES MESSAGES --------------------------------------
def tg_delete_message(message_id: int) -> bool:
    """Supprime un message par ID. Retourne True/False."""
    if not _enabled():
        return False
    try:
        url = f"{TELEGRAM_API}/deleteMessage"
        r = requests.post(url, json={"chat_id": TG_CHAT_ID, "message_id": message_id}, timeout=10)
        data = r.json()
        if not data.get("ok"):
            # Certaines erreurs (message trop ancien, déjà supprimé…) ne sont pas critiques
            print("[NOTIFIER] deleteMessage:", data)
            return False
        return True
    except Exception as e:
        print("[NOTIFIER] deleteMessage exception:", e)
        return False

# Supprime en lot une liste d’IDs (best-effort, continue même en cas d’échec)
def tg_delete_many(message_ids: List[int]) -> Tuple[int, int]:
    ok = 0
    ko = 0
    for mid in message_ids:
        if tg_delete_message(mid):
            ok += 1
        else:
            ko += 1
        # petit throttle pour éviter le flood
        time.sleep(0.06)
    return ok, ko

# --- RECEPTION / COMMANDES ----------------------------------------------------
def tg_get_updates(offset: Optional[int] = None, timeout: int = 0) -> Dict[str, Any]:
    """
    Récupère les updates Telegram (polling simple).
    - offset : update_id à partir duquel lire (ex: last_update_id + 1)
    - timeout : long-polling en secondes (0 = pas d’attente)
    Retour JSON complet de l’API Telegram.
    """
    if not _enabled():
        return {"ok": True, "result": []}

    params: Dict[str, Any] = {}
    if offset is not None:
        params["offset"] = offset
    if timeout and timeout > 0:
        params["timeout"] = int(timeout)

    url = f"{TELEGRAM_API}/getUpdates"
    try:
        r = requests.get(url, params=params, timeout=timeout + 10)
        data = r.json()
        if not data.get("ok"):
            print("[NOTIFIER] getUpdates error:", data)
        return data
    except Exception as e:
        print("[NOTIFIER] getUpdates exception:", e)
        return {"ok": False, "result": [], "error": str(e)}

# --- UTILITAIRES D’AFFICHAGE --------------------------------------------------
def tg_send_markdown(lines: List[str], silent: bool = False) -> Optional[int]:
    """Envoie une liste de lignes en Markdown (avec jointure newline)."""
    text = "\n".join(lines)
    return tg_send(text, parse_mode="Markdown", silent=silent)

def tg_send_html(lines: List[str], silent: bool = False) -> Optional[int]:
    """Envoie une liste de lignes en HTML (avec jointure newline)."""
    text = "\n".join(lines)
    return tg_send(text, parse_mode="HTML", silent=silent)

# --- PLACEHOLDERS (si besoin par main.py) -------------------------------------
def tg_pin_message(message_id: int) -> bool:
    """Facultatif : épingle un message (si le bot a les droits)."""
    if not _enabled():
        return False
    try:
        url = f"{TELEGRAM_API}/pinChatMessage"
        r = requests.post(url, json={"chat_id": TG_CHAT_ID, "message_id": message_id}, timeout=10)
        data = r.json()
        return bool(data.get("ok"))
    except Exception as e:
        print("[NOTIFIER] pinChatMessage exception:", e)
        return False

def tg_unpin_all() -> bool:
    """Facultatif : dés-épingle tous les messages."""
    if not _enabled():
        return False
    try:
        url = f"{TELEGRAM_API}/unpinAllChatMessages"
        r = requests.post(url, json={"chat_id": TG_CHAT_ID}, timeout=10)
        data = r.json()
        return bool(data.get("ok"))
    except Exception as e:
        print("[NOTIFIER] unpinAllChatMessages exception:", e)
        return False
