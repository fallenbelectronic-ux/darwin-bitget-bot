# /app/notifier.py
import os
import time
import requests
from typing import Any, Dict, List, Optional, Tuple

# --- ENV ----------------------------------------------------------------------
TG_TOKEN   = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
TELEGRAM_API = f"https://api.telegram.org/bot{TG_TOKEN}"

def _enabled() -> bool:
    return bool(TG_TOKEN) and bool(TG_CHAT_ID)

# --- ENVOI DE MESSAGES --------------------------------------------------------
def tg_send(
    text: str,
    parse_mode: Optional[str] = "Markdown",
    disable_web_page_preview: bool = True,
    silent: bool = False,
) -> Optional[int]:
    """Envoie un message Telegram et retourne le message_id ou None."""
    if not _enabled():
        print("[NOTIFIER] tg_send ignoré (token/chat_id manquants).")
        return None
    try:
        r = requests.post(
            f"{TELEGRAM_API}/sendMessage",
            json={
                "chat_id": TG_CHAT_ID,
                "text": text,
                "parse_mode": parse_mode,
                "disable_web_page_preview": disable_web_page_preview,
                "disable_notification": silent,
            },
            timeout=10,
        )
        data = r.json()
        if not data.get("ok"):
            print("[NOTIFIER] sendMessage error:", data)
            return None
        return data["result"]["message_id"]
    except Exception as e:
        print("[NOTIFIER] sendMessage exception:", e)
        return None

# --- SUPPRESSION --------------------------------------------------------------
def tg_delete_message(message_id: int) -> bool:
    if not _enabled():
        return False
    try:
        r = requests.post(
            f"{TELEGRAM_API}/deleteMessage",
            json={"chat_id": TG_CHAT_ID, "message_id": message_id},
            timeout=10,
        )
        data = r.json()
        if not data.get("ok"):
            # Non bloquant si message déjà supprimé / trop ancien, etc.
            print("[NOTIFIER] deleteMessage:", data)
            return False
        return True
    except Exception as e:
        print("[NOTIFIER] deleteMessage exception:", e)
        return False

def tg_delete_many(message_ids: List[int]) -> Tuple[int, int]:
    ok = ko = 0
    for mid in message_ids:
        if tg_delete_message(mid):
            ok += 1
        else:
            ko += 1
        time.sleep(0.06)  # petit throttle
    return ok, ko

# --- RECEPTION / COMMANDES ----------------------------------------------------
def tg_get_updates(offset: Optional[int] = None, timeout: int = 0) -> Dict[str, Any]:
    """Retourne le JSON getUpdates; si non configuré, retourne {ok:True,result:[]}."""
    if not _enabled():
        return {"ok": True, "result": []}
    params: Dict[str, Any] = {}
    if offset is not None:
        params["offset"] = offset
    if timeout and timeout > 0:
        params["timeout"] = int(timeout)
    try:
        r = requests.get(f"{TELEGRAM_API}/getUpdates", params=params, timeout=timeout + 10)
        data = r.json()
        if not data.get("ok"):
            print("[NOTIFIER] getUpdates error:", data)
        return data
    except Exception as e:
        print("[NOTIFIER] getUpdates exception:", e)
        return {"ok": False, "result": [], "error": str(e)}

# --- PIN/UNPIN (optionnel) ----------------------------------------------------
def tg_pin_message(message_id: int) -> bool:
    if not _enabled():
        return False
    try:
        r = requests.post(
            f"{TELEGRAM_API}/pinChatMessage",
            json={"chat_id": TG_CHAT_ID, "message_id": message_id},
            timeout=10,
        )
        return bool(r.json().get("ok"))
    except Exception as e:
        print("[NOTIFIER] pinChatMessage exception:", e)
        return False

def tg_unpin_all() -> bool:
    if not _enabled():
        return False
    try:
        r = requests.post(
            f"{TELEGRAM_API}/unpinAllChatMessages",
            json={"chat_id": TG_CHAT_ID},
            timeout=10,
        )
        return bool(r.json().get("ok"))
    except Exception as e:
        print("[NOTIFIER] unpinAllChatMessages exception:", e)
        return False

# --- STUBS POUR COMPATIBILITE AVEC main.py -----------------------------------
def purge_chat(*args, **kwargs) -> Tuple[int, int]:
    """
    Stub inoffensif : ne purge rien, retourne (0,0).
    Gardé pour compatibilité si main.py continue d'importer purge_chat.
    """
    # Si tu veux quand même un petit accusé :
    # tg_send("🧹 Purge désactivée (aucun message supprimé).", silent=True)
    return (0, 0)

def nightly_signals_purge(*args, **kwargs) -> None:
    """
    Stub inoffensif : ne fait rien. Gardé si main.py l'importe encore.
    """
    return
