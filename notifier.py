# ========= notifier.py  (remplacer intégralement ce fichier) =========
import os
import time
from datetime import datetime, timezone, date
import requests

# --- ENV
TG_TOKEN   = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")  # peut être vide au démarrage ; le bot peut l’annoncer et tu le fixes ensuite
TZ_NAME    = os.getenv("TIMEZONE", "Europe/Lisbon")

API_BASE   = f"https://api.telegram.org/bot{TG_TOKEN}" if TG_TOKEN else None

# ---- Utils bas niveau ------------------------------------------------
def _api_get(method: str, params: dict | None = None, timeout: int = 10):
    if not API_BASE:
        return None
    try:
        r = requests.get(f"{API_BASE}/{method}", params=params or {}, timeout=timeout)
        return r.json()
    except Exception:
        return None

def _api_post(method: str, data: dict | None = None, timeout: int = 10):
    if not API_BASE:
        return None
    try:
        r = requests.post(f"{API_BASE}/{method}", data=data or {}, timeout=timeout)
        return r.json()
    except Exception:
        return None

# ---- Fonctions publiques de santé -----------------------------------
def tg_delete_webhook():
    """Supprime un webhook laissé actif (sinon getUpdates ne renvoie rien)."""
    return _api_get("deleteWebhook")

def tg_get_me():
    """Renvoie les infos du bot (username, id…)."""
    return _api_get("getMe")

# ---- Envoi de messages -----------------------------------------------
def tg_send(text: str, kind: str | None = None, chat_id: str | None = None, disable_preview: bool = True):
    """
    Envoie un message Markdown V2 (simple Markdown sécurisé) au chat.
    kind n’est pas utilisé par l’API, mais sert à nos purges (étiquette logique).
    """
    cid = str(chat_id or TG_CHAT_ID or "").strip()
    if not API_BASE or not cid:
        # On n'échoue pas brutalement : utile pendant l'initialisation
        print("[tg_send skipped] chat_id or token missing")
        return None
    # On n’ajoute aucun préfixe parasite : filtrage via heuristique plus bas
    payload = {
        "chat_id": cid,
        "text": text,
        "parse_mode": "Markdown",
        "disable_web_page_preview": disable_preview,
    }
    return _api_post("sendMessage", payload)

# ---- Lecture d’updates (long-poll ou ponctuel) -----------------------
def tg_get_updates_raw(offset: int | None = None, timeout: int = 50):
    params = {"timeout": timeout}
    if offset is not None:
        params["offset"] = offset
    return _api_get("getUpdates", params, timeout=timeout+5)

# ---- Suppression ------------------------------------------------------
def delete_message(message_id: int, chat_id: str | None = None):
    cid = str(chat_id or TG_CHAT_ID or "").strip()
    if not cid or not API_BASE:
        return None
    return _api_post("deleteMessage", {"chat_id": cid, "message_id": message_id})

# ---- Heuristiques "type" de message (pour purge sélective) -----------
def _infer_kind_from_text(txt: str) -> str:
    """
    Classe grossière pour purge:
      'signal' si le message contient 'Signal' et des champs Entrée/SL/TP
      'trade'  si 'Trade exécuté' ou 'Trade clos'
      sinon 'info'
    """
    t = (txt or "").lower()
    if "signal" in t and ("entrée" in t or "entree" in t) and ("sl" in t and "tp" in t):
        return "signal"
    if ("trade exécuté" in t) or ("trade execute" in t) or ("trade clos" in t):
        return "trade"
    return "info"

def _is_today_utc(ts: int) -> bool:
    d = datetime.fromtimestamp(ts, tz=timezone.utc).date()
    return d == date.today()

# ---- Purges ----------------------------------------------------------
def purge_chat(keep_kinds: tuple[str, ...] = ("signal", "trade"), keep_only_today: bool = True, scan_pages: int = 5):
    """
    Supprime un maximum de messages récents envoyés par / à destination du bot,
    en conservant:
      - uniquement les 'signal' & 'trade' (par défaut)
      - et, optionnellement, uniquement ceux d’aujourd’hui (keep_only_today=True)
    NB : un bot ne peut pas "lister l’historique" ; on re-parcourt les updates récents.
    """
    last_id = None
    kept = 0
    deleted = 0
    for _ in range(max(1, scan_pages)):
        data = tg_get_updates_raw(offset=last_id+1 if last_id else None, timeout=2)
        if not data or not data.get("ok"):
            break
        items = data.get("result", [])
        if not items:
            break
        for upd in items:
            last_id = upd.get("update_id", last_id)
            msg = upd.get("message") or upd.get("edited_message") or upd.get("channel_post") or upd.get("edited_channel_post")
            if not msg:
                continue
            # On ne purge que les messages du chat ciblé (si renseigné)
            chat = msg.get("chat", {})
            if str(TG_CHAT_ID or chat.get("id")) != str(chat.get("id")):
                # Si TG_CHAT_ID est vide, on le "fixe" implicitement à ce chat :
                if not TG_CHAT_ID:
                    pass
                else:
                    continue

            mid  = msg.get("message_id")
            text = msg.get("text") or ""
            kind = _infer_kind_from_text(text)
            ts   = int(msg.get("date", 0))

            keep = True
            if keep_kinds and kind not in keep_kinds:
                keep = False
            if keep_only_today and not _is_today_utc(ts):
                keep = False

            if not keep and mid:
                delete_message(mid)
                deleted += 1
            else:
                kept += 1

        # petite pause entre pages
        time.sleep(0.4)

    tg_send(f"🧹 Purge: *{deleted}* supprimé(s), *{kept}* conservé(s).", kind="info")


def purge_last(n: int = 100):
    """Supprime au mieux les n derniers messages accessibles via getUpdates."""
    deleted = 0
    # on récupère un "lot" récent
    data = tg_get_updates_raw(timeout=2)
    if not data or not data.get("ok"):
        tg_send("⚠️ Purge: impossible de lire les updates.", kind="info")
        return
    items = list(reversed(data.get("result", [])))  # derniers en premier
    for upd in items:
        if deleted >= n:
            break
        msg = upd.get("message") or upd.get("edited_message") or upd.get("channel_post") or upd.get("edited_channel_post")
        if not msg:
            continue
        chat = msg.get("chat", {})
        if str(TG_CHAT_ID or chat.get("id")) != str(chat.get("id")):
            if TG_CHAT_ID:
                continue
        mid = msg.get("message_id")
        if mid:
            delete_message(mid); deleted += 1
    tg_send(f"🧹 Purge {n}: *{deleted}* supprimé(s).", kind="info")

def purge_last_100():
    purge_last(100)

def delete_last_100():
    purge_last_100()

def nightly_signals_purge():
    """
    À lancer en fin de journée si tu le souhaites :
    on garde uniquement les signaux & trades *du jour* (tout le reste est supprimé).
    """
    purge_chat(keep_kinds=("signal","trade"), keep_only_today=True, scan_pages=8)
# ========= fin notifier.py ============================================
