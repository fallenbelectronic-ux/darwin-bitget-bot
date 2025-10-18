# notifier.py
import os
import json
import time
import threading
from datetime import datetime, date
from zoneinfo import ZoneInfo
import requests

# ========= ENV =========
TG_TOKEN   = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
TZ         = os.getenv("TIMEZONE", "Europe/Lisbon")
INDEX_PATH = os.getenv("TG_INDEX_PATH", "/app/tg_index.json")

# Index des messages envoyés par le bot :
# [ {"id": 123, "ts": "2025-10-18T22:10:11Z", "kind": "signal"}, ... ]
_lock = threading.Lock()
_last_nightly_purge_day = None


# ========= Utils internes =========
def _now_utc_iso():
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"

def _now_local_date():
    try:
        return datetime.now(ZoneInfo(TZ)).date()
    except Exception:
        return datetime.utcnow().date()

def _load_index():
    if not os.path.exists(INDEX_PATH):
        return []
    try:
        with open(INDEX_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []

def _save_index(idx):
    try:
        os.makedirs(os.path.dirname(INDEX_PATH), exist_ok=True)
    except Exception:
        pass
    tmp = INDEX_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(idx, f, ensure_ascii=False)
    os.replace(tmp, INDEX_PATH)

def _append_message(msg_id: int, kind: str):
    with _lock:
        idx = _load_index()
        idx.append({"id": int(msg_id), "ts": _now_utc_iso(), "kind": str(kind or "info")})
        _save_index(idx)

def _tg_api(method: str, payload: dict):
    """Appelle l'API Telegram avec quelques retries doux."""
    if not TG_TOKEN or not TG_CHAT_ID:
        return None
    url = f"https://api.telegram.org/bot{TG_TOKEN}/{method}"
    for attempt in range(3):
        try:
            r = requests.post(url, json=payload, timeout=10)
            data = r.json()
            if data.get("ok"):
                return data
            time.sleep(0.6 + attempt * 0.8)  # backoff léger
        except Exception:
            time.sleep(0.6 + attempt * 0.8)
    return None


# ========= ENVOI / SUPPRESSION =========
def tg_send(text: str, kind: str = "info"):
    """
    Envoie un message Markdown et mémorise son id + type (kind).
    Types typiques : 'signal', 'trade', 'stat', 'info'.
    """
    if not TG_TOKEN or not TG_CHAT_ID:
        return None

    payload = {
        "chat_id": TG_CHAT_ID,
        "text": text,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True,
    }
    data = _tg_api("sendMessage", payload)
    if data and data.get("result"):
        msg_id = data["result"]["message_id"]
        _append_message(msg_id, kind=kind or "info")
        return msg_id
    return None

def tg_delete_message(msg_id: int):
    """Supprime un message du bot (silencieux si permission manquante)."""
    if not TG_TOKEN or not TG_CHAT_ID:
        return
    try:
        _tg_api("deleteMessage", {"chat_id": TG_CHAT_ID, "message_id": int(msg_id)})
    except Exception:
        pass


# ========= PURGES =========
def _is_same_local_day(ts_iso: str, d: date) -> bool:
    """Compare la date locale du timestamp ISO (UTC) avec 'd' (date locale)."""
    try:
        dt_utc = datetime.fromisoformat(ts_iso.replace("Z", "+00:00"))
        dt_local = dt_utc.astimezone(ZoneInfo(TZ))
        return dt_local.date() == d
    except Exception:
        return False

def purge_chat(keep_kinds=("signal", "trade")):
    """
    Purge sélective :
      - conserve uniquement les messages *du jour courant* dont le type ∈ keep_kinds
      - supprime tout le reste
    Si keep_kinds est vide ((), [], set()), on supprime *tout* l'historique du bot.
    """
    with _lock:
        idx = _load_index()
        if not idx:
            tg_send("🧹 Purge: rien à supprimer.", kind="info")
            return

        today = _now_local_date()
        new_index = []
        removed = 0
        keep_set = set(keep_kinds or ())

        for rec in idx:
            mid = rec.get("id")
            ts  = rec.get("ts", "")
            kind = rec.get("kind", "info")

            keep_this = False
            if keep_set:
                if _is_same_local_day(ts, today) and (kind in keep_set):
                    keep_this = True

            if keep_this:
                new_index.append(rec)
            else:
                tg_delete_message(mid)
                removed += 1

        _save_index(new_index)
        tg_send(
            f"🧹 Purge: *{removed}* messages supprimés. "
            f"{'Seuls les trades & signaux du jour sont conservés.' if keep_set else 'Historique entièrement effacé.'}",
            kind="info"
        )

def nightly_signals_purge():
    """
    Purge automatique (à la première itération après minuit, TZ locale) :
    conserve trades & signaux du jour, supprime le reste.
    À appeler régulièrement dans la boucle principale.
    """
    global _last_nightly_purge_day
    today = _now_local_date()
    if _last_nightly_purge_day == today:
        return
    purge_chat(keep_kinds=("signal", "trade"))
    _last_nightly_purge_day = today


# ========= PURGES ciblées (n derniers) =========
def purge_last(n: int = 100):
    """
    Supprime en masse les n *derniers* messages envoyés par le bot.
    """
    if n <= 0:
        return tg_send("🧹 Purge: n doit être > 0.", kind="info")

    with _lock:
        idx = _load_index()
        if not idx:
            return tg_send("🧹 Purge: aucun message à supprimer.", kind="info")

        to_delete = idx[-n:]
        keep      = idx[:-n] if len(idx) > n else []

        removed = 0
        for rec in reversed(to_delete):
            mid = rec.get("id")
            if mid is not None:
                tg_delete_message(mid)
                removed += 1

        _save_index(keep)
        tg_send(f"🧹 Purge rapide: *{removed}* derniers messages supprimés.", kind="info")

def purge_last_100():
    """Raccourci : supprime les 100 derniers messages du bot."""
    purge_last(100)

def delete_last_100():
    """
    Supprime directement les 100 derniers messages, sans confirmation ni affichage intermédiaire.
    À mapper sur une commande Telegram (/delete100 ou /select100).
    """
    with _lock:
        idx = _load_index()
        if not idx:
            tg_send("🧹 Aucun message à supprimer.", kind="info")
            return

        to_delete = idx[-100:]
        keep = idx[:-100] if len(idx) > 100 else []

        deleted = 0
        for rec in reversed(to_delete):
            mid = rec.get("id")
            if mid is not None:
                tg_delete_message(mid)
                deleted += 1

        _save_index(keep)
        tg_send(f"🧹 {deleted} derniers messages supprimés.", kind="info")
