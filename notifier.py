import os, time, requests
from datetime import datetime, timedelta, timezone

TG_TOKEN   = os.getenv("TELEGRAM_BOT_TOKEN","")
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID","")

# Mémoire des messages (pour purge manuelle et /signals)
_last_msg_ids         = []           # tous messages envoyés par le bot (IDs)
_last_hour_signals    = []           # [(ts_utc, msg_id, symbol, sigdict)]
_MAX_REMEMBERED       = 200

def _api_url(method):
    return f"https://api.telegram.org/bot{TG_TOKEN}/{method}"

def tg_send(text, remember_for_signals=False):
    """
    Envoie un message ; retourne message_id. 
    Si remember_for_signals=True, il sera visible dans /signals (1h).
    """
    if not TG_TOKEN or not TG_CHAT_ID:
        return None
    try:
        r = requests.post(_api_url("sendMessage"), json={
            "chat_id": TG_CHAT_ID,
            "text": text,
            "parse_mode": "Markdown"
        }, timeout=10).json()
        if r.get("ok"):
            msg_id = r["result"]["message_id"]
            _remember_msg_id(msg_id)
            if remember_for_signals:
                _remember_signal(msg_id, text)
            return msg_id
    except Exception:
        pass
    return None

def send_document(filepath, filename=None):
    if not TG_TOKEN or not TG_CHAT_ID or not os.path.exists(filepath):
        return
    files = {"document": open(filepath, "rb")}
    data  = {"chat_id": TG_CHAT_ID}
    if filename:
        data["caption"] = filename
    try:
        r = requests.post(_api_url("sendDocument"), data=data, files=files, timeout=20).json()
        if r.get("ok"):
            _remember_msg_id(r["result"]["message_id"])
    except Exception:
        pass
    finally:
        try: files["document"].close()
        except Exception: pass

def tg_get_updates(offset=None):
    url = _api_url("getUpdates")
    if offset is not None:
        url += f"?offset={offset}"
    try:
        return requests.get(url, timeout=10).json()
    except Exception:
        return {"ok": False, "result": []}

# ---------- Mémoire messages ----------
def _remember_msg_id(msg_id):
    if not msg_id: 
        return
    _last_msg_ids.append(int(msg_id))
    if len(_last_msg_ids) > _MAX_REMEMBERED:
        del _last_msg_ids[:len(_last_msg_ids)-_MAX_REMEMBERED]

def _remember_signal(msg_id, text):
    # texte déjà formaté, mais on mémorise structure minimale
    ts = datetime.now(timezone.utc)
    _last_hour_signals.append((ts, msg_id, text))
    # keep only last hour
    cutoff = ts - timedelta(hours=1)
    while _last_hour_signals and _last_hour_signals[0][0] < cutoff:
        _last_hour_signals.pop(0)

def remember_signal_message(msg_id, symbol, sigdict):
    """
    API appelée par main.notify_signal – garde la structure exploitable si besoin.
    """
    ts = datetime.now(timezone.utc)
    text = (
        f"{symbol} — {sigdict['side']} ({sigdict['regime']}) "
        f"RR x{sigdict['rr']:.2f} @ {sigdict['entry']:.6f}"
    )
    _last_hour_signals.append((ts, msg_id, text))
    cutoff = ts - timedelta(hours=1)
    while _last_hour_signals and _last_hour_signals[0][0] < cutoff:
        _last_hour_signals.pop(0)

def signals_last_hour_text():
    ts_now = datetime.now(timezone.utc)
    cutoff = ts_now - timedelta(hours=1)
    kept   = [(ts, mid, txt) for (ts, mid, txt) in _last_hour_signals if ts >= cutoff]
    if not kept:
        return "Aucun signal durant l’heure écoulée."
    lines = ["*Signaux de la dernière heure*"]
    for ts, mid, txt in kept[-20:]:
        hhmm = ts.astimezone().strftime("%H:%M")
        lines.append(f"• {hhmm} — {txt}")
    return "\n".join(lines)

# ---------- Purgers manuels ----------
def purge_chat(silent=True):
    """
    Supprime les messages envoyés par CE bot (connus via _last_msg_ids).
    Ne touche pas aux messages que le bot n’a pas mémorisés.
    """
    deleted=0
    for mid in list(reversed(_last_msg_ids)):
        if _delete_message(mid):
            deleted += 1
            try: _last_msg_ids.remove(mid)
            except Exception: pass
    if not silent:
        tg_send(f"🧹 Purge: {deleted} supprimé(s).")
    return deleted

def purge_last_100(silent=True):
    """
    Supprime les 100 derniers messages du bot mémorisés.
    """
    deleted=0
    for mid in list(reversed(_last_msg_ids[-100:])):
        if _delete_message(mid):
            deleted += 1
            try: _last_msg_ids.remove(mid)
            except Exception: pass
    if not silent:
        tg_send(f"🧹 Purge(100): {deleted} supprimé(s).")
    return deleted

def purge_all(silent=True):
    """
    Supprime tout ce qui est mémorisé (équivaut à un 'vider' local).
    """
    deleted = purge_chat(silent=True)
    _last_hour_signals.clear()
    if not silent:
        tg_send(f"🧹 Purge totale: {deleted} supprimé(s).")

def _delete_message(message_id:int):
    if not TG_TOKEN or not TG_CHAT_ID or not message_id:
        return False
    try:
        r = requests.post(_api_url("deleteMessage"), json={
            "chat_id": TG_CHAT_ID, "message_id": int(message_id)
        }, timeout=10).json()
        return bool(r.get("ok"))
    except Exception:
        return False
