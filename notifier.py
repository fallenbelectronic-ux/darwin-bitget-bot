import os, json, time, requests
from datetime import datetime
from zoneinfo import ZoneInfo

TG_TOKEN   = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
TZ         = os.getenv("TIMEZONE", "Europe/Lisbon")
INDEX_PATH = os.getenv("TG_INDEX_PATH", "/app/tg_index.json")

# ------------- util -------------
def _today():
    return datetime.now(ZoneInfo(TZ)).strftime("%Y-%m-%d")

def _load_index():
    try:
        with open(INDEX_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []

def _save_index(rows):
    os.makedirs(os.path.dirname(INDEX_PATH), exist_ok=True)
    with open(INDEX_PATH, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)

def _tg(method, payload):
    if not TG_TOKEN or not TG_CHAT_ID:
        return None
    url = f"https://api.telegram.org/bot{TG_TOKEN}/{method}"
    try:
        r = requests.post(url, json=payload, timeout=8)
        j = r.json()
        if not j.get("ok"):
            return None
        return j.get("result")
    except Exception:
        return None

# ------------- API publique -------------
def tg_send(text, kind="info", keep=False, parse_mode="Markdown"):
    """
    kind: "signal", "trade", "info", "stat", ...
    keep: n'empêche pas la purge (/purge garde seulement trade/signal du jour)
    """
    if not TG_TOKEN or not TG_CHAT_ID:
        return None

    payload = {
        "chat_id": TG_CHAT_ID,
        "text": text,
        "disable_web_page_preview": True,
    }
    if parse_mode:
        payload["parse_mode"] = parse_mode

    res = _tg("sendMessage", payload)
    if not res:
        return None

    mid = res.get("message_id")
    rows = _load_index()
    rows.append({
        "message_id": mid,
        "chat_id": TG_CHAT_ID,
        "kind": kind,
        "date": _today(),
        "keep": bool(keep),
        "ts": int(time.time())
    })
    _save_index(rows)
    return mid

def tg_delete(message_id):
    if not TG_TOKEN or not TG_CHAT_ID or not message_id:
        return False
    res = _tg("deleteMessage", {"chat_id": TG_CHAT_ID, "message_id": message_id})
    return bool(res)

def purge_chat(keep_kinds=("signal", "trade"), keep_date=None):
    """
    Supprime tous les messages du bot sauf:
      - ceux dont kind ∈ keep_kinds ET date == keep_date
    """
    keep_date = keep_date or _today()
    rows = _load_index()
    kept = []
    for row in rows:
        kind = row.get("kind")
        date = row.get("date")
        mid  = row.get("message_id")
        # on conserve seulement les trades & signaux DU JOUR
        if (kind in keep_kinds) and (date == keep_date):
            kept.append(row)
            continue
        # sinon suppression
        tg_delete(mid)
    _save_index(kept)
    tg_send("🧹 *Nettoyage*", kind="info")
    return True

_last_nightly_key = None
def nightly_signals_purge():
    """
    À appeler dans la boucle: à minuit locale, supprime tous les messages (même signaux)
    antérieurs au jour courant. On laisse les signaux/trades du jour vivant.
    """
    global _last_nightly_key
    now = datetime.now(ZoneInfo(TZ))
    key = now.strftime("%Y-%m-%d")
    # déclenche une seule fois par minuit
    if now.hour == 0 and _last_nightly_key != key:
        _last_nightly_key = key
        rows = _load_index()
        today = _today()
        kept = []
        for row in rows:
            # on garde seulement le jour courant
            if row.get("date") == today:
                kept.append(row)
            else:
                tg_delete(row.get("message_id"))
        _save_index(kept)
        # message de confirmation minimal (et il sera du jour, donc conservé)
        tg_send("🧽 Purge nocturne effectuée (historique nettoyé).", kind="info")
