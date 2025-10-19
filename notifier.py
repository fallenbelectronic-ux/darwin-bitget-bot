# notifier.py
import os
import time
import json
import requests
from datetime import datetime, timedelta

TG_TOKEN   = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

_API = f"https://api.telegram.org/bot{TG_TOKEN}" if TG_TOKEN else None

# — mémoire locale des signaux envoyés (pour /signals)
#   [(ts_utc, symbol, side, rr, message_id)]
_signal_log = []

def _ok():
    return bool(TG_TOKEN and TG_CHAT_ID and _API)

def tg_send(text: str, parse_mode: str = "Markdown", disable_notification: bool=False) -> int | None:
    """Envoie un message Telegram. Retourne message_id si succès."""
    if not _ok():
        print("[NOTIF] tg_send skipped (token/chat missing)")
        return None
    try:
        r = requests.post(
            _API + "/sendMessage",
            json={
                "chat_id": TG_CHAT_ID,
                "text": text,
                "parse_mode": parse_mode,
                "disable_web_page_preview": True,
                "disable_notification": disable_notification,
            },
            timeout=10,
        ).json()
        if not r.get("ok"):
            print("[NOTIF] sendMessage error:", r)
            return None
        return r["result"]["message_id"]
    except Exception as e:
        print("[NOTIF] sendMessage exception:", e)
        return None

def tg_get_updates(last_update_id: int | None) -> tuple[int | None, list[dict]]:
    """
    Récupère les updates (polling). Retourne (new_last_update_id, messages list)
    Chaque message = {"text": "...", "chat_id": str, "message_id": int}
    """
    if not _ok():
        return last_update_id, []
    params = {"timeout": 0}
    if last_update_id is not None:
        params["offset"] = last_update_id + 1
    try:
        r = requests.get(_API + "/getUpdates", params=params, timeout=10).json()
        if not r.get("ok"):
            return last_update_id, []
        out = []
        new_last = last_update_id
        for upd in r.get("result", []):
            new_last = max(new_last or 0, upd["update_id"])
            msg = upd.get("message") or upd.get("edited_message")
            if not msg:
                continue
            text = (msg.get("text") or "").strip()
            chat = str(msg["chat"]["id"])
            out.append({"text": text, "chat_id": chat, "message_id": msg.get("message_id")})
        return new_last, out
    except Exception as e:
        print("[NOTIF] getUpdates exception:", e)
        return last_update_id, []

# ===== Signaux récents (pour /signals) =====

def remember_signal_message(symbol: str, side: str, rr: float, message_id: int | None = None):
    """Stocke un signal afin de pouvoir lister les signaux récents."""
    now = datetime.utcnow()
    _signal_log.append((now, symbol, side, float(rr), message_id))
    # garder 2h rolling
    limit = now - timedelta(hours=2)
    while _signal_log and _signal_log[0][0] < limit:
        _signal_log.pop(0)

def signals_last_hour_text() -> str:
    """Retourne un texte formaté des signaux sur la dernière heure."""
    if not _signal_log:
        return "Aucun signal sur l’heure écoulée."
    now = datetime.utcnow()
    recent = [s for s in _signal_log if (now - s[0]).total_seconds() <= 3600]
    if not recent:
        return "Aucun nouveau signal cette heure-ci."
    lines = ["🕐 *Signaux de la dernière heure*"]
    for ts, sym, side, rr, _mid in recent:
        t = ts.strftime("%H:%M")
        lines.append(f"• `{sym}` {side.upper()}  RR×{rr:.2f}  ({t} UTC)")
    return "\n".join(lines)

# ===== Helpers informatifs =====

def tg_send_start_banner(mode:str, tf:str, risk_pct:int, rr_min:float):
    return tg_send(f"🔔 Démarrage — *{mode}* • TF {tf} • Risk {risk_pct}% • RR≥{rr_min}")

def tg_send_signal_card(symbol:str, side:str, entry:float, sl:float, tp:float,
                        rr:float, bullets:list[str], regime:str, paper:bool) -> int | None:
    tag = "[PAPER]" if paper else ""
    side_txt = "LONG" if side=="buy" else "SHORT"
    header = f"📈 Signal {tag} | `{symbol}` {side_txt}\n"
    core   = f"Entrée `{entry:.6f}` | SL `{sl:.6f}` | TP `{tp:.6f}`\nRR x{rr:.2f}\n"
    btxt   = "\n".join([f"• {b}" for b in bullets])
    return tg_send(header + core + btxt)

def tg_send_trade_exec(symbol:str, side:str, price:float, rr:float, paper:bool):
    tag = "PAPER " if paper else ""
    side_txt = "BUY" if side=="buy" else "SELL"
    tg_send(f"🎯 {tag}`{symbol}` {side_txt} @ `{price:.6f}`  RR={rr:.2f}")
