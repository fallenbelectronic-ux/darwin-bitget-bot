# notifier.py
import os
import time
import json
import requests
from html import escape
from datetime import datetime, timedelta, timezone

# =======================
# ENV
# =======================
TG_TOKEN   = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
TG_API     = f"https://api.telegram.org/bot{TG_TOKEN}"

if not TG_TOKEN or not TG_CHAT_ID:
    print("[NOTIF] Telegram not configured (TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID)")

# Mémoire en RAM pour les messages de signaux de la dernière heure
# items: {"ts": datetime_utc, "symbol": str, "side": str, "rr": float, "message_id": int}
_LAST_HOUR_SIGNALS = []


# =======================
# CORE HELPERS
# =======================
def _tg_request(method: str, payload: dict = None, files=None, timeout=10):
    """
    Enveloppe générique Telegram. Ne lève pas d'exception pour éviter
    de spammer la boucle principale : log et retourne le JSON (ou {}).
    """
    if not TG_TOKEN or not TG_CHAT_ID:
        return {}

    url = f"{TG_API}/{method}"
    try:
        if files:
            resp = requests.post(url, data=payload or {}, files=files, timeout=timeout)
        else:
            # Important: on passe toujours via form-data (data=)
            # pour éviter des surprises de parse_mode.
            resp = requests.post(url, data=payload or {}, timeout=timeout)
        data = resp.json()
        if not data.get("ok"):
            # Log minimal et silencieux côté main
            print(f"[NOTIF] {method} error: {data}")
        return data
    except Exception as e:
        print(f"[NOTIF] {method} exception: {e}")
        return {}


def tg_send(text: str, parse_mode: str = "HTML", disable_preview: bool = True):
    """
    Envoie un message au chat configuré. Par défaut en HTML (sécurisé par escape() côté appelant).
    Retourne le message_id si succès, sinon None.
    """
    if not TG_TOKEN or not TG_CHAT_ID:
        return None

    payload = {
        "chat_id": TG_CHAT_ID,
        "text": text,
        "disable_web_page_preview": "true" if disable_preview else "false",
        "parse_mode": parse_mode,
    }
    data = _tg_request("sendMessage", payload)
    try:
        return data.get("result", {}).get("message_id")
    except Exception:
        return None


def tg_edit(message_id: int, new_text: str, parse_mode: str = "HTML"):
    if not TG_TOKEN or not TG_CHAT_ID or not message_id:
        return False
    payload = {
        "chat_id": TG_CHAT_ID,
        "message_id": message_id,
        "text": new_text,
        "parse_mode": parse_mode,
        "disable_web_page_preview": "true",
    }
    data = _tg_request("editMessageText", payload)
    return bool(data.get("ok"))


def tg_delete(message_id: int):
    if not TG_TOKEN or not TG_CHAT_ID or not message_id:
        return False
    payload = {
        "chat_id": TG_CHAT_ID,
        "message_id": message_id,
    }
    data = _tg_request("deleteMessage", payload)
    return bool(data.get("ok"))


def tg_get_updates(offset: int | None = None, timeout: int = 0):
    """
    Récupère brut les updates Telegram (à filtrer dans main).
    offset: dernier update_id traité + 1
    """
    if not TG_TOKEN:
        return []
    payload = {}
    if offset is not None:
        payload["offset"] = str(offset)
    if timeout > 0:
        payload["timeout"] = str(timeout)
    data = _tg_request("getUpdates", payload)
    return data.get("result", [])


# =======================
# SIGNAL HELPERS (CARDS)
# =======================
def tg_send_signal_card(
    symbol: str,
    side: str,          # "buy" / "sell"
    entry: float,
    sl: float,
    tp: float,
    rr: float,
    regime: str,        # "trend" / "counter"
    bullets: list[str] | None = None,
    paper: bool = True
):
    """
    Carte signal compacte, HTML-safe.
    """
    tag = "[PAPER]" if paper else "[LIVE]"
    side_txt = "LONG" if side == "buy" else "SHORT"

    header = f"📈 Signal {escape(tag)} | <code>{escape(symbol)}</code> {escape(side_txt)}\n"
    core   = (
        f"Entrée <code>{entry:.6f}</code> | SL <code>{sl:.6f}</code> | "
        f"TP <code>{tp:.6f}</code>\nRR x{rr:.2f}\n"
    )
    details = []
    if bullets:
        for b in bullets:
            details.append(f"• {escape(b)}")
    details.append("• Tendance" if regime == "trend" else "• Contre-tendance")

    text = header + core + "\n".join(details)
    mid = tg_send(text, parse_mode="HTML")
    # mémoriser pour le récap de l'heure
    try:
        remember_signal_message(mid, symbol, side, rr)
    except Exception:
        pass
    return mid


def tg_send_trade_exec(symbol: str, side: str, price: float, rr: float, paper=True):
    tag = "PAPER" if paper else "LIVE"
    side_txt = "BUY" if side == "buy" else "SELL"
    txt = (
        f"🎯 <b>{escape(tag)}</b> <code>{escape(symbol)}</code> "
        f"{escape(side_txt)} @ <code>{price:.6f}</code> RR={rr:.2f}"
    )
    return tg_send(txt)


def tg_send_close(symbol: str, pnl_pct: float, rr: float, paper=True):
    tag = "PAPER" if paper else "LIVE"
    emo = "✅" if pnl_pct >= 0 else "❌"
    txt = (
        f"{emo} <b>Trade clos {escape(tag)}</b> "
        f"<code>{escape(symbol)}</code>  P&L <code>{pnl_pct:+.2f}%</code> | RR x{rr:.2f}"
    )
    return tg_send(txt)


def tg_send_error(context: str, err: Exception | str):
    ctx = escape(str(context))
    msg = escape(str(err))
    return tg_send(f"⚠️ <b>Erreur</b> <code>{ctx}</code>\n<code>{msg}</code>")


# =======================
# LAST-HOUR SIGNALS MEMORY
# =======================
def _now_utc():
    return datetime.now(timezone.utc)


def remember_signal_message(message_id: int | None, symbol: str, side: str, rr: float):
    """
    Mémorise un signal pour l’agrégat de l’heure.
    """
    if message_id is None:
        return
    # purge vieux
    cutoff = _now_utc() - timedelta(hours=1)
    global _LAST_HOUR_SIGNALS
    _LAST_HOUR_SIGNALS = [x for x in _LAST_HOUR_SIGNALS if x["ts"] >= cutoff]

    _LAST_HOUR_SIGNALS.append({
        "ts": _now_utc(),
        "symbol": symbol,
        "side": side,
        "rr": float(rr),
        "message_id": int(message_id),
    })


def signals_last_hour_text():
    """
    Retourne un petit bloc texte listant les signaux de la dernière heure.
    """
    cutoff = _now_utc() - timedelta(hours=1)
    items = [x for x in _LAST_HOUR_SIGNALS if x["ts"] >= cutoff]
    if not items:
        return "Aucun signal sur la dernière heure."

    # tri par date croissante
    items.sort(key=lambda x: x["ts"])
    lines = ["🕒 Signaux de la dernière heure:"]
    for it in items:
        t = it["ts"].strftime("%H:%M")
        sym = escape(it["symbol"])
        side = it["side"].upper()
        rr = it["rr"]
        lines.append(f"• <code>{sym}</code> {side} — RR×{rr:.2f} — {t} UTC")
    return "\n".join(lines)
