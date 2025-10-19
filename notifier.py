# notifier.py
import os
import requests
from html import escape
from datetime import datetime, timedelta, timezone

# =======================
# CONFIG
# =======================
TG_TOKEN   = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
TG_API     = f"https://api.telegram.org/bot{TG_TOKEN}"

if not TG_TOKEN or not TG_CHAT_ID:
    print("[NOTIFIER] Telegram not configured (TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID missing)")

# Mémoire en RAM pour les messages de signaux récents
_LAST_HOUR_SIGNALS = []


# =======================
# CORE HELPERS
# =======================
def _tg_request(method: str, payload: dict = None, timeout=10):
    """Appel Telegram générique — renvoie la réponse JSON."""
    if not TG_TOKEN or not TG_CHAT_ID:
        return {}

    url = f"{TG_API}/{method}"
    try:
        resp = requests.post(url, data=payload or {}, timeout=timeout)
        data = resp.json()
        if not data.get("ok"):
            print(f"[NOTIF] {method} error: {data}")
        return data
    except Exception as e:
        print(f"[NOTIF] {method} exception: {e}")
        return {}


def tg_send(text: str, parse_mode: str = "HTML", disable_preview: bool = True):
    """Envoie un message Telegram et renvoie message_id si succès."""
    if not TG_TOKEN or not TG_CHAT_ID:
        return None
    payload = {
        "chat_id": TG_CHAT_ID,
        "text": text,
        "parse_mode": parse_mode,
        "disable_web_page_preview": "true" if disable_preview else "false",
    }
    data = _tg_request("sendMessage", payload)
    try:
        return data.get("result", {}).get("message_id")
    except Exception:
        return None


def tg_get_updates(offset: int | None = None, timeout: int = 0):
    """Récupère les updates Telegram."""
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
# SIGNALS
# =======================
def tg_send_start_banner(mode: str, tf: str, risk_pct: int, rr_min: float):
    """Message de démarrage du bot."""
    msg = (
        f"🔔 <b>Démarrage</b> — <b>{escape(mode)}</b>\n"
        f"⏱ TF: <code>{escape(tf)}</code>\n"
        f"💰 Risk: <code>{risk_pct}%</code> | RR≥<code>{rr_min}</code>"
    )
    return tg_send(msg)


def tg_send_signal_card(
    symbol: str,
    side: str,
    entry: float,
    sl: float,
    tp: float,
    rr: float,
    bullets: list[str] | None = None,
    regime: str = "trend",
    paper: bool = True,
):
    """Carte signal HTML-safe."""
    tag = "[PAPER]" if paper else "[LIVE]"
    side_txt = "LONG" if side == "buy" else "SHORT"

    header = f"📈 Signal {escape(tag)} | <code>{escape(symbol)}</code> {escape(side_txt)}\n"
    core = (
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
    remember_signal_message(mid, symbol, side, rr)
    return mid


def tg_send_trade_exec(symbol: str, side: str, price: float, rr: float, paper=True):
    """Message d’exécution papier ou réel."""
    tag = "PAPER" if paper else "LIVE"
    side_txt = "BUY" if side == "buy" else "SELL"
    txt = (
        f"🎯 <b>{escape(tag)}</b> <code>{escape(symbol)}</code> "
        f"{escape(side_txt)} @ <code>{price:.6f}</code> RR={rr:.2f}"
    )
    return tg_send(txt)


# =======================
# MEMO SIGNALS
# =======================
def _now_utc():
    return datetime.now(timezone.utc)


def remember_signal_message(message_id: int | None, symbol: str, side: str, rr: float):
    """Stocke un signal pour /signals."""
    if message_id is None:
        return
    global _LAST_HOUR_SIGNALS
    cutoff = _now_utc() - timedelta(hours=1)
    _LAST_HOUR_SIGNALS = [x for x in _LAST_HOUR_SIGNALS if x["ts"] >= cutoff]
    _LAST_HOUR_SIGNALS.append({
        "ts": _now_utc(),
        "symbol": symbol,
        "side": side,
        "rr": float(rr),
        "message_id": int(message_id),
    })


def signals_last_hour_text():
    """Résumé des signaux de la dernière heure."""
    cutoff = _now_utc() - timedelta(hours=1)
    items = [x for x in _LAST_HOUR_SIGNALS if x["ts"] >= cutoff]
    if not items:
        return "Aucun signal sur la dernière heure."

    items.sort(key=lambda x: x["ts"])
    lines = ["🕒 <b>Signaux de la dernière heure :</b>"]
    for it in items:
        t = it["ts"].strftime("%H:%M")
        sym = escape(it["symbol"])
        side = it["side"].upper()
        rr = it["rr"]
        lines.append(f"• <code>{sym}</code> {side} — RR×{rr:.2f} — {t} UTC")
    return "\n".join(lines)
