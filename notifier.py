import os
import time
import html
import requests
from typing import List, Dict, Any, Optional

# =========================
# ENV – Telegram
# =========================
TG_TOKEN   = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

TELEGRAM_API = f"https://api.telegram.org/bot{TG_TOKEN}"

# Mémoire locale des messages de signaux (pour /signaux)
_SIGNALS_BUFFER: List[Dict[str, Any]] = []     # [{"ts": epoch, "text": "...", "symbol": "BTC/USDT"...}]
_BUFFER_HORIZON_SEC = 3600                      # conserver ~1h


def _ensure_conf():
    if not TG_TOKEN or not TG_CHAT_ID:
        raise RuntimeError("TELEGRAM_BOT_TOKEN ou TELEGRAM_CHAT_ID manquant.")


def _escape_html(text: str) -> str:
    """Évite l’erreur Telegram 'can't parse entities' en mode HTML."""
    return html.escape(text, quote=True)


def tg_send(text: str, disable_notification: bool = False) -> Optional[int]:
    """
    Envoie un message dans Telegram (mode HTML, texte échappé).
    Retourne l'ID du message (int) si OK, sinon None.
    """
    try:
        _ensure_conf()
        payload = {
            "chat_id": TG_CHAT_ID,
            "text": text if ("<" in text or "&" in text) else _escape_html(text),
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
            "disable_notification": disable_notification,
        }
        r = requests.post(f"{TELEGRAM_API}/sendMessage", json=payload, timeout=10)
        data = r.json()
        if not data.get("ok"):
            # Fallback sans parse_mode si Telegram n'aime pas
            payload.pop("parse_mode", None)
            payload["text"] = f"[TXT] {text}"
            requests.post(f"{TELEGRAM_API}/sendMessage", json=payload, timeout=10)
            return None
        return int(data["result"]["message_id"])
    except Exception:
        return None


def tg_send_start_banner(mode_text: str) -> None:
    """Bannière propre au démarrage."""
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    tg_send(f"🔔 <b>Démarrage</b> — { _escape_html(mode_text) }\n<code>{now}</code>")


def tg_get_updates(offset: Optional[int] = None) -> Dict[str, Any]:
    """
    Récupère les updates. Renvoie un dict {"ok": bool, "result": [...] } compatible.
    Jamais de tuple à déballer → pas de “not enough values to unpack”.
    """
    try:
        _ensure_conf()
        params = {"timeout": 0}
        if offset is not None:
            params["offset"] = int(offset)
        r = requests.get(f"{TELEGRAM_API}/getUpdates", params=params, timeout=10)
        data = r.json()
        if not isinstance(data, dict):
            return {"ok": False, "result": []}
        return {"ok": bool(data.get("ok")), "result": data.get("result", [])}
    except Exception:
        return {"ok": False, "result": []}


def remember_signal_message(symbol: str, side: str, rr: float, text: str) -> None:
    """Mémorise un message de signal (pour résumé /signaux)."""
    now = int(time.time())
    _SIGNALS_BUFFER.append({
        "ts": now,
        "symbol": symbol,
        "side": side,
        "rr": rr,
        "text": text
    })
    # Nettoyage horizon (évite de gonfler)
    horizon = now - _BUFFER_HORIZON_SEC
    while _SIGNALS_BUFFER and _SIGNALS_BUFFER[0]["ts"] < horizon:
        _SIGNALS_BUFFER.pop(0)


def signals_last_hour_text() -> str:
    """Construit le résumé des signaux mémorisés sur ~1 heure."""
    now = int(time.time())
    horizon = now - _BUFFER_HORIZON_SEC
    kept = [s for s in _SIGNALS_BUFFER if s["ts"] >= horizon]
    if not kept:
        return "🕒 Aucun signal sur l’heure écoulée."
    lines = ["🕒 <b>Signaux de l’heure écoulée</b>"]
    for s in kept:
        hhmm = time.strftime("%H:%M", time.localtime(s["ts"]))
        lines.append(
            f"• <code>{_escape_html(s['symbol'])}</code> {s['side'].upper()} "
            f"RR x{s['rr']:.2f} — {hhmm}"
        )
    return "\n".join(lines)
