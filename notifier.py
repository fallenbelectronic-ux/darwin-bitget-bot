# -*- coding: utf-8 -*-
import os
import requests

TG_TOKEN   = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

def _enabled():
    return bool(TG_TOKEN and TG_CHAT_ID)

def tg_send(text: str):
    """Envoie un message Markdown à Telegram (silencieux si non configuré)."""
    if not _enabled():
        print("[TG]", text)
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            data={"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "Markdown"},
            timeout=10,
        )
    except Exception as e:
        print("[TG ERR]", e, text[:200])

def tg_send_document(path: str, caption: str = ""):
    """Envoie un fichier (CSV, etc.)."""
    if not _enabled():
        print(f"[TG DOC] {path} {caption}")
        return
    try:
        with open(path, "rb") as f:
            requests.post(
                f"https://api.telegram.org/bot{TG_TOKEN}/sendDocument",
                data={"chat_id": TG_CHAT_ID, "caption": caption},
                files={"document": (os.path.basename(path), f)},
                timeout=30,
            )
    except Exception as e:
        print("[TG DOC ERR]", e, path)

def tg_send_codeblock(lines):
    """Envoie un bloc de code formaté."""
    text = "```\n" + "\n".join(lines) + "\n```"
    tg_send(text)
