# --- AJOUTER À LA FIN DE notifier.py ---

def list_last(n: int = 100):
    """
    Liste les n derniers messages envoyés par le bot, avec leur type et date.
    (Ne supprime rien)
    """
    with _lock:
        idx = _load_index()
        if not idx:
            tg_send("🧾 Aucun message trouvé dans l’historique.", kind="info")
            return

        last = idx[-n:]
        lines = []
        for rec in reversed(last):  # les plus récents en haut
            ts = rec.get("ts", "")
            kind = rec.get("kind", "info")
            mid = rec.get("id", "?")
            try:
                dt_utc = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                dt_local = dt_utc.astimezone(ZoneInfo(TZ))
                date_txt = dt_local.strftime("%d/%m %H:%M")
            except Exception:
                date_txt = ts
            lines.append(f"{date_txt} — {kind} (id {mid})")

        msg = "🧾 *Derniers messages envoyés*\n\n" + "\n".join(lines)
        tg_send(msg, kind="info")
