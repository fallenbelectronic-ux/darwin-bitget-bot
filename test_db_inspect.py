# Fichier: test_db_inspect.py
import time
import json
import database


def main():
    # 1) Trades fermés (table TRADES)
    since_ts = 0  # on prend TOUT l'historique
    try:
        trades = database.get_closed_trades_since(since_ts) or []
    except Exception as e:
        print(f"[TRADES] Erreur get_closed_trades_since: {e}")
        trades = []

    print(f"\n===== TRADES FERMÉS (get_closed_trades_since) =====")
    print(f"Nombre total de trades fermés: {len(trades)}\n")

    for i, t in enumerate(trades[:10], start=1):
        try:
            d = dict(t)
        except Exception:
            d = t
        print(f"--- TRADE #{i} ---")
        print(json.dumps(d, ensure_ascii=False, default=str, indent=2))

    # 2) Exécutions (table EXECUTIONS_LOG)
    print(f"\n===== EXECUTIONS_LOG (fetch_recent_executions) =====")
    try:
        execs = database.fetch_recent_executions(hours=None, limit=50) or []
        print(f"Nombre total d'exécutions retournées: {len(execs)}\n")
        for i, e in enumerate(execs[:10], start=1):
            print(f"--- EXECUTION #{i} ---")
            print(json.dumps(e, ensure_ascii=False, default=str, indent=2))
    except Exception as e:
        print(f"[EXECUTIONS_LOG] Erreur fetch_recent_executions: {e}")


if __name__ == "__main__":
    main()
