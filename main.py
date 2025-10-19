# Fichier: main.py
import os, time, ccxt, pandas as pd
from ta.volatility import BollingerBands
import database, trader, notifier

# --- (Toute la section des PARAMS et des fonctions de base (create_exchange, etc.) reste ici) ---
# ... (copiez-collez cette section depuis la réponse précédente, elle est stable)

_last_update_id = None
_paused = False

def process_callback_query(callback_query: Dict):
    """Gère les clics sur les boutons interactifs."""
    global _paused
    data = callback_query.get('data', '')
    
    if data == 'pause':
        _paused = True
        notifier.tg_send("⏸️ Bot mis en pause.")
    elif data == 'resume':
        _paused = False
        notifier.tg_send("▶️ Bot relancé.")
    elif data == 'list_positions':
        positions = database.get_open_positions()
        notifier.format_open_positions(positions)
    elif data.startswith('close_trade_'):
        try:
            trade_id = int(data.split('_')[-1])
            notifier.tg_send(f"Ordre de fermeture pour le trade #{trade_id} en cours...")
            trader.close_position_manually(create_exchange(), trade_id)
        except (ValueError, IndexError):
            notifier.tg_send("Commande de fermeture invalide.")

def process_message(message: Dict):
    """Gère les commandes textuelles de l'utilisateur."""
    text = message.get("text", "").strip().lower()
    if text.startswith("/start"):
        notifier.format_start_message(_paused)
    elif text.startswith("/pos"):
        positions = database.get_open_positions()
        notifier.format_open_positions(positions)

def poll_telegram_updates():
    """Récupère et distribue les mises à jour de Telegram."""
    global _last_update_id
    updates = notifier.tg_get_updates(_last_update_id + 1 if _last_update_id else None)
    for upd in updates:
        _last_update_id = upd.get("update_id", _last_update_id)
        if 'callback_query' in upd:
            process_callback_query(upd['callback_query'])
        elif 'message' in upd:
            process_message(upd['message'])

def main():
    ex = create_exchange()
    database.setup_database()

    print("Bot démarré. Envoyez /start sur Telegram pour afficher le menu.")
    
    universe = build_universe(ex)
    last_ts_seen = {}
    state = {}

    while True:
        try:
            poll_telegram_updates()
            
            if _paused:
                time.sleep(LOOP_DELAY)
                continue
            
            trader.manage_open_positions(ex)
            
            for sym in universe:
                df = fetch_ohlcv_df(ex, sym, TIMEFRAME)
                if df is None or last_ts_seen.get(sym) == df.index[-1]: continue
                last_ts_seen[sym] = df.index[-1]
                
                sig = detect_signal(df, state, sym)
                if sig:
                    trader.execute_trade(ex, sym, sig, df)
            
            time.sleep(LOOP_DELAY)

        except KeyboardInterrupt:
            notifier.tg_send("⛔ Arrêt manuel.")
            break
        except Exception as e:
            notifier.tg_send_error("Erreur critique (boucle)", e)
            print(f"Erreur critique: {e}")
            time.sleep(15)

if __name__ == "__main__":
    main()
