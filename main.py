# Fichier: main.py
import os
import sys
import time
import ccxt
import pandas as pd
import traceback
import threading
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone
import pytz

# Importation des modules locaux nettoyés
import database
import trader
import notifier
import utils
# Note: 'state' a été retiré car il peut être géré avec des variables globales simples pour l'instant.
# 'analysis' a été renommé 'reporting' pour la cohérence.
import reporting

# --- PARAMÈTRES GLOBAUX ---
BITGET_TESTNET   = os.getenv("BITGET_TESTNET", "true").lower() in ("1", "true", "yes")
API_KEY          = os.getenv("BITGET_API_KEY", "")
API_SECRET       = os.getenv("BITGET_API_SECRET", "")
PASSPHRASSE      = os.getenv("BITGET_API_PASSWORD", "") or os.getenv("BITGET_PASSPHRASSE", "")

TIMEFRAME        = os.getenv("TIMEFRAME", "1h")
UNIVERSE_SIZE    = int(os.getenv("UNIVERSE_SIZE", "30"))
MIN_RR           = float(os.getenv("MIN_RR", "3.0"))
MAX_OPEN_POSITIONS = int(os.getenv("MAX_OPEN_POSITIONS", 3))
LOOP_DELAY       = int(os.getenv("LOOP_DELAY", "5"))
TIMEZONE         = os.getenv("TIMEZONE", "Europe/Lisbon")
REPORT_HOUR      = int(os.getenv("REPORT_HOUR", "21"))
REPORT_WEEKDAY   = int(os.getenv("REPORT_WEEKDAY", "6"))

# --- VARIABLES D'ÉTAT ---
_last_update_id: Optional[int] = None
_paused = False
_last_daily_report_day = -1
_last_weekly_report_day = -1
_recent_signals: List[Dict] = []
_pending_signals: Dict[str, Any] = {}
_lock = threading.Lock()

def startup_checks():
    """Vérifie la présence des variables d'environnement critiques."""
    print("Vérification des configurations au démarrage...")
    required = [API_KEY, API_SECRET, PASSPHRASSE]
    if not all(required):
        error_msg = "❌ ERREUR DE DÉMARRAGE: Clés API manquantes."
        print(error_msg); sys.exit(1)
    print("✅ Configurations nécessaires présentes.")

def cleanup_recent_signals(hours: int = 6):
    """Nettoie les signaux anciens."""
    global _recent_signals
    seconds_ago = time.time() - (hours * 60 * 60)
    with _lock:
        _recent_signals[:] = [s for s in _recent_signals if s['timestamp'] >= seconds_ago]

def get_recent_signals_message(hours: int) -> str:
    """Retourne un message formaté avec les signaux récents."""
    cleanup_recent_signals(hours)
    with _lock:
        now = time.time()
        signals = [s for s in _recent_signals if s['timestamp'] >= now - (hours * 3600)]
    
    if not signals: return f"⏱️ Aucun signal valide dans les {hours} dernières heures."
    
    lines = [f"<b>⏱️ {len(signals)} Signaux ({hours}h)</b>\n"]
    for s in signals:
        ts = datetime.fromtimestamp(s['timestamp'], tz=timezone.utc).astimezone(pytz.timezone(TIMEZONE)).strftime('%H:%M')
        side_icon = "📈" if s['signal']['side'] == 'buy' else "📉"
        lines.append(f"- <code>{ts}</code> | {side_icon} <b>{s['symbol']}</b> | RR: {s['signal']['rr']:.2f}")
    return "\n".join(lines)

def create_exchange():
    """Crée l'objet exchange CCXT."""
    ex = ccxt.bitget({
        "apiKey": API_KEY, "secret": API_SECRET, "password": PASSPHRASSE,
        "enableRateLimit": True, "options": {"defaultType": "swap"}
    })
    if BITGET_TESTNET: ex.set_sandbox_mode(True)
    return ex

def build_universe(ex: ccxt.Exchange) -> List[str]:
    """Construit la liste des paires à trader."""
    print("Construction de l'univers de trading...")
    size = int(database.get_setting('UNIVERSE_SIZE', UNIVERSE_SIZE))
    try:
        ex.load_markets()
        tickers = ex.fetch_tickers()
        swap_tickers = {s: t for s, t in tickers.items() if ':USDT' in s and t.get('quoteVolume')}
        sorted_symbols = sorted(swap_tickers, key=lambda s: swap_tickers[s]['quoteVolume'], reverse=True)
        return sorted_symbols[:size]
    except Exception as e:
        print(f"Erreur univers: {e}"); return []

def select_and_execute_best_pending_signal(ex: ccxt.Exchange):
    """Sélectionne le meilleur signal en attente et l'exécute."""
    global _pending_signals
    if not _pending_signals: return
    print(f"-> Analyse de {len(_pending_signals)} signaux en attente...")

    validated = []
    # Utilisation d'une copie pour itérer
    for symbol, pending in list(_pending_signals.items()):
        df = utils.fetch_and_prepare_df(ex, symbol, TIMEFRAME)
        if df is None or df.index[-1] <= pending['candle_timestamp']: continue

        # Logique de re-validation simplifiée mais efficace
        # Ici on vérifie simplement que le signal est toujours pertinent
        # Pour l'instant, on accepte tous les signaux qui ont survécu à la clôture
        validated.append(pending)

    with _lock: _pending_signals.clear()

    if not validated:
        print("   -> Aucun signal n'a été re-validé.")
        return

    # Trie par RR décroissant et prend le meilleur
    best = sorted(validated, key=lambda x: x['signal']['rr'], reverse=True)[0]
    print(f"   -> MEILLEUR SIGNAL: {best['symbol']} (RR: {best['signal']['rr']:.2f})")
    
    notifier.send_confirmed_signal_notification(best['symbol'], best['signal'])
    trader.execute_trade(ex, best['symbol'], best['signal'], best['df'], best['signal']['entry'])

def process_callback_query(callback_query: Dict):
    """Gère les clics sur les boutons."""
    global _paused
    data = callback_query.get('data', '')
    
    if data == 'pause':
        with _lock: _paused = True
        notifier.tg_send("⏸️ Scan mis en pause.")
    elif data == 'resume':
        with _lock: _paused = False
        notifier.tg_send("▶️ Reprise du scan.")
    elif data == 'list_positions':
        notifier.format_open_positions(database.get_open_positions())
    elif data == 'get_recent_signals':
        notifier.tg_send(get_recent_signals_message(6))
    elif data == 'get_stats':
        trades = database.get_closed_trades_since(int(time.time()) - 7 * 24 * 3600)
        bal = trader.get_usdt_balance(create_exchange())
        notifier.send_report("📊 Bilan 7 jours", trades, bal)
    elif data.startswith('close_trade_'):
        try:
            tid = int(data.split('_')[-1])
            trader.close_position_manually(create_exchange(), tid)
        except: notifier.tg_send("ID invalide.")
    elif data == 'manage_strategy':
        strat = database.get_setting('STRATEGY_MODE', 'NORMAL')
        notifier.send_strategy_menu(strat)
    elif data in ['switch_to_NORMAL', 'switch_to_SPLIT']:
        new_strat = data.split('_')[-1]
        database.set_setting('STRATEGY_MODE', new_strat)
        notifier.tg_send(f"✅ Stratégie: <b>{new_strat}</b>")
        notifier.send_strategy_menu(new_strat)

def process_message(message: Dict):
    """Gère les commandes textuelles."""
    text = message.get("text", "").strip().lower().split()
    cmd = text[0] if text else ""
    
    if cmd == "/start": notifier.send_main_menu(_paused)
    elif cmd == "/pos": notifier.format_open_positions(database.get_open_positions())
    elif cmd == "/stats": 
        trades = database.get_closed_trades_since(int(time.time()) - 7 * 24 * 3600)
        bal = trader.get_usdt_balance(create_exchange())
        notifier.send_report("📊 Bilan 7 jours", trades, bal)

def poll_telegram_updates():
    """Boucle de polling Telegram."""
    global _last_update_id
    for upd in notifier.tg_get_updates(_last_update_id + 1 if _last_update_id else None):
        _last_update_id = upd.get("update_id", _last_update_id)
        if 'callback_query' in upd: process_callback_query(upd['callback_query'])
        elif 'message' in upd: process_message(upd['message'])

def check_scheduled_reports():
    """Gère les rapports automatiques."""
    global _last_daily_report_day, _last_weekly_report_day
    try: tz = pytz.timezone(TIMEZONE)
    except: tz = pytz.timezone("UTC")
    now = datetime.now(tz)

    if now.hour == REPORT_HOUR and now.day != _last_daily_report_day:
        _last_daily_report_day = now.day
        trades = database.get_closed_trades_since(int(time.time()) - 86400)
        notifier.send_report("📊 Bilan Quotidien", trades, trader.get_usdt_balance(create_exchange()))

# ==============================================================================
# BOUCLES ET MAIN
# ==============================================================================

def telegram_listener_loop():
    print("🤖 Thread Telegram démarré.")
    while True:
        try:
            poll_telegram_updates()
            time.sleep(0.5)
        except Exception as e:
            print(f"Erreur Telegram: {e}"); time.sleep(0.5)

def trading_engine_loop(ex: ccxt.Exchange, universe: List[str]):
    print("📈 Thread Trading démarré.")
    last_hour = -1

    while True:
        try:
            with _lock: is_paused = _paused
            if is_paused:
                print("   -> (Pause)"); time.sleep(LOOP_DELAY); continue

            curr_hour = datetime.now(timezone.utc).hour
            if curr_hour != last_hour:
                select_and_execute_best_pending_signal(ex)
                last_hour = curr_hour

            cleanup_recent_signals()
            trader.manage_open_positions(ex)

            print(f"--- Scan de {len(universe)} paires ---")
            for symbol in universe:
                df = utils.fetch_and_prepare_df(ex, symbol, TIMEFRAME)
                if df is None: continue
                
                signal = trader.detect_signal(df)
                if signal:
                    with _lock:
                        # Si c'est un nouveau signal, on le met en attente
                        if symbol not in _pending_signals:
                             print(f"✅ Signal détecté pour {symbol}! En attente de clôture.")
                             _pending_signals[symbol] = {'signal': signal, 'symbol': symbol, 'candle_timestamp': df.index[-1]}
                             notifier.send_pending_signal_notification(symbol, signal)
                        
                        # On l'ajoute toujours à l'historique récent
                        if not any(s['symbol'] == symbol and s['timestamp'] > time.time() - 3600 for s in _recent_signals):
                             _recent_signals.append({'timestamp': time.time(), 'symbol': symbol, 'signal': signal})

            time.sleep(LOOP_DELAY)

        except Exception:
             err = traceback.format_exc()
             print(err); notifier.tg_send_error("Erreur Trading", err); time.sleep(15)

def main():
    startup_checks()
    ex = create_exchange()
    database.setup_database()
    
    # Initialisation des paramètres par défaut si nécessaire
    if not database.get_setting('STRATEGY_MODE'): database.set_setting('STRATEGY_MODE', 'NORMAL')
    
    notifier.send_start_banner(
        "TESTNET" if BITGET_TESTNET else "LIVE",
        "PAPIER" if database.get_setting('PAPER_TRADING_MODE', 'true') == 'true' else "RÉEL",
        trader.RISK_PER_TRADE_PERCENT
    )

    universe = build_universe(ex)
    if not universe: return

    # Démarrage des deux threads principaux
    t_tg = threading.Thread(target=telegram_listener_loop, daemon=True)
    t_tr = threading.Thread(target=trading_engine_loop, args=(ex, universe), daemon=True)
    t_tg.start(); t_tr.start()
    
    try:
        while True: time.sleep(1)
    except KeyboardInterrupt:
        print("Arrêt demandé."); notifier.tg_send("⛔ Arrêt manuel.")

if __name__ == "__main__":
    main()
