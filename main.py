# Fichier: main.py
# Version finale, complète et vérifiée, intégrant les fonctionnalités avancées dans une structure stable.

import os
import sys
import time
import ccxt
import pandas as pd
import traceback
from typing import List, Dict, Any, Optional
from datetime import datetime
import pytz

# --- Importation des modules locaux du projet ---
import database
import trader
import notifier
import utils
import reporting

# ==============================================================================
# PARAMÈTRES ET CONFIGURATION
# ==============================================================================
BITGET_TESTNET   = os.getenv("BITGET_TESTNET", "true").lower() in ("1", "true", "yes")
API_KEY          = os.getenv("BITGET_API_KEY", "")
API_SECRET       = os.getenv("BITGET_API_SECRET", "")
PASSPHRASSE      = os.getenv("BITGET_API_PASSWORD", "") or os.getenv("BITGET_PASSPHRASSE", "")

TIMEFRAME        = os.getenv("TIMEFRAME", "1h")
UNIVERSE_SIZE    = int(os.getenv("UNIVERSE_SIZE", "30"))
MIN_RR           = float(os.getenv("MIN_RR", "3.0"))
MAX_OPEN_POSITIONS = int(os.getenv("MAX_OPEN_POSITIONS", 3))
LOOP_DELAY       = int(os.getenv("LOOP_DELAY", "10"))
TIMEZONE         = os.getenv("TIMEZONE", "Europe/Lisbon")
REPORT_HOUR      = int(os.getenv("REPORT_HOUR", "21"))
REPORT_WEEKDAY   = int(os.getenv("REPORT_WEEKDAY", "6")) # 0=Lundi, 6=Dimanche
FALLBACK_TESTNET = ["BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT"]

# ==============================================================================
# VARIABLES GLOBALES D'ÉTAT
# ==============================================================================
_last_update_id: Optional[int] = None
_paused = False
_last_daily_report_day = -1
_last_weekly_report_day = -1

# ==============================================================================
# FONCTIONS DE BASE DU BOT
# ==============================================================================

def startup_checks():
    """Vérifie la présence des variables d'environnement critiques au démarrage."""
    print("Vérification des configurations au démarrage...")
    if not all([API_KEY, API_SECRET, PASSPHRASSE]):
        error_msg = "❌ ERREUR DE DÉMARRAGE: Les clés API (KEY, SECRET, PASSPHRASSE) sont manquantes."
        print(error_msg); notifier.tg_send(error_msg); sys.exit(1)
    print("✅ Configuration de base validée.")

def create_exchange() -> ccxt.Exchange:
    """Initialise et retourne l'objet de l'exchange CCXT."""
    ex = ccxt.bitget({"apiKey": API_KEY, "secret": API_SECRET, "password": PASSPHRASSE, "enableRateLimit": True, "options": {"defaultType": "swap"}})
    if BITGET_TESTNET: ex.set_sandbox_mode(True)
    return ex

def build_universe(ex: ccxt.Exchange) -> List[str]:
    """Construit la liste des paires à trader, triées par volume."""
    print("Construction de l'univers de trading...")
    try:
        ex.load_markets()
        tickers = ex.fetch_tickers()
        swap_tickers = {s: t for s, t in tickers.items() if ':USDT' in s and t.get('quoteVolume')}
        sorted_symbols = sorted(swap_tickers, key=lambda s: swap_tickers[s]['quoteVolume'], reverse=True)
        print(f"Top {UNIVERSE_SIZE} paires par volume sélectionnées.")
        return sorted_symbols[:UNIVERSE_SIZE]
    except Exception as e:
        print(f"Impossible de construire l'univers via l'API. Erreur: {e}. Utilisation de la liste de secours."); return FALLBACK_TESTNET

# ==============================================================================
# GESTION DES INTERACTIONS TELEGRAM
# ==============================================================================

def process_callback_query(callback_query: Dict):
    """Gère les clics sur les boutons interactifs."""
    global _paused
    data = callback_query.get('data', '')
    if data == 'pause': _paused = True; notifier.tg_send("⏸️ Bot mis en pause.")
    elif data == 'resume': _paused = False; notifier.tg_send("▶️ Bot relancé.")
    elif data == 'list_positions': notifier.format_open_positions(database.get_open_positions())
    elif data == 'get_stats':
        trades = database.get_closed_trades_since(int(time.time()) - 7*24*60*60)
        notifier.send_report("📊 Bilan des 7 derniers jours", trades)
    elif data.startswith('close_trade_'):
        try:
            trade_id = int(data.split('_')[-1])
            trader.close_position_manually(create_exchange(), trade_id)
        except (ValueError, IndexError): notifier.tg_send("Commande de fermeture invalide.")

def process_message(message: Dict):
    """Gère les commandes textuelles."""
    text = message.get("text", "").strip().lower()
    parts = text.split()
    command = parts[0]
    
    if command == "/start": notifier.send_main_menu(_paused)
    elif command == "/pos": notifier.format_open_positions(database.get_open_positions())
    elif command == "/ping": notifier.tg_send("🛰️ Pong ! Le bot est en ligne.")
    elif command == "/config":
        current_max_pos = database.get_setting('MAX_OPEN_POSITIONS', MAX_OPEN_POSITIONS)
        notifier.send_config_message(min_rr=MIN_RR, risk=trader.RISK_PER_TRADE_PERCENT, max_pos=current_max_pos, leverage=trader.LEVERAGE)
    elif command == "/setmaxpos":
        if len(parts) < 2: notifier.tg_send("Usage: <code>/setmaxpos &lt;nombre&gt;</code>"); return
        try:
            new_max = int(parts[1])
            if new_max >= 0:
                database.set_setting('MAX_OPEN_POSITIONS', new_max)
                notifier.tg_send(f"✅ Nombre max de positions mis à jour à <b>{new_max}</b>.")
            else: notifier.tg_send("❌ Le nombre doit être >= 0.")
        except ValueError: notifier.tg_send("❌ Valeur invalide.")

def poll_telegram_updates():
    """Récupère et distribue les mises à jour de Telegram."""
    global _last_update_id
    updates = notifier.tg_get_updates(_last_update_id + 1 if _last_update_id else None)
    for upd in updates:
        _last_update_id = upd.get("update_id", _last_update_id)
        if 'callback_query' in upd: process_callback_query(upd['callback_query'])
        elif 'message' in upd: process_message(upd['message'])

# ==============================================================================
# GESTION DES RAPPORTS AUTOMATIQUES
# ==============================================================================

def check_scheduled_reports():
    """Vérifie s'il est temps d'envoyer un rapport quotidien ou hebdomadaire."""
    global _last_daily_report_day, _last_weekly_report_day
    try: tz = pytz.timezone(TIMEZONE)
    except pytz.UnknownTimeZoneError: tz = pytz.timezone("UTC")
    now = datetime.now(tz)
    
    # Rapport quotidien
    if now.hour == REPORT_HOUR and now.day != _last_daily_report_day:
        _last_daily_report_day = now.day
        trades = database.get_closed_trades_since(int(time.time()) - 24*60*60)
        notifier.send_report("📊 Bilan Quotidien (24h)", trades)
    
    # Rapport hebdomadaire
    if now.weekday() == REPORT_WEEKDAY and now.hour == REPORT_HOUR and now.day != _last_weekly_report_day:
        _last_weekly_report_day = now.day
        trades = database.get_closed_trades_since(int(time.time()) - 7*24*60*60)
        notifier.send_report("🗓️ Bilan Hebdomadaire", trades)

# ==============================================================================
# BOUCLE PRINCIPALE DE TRADING
# ==============================================================================

def main():
    """Fonction principale du bot."""
    startup_checks()
    ex = create_exchange()
    database.setup_database()
    notifier.send_start_banner("TESTNET" if BITGET_TESTNET else "LIVE", "PAPIER" if trader.PAPER_TRADING_MODE else "RÉEL", trader.RISK_PER_TRADE_PERCENT)
    universe = build_universe(ex)
    last_ts_seen = {}

    while True:
        try:
            poll_telegram_updates()
            check_scheduled_reports()
            
            if _paused:
                print("   -> Bot en pause, attente...")
                time.sleep(LOOP_DELAY)
                continue
            
            pending_signals = []
            print(f"\n--- [{time.strftime('%Y-%m-%d %H:%M:%S')}] Début du scan de l'univers ---")
            for symbol in universe:
                df = utils.fetch_and_prepare_df(ex, symbol, TIMEFRAME)
                if df is None or (symbol in last_ts_seen and last_ts_seen[symbol] == df.index[-1]): continue
                last_ts_seen[symbol] = df.index[-1]
                
                signal = trader.detect_signal(df, symbol)
                if signal:
                    print(f"  -> Opportunité trouvée pour {symbol} (RR: {signal['rr']:.2f})")
                    pending_signals.append({'symbol': symbol, 'signal': signal, 'df': df})
            
            if pending_signals:
                pending_signals.sort(key=lambda item: item['signal']['rr'], reverse=True)
                best = pending_signals[0]
                
                notifier.send_confirmed_signal_notification(best['symbol'], best['signal'], len(pending_signals))
                trader.execute_trade(ex, best['symbol'], best['signal'], best['df'])
            else:
                print("--- Fin du scan. Aucune opportunité valide trouvée. ---")

            time.sleep(LOOP_DELAY)

        except KeyboardInterrupt:
            notifier.tg_send("⛔ Arrêt manuel.")
            break
        except Exception:
            error_details = traceback.format_exc()
            print(error_details)
            notifier.tg_send_error("Erreur critique (boucle)", error_details)
            time.sleep(15)

if __name__ == "__main__":
    main()
