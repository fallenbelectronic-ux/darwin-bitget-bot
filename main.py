# Fichier: main.py
import os
import time
import ccxt
import pandas as pd
from ta.volatility import BollingerBands
from typing import List, Dict, Any, Optional
from datetime import datetime
import pytz
import traceback

import database
import trader
import notifier
import utils

# --- PARAMÈTRES ---
BITGET_TESTNET   = os.getenv("BITGET_TESTNET", "true").lower() in ("1", "true", "yes")
API_KEY          = os.getenv("BITGET_API_KEY", "")
API_SECRET       = os.getenv("BITGET_API_SECRET", "")
PASSPHRASSE      = os.getenv("BITGET_API_PASSWORD", "") or os.getenv("BITGET_PASSPHRASSE", "")
TIMEFRAME        = os.getenv("TIMEFRAME", "1h")
UNIVERSE_SIZE    = int(os.getenv("UNIVERSE_SIZE", "30"))
MIN_RR           = float(os.getenv("MIN_RR", "3.0"))
MM_DEAD_ZONE_PERCENT = float(os.getenv("MM_DEAD_ZONE_PERCENT", "0.1"))
TICK_RATIO       = 0.0005
LOOP_DELAY       = int(os.getenv("LOOP_DELAY", "5"))
FALLBACK_TESTNET = ["BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT", "LINK/USDT:USDT"]
TIMEZONE         = os.getenv("TIMEZONE", "Europe/Lisbon")
REPORT_HOUR      = int(os.getenv("REPORT_HOUR", "21"))
REPORT_WEEKDAY   = int(os.getenv("REPORT_WEEKDAY", "6"))

# --- VARIABLES GLOBALES DE STATUT ---
_last_update_id: Optional[int] = None
_paused = False
_last_daily_report_day = -1
_last_weekly_report_day = -1

# --- FONCTIONS UTILITAIRES ---
def create_exchange():
    ex = ccxt.bitget({"apiKey": API_KEY, "secret": API_SECRET, "password": PASSPHRASSE, "enableRateLimit": True, "options": {"defaultType": "swap", "testnet": BITGET_TESTNET}})
    if BITGET_TESTNET: ex.set_sandbox_mode(True)
    return ex

def build_universe(ex: ccxt.Exchange) -> List[str]:
    print("Construction de l'univers de trading...")
    try:
        markets = ex.load_markets()
        symbols = [m['symbol'] for m in markets.values() if m.get('swap') and m.get('quote') == 'USDT' and m.get('linear')]
        if not symbols: return FALLBACK_TESTNET
        return symbols[:UNIVERSE_SIZE]
    except Exception as e:
        print(f"Impossible de construire l'univers via l'API. Erreur: {e}. Utilisation de la liste de secours.")
        return FALLBACK_TESTNET

# --- LOGIQUE DE TRADING ---
def detect_signal(df: pd.DataFrame) -> Optional[Dict[str, Any]]:
    if df is None or len(df) < 81: return None
    bb_20 = BollingerBands(close=df['close'], window=20, window_dev=2); df['bb20_up'], df['bb20_mid'], df['bb20_lo'] = bb_20.bollinger_hband(), bb_20.bollinger_mavg(), bb_20.bollinger_lband()
    bb_80 = BollingerBands(close=df['close'], window=80, window_dev=2); df['bb80_up'], df['bb80_mid'], df['bb80_lo'] = bb_80.bollinger_hband(), bb_80.bollinger_mavg(), bb_80.bollinger_lband()
    last = df.iloc[-1]; is_uptrend = last['close'] > last['bb20_mid']; is_downtrend = last['close'] < last['bb20_mid']; dead_zone = last['bb20_mid'] * (MM_DEAD_ZONE_PERCENT / 100); is_in_dead_zone = abs(last['close'] - last['bb20_mid']) < dead_zone; signal, tick_size = None, last['close'] * TICK_RATIO
    if not is_in_dead_zone:
        if is_uptrend and last['low'] <= last['bb20_lo']:
            entry, sl, tp = last['close'], last['low']-(2*tick_size), last['bb20_mid']
            if entry > sl and (tp-entry)/(entry-sl) >= MIN_RR: signal = {"side":"buy", "regime":"Tendance", "entry":entry, "sl":sl, "tp":tp, "rr":(tp-entry)/(entry-sl)}
        elif is_downtrend and last['high'] >= last['bb20_up']:
            entry, sl, tp = last['close'], last['high']+(2*tick_size), last['bb20_mid']
            if entry < sl and (entry-tp)/(sl-entry) >= MIN_RR: signal = {"side":"sell", "regime":"Tendance", "entry":entry, "sl":sl, "tp":tp, "rr":(entry-tp)/(sl-entry)}
    if not signal:
        if last['low'] <= last['bb20_lo'] and last['low'] <= last['bb80_lo'] and last['close'] > last['bb20_lo']:
            entry, sl, tp = last['close'], last['low']-(2*tick_size), last['bb20_mid']
            if entry > sl and (tp-entry)/(entry-sl) >= MIN_RR: signal = {"side":"buy", "regime":"Contre-tendance", "entry":entry, "sl":sl, "tp":tp, "rr":(tp-entry)/(entry-sl)}
        elif last['high'] >= last['bb20_up'] and last['high'] >= last['bb80_up'] and last['close'] < last['bb20_up']:
            entry, sl, tp = last['close'], last['high']+(2*tick_size), last['bb20_mid']
            if entry < sl and (entry-tp)/(sl-entry) >= MIN_RR: signal = {"side":"sell", "regime":"Contre-tendance", "entry":entry, "sl":sl, "tp":tp, "rr":(entry-tp)/(sl-entry)}
    if signal: signal['bb20_mid'] = last['bb20_mid']; return signal
    return None

# --- GESTION TELEGRAM ---
def process_callback_query(callback_query: Dict):
    global _paused; data = callback_query.get('data', '')
    if data == 'pause': _paused = True; notifier.tg_send("⏸️ Bot mis en pause.")
    elif data == 'resume': _paused = False; notifier.tg_send("▶️ Bot relancé.")
    elif data == 'list_positions': notifier.format_open_positions(database.get_open_positions())
    elif data == 'get_stats':
        notifier.tg_send("📊 Calcul du bilan hebdomadaire..."); seven_days_ago = int(time.time()) - 7 * 24 * 60 * 60; trades = database.get_closed_trades_since(seven_days_ago); notifier.send_report("📊 Bilan Hebdomadaire (7 derniers jours)", trades)
    elif data.startswith('close_trade_'):
        try: trade_id = int(data.split('_')[-1]); trader.close_position_manually(create_exchange(), trade_id)
        except (ValueError, IndexError): notifier.tg_send("Commande de fermeture invalide.")

def process_message(message: Dict):
    text = message.get("text", "").strip().lower()
    if text.startswith(("/start", "/menu")): notifier.send_main_menu(_paused)
    elif text.startswith("/pos"): notifier.format_open_positions(database.get_open_positions())
    elif text.startswith("/stats"):
        notifier.tg_send("📊 Calcul du bilan hebdomadaire..."); seven_days_ago = int(time.time()) - 7 * 24 * 60 * 60; trades = database.get_closed_trades_since(seven_days_ago); notifier.send_report("📊 Bilan Hebdomadaire (7 derniers jours)", trades)

def poll_telegram_updates():
    """Récupère et distribue les mises à jour de Telegram. VERSION CORRIGÉE ET STABLE."""
    global _last_update_id
    updates = notifier.tg_get_updates(_last_update_id + 1 if _last_update_id else None)
    for upd in updates:
        _last_update_id = upd.get("update_id", _last_update_id)
        if 'callback_query' in upd:
            process_callback_query(upd['callback_query'])
        elif 'message' in upd:
            process_message(upd['message'])

# --- GESTION DES RAPPORTS ---
def check_scheduled_reports():
    global _last_daily_report_day, _last_weekly_report_day
    try: tz = pytz.timezone(TIMEZONE)
    except pytz.UnknownTimeZoneError: tz = pytz.timezone("UTC")
    now = datetime.now(tz)
    if now.hour == REPORT_HOUR and now.day != _last_daily_report_day:
        _last_daily_report_day = now.day; one_day_ago = int(time.time()) - 24 * 60 * 60; trades = database.get_closed_trades_since(one_day_ago); notifier.send_report("📊 Bilan Quotidien (24h)", trades)
    if now.weekday() == REPORT_WEEKDAY and now.hour == REPORT_HOUR and now.day != _last_weekly_report_day:
        _last_weekly_report_day = now.day; seven_days_ago = int(time.time()) - 7 * 24 * 60 * 60; trades = database.get_closed_trades_since(seven_days_ago); notifier.send_report("🗓️ Bilan Hebdomadaire", trades)

# --- BOUCLE PRINCIPALE ---
def main():
    ex = create_exchange()
    database.setup_database()
    notifier.send_start_banner("TESTNET" if BITGET_TESTNET else "LIVE", "PAPER" if trader.PAPER_TRADING_MODE else "RÉEL", trader.RISK_PER_TRADE_PERCENT)
    universe = build_universe(ex)
    if not universe: notifier.tg_send("❌ Impossible de construire l'univers de trading. Arrêt du bot."); return
    print(f"Univers de trading chargé avec {len(universe)} paires.")
    
    last_ts_seen = {}
    
    while True:
        print(f"\n--- [{time.strftime('%Y-%m-%d %H:%M:%S')}] Nouveau cycle de la boucle ---")
        try:
            print("1. Vérification des mises à jour Telegram...")
            poll_telegram_updates()
            print("2. Vérification des rapports planifiés...")
            check_scheduled_reports()
            if _paused:
                print("   -> Bot en pause, attente..."); time.sleep(LOOP_DELAY); continue
            print("3. Gestion des positions ouvertes (TP dynamique)...")
            trader.manage_open_positions(ex)
            print(f"4. Début du scan de l'univers ({len(universe)} paires)...")
            for symbol in universe:
                df = utils.fetch_ohlcv_df(ex, symbol, TIMEFRAME)
                if df is None or df.empty: continue
                last_candle_ts = df.index[-1]
                if symbol in last_ts_seen and last_ts_seen[symbol] == last_candle_ts: continue 
                last_ts_seen[symbol] = last_candle_ts
                signal = detect_signal(df)
                if signal:
                    print(f"✅ Signal '{signal['regime']}' détecté pour {symbol}!")
                    trader.execute_trade(ex, symbol, signal, df)
            print(f"--- Fin du cycle. Attente de {LOOP_DELAY} secondes. ---")
            time.sleep(LOOP_DELAY)
        except KeyboardInterrupt: notifier.tg_send("⛔ Arrêt manuel."); break
        except Exception:
            print("\n--- ERREUR CRITIQUE DANS LA BOUCLE PRINCIPALE ---")
            error_details = traceback.format_exc()
            print(error_details)
            notifier.tg_send_error("Erreur critique (boucle)", error_details)
            print("--------------------------------------------------")
            time.sleep(15)

if __name__ == "__main__":
    main()
