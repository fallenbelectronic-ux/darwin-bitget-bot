# Fichier: main.py
import os, time, ccxt, pandas as pd, traceback
from ta.volatility import BollingerBands
from typing import List, Dict, Any, Optional
from datetime import datetime
import pytz
import database, trader, notifier, utils

# --- PARAMÈTRES ---
# ... (inchangés)
BITGET_TESTNET, API_KEY, API_SECRET, PASSPHRASSE = os.getenv("BITGET_TESTNET", "true").lower() in ("1", "true", "yes"), os.getenv("BITGET_API_KEY", ""), os.getenv("BITGET_API_SECRET", ""), os.getenv("BITGET_API_PASSWORD", "") or os.getenv("BITGET_PASSPHRASSE", "")
TIMEFRAME, UNIVERSE_SIZE, MIN_RR = os.getenv("TIMEFRAME", "1h"), int(os.getenv("UNIVERSE_SIZE", "30")), float(os.getenv("MIN_RR", "3.0"))
MM_DEAD_ZONE_PERCENT, TICK_RATIO, LOOP_DELAY = float(os.getenv("MM_DEAD_ZONE_PERCENT", "0.1")), 0.0005, int(os.getenv("LOOP_DELAY", "5"))
FALLBACK_TESTNET, TIMEZONE, REPORT_HOUR, REPORT_WEEKDAY = ["BTC/USDT:USDT", "ETH/USDT:USDT"], os.getenv("TIMEZONE", "Europe/Lisbon"), int(os.getenv("REPORT_HOUR", "21")), int(os.getenv("REPORT_WEEKDAY", "6"))
_last_update_id: Optional[int] = None; _paused = False; _last_daily_report_day, _last_weekly_report_day = -1, -1

# --- Fonctions de logique (inchangées) ---
def create_exchange(): ex = ccxt.bitget({"apiKey": API_KEY, "secret": API_SECRET, "password": PASSPHRASSE, "enableRateLimit": True, "options": {"defaultType": "swap", "testnet": BITGET_TESTNET}}); ex.set_sandbox_mode(True) if BITGET_TESTNET else None; return ex
def build_universe(ex: ccxt.Exchange) -> List[str]:
    print("Construction de l'univers de trading..."); try: markets = ex.load_markets(); symbols = [m['symbol'] for m in markets.values() if m.get('swap') and m.get('quote') == 'USDT' and m.get('linear')]; return symbols[:UNIVERSE_SIZE] if symbols else FALLBACK_TESTNET
    except Exception as e: print(f"Impossible de construire l'univers via l'API. Erreur: {e}."); return FALLBACK_TESTNET
def detect_signal(symbol: str, df: pd.DataFrame) -> Optional[Dict[str, Any]]:
    if df is None or len(df) < 81: return None; df = trader._get_indicators(df.copy());
    if df is None: return None; last = df.iloc[-1]
    is_uptrend = last['close'] > last['bb80_mid']; is_downtrend = last['close'] < last['bb80_mid']; is_in_dead_zone = abs(last['close'] - last['bb80_mid']) < (last['bb80_mid'] * (MM_DEAD_ZONE_PERCENT/100))
    signal, tick_size = None, last['close'] * TICK_RATIO
    if not is_in_dead_zone:
        if is_uptrend and last['low'] <= last['bb20_lo'] and last['close'] > last['bb20_lo']:
            entry = (last['open'] + last['close']) / 2; sl, tp = last['low']-(2*tick_size), last['bb20_up']; rr = (tp-entry)/(entry-sl) if (entry-sl) > 0 else 0
            if rr >= MIN_RR: signal = {"side":"buy", "regime":"Tendance", "entry":entry, "sl":sl, "tp":tp, "rr":rr}
            else: print(f"  -> INFO: Setup d'ACHAT (Tendance) pour {symbol} rejeté. Ratio RR: {rr:.2f} (Min: {MIN_RR})")
        elif is_downtrend and last['high'] >= last['bb20_up'] and last['close'] < last['bb20_up']:
            entry = (last['open'] + last['close']) / 2; sl, tp = last['high']+(2*tick_size), last['bb20_lo']; rr = (entry-tp)/(sl-entry) if (sl-entry) > 0 else 0
            if rr >= MIN_RR: signal = {"side":"sell", "regime":"Tendance", "entry":entry, "sl":sl, "tp":tp, "rr":rr}
            else: print(f"  -> INFO: Setup de VENTE (Tendance) pour {symbol} rejeté. Ratio RR: {rr:.2f} (Min: {MIN_RR})")
    if not signal:
        if last['low'] <= last['bb20_lo'] and last['low'] <= last['bb80_lo'] and last['close'] > last['bb20_lo']:
            entry = (last['open'] + last['close']) / 2; sl, tp = last['low']-(2*tick_size), last['bb20_up']; rr = (tp-entry)/(entry-sl) if (entry-sl) > 0 else 0
            if rr >= MIN_RR: signal = {"side":"buy", "regime":"Contre-tendance", "entry":entry, "sl":sl, "tp":tp, "rr":rr}
            else: print(f"  -> INFO: Setup d'ACHAT (Contre-tendance) pour {symbol} rejeté. Ratio RR: {rr:.2f} (Min: {MIN_RR})")
        elif last['high'] >= last['bb20_up'] and last['high'] >= last['bb80_up'] and last['close'] < last['bb20_up']:
            entry = (last['open'] + last['close']) / 2; sl, tp = last['high']+(2*tick_size), last['bb20_lo']; rr = (entry-tp)/(sl-entry) if (sl-entry) > 0 else 0
            if rr >= MIN_RR: signal = {"side":"sell", "regime":"Contre-tendance", "entry":entry, "sl":sl, "tp":tp, "rr":rr}
            else: print(f"  -> INFO: Setup de VENTE (Contre-tendance) pour {symbol} rejeté. Ratio RR: {rr:.2f} (Min: {MIN_RR})")
    if signal: signal['bb20_mid'] = last['bb20_mid']; return signal
    return None

# --- MISE À JOUR DE LA GESTION DES COMMANDES TELEGRAM ---
def process_callback_query(callback_query: Dict):
    global _paused
    data = callback_query.get('data', '')
    
    # Commandes existantes
    if data == 'pause': _paused = True; notifier.tg_send("⏸️ Bot mis en pause.")
    elif data == 'resume': _paused = False; notifier.tg_send("▶️ Bot relancé.")
    elif data == 'list_positions': notifier.format_open_positions(database.get_open_positions())
    elif data.startswith('close_trade_'):
        try: trade_id = int(data.split('_')[-1]); trader.close_position_manually(create_exchange(), trade_id)
        except (ValueError, IndexError): notifier.tg_send("Commande de fermeture invalide.")
    elif data == 'get_stats':
        notifier.tg_send("📊 Calcul du bilan hebdomadaire..."); seven_days_ago = int(time.time()) - 7*24*60*60; trades = database.get_closed_trades_since(seven_days_ago); notifier.send_report("📊 Bilan Hebdomadaire (7 derniers jours)", trades)
    
    # NOUVELLES COMMANDES POUR LA GESTION DE STRATÉGIE
    elif data == 'manage_strategy':
        current_strategy = database.get_setting('STRATEGY_MODE', os.getenv('STRATEGY_MODE', 'NORMAL').upper())
        notifier.send_strategy_menu(current_strategy)
    elif data == 'switch_to_NORMAL':
        database.set_setting('STRATEGY_MODE', 'NORMAL'); notifier.tg_send("✅ Stratégie changée en <b>NORMAL</b>.")
        notifier.send_strategy_menu('NORMAL')
    elif data == 'switch_to_SPLIT':
        database.set_setting('STRATEGY_MODE', 'SPLIT'); notifier.tg_send("✅ Stratégie changée en <b>SPLIT</b>.")
        notifier.send_strategy_menu('SPLIT')
    elif data == 'back_to_main':
        notifier.send_main_menu(_paused)

# --- BOUCLE PRINCIPALE ET AUTRES FONCTIONS (INCHANGÉES) ---
def process_message(message: Dict):
    text = message.get("text", "").strip().lower()
    if text.startswith(("/start", "/menu")): notifier.send_main_menu(_paused)
    elif text.startswith("/pos"): notifier.format_open_positions(database.get_open_positions())
    elif text.startswith("/stats"): notifier.tg_send("📊 Calcul du bilan hebdomadaire..."); seven_days_ago = int(time.time()) - 7*24*60*60; trades = database.get_closed_trades_since(seven_days_ago); notifier.send_report("📊 Bilan Hebdomadaire (7 derniers jours)", trades)
def poll_telegram_updates():
    global _last_update_id; updates = notifier.tg_get_updates(_last_update_id + 1 if _last_update_id else None)
    for upd in updates: _last_update_id = upd.get("update_id", _last_update_id)
    if 'callback_query' in upd: process_callback_query(upd['callback_query'])
    elif 'message' in upd: process_message(upd['message'])
def check_scheduled_reports():
    global _last_daily_report_day, _last_weekly_report_day; try: tz = pytz.timezone(TIMEZONE)
    except pytz.UnknownTimeZoneError: tz = pytz.timezone("UTC");
    now = datetime.now(tz)
    if now.hour == REPORT_HOUR and now.day != _last_daily_report_day: _last_daily_report_day = now.day; one_day_ago = int(time.time()) - 24*60*60; trades = database.get_closed_trades_since(one_day_ago); notifier.send_report("📊 Bilan Quotidien (24h)", trades)
    if now.weekday() == REPORT_WEEKDAY and now.hour == REPORT_HOUR and now.day != _last_weekly_report_day: _last_weekly_report_day = now.day; seven_days_ago = int(time.time()) - 7*24*60*60; trades = database.get_closed_trades_since(seven_days_ago); notifier.send_report("🗓️ Bilan Hebdomadaire", trades)
def main():
    ex = create_exchange(); database.setup_database()
    # Initialiser la stratégie dans la DB si elle n'existe pas
    if not database.get_setting('STRATEGY_MODE'): database.set_setting('STRATEGY_MODE', os.getenv('STRATEGY_MODE', 'NORMAL').upper())
    notifier.send_start_banner("TESTNET" if BITGET_TESTNET else "LIVE", "PAPIER" if trader.PAPER_TRADING_MODE else "RÉEL", trader.RISK_PER_TRADE_PERCENT)
    universe = build_universe(ex)
    if not universe: notifier.tg_send("❌ Impossible de construire l'univers de trading. Arrêt du bot."); return
    print(f"Univers de trading chargé avec {len(universe)} paires.")
    last_ts_seen = {}
    while True:
        print(f"\n--- [{time.strftime('%Y-%m-%d %H:%M:%S')}] Nouveau cycle de la boucle ---")
        try:
            print("1. Vérification des mises à jour Telegram..."); poll_telegram_updates()
            print("2. Vérification des rapports planifiés..."); check_scheduled_reports()
            if _paused: print("   -> Bot en pause, attente..."); time.sleep(LOOP_DELAY); continue
            print("3. Gestion des positions ouvertes..."); trader.manage_open_positions(ex)
            print(f"4. Début du scan de l'univers ({len(universe)} paires)...")
            for symbol in universe:
                df = utils.fetch_ohlcv_df(ex, symbol, TIMEFRAME)
                if df is None or df.empty: continue
                last_candle_ts = df.index[-1]
                if symbol in last_ts_seen and last_ts_seen[symbol] == last_candle_ts: continue 
                last_ts_seen[symbol] = last_candle_ts
                signal = detect_signal(symbol, df)
                if signal:
                    print(f"✅✅✅ Signal '{signal['regime']}' DÉTECTÉ ET VALIDÉ pour {symbol}!")
                    trader.execute_trade(ex, symbol, signal, df)
            print(f"--- Fin du cycle. Attente de {LOOP_DELAY} secondes. ---"); time.sleep(LOOP_DELAY)
        except KeyboardInterrupt: notifier.tg_send("⛔ Arrêt manuel."); break
        except Exception:
            print("\n--- ERREUR CRITIQUE DANS LA BOUCLE PRINCIPALE ---"); error_details = traceback.format_exc(); print(error_details); notifier.tg_send_error("Erreur critique (boucle)", error_details)
            print("--------------------------------------------------"); time.sleep(15)
if __name__ == "__main__": main()
