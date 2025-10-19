# Fichier: main.py
import os
import time
import ccxt
import pandas as pd
from ta.volatility import BollingerBands
from typing import List, Dict, Any, Optional
from datetime import datetime
import pytz # Importation pour la gestion des fuseaux horaires

import database
import trader
import notifier

# ... (Toute la section des PARAMÈTRES est inchangée) ...
# --- PARAMÈTRES CHARGÉS DEPUIS L'ENVIRONNEMENT ---
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
# Nouveaux paramètres pour les rapports
TIMEZONE         = os.getenv("TIMEZONE", "UTC")
REPORT_HOUR      = int(os.getenv("REPORT_HOUR", "21"))
REPORT_WEEKDAY   = int(os.getenv("REPORT_WEEKDAY", "6"))


# ... (Toutes les fonctions create_exchange, fetch_ohlcv_df, build_universe, detect_signal restent INCHANGÉES) ...
def create_exchange():
    ex = ccxt.bitget({"apiKey": API_KEY, "secret": API_SECRET, "password": PASSPHRASSE, "enableRateLimit": True, "options": {"defaultType": "swap", "testnet": BITGET_TESTNET}})
    if BITGET_TESTNET: ex.set_sandbox_mode(True)
    return ex
def fetch_ohlcv_df(ex: ccxt.Exchange, symbol: str, timeframe: str, limit: int = 120) -> Optional[pd.DataFrame]:
    try:
        ohlcv = ex.fetch_ohlcv(symbol, timeframe, limit=limit)
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df.set_index('timestamp', inplace=True)
        return df
    except Exception: return None
def build_universe(ex: ccxt.Exchange) -> List[str]:
    print("Construction de l'univers de trading...")
    try:
        markets = ex.load_markets()
        symbols = [m['symbol'] for m in markets.values() if m.get('swap') and m.get('quote') == 'USDT' and m.get('linear')]
        if not symbols: return FALLBACK_TESTNET
        return symbols[:UNIVERSE_SIZE]
    except Exception: return FALLBACK_TESTNET
def detect_signal(df: pd.DataFrame) -> Optional[Dict[str, Any]]:
    if df is None or len(df) < 81: return None
    bb_20 = BollingerBands(close=df['close'], window=20, window_dev=2)
    df['bb20_up'], df['bb20_mid'], df['bb20_lo'] = bb_20.bollinger_hband(), bb_20.bollinger_mavg(), bb_20.bollinger_lband()
    bb_80 = BollingerBands(close=df['close'], window=80, window_dev=2)
    df['bb80_up'], df['bb80_mid'], df['bb80_lo'] = bb_80.bollinger_hband(), bb_80.bollinger_mavg(), bb_80.bollinger_lband()
    last = df.iloc[-1]
    is_uptrend = last['close'] > last['bb20_mid']
    is_downtrend = last['close'] < last['bb20_mid']
    dead_zone = last['bb20_mid'] * (MM_DEAD_ZONE_PERCENT / 100)
    is_in_dead_zone = abs(last['close'] - last['bb20_mid']) < dead_zone
    signal, tick_size = None, last['close'] * TICK_RATIO
    if not is_in_dead_zone:
        if is_uptrend and last['low'] <= last['bb20_lo']:
            entry_price, sl_price, tp_price = last['close'], last['low'] - (2 * tick_size), last['bb20_mid']
            if entry_price > sl_price and (tp_price - entry_price) / (entry_price - sl_price) >= MIN_RR:
                signal = {"side": "buy", "regime": "Tendance", "entry": entry_price, "sl": sl_price, "tp": tp_price, "rr": (tp_price - entry_price) / (entry_price - sl_price)}
        elif is_downtrend and last['high'] >= last['bb20_up']:
            entry_price, sl_price, tp_price = last['close'], last['high'] + (2 * tick_size), last['bb20_mid']
            if entry_price < sl_price and (entry_price - tp_price) / (sl_price - entry_price) >= MIN_RR:
                signal = {"side": "sell", "regime": "Tendance", "entry": entry_price, "sl": sl_price, "tp": tp_price, "rr": (entry_price - tp_price) / (sl_price - entry_price)}
    if not signal:
        if last['low'] <= last['bb20_lo'] and last['low'] <= last['bb80_lo'] and last['close'] > last['bb20_lo']:
            entry_price, sl_price, tp_price = last['close'], last['low'] - (2 * tick_size), last['bb20_mid']
            if entry_price > sl_price and (tp_price - entry_price) / (entry_price - sl_price) >= MIN_RR:
                signal = {"side": "buy", "regime": "Contre-tendance", "entry": entry_price, "sl": sl_price, "tp": tp_price, "rr": (tp_price - entry_price) / (entry_price - sl_price)}
        elif last['high'] >= last['bb20_up'] and last['high'] >= last['bb80_up'] and last['close'] < last['bb20_up']:
            entry_price, sl_price, tp_price = last['close'], last['high'] + (2 * tick_size), last['bb20_mid']
            if entry_price < sl_price and (entry_price - tp_price) / (sl_price - entry_price) >= MIN_RR:
                signal = {"side": "sell", "regime": "Contre-tendance", "entry": entry_price, "sl": sl_price, "tp": tp_price, "rr": (entry_price - tp_price) / (sl_price - entry_price)}
    if signal:
        signal['bb20_mid'] = last['bb20_mid']
        return signal
    return None

# --- MISE À JOUR DE LA GESTION DES COMMANDES TELEGRAM ---
def process_message(message: Dict):
    """Gère les commandes textuelles, ajout de la commande /stats."""
    text = message.get("text", "").strip().lower()
    if text.startswith("/start"):
        notifier.send_main_menu(_paused)
    elif text.startswith("/pos"):
        positions = database.get_open_positions()
        notifier.format_open_positions(positions)
    elif text.startswith("/stats"):
        # La commande manuelle /stats affiche toujours le bilan des 7 derniers jours
        notifier.tg_send("📊 Calcul du bilan hebdomadaire en cours...")
        seven_days_ago = int(time.time()) - 7 * 24 * 60 * 60
        trades = database.get_closed_trades_since(seven_days_ago)
        notifier.send_report("📊 Bilan Hebdomadaire (7 derniers jours)", trades)

# --- NOUVELLE FONCTION POUR LES RAPPORTS PLANIFIÉS ---
_last_daily_report_day = -1
_last_weekly_report_day = -1

def check_scheduled_reports():
    """Vérifie s'il est temps d'envoyer un rapport quotidien ou hebdomadaire."""
    global _last_daily_report_day, _last_weekly_report_day
    
    try:
        tz = pytz.timezone(TIMEZONE)
        now = datetime.now(tz)
    except pytz.UnknownTimeZoneError:
        print(f"Fuseau horaire '{TIMEZONE}' invalide. Utilisation de l'UTC.")
        tz = pytz.timezone("UTC")
        now = datetime.now(tz)

    # Rapport Quotidien
    if now.hour == REPORT_HOUR and now.day != _last_daily_report_day:
        print("Heure du rapport quotidien atteinte. Génération du bilan...")
        _last_daily_report_day = now.day
        one_day_ago = int(time.time()) - 24 * 60 * 60
        trades = database.get_closed_trades_since(one_day_ago)
        notifier.send_report("📊 Bilan Quotidien (24h)", trades)

    # Rapport Hebdomadaire
    if now.weekday() == REPORT_WEEKDAY and now.hour == REPORT_HOUR and now.day != _last_weekly_report_day:
        print("Jour et heure du rapport hebdomadaire atteints. Génération du bilan...")
        _last_weekly_report_day = now.day # Empêche le double envoi avec le quotidien
        seven_days_ago = int(time.time()) - 7 * 24 * 60 * 60
        trades = database.get_closed_trades_since(seven_days_ago)
        notifier.send_report("🗓️ Bilan Hebdomadaire", trades)


# ... (process_callback_query et poll_telegram_updates restent INCHANGÉES) ...
def process_callback_query(callback_query: Dict):
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
            trader.close_position_manually(create_exchange(), trade_id)
        except (ValueError, IndexError): notifier.tg_send("Commande de fermeture invalide.")
def poll_telegram_updates():
    global _last_update_id
    updates = notifier.tg_get_updates(_last_update_id + 1 if _last_update_id else None)
    for upd in updates:
        _last_update_id = upd.get("update_id", _last_update_id)
        if 'callback_query' in upd: process_callback_query(upd['callback_query'])
        elif 'message' in upd: process_message(upd['message'])


_last_update_id: Optional[int] = None
_paused = False

def main():
    ex = create_exchange()
    database.setup_database()
    notifier.send_start_banner("TESTNET" if BITGET_TESTNET else "LIVE", "PAPIER" if trader.PAPER_TRADING_MODE else "RÉEL", trader.RISK_PER_TRADE_PERCENT)
    universe = build_universe(ex)
    if not universe:
        notifier.tg_send("❌ Impossible de construire l'univers de trading. Arrêt du bot.")
        return
    print(f"Univers de trading chargé avec {len(universe)} paires.")
    
    last_ts_seen = {}

    while True:
        try:
            poll_telegram_updates()
            check_scheduled_reports() # Appel de la fonction de vérification à chaque cycle
            
            if _paused:
                time.sleep(LOOP_DELAY)
                continue
            
            trader.manage_open_positions(ex)
            
            for symbol in universe:
                df = fetch_ohlcv_df(ex, symbol, TIMEFRAME)
                if df is None or df.empty: continue
                
                last_candle_ts = df.index[-1]
                if symbol in last_ts_seen and last_ts_seen[symbol] == last_candle_ts: continue 
                last_ts_seen[symbol] = last_candle_ts
                
                signal = detect_signal(df)
                if signal:
                    print(f"✅ Signal '{signal['regime']}' détecté pour {symbol}!")
                    trader.execute_trade(ex, symbol, signal, df)

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
