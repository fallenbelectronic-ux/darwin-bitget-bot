# Fichier: main.py
import os
import sys
import time
import ccxt
import pandas as pd
import traceback
import threading
from ta.volatility import BollingerBands, AverageTrueRange
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone
import pytz

import database
import trader
import notifier
import utils
import state

# --- PARAMÈTRES GLOBAUX ---
BITGET_TESTNET   = os.getenv("BITGET_TESTNET", "true").lower() in ("1", "true", "yes")
API_KEY, API_SECRET, PASSPHRASSE = os.getenv("BITGET_API_KEY", ""), os.getenv("BITGET_API_SECRET", ""), os.getenv("BITGET_API_PASSWORD", "") or os.getenv("BITGET_PASSPHRASSE", "")
TIMEFRAME, UNIVERSE_SIZE, MIN_RR = os.getenv("TIMEFRAME", "1h"), int(os.getenv("UNIVERSE_SIZE", "30")), float(os.getenv("MIN_RR", "3.0"))
MAX_OPEN_POSITIONS = int(os.getenv("MAX_OPEN_POSITIONS", 3))
LOOP_DELAY, TIMEZONE, REPORT_HOUR, REPORT_WEEKDAY = int(os.getenv("LOOP_DELAY", "5")), os.getenv("TIMEZONE", "Europe/Lisbon"), int(os.getenv("REPORT_HOUR", "21")), int(os.getenv("REPORT_WEEKDAY", "6"))

# --- VARIABLES D'ÉTAT PARTAGÉES ET SÉCURISÉES ---
_last_update_id: Optional[int] = None
_paused = False
_last_daily_report_day = -1
_last_weekly_report_day = -1
_recent_signals: List[Dict] = []
_lock = threading.Lock()

# ==============================================================================
# DÉFINITION DE TOUTES LES FONCTIONS UTILITAIRES
# ==============================================================================

def startup_checks():
    """Vérifie la présence des variables d'environnement critiques au démarrage."""
    print("Vérification des configurations au démarrage...")
    required = {'BITGET_API_KEY', 'BITGET_API_SECRET'}
    if not os.getenv('BITGET_PASSPHRASSE') and not os.getenv('BITGET_API_PASSWORD'):
        error_msg = "❌ ERREUR DE DÉMARRAGE: La variable 'BITGET_PASSPHRASSE' ou 'BITGET_API_PASSWORD' est manquante."
        print(error_msg); notifier.tg_send(error_msg); sys.exit(1)
    for key in required:
        if not os.getenv(key):
            error_msg = f"❌ ERREUR DE DÉMARRAGE: La variable '{key}' est manquante."
            print(error_msg); notifier.tg_send(error_msg); sys.exit(1)
    print("✅ Configurations nécessaires présentes.")

def cleanup_recent_signals(hours: int = 6):
    global _recent_signals
    seconds_ago = time.time() - (hours * 60 * 60)
    with _lock:
        _recent_signals[:] = [s for s in _recent_signals if s['timestamp'] >= seconds_ago]

def get_recent_signals_message(hours: int) -> str:
    cleanup_recent_signals(hours)
    with _lock:
        now = time.time()
        signals_in_period = [s for s in _recent_signals if s['timestamp'] >= now - (hours * 60 * 60)]
    if not signals_in_period: return f"⏱️ Aucun signal valide détecté dans les {hours} dernières heures."
    lines = [f"<b>⏱️ {len(signals_in_period)} Signaux ({'dernière heure' if hours == 1 else f'{hours}h'})</b>\n"]
    for s in signals_in_period:
        ts = datetime.fromtimestamp(s['timestamp'], tz=timezone.utc).astimezone(pytz.timezone(TIMEZONE)).strftime('%H:%M')
        side_icon = "📈" if s['signal']['side'] == 'buy' else "📉"
        lines.append(f"- <code>{ts}</code> | {side_icon} <b>{s['symbol']}</b> | {s['signal']['regime']} | RR: {s['signal']['rr']:.2f}")
    return "\n".join(lines)

def create_exchange():
    ex = ccxt.bitget({"apiKey": API_KEY, "secret": API_SECRET, "password": PASSPHRASSE, "enableRateLimit": True, "options": {"defaultType": "swap", "testnet": BITGET_TESTNET}})
    if BITGET_TESTNET: ex.set_sandbox_mode(True)
    return ex

def build_universe(ex: ccxt.Exchange) -> List[str]:
    print("Construction de l'univers de trading...")
    size = int(database.get_setting('UNIVERSE_SIZE', UNIVERSE_SIZE))
    print(f"Taille de l'univers configurée à {size} paires.")
    try:
        markets = ex.load_markets()
        symbols = [m['symbol'] for m in markets.values() if m.get('swap') and m.get('quote') == 'USDT' and m.get('linear')]
        return symbols[:size] if symbols else []
    except Exception as e:
        print(f"Impossible de construire l'univers via l'API. Erreur: {e}."); return []

def select_and_execute_best_pending_signal(ex: ccxt.Exchange):
    if not state.pending_signals: return
    print(f"  -> NOUVELLE BOUGIE détectée. Analyse de {len(state.pending_signals)} signaux en attente...")
    
    validated_signals = []
    for symbol, pending in list(state.pending_signals.items()):
        df = utils.fetch_ohlcv_df(ex, symbol, TIMEFRAME)
        if df is None or df.index[-1] <= pending['candle_timestamp']: continue

        new_entry_price = df['open'].iloc[-1]; sl_price = pending['signal']['sl']
        df_with_indicators = trader._get_indicators(df.copy())
        if df_with_indicators is None: continue
        
        last_indicators = df_with_indicators.iloc[-1]; is_long = pending['signal']['side'] == 'buy'
        new_tp_price = last_indicators['bb80_mid'] if pending['signal']['regime'] == 'Tendance' else \
                       last_indicators['bb20_up'] if is_long else last_indicators['bb20_lo']
        
        new_rr = (new_tp_price - new_entry_price) / (new_entry_price - sl_price) if is_long and (new_entry_price - sl_price) > 0 else \
                 (new_entry_price - new_tp_price) / (sl_price - new_entry_price) if not is_long and (sl_price - new_entry_price) > 0 else 0

        if new_rr >= MIN_RR:
            pending['signal']['tp'] = new_tp_price; pending['signal']['rr'] = new_rr
            pending['new_entry_price'] = new_entry_price
            pending['signal']['symbol'] = symbol # Assurer que le symbole est présent
            validated_signals.append(pending)
        else:
            print(f"   -> Signal pour {symbol} invalidé. R/R à l'ouverture ({new_rr:.2f}) < {MIN_RR}.")
            notifier.send_validated_signal_report(symbol, pending['signal'], False, f"Invalidé: R/R à l'ouverture ({new_rr:.2f}) < {MIN_RR}")

    state.pending_signals.clear()
    if not validated_signals:
        print("   -> Aucun signal n'a passé la re-validation du R/R."); return

    best_signal_data = sorted(validated_signals, key=lambda x: x['signal']['rr'], reverse=True)[0]
    symbol = best_signal_data['signal']['symbol']
    print(f"   -> MEILLEUR SIGNAL SÉLECTIONNÉ: {symbol} avec un R/R de {best_signal_data['signal']['rr']:.2f}")

    is_taken, reason = trader.execute_trade(ex, symbol, best_signal_data['signal'], best_signal_data['df'], best_signal_data['new_entry_price'])
    if not is_taken:
        notifier.send_validated_signal_report(symbol, best_signal_data['signal'], is_taken, reason)

def detect_signal(symbol: str, df: pd.DataFrame) -> Optional[Dict[str, Any]]:
    if df is None or len(df) < 83: return None
    df_with_indicators = trader._get_indicators(df.copy())
    if df_with_indicators is None: return None
    
    last_candle = df_with_indicators.iloc[-1]
    
    for i in range(2, 4):
        contact_candle = df_with_indicators.iloc[-i]
        signal = None
        
        is_uptrend = contact_candle['close'] > contact_candle['bb80_mid']
        is_downtrend = contact_candle['close'] < contact_candle['bb80_mid']

        # Achat
        buy_tendance = is_uptrend and contact_candle['low'] <= contact_candle['bb20_lo']
        buy_ct = (contact_candle['low'] <= contact_candle['bb20_lo'] and contact_candle['low'] <= contact_candle['bb80_lo'])
        
        if buy_tendance or buy_ct:
            if trader.is_valid_reaction_candle(last_candle, 'buy'):
                reintegration_ok = last_candle['close'] > contact_candle['bb20_lo']
                if buy_ct: reintegration_ok = reintegration_ok and last_candle['close'] > contact_candle['bb80_lo']
                if reintegration_ok:
                    regime = "Tendance" if buy_tendance else "Contre-tendance"
                    entry = (last_candle['open'] + last_candle['close']) / 2
                    sl = contact_candle['low'] - (contact_candle['atr'] * 0.25)
                    tp = last_candle['bb80_mid'] if regime == 'Tendance' else last_candle['bb20_up']
                    rr = (tp - entry) / (entry - sl) if (entry - sl) > 0 else 0
                    if rr >= MIN_RR: signal = {"side": "buy", "regime": regime, "entry": entry, "sl": sl, "tp": tp, "rr": rr}

        # Vente
        sell_tendance = is_downtrend and contact_candle['high'] >= contact_candle['bb20_up']
        sell_ct = (contact_candle['high'] >= contact_candle['bb20_up'] and contact_candle['high'] >= contact_candle['bb80_up'])

        if not signal and (sell_tendance or sell_ct):
            if trader.is_valid_reaction_candle(last_candle, 'sell'):
                reintegration_ok = last_candle['close'] < contact_candle['bb20_up']
                if sell_ct: reintegration_ok = reintegration_ok and last_candle['close'] < contact_candle['bb80_up']
                if reintegration_ok:
                    regime = "Tendance" if sell_tendance else "Contre-tendance"
                    entry = (last_candle['open'] + last_candle['close']) / 2
                    sl = contact_candle['high'] + (contact_candle['atr'] * 0.25)
                    tp = last_candle['bb80_mid'] if regime == 'Tendance' else last_candle['bb20_lo']
                    rr = (entry - tp) / (sl - entry) if (sl - entry) > 0 else 0
                    if rr >= MIN_RR: signal = {"side": "sell", "regime": regime, "entry": entry, "sl": sl, "tp": tp, "rr": rr}

        if signal:
            signal['bb20_mid'] = last_candle['bb20_mid']
            return signal
    return None

def process_callback_query(callback_query: Dict):
    global _paused; data = callback_query.get('data', '')
    if data == 'pause':
        with _lock: _paused = True
        notifier.tg_send("⏸️ Scan mis en pause.")
    elif data == 'resume':
        with _lock: _paused = False
        notifier.tg_send("▶️ Reprise du scan.")
    elif data == 'list_positions': notifier.format_open_positions(database.get_open_positions())
    elif data == 'get_recent_signals': notifier.tg_send(get_recent_signals_message(6))
    elif data.startswith('close_trade_'):
        try: trade_id = int(data.split('_')[-1]); trader.close_position_manually(create_exchange(), trade_id)
        except (ValueError, IndexError): notifier.tg_send("Commande de fermeture invalide.")
    elif data == 'get_stats':
        ex = create_exchange(); balance = trader.get_usdt_balance(ex)
        trades = database.get_closed_trades_since(int(time.time()) - 7 * 24 * 60 * 60)
        notifier.send_report("📊 Bilan Hebdomadaire (7 derniers jours)", trades, balance)
    elif data == 'manage_strategy':
        current_strategy = database.get_setting('STRATEGY_MODE', os.getenv('STRATEGY_MODE', 'NORMAL').upper()); notifier.send_strategy_menu(current_strategy)
    elif data == 'switch_to_NORMAL': database.set_setting('STRATEGY_MODE', 'NORMAL'); notifier.tg_send("✅ Stratégie changée en <b>NORMAL</b>."); notifier.send_strategy_menu('NORMAL')
    elif data == 'switch_to_SPLIT': database.set_setting('STRATEGY_MODE', 'SPLIT'); notifier.tg_send("✅ Stratégie changée en <b>SPLIT</b>."); notifier.send_strategy_menu('SPLIT')
    elif data == 'switch_to_REAL': database.set_setting('PAPER_TRADING_MODE', 'false'); notifier.tg_send("🚨 <b>ATTENTION:</b> Bot en mode <b>RÉEL</b>."); notifier.send_mode_message(BITGET_TESTNET, False)
    elif data == 'switch_to_PAPER': database.set_setting('PAPER_TRADING_MODE', 'true'); notifier.tg_send("✅ Bot en mode <b>PAPIER</b>."); notifier.send_mode_message(BITGET_TESTNET, True)
    elif data == 'back_to_main': notifier.send_main_menu(_paused)

def process_message(message: Dict):
    global _paused; text = message.get("text", "").strip().lower(); parts = text.split(); command = parts[0]
    if command == "/start":
        help_message = ("🤖 <b>PANNEAU DE CONTRÔLE</b>\n\n🚦 <b>GESTION</b>\n/start\n/pause\n/resume\n/ping\n\n⚙️ <b>CONFIG</b>\n/config\n/mode\n/strategy\n/setuniverse <code>&lt;n&gt;</code>\n/setmaxpos <code>&lt;n&gt;</code>\n\n📈 <b>TRADING</b>\n/signals\n/recent\n/stats\n/pos")
        notifier.tg_send(help_message); notifier.send_main_menu(_paused)
    elif command == "/pause":
        with _lock: _paused = True
        notifier.tg_send("⏸️ Scan mis en pause.")
    elif command == "/resume":
        with _lock: _paused = False
        notifier.tg_send("▶️ Reprise du scan.")
    elif command == "/ping": notifier.tg_send("🛰️ Pong ! Le bot est en ligne.")
    elif command == "/config":
        current_max_pos = int(database.get_setting('MAX_OPEN_POSITIONS', MAX_OPEN_POSITIONS)); notifier.send_config_message(min_rr=MIN_RR, risk=trader.RISK_PER_TRADE_PERCENT, max_pos=current_max_pos, leverage=trader.LEVERAGE)
    elif command == "/mode":
        current_paper_mode = database.get_setting('PAPER_TRADING_MODE', 'true') == 'true'; notifier.send_mode_message(is_testnet=BITGET_TESTNET, is_paper=current_paper_mode)
    elif command == "/strategy":
        current_strategy = database.get_setting('STRATEGY_MODE', os.getenv('STRATEGY_MODE', 'NORMAL').upper()); notifier.send_strategy_menu(current_strategy)
    elif command == "/signals": notifier.tg_send(get_recent_signals_message(1))
    elif command == "/recent": notifier.tg_send(get_recent_signals_message(6))
    elif command == "/stats":
        ex = create_exchange(); balance = trader.get_usdt_balance(ex)
        trades = database.get_closed_trades_since(int(time.time()) - 7 * 24 * 60 * 60)
        notifier.send_report("📊 Bilan des 7 derniers jours", trades, balance)
    elif command == "/pos": notifier.format_open_positions(database.get_open_positions())
    elif command == "/setuniverse":
        if len(parts) < 2: notifier.tg_send("Usage: <code>/setuniverse &lt;nombre&gt;</code>"); return
        try:
            new_size = int(parts[1])
            if new_size > 0: database.set_setting('UNIVERSE_SIZE', new_size); notifier.tg_send(f"✅ Taille du scan mise à jour à <b>{new_size}</b> paires.\n<i>(Sera appliqué au prochain redémarrage)</i>")
            else: notifier.tg_send("❌ Le nombre doit être > 0.")
        except ValueError: notifier.tg_send("❌ Valeur invalide.")
    elif command == "/setmaxpos":
        if len(parts) < 2: notifier.tg_send("Usage: <code>/setmaxpos &lt;nombre&gt;</code>"); return
        try:
            new_max = int(parts[1])
            if new_max >= 0: database.set_setting('MAX_OPEN_POSITIONS', new_max); notifier.tg_send(f"✅ Nombre max de positions mis à jour à <b>{new_max}</b>.")
            else: notifier.tg_send("❌ Le nombre doit être >= 0.")
        except ValueError: notifier.tg_send("❌ Valeur invalide.")

def poll_telegram_updates():
    global _last_update_id
    updates = notifier.tg_get_updates(_last_update_id + 1 if _last_update_id else None)
    for upd in updates:
        _last_update_id = upd.get("update_id", _last_update_id)
        if 'callback_query' in upd: process_callback_query(upd['callback_query'])
        elif 'message' in upd: process_message(upd['message'])

def check_scheduled_reports():
    global _last_daily_report_day, _last_weekly_report_day
    try: tz = pytz.timezone(TIMEZONE)
    except pytz.UnknownTimeZoneError: tz = pytz.timezone("UTC")
    now = datetime.now(tz)
    if now.hour == REPORT_HOUR and now.day != _last_daily_report_day:
        _last_daily_report_day = now.day; ex = create_exchange(); balance = trader.get_usdt_balance(ex)
        trades = database.get_closed_trades_since(int(time.time()) - 24 * 60 * 60)
        notifier.send_report("📊 Bilan Quotidien (24h)", trades, balance)
    if now.weekday() == REPORT_WEEKDAY and now.hour == REPORT_HOUR and now.day != _last_weekly_report_day:
        _last_weekly_report_day = now.day; ex = create_exchange(); balance = trader.get_usdt_balance(ex)
        trades = database.get_closed_trades_since(int(time.time()) - 7 * 24 * 60 * 60)
        notifier.send_report("🗓️ Bilan Hebdomadaire", trades, balance)

def telegram_listener_loop():
    """Boucle dédiée à l'écoute des commandes Telegram."""
    print("🤖 Thread d'écoute Telegram démarré.")
    while True:
        try:
            poll_telegram_updates()
            time.sleep(1)
        except Exception as e:
            print(f"Erreur dans le thread Telegram: {e}"); time.sleep(5)

def trading_engine_loop(ex: ccxt.Exchange, universe: List[str]):
    """Boucle principale dédiée au trading."""
    print("📈 Thread de trading démarré.")
    last_processed_hour = -1

    while True:
        try:
            with _lock: is_paused = _paused
            if is_paused:
                print("   -> (Moteur de Trading en pause)"); time.sleep(LOOP_DELAY); continue

            current_hour = datetime.now(pytz.timezone(TIMEZONE)).hour
            if current_hour != last_processed_hour:
                select_and_execute_best_pending_signal(ex)
                last_processed_hour = current_hour

            cleanup_recent_signals(); check_scheduled_reports(); trader.manage_open_positions(ex)
            
            print(f"4. Début du scan de l'univers ({len(universe)} paires)...")
            for symbol in universe:
                df = utils.fetch_ohlcv_df(ex, symbol, TIMEFRAME)
                if df is None or len(df) < 83: continue
                
                signal = detect_signal(symbol, df)
                if signal and symbol not in state.pending_signals:
                    print(f"✅✅✅ Signal '{signal['regime']}' DÉTECTÉ pour {symbol}. MISE EN ATTENTE...")
                    with _lock:
                        state.pending_signals[symbol] = {'signal': signal, 'df': df.copy(), 'candle_timestamp': df.index[-1]}
                        _recent_signals.append({'timestamp': time.time(), 'symbol': symbol, 'signal': signal})
                    notifier.send_validated_signal_report(symbol, signal, False, "En attente de la clôture horaire.")

            print(f"--- Fin du cycle de scan. Attente de {LOOP_DELAY} secondes. ---")
            time.sleep(LOOP_DELAY)
        
        except KeyboardInterrupt: notifier.tg_send("⛔ Arrêt manuel du thread de trading."); break
        except Exception:
            print("\n--- ERREUR CRITIQUE DANS LE THREAD DE TRADING ---"); error_details = traceback.format_exc()
            print(error_details); notifier.tg_send_error("Erreur critique (Moteur de Trading)", error_details)
            print("--------------------------------------------------")
            time.sleep(15)

def main():
    """Point d'entrée principal du bot."""
    startup_checks()
    ex = create_exchange(); database.setup_database()
    if not database.get_setting('STRATEGY_MODE'): database.set_setting('STRATEGY_MODE', os.getenv('STRATEGY_MODE', 'NORMAL').upper())
    if not database.get_setting('UNIVERSE_SIZE'): database.set_setting('UNIVERSE_SIZE', UNIVERSE_SIZE)
    if not database.get_setting('MAX_OPEN_POSITIONS'): database.set_setting('MAX_OPEN_POSITIONS', MAX_OPEN_POSITIONS)
    if not database.get_setting('PAPER_TRADING_MODE'): database.set_setting('PAPER_TRADING_MODE', os.getenv("PAPER_TRADING_MODE", "true").lower())
    
    notifier.send_start_banner("TESTNET" if BITGET_TESTNET else "LIVE", "PAPIER" if database.get_setting('PAPER_TRADING_MODE') == 'true' else "RÉEL", trader.RISK_PER_TRADE_PERCENT)
    universe = build_universe(ex)
    if not universe: notifier.tg_send("❌ Impossible de construire l'univers de trading."); return
    print(f"Univers de trading chargé avec {len(universe)} paires.")

    telegram_thread = threading.Thread(target=telegram_listener_loop, daemon=True)
    trading_thread = threading.Thread(target=trading_engine_loop, args=(ex, universe))

    telegram_thread.start()
    trading_thread.start()
    trading_thread.join()

if __name__ == "__main__":
    main()
