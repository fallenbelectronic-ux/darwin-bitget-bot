# Fichier: main.py
 import os
 import sys
 import time
 import ccxt
 import pandas as pd
 import traceback
 import threading
 import importlib.util
 from pathlib import Path
 from ta.volatility import BollingerBands
 from typing import List, Dict, Any, Optional
 from datetime import datetime, timezone
 import pytz
 from tabulate import tabulate
 
 import database
 import trader
 import notifier
 import utils
 import state
 import analysis
 
# S'assure que le module trader importé expose bien la fonction detect_signal.
if not hasattr(trader, 'detect_signal'):
    trader_path = Path(__file__).resolve().parent / 'trader.py'
    spec = importlib.util.spec_from_file_location('trader', trader_path)
    if spec and spec.loader:
        trader_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(trader_module)
        sys.modules['trader'] = trader_module
       trader = trader_module
        if hasattr(database, 'trader'):
            database.trader = trader_module
    else:
        raise ImportError("Impossible de charger le module trader attendu depuis trader.py")

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

def select_and_execute_best_pending_signal(ex: ccxt.Exchange, pending_signals: List[Dict[str, Any]]):
    """
    Parmi une liste de signaux potentiels, sélectionne le meilleur (plus haut RR) et l'exécute.
    """
    if not pending_signals:
        print("Aucun signal valide trouvé dans ce cycle de scan.")
        return

    # Trie les signaux par RR décroissant
    pending_signals.sort(key=lambda item: item['signal']['rr'], reverse=True)
    
    # Le meilleur signal est le premier de la liste
    best_signal_item = pending_signals[0]
    symbol = best_signal_item['symbol']
    signal = best_signal_item['signal']
    df = best_signal_item['df']
    
    # Notifier quel signal a été choisi
    notifier.send_confirmed_signal_notification(symbol, signal, len(pending_signals))
    
    # Exécuter le trade
    trader.execute_trade(ex, symbol, signal, df)

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
@@ -148,163 +164,226 @@ def select_and_execute_best_pending_signal(ex: ccxt.Exchange):
 
 def process_callback_query(callback_query: Dict):
     global _paused; data = callback_query.get('data', '')
     if data == 'pause':
         with _lock: _paused = True
         notifier.tg_send("⏸️ Scan mis en pause.")
     elif data == 'resume':
         with _lock: _paused = False
         notifier.tg_send("▶️ Reprise du scan.")
     elif data == 'list_positions': 
         ex = create_exchange()
         try:
             exchange_positions = ex.fetch_positions()
             db_positions = database.get_open_positions()
             notifier.format_synced_open_positions(exchange_positions, db_positions)
         except Exception as e:
             notifier.tg_send(f"❌ Erreur de synchro /pos: {e}")
             notifier.format_open_positions(database.get_open_positions())
     elif data == 'get_recent_signals': notifier.tg_send(get_recent_signals_message(6))
     elif data.startswith('close_trade_'):
         try: trade_id = int(data.split('_')[-1]); trader.close_position_manually(create_exchange(), trade_id)
         except (ValueError, IndexError): notifier.tg_send("Commande de fermeture invalide.")
     elif data == 'get_stats':
         ex = create_exchange(); balance = trader.get_usdt_balance(ex)
         trades = database.get_all_closed_trades()
        notifier.send_report("📊 Bilan des 7 derniers jours", trades, balance)
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
         help_message = (
             "🤖 <b>PANNEAU DE CONTRÔLE</b>\n\n🚦 <b>GESTION</b>\n/start\n/pause\n/resume\n/ping\n\n"
             "⚙️ <b>CONFIG</b>\n/config\n/mode\n/strategy\n/setuniverse <code>&lt;n&gt;</code>\n/setmaxpos <code>&lt;n&gt;</code>\n\n"
             "📈 <b>TRADING & ANALYSE</b>\n/signals\n/recent\n/stats\n/pos\n/history"
         )
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
         trades = database.get_all_closed_trades()
        notifier.send_report("📊 Bilan des 7 derniers jours", trades, balance)
     elif command == "/pos":
         ex = create_exchange()
         try:
             exchange_positions = ex.fetch_positions()
             db_positions = database.get_open_positions()
             notifier.format_synced_open_positions(exchange_positions, db_positions)
         except Exception as e:
             notifier.tg_send(f"❌ Erreur de synchro /pos: {e}")
             notifier.format_open_positions(database.get_open_positions())
     elif command == "/history":
         notifier.tg_send("🔍 Recherche de l'historique des trades sur Bitget...")
         try:
             ex = create_exchange()
             since = int((time.time() - 7 * 24 * 60 * 60) * 1000)
            ex.load_markets()
            available_symbols = set(ex.symbols)

            candidate_symbols = []
            for source in (database.get_all_closed_trades(), database.get_open_positions()):
                for trade in source:
                    raw_symbol = trade.get('symbol') if isinstance(trade, dict) else None
                    if not raw_symbol:
                        continue
                    normalized_symbol = raw_symbol
                    if normalized_symbol not in available_symbols:
                        if ':' not in normalized_symbol and '/' in normalized_symbol:
                            alt_symbol = f"{normalized_symbol}:USDT"
                            if alt_symbol in available_symbols:
                                normalized_symbol = alt_symbol
                        elif '/' not in normalized_symbol and normalized_symbol.endswith('USDT'):
                            base = normalized_symbol[:-4]
                            for candidate in (f"{base}/USDT:USDT", f"{base}/USDT"):
                                if candidate in available_symbols:
                                    normalized_symbol = candidate
                                    break
                    if normalized_symbol in available_symbols:
                        candidate_symbols.append(normalized_symbol)

            if not candidate_symbols:
                candidate_symbols = [s for s in available_symbols if s.endswith(':USDT')][:10]

            fetched_trades = []
            symbol_errors = []
            for symbol in sorted(set(candidate_symbols)):
                try:
                    symbol_trades = ex.fetch_my_trades(symbol, since=since, limit=100)
                    fetched_trades.extend(symbol_trades)
                except Exception as symbol_error:
                    symbol_errors.append(f"{symbol}: {symbol_error}")

            if not fetched_trades:
                if symbol_errors:
                    notifier.tg_send_error("Historique Bitget", "; ".join(symbol_errors))
                else:
                    notifier.tg_send("Aucun trade exécuté sur Bitget dans les 7 derniers jours.")
                 return           

             orders = {}

            for trade in fetched_trades:
                order_id = trade.get('order') or trade.get('id')
                if not order_id:
                    continue
                if order_id not in orders:
                    orders[order_id] = {
                        'symbol': trade.get('symbol', 'N/A'),
                        'side': trade.get('side', ''),
                        'cost': 0.0,
                        'amount': 0.0,
                        'pnl': 0.0,
                        'timestamp': trade.get('timestamp', 0)
                    }
                orders[order_id]['cost'] += float(trade.get('cost') or 0.0)
                orders[order_id]['amount'] += float(trade.get('amount') or 0.0)
                info = trade.get('info') or {}
                pnl_value = info.get('realizedPnl')
                if pnl_value is None:
                    pnl_value = info.get('profit')
                try:
                    orders[order_id]['pnl'] += float(pnl_value or 0.0)
                except (TypeError, ValueError):
                    orders[order_id]['pnl'] += 0.0
 
             headers = ["Date", "Paire", "Sens", "Taille", "PNL ($)"]

            table_rows = []
            for _, data in sorted(orders.items(), key=lambda item: item[1]['timestamp']):
                timestamp = data['timestamp'] or 0
                dt_object = datetime.fromtimestamp(timestamp / 1000) if timestamp else datetime.now()
                 date_str = dt_object.strftime('%d/%m %H:%M')
                 side_icon = "📈" if data['side'] == 'buy' else "📉"
                 pnl_str = f"{data['pnl']:.2f}"

                table_rows.append([date_str, data['symbol'], side_icon, f"{data['cost']:.2f}", pnl_str])

            if not table_rows:
                notifier.tg_send("Aucun trade exécuté sur Bitget dans les 7 derniers jours.")
                return

            table = tabulate(table_rows[-15:], headers=headers, tablefmt="simple")
             notifier.tg_send(f"<b>📈 Historique des Trades (Bitget 7j)</b>\n<pre>{table}</pre>")

            if symbol_errors:
                notifier.tg_send(f"⚠️ Symboles ignorés: {'; '.join(symbol_errors)}")
         except Exception as e:
             notifier.tg_send_error("Historique Bitget", e)
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
         trades = database.get_all_closed_trades()
        notifier.send_report("📊 Bilan Quotidien (24h)", trades, balance, days=1)
     if now.weekday() == REPORT_WEEKDAY and now.hour == REPORT_HOUR and now.day != _last_weekly_report_day:
         _last_weekly_report_day = now.day; ex = create_exchange(); balance = trader.get_usdt_balance(ex)
         trades = database.get_all_closed_trades()
        notifier.send_report("🗓️ Bilan Hebdomadaire", trades, balance, days=7)
 
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
