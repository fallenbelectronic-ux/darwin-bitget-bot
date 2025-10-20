# Fichier: main.py
import os
import time
import ccxt
import pandas as pd
import traceback
from ta.volatility import BollingerBands
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone
import pytz

import database
import trader
import notifier
import utils

# --- PARAMÈTRES GLOBAUX ---
BITGET_TESTNET = os.getenv("BITGET_TESTNET", "true").lower() in ("1", "true", "yes")
API_KEY        = os.getenv("BITGET_API_KEY", "")
API_SECRET     = os.getenv("BITGET_API_SECRET", "")
# CORRECTION : Le code cherche maintenant les deux orthographes possibles pour la passphrase
PASSPHRASSE    = os.getenv("BITGET_PASSPHRASSE", "") or os.getenv("BITGET_PASSPHRASE", "")

TIMEFRAME      = os.getenv("TIMEFRAME", "1h")
UNIVERSE_SIZE  = int(os.getenv("UNIVERSE_SIZE", "30"))
MIN_RR         = float(os.getenv("MIN_RR", "3.0"))
MAX_OPEN_POSITIONS = int(os.getenv("MAX_OPEN_POSITIONS", 3))
LOOP_DELAY     = int(os.getenv("LOOP_DELAY", "5"))
TIMEZONE       = os.getenv("TIMEZONE", "Europe/Lisbon")
REPORT_HOUR    = int(os.getenv("REPORT_HOUR", "21"))
REPORT_WEEKDAY = int(os.getenv("REPORT_WEEKDAY", "6"))

# --- VARIABLES D'ÉTAT ---
_last_update_id: Optional[int] = None
_paused = False
_last_daily_report_day = -1
_last_weekly_report_day = -1
_recent_signals: List[Dict] = []

# --- FONCTIONS PRINCIPALES ---

def cleanup_recent_signals(hours: int = 6):
    """Supprime les signaux de l'historique qui sont plus vieux que `hours`."""
    global _recent_signals
    seconds_ago = time.time() - (hours * 60 * 60)
    _recent_signals = [s for s in _recent_signals if s['timestamp'] >= seconds_ago]

def get_recent_signals_message(hours: int) -> str:
    """Génère le message de résumé pour les signaux récents."""
    cleanup_recent_signals(hours)
    
    seconds_ago = time.time() - (hours * 60 * 60)
    signals_in_period = [s for s in _recent_signals if s['timestamp'] >= seconds_ago]
    
    if not signals_in_period:
        return f"⏱️ Aucun signal valide détecté dans les {hours} dernières heures."
        
    lines = [f"<b>⏱️ {len(signals_in_period)} Signaux ({'dernière heure' if hours == 1 else f'{hours}h'})</b>\n"]
    for s in signals_in_period:
        ts = datetime.fromtimestamp(s['timestamp'], tz=timezone.utc).astimezone(pytz.timezone(TIMEZONE)).strftime('%H:%M')
        side_icon = "📈" if s['signal']['side'] == 'buy' else "📉"
        lines.append(f"- <code>{ts}</code> | {side_icon} <b>{s['symbol']}</b> | {s['signal']['regime']} | RR: {s['signal']['rr']:.2f}")
    return "\n".join(lines)

def create_exchange():
    """Initialise et retourne l'objet d'échange CCXT."""
    ex = ccxt.bitget({
        "apiKey": API_KEY, "secret": API_SECRET, "password": PASSPHRASSE,
        "enableRateLimit": True, "options": {"defaultType": "swap", "testnet": BITGET_TESTNET}
    })
    if BITGET_TESTNET:
        ex.set_sandbox_mode(True)
    return ex

def build_universe(ex: ccxt.Exchange) -> List[str]:
    """Construit la liste des paires à trader."""
    print("Construction de l'univers de trading...")
    size = int(database.get_setting('UNIVERSE_SIZE', UNIVERSE_SIZE))
    print(f"Taille de l'univers configurée à {size} paires.")
    try:
        markets = ex.load_markets()
        symbols = [m['symbol'] for m in markets.values() if m.get('swap') and m.get('quote') == 'USDT' and m.get('linear')]
        return symbols[:size] if symbols else []
    except Exception as e:
        print(f"Impossible de construire l'univers via l'API. Erreur: {e}.")
        return []

def detect_signal(symbol: str, df: pd.DataFrame) -> Optional[Dict[str, Any]]:
    """Logique de détection de signal de la stratégie Darwin."""
    if df is None or len(df) < 81:
        return None

    df = trader._get_indicators(df.copy())
    if df is None:
        return None
    
    last = df.iloc[-1]
    is_uptrend = last['close'] > last['bb80_mid']
    is_downtrend = last['close'] < last['bb80_mid']
    signal = None

    if is_uptrend and last['low'] <= last['bb20_lo'] and last['close'] > last['bb20_lo']:
        entry = (last['open'] + last['close']) / 2
        sl = last['low'] - (2 * 0.0005 * last['close'])
        tp = last['bb80_mid']
        rr = (tp - entry) / (entry - sl) if (entry - sl) > 0 else 0
        if rr >= MIN_RR:
            signal = {"side": "buy", "regime": "Tendance", "entry": entry, "sl": sl, "tp": tp, "rr": rr}
    elif is_downtrend and last['high'] >= last['bb20_up'] and last['close'] < last['bb20_up']:
        entry = (last['open'] + last['close']) / 2
        sl = last['high'] + (2 * 0.0005 * last['close'])
        tp = last['bb80_mid']
        rr = (entry - tp) / (sl - entry) if (sl - entry) > 0 else 0
        if rr >= MIN_RR:
            signal = {"side": "sell", "regime": "Tendance", "entry": entry, "sl": sl, "tp": tp, "rr": rr}

    if not signal:
        if last['low'] <= last['bb20_lo'] and last['low'] <= last['bb80_lo'] and last['close'] > last['bb20_lo']:
            entry = (last['open'] + last['close']) / 2
            sl = last['low'] - (2 * 0.0005 * last['close'])
            tp = last['bb20_up']
            rr = (tp - entry) / (entry - sl) if (entry - sl) > 0 else 0
            if rr >= MIN_RR:
                signal = {"side": "buy", "regime": "Contre-tendance", "entry": entry, "sl": sl, "tp": tp, "rr": rr}
        elif last['high'] >= last['bb20_up'] and last['high'] >= last['bb80_up'] and last['close'] < last['bb20_up']:
            entry = (last['open'] + last['close']) / 2
            sl = last['high'] + (2 * 0.0005 * last['close'])
            tp = last['bb20_lo']
            rr = (entry - tp) / (sl - entry) if (sl - entry) > 0 else 0
            if rr >= MIN_RR:
                signal = {"side": "sell", "regime": "Contre-tendance", "entry": entry, "sl": sl, "tp": tp, "rr": rr}
    
    if signal:
        signal['bb20_mid'] = last['bb20_mid']
        return signal
    return None

def process_callback_query(callback_query: Dict):
    """Gère les clics sur les boutons interactifs de Telegram."""
    global _paused
    data = callback_query.get('data', '')

    if data == 'pause':
        _paused = True
        notifier.tg_send("⏸️ Scan mis en pause.")
    elif data == 'resume':
        _paused = False
        notifier.tg_send("▶️ Reprise du scan.")
    elif data == 'list_positions':
        notifier.format_open_positions(database.get_open_positions())
    elif data == 'get_recent_signals':
        notifier.tg_send(get_recent_signals_message(6))
    elif data.startswith('close_trade_'):
        try:
            trade_id = int(data.split('_')[-1])
            trader.close_position_manually(create_exchange(), trade_id)
        except (ValueError, IndexError):
            notifier.tg_send("Commande de fermeture invalide.")
    elif data == 'get_stats':
        trades = database.get_closed_trades_since(int(time.time()) - 7 * 24 * 60 * 60)
        notifier.send_report("📊 Bilan Hebdomadaire (7 derniers jours)", trades)
    elif data == 'manage_strategy':
        current_strategy = database.get_setting('STRATEGY_MODE', os.getenv('STRATEGY_MODE', 'NORMAL').upper())
        notifier.send_strategy_menu(current_strategy)
    elif data == 'switch_to_NORMAL':
        database.set_setting('STRATEGY_MODE', 'NORMAL')
        notifier.tg_send("✅ Stratégie changée en <b>NORMAL</b>.")
        notifier.send_strategy_menu('NORMAL')
    elif data == 'switch_to_SPLIT':
        database.set_setting('STRATEGY_MODE', 'SPLIT')
        notifier.tg_send("✅ Stratégie changée en <b>SPLIT</b>.")
        notifier.send_strategy_menu('SPLIT')
    elif data == 'back_to_main':
        notifier.send_main_menu(_paused)

def process_message(message: Dict):
    """Gère les commandes textuelles de Telegram."""
    global _paused
    text = message.get("text", "").strip().lower()
    parts = text.split()
    command = parts[0]

    if command == "/start":
        help_message = (
            "🤖 <b>PANNEAU DE CONTRÔLE - DARWIN BOT</b>\n\n"
            "🚦 <b>GESTION DU BOT</b>\n/start — Affiche ce message\n/pause — Met le scan en pause\n/resume — Reprend le scan\n/ping — Vérifie si le bot est en ligne\n\n"
            "⚙️ <b>CONFIGURATION</b>\n/config — Affiche la configuration\n/mode — Affiche les modes de fonctionnement\n/setuniverse <code>&lt;nombre&gt;</code> — Change la taille du scan\n/setmaxpos <code>&lt;nombre&gt;</code> — Change le nb max de trades\n\n"
            "📈 <b>TRADING</b>\n/signals — Affiche les signaux de la dernière heure\n/recent — Affiche les signaux des 6 dernières heures\n/stats — Affiche les statistiques"
        )
        notifier.tg_send(help_message)
        notifier.send_main_menu(_paused)
    elif command == "/pause":
        _paused = True
        notifier.tg_send("⏸️ Scan mis en pause.")
    elif command == "/resume":
        _paused = False
        notifier.tg_send("▶️ Reprise du scan.")
    elif command == "/ping":
        notifier.tg_send("🛰️ Pong ! Le bot est en ligne.")
    elif command == "/config":
        current_max_pos = int(database.get_setting('MAX_OPEN_POSITIONS', MAX_OPEN_POSITIONS))
        notifier.send_config_message(min_rr=MIN_RR, risk=trader.RISK_PER_TRADE_PERCENT, max_pos=current_max_pos, leverage=trader.LEVERAGE)
    elif command == "/mode":
        notifier.send_mode_message(is_testnet=BITGET_TESTNET, is_paper=trader.PAPER_TRADING_MODE)
    elif command == "/signals":
        notifier.tg_send(get_recent_signals_message(1))
    elif command == "/recent":
        notifier.tg_send(get_recent_signals_message(6))
    elif command == "/stats":
        trades = database.get_closed_trades_since(int(time.time()) - 7 * 24 * 60 * 60)
        notifier.send_report("📊 Bilan des 7 derniers jours", trades)
    elif command == "/setuniverse":
        if len(parts) < 2:
            notifier.tg_send("Usage: <code>/setuniverse &lt;nombre&gt;</code> (ex: /setuniverse 50)")
            return
        try:
            new_size = int(parts[1])
            if new_size > 0:
                database.set_setting('UNIVERSE_SIZE', new_size)
                notifier.tg_send(f"✅ Taille du scan mise à jour à <b>{new_size}</b> paires.\n<i>(Sera appliqué au prochain redémarrage)</i>")
            else:
                notifier.tg_send("❌ Le nombre doit être supérieur à zéro.")
        except ValueError:
            notifier.tg_send("❌ Valeur invalide. Entrez un nombre entier.")
    elif command == "/setmaxpos":
        if len(parts) < 2:
            notifier.tg_send("Usage: <code>/setmaxpos &lt;nombre&gt;</code> (ex: /setmaxpos 5)")
            return
        try:
            new_max = int(parts[1])
            if new_max >= 0:
                database.set_setting('MAX_OPEN_POSITIONS', new_max)
                notifier.tg_send(f"✅ Nombre max de positions mis à jour à <b>{new_max}</b>.\n<i>(Appliqué immédiatement)</i>")
            else:
                notifier.tg_send("❌ Le nombre doit être positif ou zéro.")
        except ValueError:
            notifier.tg_send("❌ Valeur invalide. Entrez un nombre entier.")

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

def check_scheduled_reports():
    """Vérifie s'il est temps d'envoyer les rapports planifiés."""
    global _last_daily_report_day, _last_weekly_report_day
    try:
        tz = pytz.timezone(TIMEZONE)
    except pytz.UnknownTimeZoneError:
        tz = pytz.timezone("UTC")
    now = datetime.now(tz)
    
    if now.hour == REPORT_HOUR and now.day != _last_daily_report_day:
        _last_daily_report_day = now.day
        trades = database.get_closed_trades_since(int(time.time()) - 24 * 60 * 60)
        notifier.send_report("📊 Bilan Quotidien (24h)", trades)
    
    if now.weekday() == REPORT_WEEKDAY and now.hour == REPORT_HOUR and now.day != _last_weekly_report_day:
        _last_weekly_report_day = now.day
        trades = database.get_closed_trades_since(int(time.time()) - 7 * 24 * 60 * 60)
        notifier.send_report("🗓️ Bilan Hebdomadaire", trades)

def main():
    """Boucle principale du bot."""
    ex = create_exchange()
    database.setup_database()

    if not database.get_setting('STRATEGY_MODE'):
        database.set_setting('STRATEGY_MODE', os.getenv('STRATEGY_MODE', 'NORMAL').upper())
    if not database.get_setting('UNIVERSE_SIZE'):
        database.set_setting('UNIVERSE_SIZE', UNIVERSE_SIZE)
    if not database.get_setting('MAX_OPEN_POSITIONS'):
        database.set_setting('MAX_OPEN_POSITIONS', MAX_OPEN_POSITIONS)
    
    notifier.send_start_banner("TESTNET" if BITGET_TESTNET else "LIVE", "PAPIER" if trader.PAPER_TRADING_MODE else "RÉEL", trader.RISK_PER_TRADE_PERCENT)
    universe = build_universe(ex)
    if not universe:
        notifier.tg_send("❌ Impossible de construire l'univers de trading. Le bot va s'arrêter.")
        return
    
    print(f"Univers de trading chargé avec {len(universe)} paires.")
    last_ts_seen = {}
    
    while True:
        print(f"\n--- [{time.strftime('%Y-%m-%d %H:%M:%S')}] Nouveau cycle de la boucle ---")
        try:
            cleanup_recent_signals()
            print("1. Vérification des mises à jour Telegram...")
            poll_telegram_updates()
            print("2. Vérification des rapports planifiés...")
            check_scheduled_reports()

            if _paused:
                print("   -> Bot en pause, attente...")
                time.sleep(LOOP_DELAY)
                continue
            
            print("3. Gestion des positions ouvertes...")
            trader.manage_open_positions(ex)
            
            print(f"4. Début du scan de l'univers ({len(universe)} paires)...")
            for symbol in universe:
                df = utils.fetch_ohlcv_df(ex, symbol, TIMEFRAME)
                if df is None or df.empty:
                    continue
                
                last_candle_ts = df.index[-1]
                if symbol in last_ts_seen and last_ts_seen[symbol] == last_candle_ts:
                    continue
                
                last_ts_seen[symbol] = last_candle_ts
                signal = detect_signal(symbol, df)
                
                if signal:
                    print(f"✅✅✅ Signal '{signal['regime']}' DÉTECTÉ ET VALIDÉ pour {symbol}!")
                    _recent_signals.append({'timestamp': time.time(), 'symbol': symbol, 'signal': signal})
                    
                    is_taken, reason = trader.execute_trade(ex, symbol, signal, df)
                    notifier.send_validated_signal_report(symbol, signal, is_taken, reason)

            print(f"--- Fin du cycle. Attente de {LOOP_DELAY} secondes. ---")
            time.sleep(LOOP_DELAY)
        
        except KeyboardInterrupt:
            notifier.tg_send("⛔ Arrêt manuel.")
            break
        except Exception:
            print("\n--- ERREUR CRITIQUE DANS LA BOUCLE PRINCIPALE ---")
            error_details = traceback.format_exc()
            print(error_details)
            notifier.tg_send_error("Erreur critique (boucle)", error_details)
            print("--------------------------------------------------")
            time.sleep(15)

if __name__ == "__main__":
    main()
