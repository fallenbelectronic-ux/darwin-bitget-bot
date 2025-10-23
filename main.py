# Fichier: main.py
import os
import sys
import time
import ccxt
import pandas as pd
import traceback
import threading
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

def sync_positions_on_startup(ex: ccxt.Exchange):
    """Compare les positions de l'exchange avec la DB locale au démarrage."""
    print("Synchronisation des positions au démarrage...")
    try:
        exchange_positions = ex.fetch_positions()
        open_exchange_symbols = {p['info']['symbol'] for p in exchange_positions if p.get('contracts') and float(p['contracts']) > 0}
        
        db_positions = database.get_open_positions()
        open_db_symbols = {p['symbol'].replace('/', '') for p in db_positions}
        
        ghost_symbols = open_exchange_symbols - open_db_symbols
        
        if ghost_symbols:
            message = "⚠️ <b>Positions Fantômes Détectées !</b>\nCes positions sont ouvertes sur l'exchange mais inconnues du bot :\n\n"
            for symbol in ghost_symbols:
                message += f"- <code>{symbol}</code>\n"
            notifier.tg_send(message)
        
        print(f"Synchronisation terminée. {len(ghost_symbols)} position(s) fantôme(s) trouvée(s).")
    except Exception as e:
        print(f"Erreur durant la synchronisation des positions: {e}")
        notifier.tg_send_error("Synchronisation Positions", e)

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
        if not symbols:
            print("Aucun symbole trouvé via l'API, utilisation de la liste de secours.")
            return FALLBACK_TESTNET
        return symbols[:UNIVERSE_SIZE]
    except Exception as e:
        print(f"Impossible de construire l'univers via l'API. Utilisation de la liste de secours. Erreur: {e}")
        return FALLBACK_TESTNET

def detect_signal(df: pd.DataFrame) -> Optional[Dict[str, Any]]:
    """
    Fonction de détection de signal (placeholder).
    C'est ici que votre logique de trading (croisement de MM, etc.) doit être implémentée.
    """
    # Ajout des indicateurs (Bandes de Bollinger)
    bb_20 = BollingerBands(close=df['close'], window=20, window_dev=2)
    df['bb20_up'] = bb_20.bollinger_hband()
    df['bb20_lo'] = bb_20.bollinger_lband()
    df['bb20_mid'] = bb_20.bollinger_mavg()
    
    # NOTE: Ceci est un exemple de logique. Vous devez le remplacer par votre propre stratégie.
    # Par exemple, si le dernier prix de clôture croise la bande basse :
    last_close = df['close'].iloc[-1]
    last_bb_lo = df['bb20_lo'].iloc[-1]
    
    if last_close < last_bb_lo:
        # Signal d'achat (exemple)
        entry_price = last_close
        sl_price = entry_price * 0.98  # Stop Loss 2% plus bas
        tp_price = entry_price * 1.06  # Take Profit 6% plus haut (RR 3:1)
        
        return {
            "side": "buy",
            "regime": "Contre-tendance",
            "entry": entry_price,
            "sl": sl_price,
            "tp": tp_price,
            "rr": 3.0,
            "bb20_mid": df['bb20_mid'].iloc[-1]
        }
    return None

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
            notifier.tg_send(f"Ordre de fermeture pour le trade #{trade_id} en cours...")
            trader.close_position_manually(create_exchange(), trade_id)
        except (ValueError, IndexError):
            notifier.tg_send("Commande de fermeture invalide.")

def process_message(message: Dict):
    """Gère les commandes textuelles de l'utilisateur."""
    text = message.get("text", "").strip().lower()
    if text.startswith("/start"):
        notifier.send_main_menu(_paused)
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
        _last_weekly_report_day = now.day
        seven_days_ago = int(time.time()) - 7 * 24 * 60 * 60
        trades = database.get_closed_trades_since(seven_days_ago)
        notifier.send_report("🗓️ Bilan Hebdomadaire", trades)

def telegram_listener_loop():
    """Boucle dédiée à l'écoute des commandes Telegram."""
    print("🤖 Thread d'écoute Telegram démarré.")
    while True:
        try:
            poll_telegram_updates()
            time.sleep(1)
        except Exception as e:
            print(f"Erreur dans le thread Telegram: {e}")
            time.sleep(5)

def trading_engine_loop(ex: ccxt.Exchange, universe: List[str]):
    """Boucle principale dédiée au trading."""
    print("📈 Thread de trading démarré.")
    last_ts_seen = {}
    state = {}

    while True:
        try:
            poll_telegram_updates()
            
            if _paused:
                time.sleep(LOOP_DELAY)
                continue
            
            trader.manage_open_positions(ex)
            
            # ... (la boucle de scan reste ici)
            
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
