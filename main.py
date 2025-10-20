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
BITGET_TESTNET   = os.getenv("BITGET_TESTNET", "true").lower() in ("1", "true", "yes")
API_KEY, API_SECRET, PASSPHRASSE = os.getenv("BITGET_API_KEY", ""), os.getenv("BITGET_API_SECRET", ""), os.getenv("BITGET_API_PASSWORD", "") or os.getenv("BITGET_PASSPHRASSE", "")
TIMEFRAME, UNIVERSE_SIZE, MIN_RR = os.getenv("TIMEFRAME", "1h"), int(os.getenv("UNIVERSE_SIZE", "30")), float(os.getenv("MIN_RR", "3.0"))
MAX_OPEN_POSITIONS = int(os.getenv("MAX_OPEN_POSITIONS", 3)) # Valeur par défaut
LOOP_DELAY, TIMEZONE, REPORT_HOUR, REPORT_WEEKDAY = int(os.getenv("LOOP_DELAY", "5")), os.getenv("TIMEZONE", "Europe/Lisbon"), int(os.getenv("REPORT_HOUR", "21")), int(os.getenv("REPORT_WEEKDAY", "6"))

# --- VARIABLES D'ÉTAT ---
_last_update_id: Optional[int] = None; _paused = False; _last_daily_report_day, _last_weekly_report_day = -1, -1
_recent_signals: List[Dict] = []

# --- FONCTIONS PRINCIPALES ---

def build_universe(ex: ccxt.Exchange) -> List[str]:
    """Construit la liste des paires à trader en utilisant la taille définie dans la DB."""
    print("Construction de l'univers de trading...")
    # On récupère la taille de l'univers depuis la base de données, avec le .env comme fallback
    size = int(database.get_setting('UNIVERSE_SIZE', UNIVERSE_SIZE))
    print(f"Taille de l'univers configurée à {size} paires.")
    try:
        markets = ex.load_markets()
        symbols = [m['symbol'] for m in markets.values() if m.get('swap') and m.get('quote') == 'USDT' and m.get('linear')]
        return symbols[:size] if symbols else []
    except Exception as e:
        print(f"Impossible de construire l'univers via l'API. Erreur: {e}.")
        return []

# --- GESTION DES COMMANDES TELEGRAM (ENTIÈREMENT RÉÉCRIT) ---

def process_message(message: Dict):
    """Gère toutes les commandes textuelles."""
    global _paused
    text = message.get("text", "").strip().lower()
    parts = text.split()
    command = parts[0]

    if command == "/start":
        help_message = (
            "🤖 <b>PANNEAU DE CONTRÔLE - DARWIN BOT</b>\n\n"
            "🚦 <b>GESTION DU BOT</b>\n"
            "/start — Affiche ce message\n"
            "/pause — Met le scan en pause\n"
            "/resume — Reprend le scan\n"
            "/ping — Vérifie si le bot est en ligne\n\n"
            "⚙️ <b>CONFIGURATION</b>\n"
            "/config — Affiche la configuration\n"
            "/mode — Affiche les modes de fonctionnement\n"
            "/setuniverse <code>&lt;nombre&gt;</code> — Change la taille du scan (ex: /setuniverse 50)\n"
            "/setmaxpos <code>&lt;nombre&gt;</code> — Change le nb max de trades (ex: /setmaxpos 5)\n\n"
            "📈 <b>TRADING</b>\n"
            "/signals — Affiche les signaux de la dernière heure\n"
            "/recent — Affiche les signaux des 6 dernières heures\n"
            "/stats — Affiche les statistiques"
        )
        notifier.tg_send(help_message)
        notifier.send_main_menu(_paused)
    elif command == "/pause": _paused = True; notifier.tg_send("⏸️ Scan mis en pause.")
    elif command == "/resume": _paused = False; notifier.tg_send("▶️ Reprise du scan.")
    elif command == "/ping": notifier.tg_send("🛰️ Pong ! Le bot est en ligne.")
    elif command == "/config":
        # Affiche les valeurs actuelles depuis la DB
        current_max_pos = int(database.get_setting('MAX_OPEN_POSITIONS', MAX_OPEN_POSITIONS))
        notifier.send_config_message(min_rr=MIN_RR, risk=trader.RISK_PER_TRADE_PERCENT, max_pos=current_max_pos, leverage=trader.LEVERAGE)
    elif command == "/mode":
        notifier.send_mode_message(is_testnet=BITGET_TESTNET, is_paper=trader.PAPER_TRADING_MODE)
    elif command == "/signals": notifier.tg_send(get_recent_signals_message(1))
    elif command == "/recent": notifier.tg_send(get_recent_signals_message(6))
    elif command == "/stats":
        trades = database.get_closed_trades_since(int(time.time()) - 7*24*60*60)
        notifier.send_report("📊 Bilan des 7 derniers jours", trades)
    
    # NOUVELLES COMMANDES DE CONFIGURATION
    elif command == "/setuniverse":
        if len(parts) < 2:
            notifier.tg_send("Usage: <code>/setuniverse &lt;nombre&gt;</code> (ex: /setuniverse 50)")
            return
        try:
            new_size = int(parts[1])
            if new_size > 0:
                database.set_setting('UNIVERSE_SIZE', new_size)
                notifier.tg_send(f"✅ Taille du scan de l'univers mise à jour à <b>{new_size}</b> paires.\n<i>(Sera appliqué au prochain redémarrage du bot)</i>")
            else:
                notifier.tg_send("❌ Veuillez entrer un nombre supérieur à zéro.")
        except ValueError:
            notifier.tg_send("❌ Valeur invalide. Veuillez entrer un nombre entier.")
            
    elif command == "/setmaxpos":
        if len(parts) < 2:
            notifier.tg_send("Usage: <code>/setmaxpos &lt;nombre&gt;</code> (ex: /setmaxpos 5)")
            return
        try:
            new_max = int(parts[1])
            if new_max >= 0:
                database.set_setting('MAX_OPEN_POSITIONS', new_max)
                notifier.tg_send(f"✅ Nombre maximum de positions ouvertes mis à jour à <b>{new_max}</b>.\n<i>(Appliqué immédiatement)</i>")
            else:
                notifier.tg_send("❌ Veuillez entrer un nombre positif ou zéro.")
        except ValueError:
            notifier.tg_send("❌ Valeur invalide. Veuillez entrer un nombre entier.")

def main():
    """Boucle principale du bot."""
    ex = create_exchange()
    database.setup_database()

    # Initialiser les paramètres dans la DB s'ils n'existent pas
    if not database.get_setting('STRATEGY_MODE'): database.set_setting('STRATEGY_MODE', os.getenv('STRATEGY_MODE', 'NORMAL').upper())
    if not database.get_setting('UNIVERSE_SIZE'): database.set_setting('UNIVERSE_SIZE', UNIVERSE_SIZE)
    if not database.get_setting('MAX_OPEN_POSITIONS'): database.set_setting('MAX_OPEN_POSITIONS', MAX_OPEN_POSITIONS)
    
    notifier.send_start_banner("TESTNET" if BITGET_TESTNET else "LIVE", "PAPIER" if trader.PAPER_TRADING_MODE else "RÉEL", trader.RISK_PER_TRADE_PERCENT)
    universe = build_universe(ex)
    if not universe:
        notifier.tg_send("❌ Impossible de construire l'univers de trading. Le bot va s'arrêter.")
        return
    
    # ... le reste de la fonction main et les autres fonctions sont inchangées
    
# --- Le reste du code reste identique, je l'omets pour la clarté mais vous devez le garder ---
def cleanup_recent_signals(hours: int = 6): global _recent_signals; seconds_ago = time.time() - (hours * 60 * 60); _recent_signals = [s for s in _recent_signals if s['timestamp'] >= seconds_ago]
def get_recent_signals_message(hours: int) -> str:
    cleanup_recent_signals(hours); seconds_ago = time.time() - (hours * 60 * 60); signals_in_period = [s for s in _recent_signals if s['timestamp'] >= seconds_ago]
    if not signals_in_period: return f"⏱️ Aucun signal valide détecté dans les {hours} dernières heures."
    lines = [f"<b>⏱️ {len(signals_in_period)} Signaux ({'dernière heure' if hours == 1 else f'{hours}h'})</b>\n"]
    for s in signals_in_period: ts = datetime.fromtimestamp(s['timestamp'], tz=timezone.utc).astimezone(pytz.timezone(TIMEZONE)).strftime('%H:%M'); side_icon = "📈" if s['signal']['side'] == 'buy' else "📉"; lines.append(f"- <code>{ts}</code> | {side_icon} <b>{s['symbol']}</b> | {s['signal']['regime']} | RR: {s['signal']['rr']:.2f}")
    return "\n".join(lines)
def create_exchange(): ex = ccxt.bitget({"apiKey": API_KEY, "secret": API_SECRET, "password": PASSPHRASSE, "enableRateLimit": True, "options": {"defaultType": "swap", "testnet": BITGET_TESTNET}});
if BITGET_TESTNET: ex.set_sandbox_mode(True); return ex
def detect_signal(symbol: str, df: pd.DataFrame) -> Optional[Dict[str, Any]]:
    # ...
def process_callback_query(callback_query: Dict):
    # ...
def poll_telegram_updates():
    # ...
def check_scheduled_reports():
    # ...
if __name__ == "__main__":
    main()
