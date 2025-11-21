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
import database
import trader
import notifier
import utils
import reporting
import asyncio
import ccxt.pro as ccxtpro

# --- PARAMÈTRES GLOBAUX ---
BITGET_TESTNET   = os.getenv("BITGET_TESTNET", "true").lower() in ("1", "true", "yes")
API_KEY          = os.getenv("BITGET_API_KEY", "")
API_SECRET       = os.getenv("BITGET_API_SECRET", "")
PASSPHRASSE      = os.getenv("BITGET_API_PASSWORD", "") or os.getenv("BITGET_PASSPHRASSE", "")

TIMEFRAME        = os.getenv("TIMEFRAME", "1h")
UNIVERSE_SIZE    = int(os.getenv("UNIVERSE_SIZE", "500"))
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
    is_paper_mode = str(database.get_setting('PAPER_TRADING_MODE', 'true')).lower() == 'true'
    
    if not all(required) and not is_paper_mode:
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
    
def get_pending_signals_message() -> str:
    from state import get_pending_signals
    items = list(get_pending_signals().items())
    if not items:
        return "⏱️ Aucun signal en attente."
    lines = [f"<b>⏱️ {len(items)} Signal(s) en attente</b>\n"]
    for symbol, pending in items:
        sig = (pending or {}).get('signal', {}) or {}
        side_icon = "📈" if sig.get('side') == 'buy' else "📉"
        rr = sig.get('rr', 0.0)
        regime = sig.get('regime', 'N/A')
        try:
            rr_txt = f"{float(rr):.2f}"
        except Exception:
            rr_txt = str(rr)
        lines.append(f"- {side_icon} <b>{symbol}</b> | {regime} | RR: <b>{rr_txt}</b>")
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
    """Construit la liste des paires à trader (Bitget USDT futures) avec garde-fous pour ne jamais retourner [].
    1) Tentative principale via utils.get_universe_by_market_cap(ex, size)
    2) Si vide: reload des marchés + second essai
    3) Si encore vide: fallback 'core set' filtré par marchés disponibles
    4) Persistance optionnelle pour diagnostic: settings.LAST_UNIVERSE_JSON
    """
    import json

    print("Construction de l'univers de trading (market cap, robuste)…")
    try:
        size = int(database.get_setting('UNIVERSE_SIZE', UNIVERSE_SIZE))
    except Exception:
        size = UNIVERSE_SIZE
    size = max(1, int(size))

    def _persist_and_log(symbols: List[str], note: str) -> List[str]:
        try:
            database.set_setting('LAST_UNIVERSE_JSON', json.dumps(symbols))
            database.set_setting('LAST_UNIVERSE_NOTE', note)
        except Exception:
            pass
        return symbols[:size]

    # --- 1) Tentative principale
    try:
        syms = utils.get_universe_by_market_cap(ex, size)
    except Exception as e:
        syms = []
        try:
            notifier.tg_send_error("build_universe(get_universe_by_market_cap)", e)
        except Exception:
            pass

    if syms:
        if len(syms) < size:
            print(f"⚠️ Univers partiel: {len(syms)}/{size}.")
        return _persist_and_log(syms, "primary_ok")

    # --- 2) Reload + second essai
    try:
        ex.options = ex.options or {}
        ex.options["defaultType"] = "swap"  # s’assure que les perp sont correctement tagués
    except Exception:
        pass
    try:
        ex.load_markets(reload=True)
    except Exception:
        pass

    try:
        syms2 = utils.get_universe_by_market_cap(ex, size)
    except Exception:
        syms2 = []

    if syms2:
        print(f"✅ Univers obtenu après reload: {len(syms2)}.")
        return _persist_and_log(syms2, "after_reload_ok")

    # --- 3) Fallback 'core set' (filtré par marchés dispo)
    core_set = [
        "BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT",
        "BNB/USDT:USDT", "XRP/USDT:USDT", "ADA/USDT:USDT",
        "DOGE/USDT:USDT", "LINK/USDT:USDT"
    ]
    try:
        markets = getattr(ex, "markets", {}) or {}
        available = []
        for sym in core_set:
            if sym in markets:
                available.append(sym)
            else:
                # tolère variante spot si perp absente (ultime secours)
                spot = sym.replace(":USDT", "")
                if spot in markets:
                    available.append(spot)
        if not available:
            available = core_set[:]  # dernier recours: on renvoie le core set brut
    except Exception:
        available = core_set[:]

    try:
        notifier.tg_send("⚠️ Univers principal indisponible — utilisation d’un core set de secours.")
    except Exception:
        pass

    return _persist_and_log(available, "fallback_core_set")


def get_or_build_universe(ex: ccxt.Exchange, desired_size: Optional[int] = None) -> List[str]:
    """Récupère ou reconstruit l’univers sans jamais casser la boucle:
    - Essai build_universe()
    - Si vide, reload + re-essai
    - Si encore vide, retourne un core set (≥ 1 symbole)
    - Notifie proprement l’état
    """
    try:
        size = int(desired_size) if desired_size is not None else int(database.get_setting('UNIVERSE_SIZE', UNIVERSE_SIZE))
    except Exception:
        size = UNIVERSE_SIZE
    size = max(1, int(size))

    syms = build_universe(ex)
    if syms:
        return syms[:size]

    # Second essai avec reload (au cas où build_universe aurait été court-circuité avant)
    try:
        ex.options = ex.options or {}
        ex.options["defaultType"] = "swap"
    except Exception:
        pass
    try:
        ex.load_markets(reload=True)
    except Exception:
        pass

    syms2 = build_universe(ex)
    if syms2:
        try:
            notifier.tg_send(f"✅ Univers reconstruit après reload ({len(syms2)} paires).")
        except Exception:
            pass
        return syms2[:size]

    # Dernier recours: petit core set minimal
    core_min = ["BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT"]
    try:
        notifier.tg_send("⚠️ Univers toujours vide après 2 essais — bascule sur core set minimal.")
    except Exception:
        pass
    return core_min


def start_live_sync(ex):
    t = threading.Thread(target=_live_sync_worker, args=(ex,), daemon=True)
    t.start()

def _live_sync_worker(ex):
    try:
        import asyncio
        asyncio.run(_ws_sync_loop(ex))
        return
    except Exception as e:
        try:
            notifier.tg_send_error("Live sync WS indisponible — fallback polling", e)
        except Exception:
            pass
    try:
        interval = int(database.get_setting('LIVE_POLL_SECONDS', 2))
    except Exception:
        interval = 2
    interval = max(1, interval)
    while True:
        try:
            trader.sync_positions_with_exchange(ex)
        except Exception as e:
            try:
                notifier.tg_send_error("Live sync polling", e)
            except Exception:
                pass
        time.sleep(interval)

async def _ws_sync_loop(ex_rest):
    import os
    import asyncio
    import random
    import ccxt
    import ccxt.pro as ccxtpro

    # ------------ Config & helpers ------------
    BITGET_TESTNET = os.getenv("BITGET_TESTNET", "true").lower() in ("1", "true", "yes")
    API_KEY        = os.getenv("BITGET_API_KEY", "")
    API_SECRET     = os.getenv("BITGET_API_SECRET", "")
    PASSPHRASSE    = os.getenv("BITGET_API_PASSWORD", "") or os.getenv("BITGET_PASSPHRASSE", "")

    def _make_ex_ws():
        # Exchange dédié WS (privé), options robustes
        return ccxtpro.bitget({
            "apiKey": API_KEY,
            "secret": API_SECRET,
            "password": PASSPHRASSE,
            "enableRateLimit": True,
            "timeout": 20000,
            "options": {
                "defaultType": "swap",
                "testnet": BITGET_TESTNET,
                "ws": {"gunzip": True},
            },
        })

    async def _backoff_sleep(attempt: int, base: float = 1.6, cap: float = 30.0):
        # Backoff exponentiel + jitter
        delay = min(cap, base ** attempt) + random.uniform(0.0, 0.75)
        await asyncio.sleep(delay)

    async def _recreate_exchange_safe(old_ex=None):
        try:
            if old_ex is not None:
                await old_ex.close()
        except Exception:
            pass
        return _make_ex_ws()

    ex_ws = _make_ex_ws()

    # Charger les marchés et choisir un symbole "léger" pour le keepalive
    try:
        await ex_ws.load_markets()
    except Exception:
        pass

    if hasattr(ex_ws, "symbols") and ex_ws.symbols:
        if "BTC/USDT:USDT" in ex_ws.symbols:
            _KEEPALIVE_SYMBOL = "BTC/USDT:USDT"
        elif "BTC/USDT" in ex_ws.symbols:
            _KEEPALIVE_SYMBOL = "BTC/USDT"
        else:
            _KEEPALIVE_SYMBOL = next(iter(ex_ws.symbols))
    else:
        _KEEPALIVE_SYMBOL = "BTC/USDT:USDT"

    # ------------ Loops robustes ------------
    async def watch_positions():
        nonlocal ex_ws
        attempt = 0
        while True:
            try:
                await ex_ws.watch_positions()
                attempt = 0  # reset backoff si ça vit
                trader.sync_positions_with_exchange(ex_rest)
            except (ccxt.NetworkError, ccxt.ExchangeError) as e:
                msg = str(e)
                if any(k in msg for k in ("1006", "1001", "Connection closed", "abnormal closure")):
                    try:
                        notifier.tg_send("⚠️ WS positions fermé (1006/1001). Reconnexion…")
                    except Exception:
                        pass
                    ex_ws = await _recreate_exchange_safe(ex_ws)
                    attempt += 1
                    await _backoff_sleep(attempt)
                    continue
                try:
                    notifier.tg_send(f"⚠️ WS positions erreur réseau: {e}. Retry…")
                except Exception:
                    pass
                attempt += 1
                await _backoff_sleep(attempt)
            except Exception as e:
                try:
                    notifier.tg_send(f"❌ WS positions exception: {e}. Restart loop…")
                except Exception:
                    pass
                ex_ws = await _recreate_exchange_safe(ex_ws)
                attempt = 0
                await asyncio.sleep(1.0)

    async def watch_orders():
        nonlocal ex_ws
        attempt = 0
        while True:
            try:
                await ex_ws.watch_orders()
                attempt = 0
                trader.sync_positions_with_exchange(ex_rest)
            except (ccxt.NetworkError, ccxt.ExchangeError) as e:
                msg = str(e)
                if any(k in msg for k in ("1006", "1001", "Connection closed", "abnormal closure")):
                    try:
                        notifier.tg_send("⚠️ WS orders fermé (1006/1001). Reconnexion…")
                    except Exception:
                        pass
                    ex_ws = await _recreate_exchange_safe(ex_ws)
                    attempt += 1
                    await _backoff_sleep(attempt)
                    continue
                try:
                    notifier.tg_send(f"⚠️ WS orders erreur réseau: {e}. Retry…")
                except Exception:
                    pass
                attempt += 1
                await _backoff_sleep(attempt)
            except Exception as e:
                try:
                    notifier.tg_send(f"❌ WS orders exception: {e}. Restart loop…")
                except Exception:
                    pass
                ex_ws = await _recreate_exchange_safe(ex_ws)
                attempt = 0
                await asyncio.sleep(1.0)

    # Keepalive public pour éviter certains NAT timeouts
    async def watch_keepalive():
        nonlocal ex_ws
        attempt = 0
        while True:
            try:
                await ex_ws.watch_ticker(_KEEPALIVE_SYMBOL)
                attempt = 0
            except Exception:
                attempt += 1
                await _backoff_sleep(attempt)

    # Lancer les 3 boucles en parallèle
    await asyncio.gather(
        watch_positions(),
        watch_orders(),
        watch_keepalive(),
    )


def _telegram_command_handlers() -> Dict[str, Any]:
    """
    Retourne la table des commandes Telegram -> handlers.
    Ajout de /offset pour ouvrir le panneau Offset TP/SL.
    """
    return {
        "setuniverse": notifier.set_universe_command,   
        "setmaxpos":  notifier.set_maxpos_command,      
        "offset":     notifier.offset_command,          
        "help":       notifier.send_commands_help,      
    }


def select_and_execute_best_pending_signal(ex: ccxt.Exchange):
    """Sélectionne le meilleur signal en attente (RR max), marque en DB (pris / non pris) et exécute le meilleur."""
    from state import get_pending_signals, clear_pending_signals
    pendings = list(get_pending_signals().values())
    if not pendings:
        return
    print(f"-> Analyse de {len(pendings)} signaux en attente...")

    validated = []
    for pending in pendings:
        try:
            symbol = pending['symbol']
            df = utils.fetch_and_prepare_df(ex, symbol, TIMEFRAME)
            # Re-valide si on a une bougie close postérieure à la bougie du signal
            if df is None or df.index[-1] <= pending.get('candle_timestamp'):
                continue
            validated.append({**pending, 'df': df})
        except Exception:
            continue

    clear_pending_signals()

    if not validated:
        print("   -> Aucun signal n'a été re-validé.")
        return

    # Choix du meilleur par RR
    best = sorted(validated, key=lambda x: x['signal']['rr'], reverse=True)[0]
    print(f"   -> MEILLEUR SIGNAL: {best['symbol']} (RR: {best['signal']['rr']:.2f})")

    # Marquage DB pour TOUS les validés (non pris)
    for v in validated:
        try:
            ts = int(pd.Timestamp(v.get('candle_timestamp')).value // 10**6)
        except Exception:
            ts = int(time.time() * 1000)
        database.mark_signal_validated(v['symbol'], ts, {**v['signal'], "timeframe": TIMEFRAME}, taken=False)

    # Exécute le meilleur et marque 'pris'
    try:
        symbol = best['symbol']
        sig    = best['signal']
        # Appel corrigé: (ex, symbol, timeframe, signal)
        ok, msg = trader.execute_trade(ex, symbol, TIMEFRAME, sig)
        try:
            ts = int(pd.Timestamp(best.get('candle_timestamp')).value // 10**6)
        except Exception:
            ts = int(time.time() * 1000)
        database.mark_signal_validated(symbol, ts, {**sig, "timeframe": TIMEFRAME}, taken=True)
        if not ok:
            notifier.tg_send(f"⚠️ Exécution du meilleur signal non aboutie: {msg}")
    except Exception as e:
        notifier.tg_send_error("Exécution du meilleur signal", e)

def process_callback_query(callback_query: Dict):
    """Gère les clics sur les boutons interactifs de manière robuste et lisible."""
    global _paused
    data = callback_query.get('data', '')

    try:
        notifier.tg_answer_callback_query(callback_query.get('id'), "")
    except Exception:
        pass

    try:
        if data == 'pause':
            with _lock: _paused = True
            notifier.tg_send("⏸️ Bot mis en pause.")
            database.set_setting('PAUSED', 'true')
            notifier.send_main_menu(_paused)

        elif data == 'resume':
            with _lock: _paused = False
            notifier.tg_send("▶️ Bot relancé.")
            database.set_setting('PAUSED', 'false')
            notifier.send_main_menu(_paused)

        elif data == 'ping':
            notifier.send_main_menu(_paused)

        elif data == 'list_positions':
            try:
                ex = create_exchange()
                trader.sync_positions_with_exchange(ex)
                try:
                    exchange_positions = ex.fetch_positions()
                except Exception:
                    exchange_positions = []
                db_positions = database.get_open_positions()
                notifier.format_synced_open_positions(exchange_positions, db_positions)
            except Exception as e:
                notifier.tg_send_error("Sync positions (manual view)", e)

        elif data == 'get_stats':
            ex = create_exchange()
            balance = trader.get_usdt_balance(ex)

            # Sauvegarde si solde valide, sinon fallback sur la dernière valeur connue
            if balance is not None:
                try:
                    database.set_setting('CURRENT_BALANCE_USDT', f"{float(balance):.2f}")
                except Exception:
                    pass
            else:
                try:
                    raw = database.get_setting('CURRENT_BALANCE_USDT', None)
                    if raw is not None:
                        balance = float(raw)
                except Exception:
                    balance = None

            trades = database.get_closed_trades_since(int(time.time()) - 7 * 86400)
            notifier.send_report("📊 Bilan Hebdomadaire (7 derniers jours)", trades, balance)

        elif data == 'toggle_cutwick':
            new_val = database.toggle_setting_bool('CUT_WICK_FOR_RR', default_true=False)
            notifier.send_config_menu()

        elif data == 'menu_config':
            notifier.send_config_menu()

        elif data == 'show_config':
            max_pos = database.get_setting('MAX_OPEN_POSITIONS', MAX_OPEN_POSITIONS)
            config = {
                "RR Min": MIN_RR,
                "Risque/Trade": f"{trader.RISK_PER_TRADE_PERCENT}%",
                "Positions Max": max_pos,
                "Levier": trader.LEVERAGE
            }
            notifier.send_config_message(config)

        elif data == 'menu_signals':
            notifier.send_signals_menu()

        elif data == 'main_menu':
            notifier.send_main_menu(_paused)

        elif data == 'manage_strategy':
            current_strategy = database.get_setting('STRATEGY_MODE', 'NORMAL')
            notifier.send_strategy_menu(current_strategy)

        elif data == 'show_mode':
            current_paper_mode = str(database.get_setting('PAPER_TRADING_MODE', 'true')).lower() == 'true'
            notifier.send_mode_message(is_testnet=BITGET_TESTNET, is_paper=current_paper_mode)

        elif data == 'switch_to_REAL':
            database.set_setting('PAPER_TRADING_MODE', 'false')
            notifier.send_mode_message(is_testnet=BITGET_TESTNET, is_paper=False)

        elif data == 'switch_to_PAPER':
            database.set_setting('PAPER_TRADING_MODE', 'true')
            notifier.send_mode_message(is_testnet=BITGET_TESTNET, is_paper=True)

        elif data.startswith('switch_to_'):
            new_strategy = data.replace('switch_to_', '')
            if new_strategy in ['NORMAL', 'SPLIT']:
                database.set_setting('STRATEGY_MODE', new_strategy)
                notifier.tg_send(f"✅ Stratégie mise à jour en <b>{new_strategy}</b>.")
                notifier.send_strategy_menu(new_strategy)
                notifier.send_main_menu(_paused)

        elif data.startswith('close_trade_'):
            try:
                trade_id = int(data.replace('close_trade_', ''))
                trader.close_position_manually(create_exchange(), trade_id)
            except (ValueError, IndexError):
                notifier.tg_send("❌ Erreur : ID de trade invalide.")

    except Exception as e:
        print(f"Erreur lors du traitement du callback '{data}': {e}")
        notifier.tg_send_error(f"Commande '{data}'", "Une erreur inattendue est survenue.")

        
def process_message(message: Dict):
    """Gère les commandes textuelles pour les actions non couvertes par les boutons."""
    global _paused
    text = message.get("text", "").strip().lower()
    parts = text.split()
    command = parts[0] if parts else ""

    if command == "/start":
        notifier.send_main_menu(_paused)

    elif command == "/mode":
        current_paper_mode = str(database.get_setting('PAPER_TRADING_MODE', 'true')).lower() == 'true'
        notifier.send_mode_message(is_testnet=BITGET_TESTNET, is_paper=current_paper_mode)

    elif command == "/offset":
        chat_id = (message.get("chat") or {}).get("id")
        notifier.offset_command(chat_id=chat_id)

    elif command.startswith("/set"):
        if command == "/setuniverse" and len(parts) > 1:
            try:
                size = int(parts[1])
                if size > 0:
                    database.set_setting('UNIVERSE_SIZE', str(size))
                    notifier.tg_send(f"✅ Taille de l'univers mise à <b>{size}</b> (prise en compte immédiate).")
                    notifier.send_main_menu(_paused)
                else:
                    notifier.tg_send("❌ Le nombre doit être > 0.")
            except ValueError:
                notifier.tg_send("❌ Valeur invalide. Utilisez: /setuniverse 500")

        elif command == "/setmaxpos" and len(parts) > 1:
            try:
                max_p = int(parts[1])
                if max_p >= 0:
                    database.set_setting('MAX_OPEN_POSITIONS', max_p)
                    notifier.tg_send(f"✅ Positions max mises à <b>{max_p}</b>.")
                else:
                    notifier.tg_send("❌ Le nombre doit être >= 0.")
            except ValueError:
                notifier.tg_send("❌ Valeur invalide. Utilisez: /setmaxpos 3")

    elif command == "/stats":
        ex = create_exchange()
        balance = trader.get_usdt_balance(ex)

        # Sauvegarde si solde valide, sinon fallback sur la dernière valeur connue
        if balance is not None:
            try:
                database.set_setting('CURRENT_BALANCE_USDT', f"{float(balance):.2f}")
            except Exception:
                pass
        else:
            try:
                raw = database.get_setting('CURRENT_BALANCE_USDT', None)
                if raw is not None:
                    balance = float(raw)
            except Exception:
                balance = None

        trades = database.get_closed_trades_since(int(time.time()) - 7 * 24 * 60 * 60)
        notifier.send_report("📊 Bilan des 7 derniers jours", trades, balance)

def check_scheduled_reports():
    """Gère les rapports automatiques."""
    global _last_daily_report_day, _last_weekly_report_day
    try:
        tz = pytz.timezone(TIMEZONE)
    except Exception:
        tz = pytz.timezone("UTC")
    now = datetime.now(tz)

    # Protéger les variables _last_* contre les accès concurrents
    with _lock:
        # Rapport quotidien
        if now.hour == REPORT_HOUR and now.day != _last_daily_report_day:
            _last_daily_report_day = now.day
            trades = database.get_closed_trades_since(int(time.time()) - 86400)  # 24 heures

            ex = create_exchange()
            balance = trader.get_usdt_balance(ex)
            if balance is not None:
                try:
                    database.set_setting('CURRENT_BALANCE_USDT', f"{float(balance):.2f}")
                except Exception:
                    pass
            else:
                try:
                    raw = database.get_setting('CURRENT_BALANCE_USDT', None)
                    if raw is not None:
                        balance = float(raw)
                except Exception:
                    balance = None

            notifier.send_report("📊 Bilan Quotidien (24h)", trades, balance)

        # Rapport hebdomadaire (dimanche = 6 en Europe/Lisbon par défaut)
        if now.weekday() == REPORT_WEEKDAY and now.hour == REPORT_HOUR and now.day != _last_weekly_report_day:
            _last_weekly_report_day = now.day
            trades = database.get_closed_trades_since(int(time.time()) - 7 * 86400)  # 7 jours

            ex = create_exchange()
            balance = trader.get_usdt_balance(ex)
            if balance is not None:
                try:
                    database.set_setting('CURRENT_BALANCE_USDT', f"{float(balance):.2f}")
                except Exception:
                    pass
            else:
                try:
                    raw = database.get_setting('CURRENT_BALANCE_USDT', None)
                    if raw is not None:
                        balance = float(raw)
                except Exception:
                    balance = None

            notifier.send_report("🗓️ Bilan Hebdomadaire", trades, balance)

# ==============================================================================
# BOUCLES ET MAIN
# ==============================================================================
def check_restart_request():
    """
    Si RESTART_REQUESTED == 'true', consomme prudemment les updates restants (si offset connu),
    réinitialise le drapeau puis relance le process via os.execl.
    """
    try:
        flag = str(database.get_setting('RESTART_REQUESTED', 'false')).lower() == 'true'
    except Exception:
        flag = False

    if not flag:
        return

    # Tenter d'avancer l'offset Telegram si on a mémorisé un update_id
    try:
        last_uid_raw = database.get_setting('LAST_TELEGRAM_UPDATE_ID', None)
        if last_uid_raw is not None and str(last_uid_raw).strip() != "":
            try:
                import requests  # local import pour éviter hard deps au chargement
                from notifier import TELEGRAM_API  # réutilise la même base d'URL
                offset = int(last_uid_raw) + 1
                requests.get(f"{TELEGRAM_API}/getUpdates", params={"offset": offset, "timeout": 0}, timeout=3)
            except Exception:
                pass
    except Exception:
        pass

    # Réarmer le drapeau AVANT relance
    try:
        database.set_setting('RESTART_REQUESTED', 'false')
    except Exception:
        pass

    # Relance "propre" du process
    try:
        import os, sys
        os.execl(sys.executable, sys.executable, *sys.argv)
    except Exception:
        # En cas d'échec execl, on termine en dernier recours
        import os
        os._exit(0)

def route_inline_restart_callback(update: Dict[str, Any]) -> bool:
    """
    À appeler en tête de la boucle qui parcourt les updates Telegram.
    Retourne True si le callback (ex. restart) a été géré ici, sinon False.
    """
    try:
        if not update or 'callback_query' not in update:
            return False
        return notifier.try_handle_inline_callback(update['callback_query'])
    except Exception as e:
        notifier.tg_send_error("Loop callback routing", e)
        return False

def poll_telegram_updates():
    """Récupère et distribue les mises à jour de Telegram. C'est le cœur de la réactivité."""
    if not hasattr(poll_telegram_updates, "_last_cb_id"):
        poll_telegram_updates._last_cb_id = None

    global _last_update_id
    updates = notifier.tg_get_updates(_last_update_id + 1 if _last_update_id else None)
    for upd in updates:
        _last_update_id = upd.get("update_id", _last_update_id)
        try:
            # Sauvegarde l’offset courant pour un redémarrage propre
            if _last_update_id is not None:
                database.set_setting('LAST_TELEGRAM_UPDATE_ID', str(int(_last_update_id)))
        except Exception:
            pass

        # Routage prioritaire (OFS:, signaux, restart...) déjà géré côté notifier
        if route_inline_restart_callback(upd):
            continue

        if 'callback_query' in upd:
            cb = upd['callback_query']
            cb_id = cb.get('id')
            if cb_id and cb_id == poll_telegram_updates._last_cb_id:
                continue
            poll_telegram_updates._last_cb_id = cb_id
            process_callback_query(cb)
        elif 'message' in upd:
            process_message(upd['message'])

            
def telegram_listener_loop():
    """Thread dédié qui exécute la boucle de polling Telegram."""
    print("🤖 Thread Telegram démarré.")
    while True:
        try:
            # ← vérifie en tête de boucle si un redémarrage a été demandé
            check_restart_request()

            poll_telegram_updates()
            check_scheduled_reports()
            time.sleep(0.5)
        except Exception as e:
            print(f"Erreur dans le thread Telegram: {e}")
            time.sleep(5)


def trading_engine_loop(ex: ccxt.Exchange, universe: List[str]):
    print("📈 Thread Trading démarré.")
    last_hour = -1
    last_day = -1  # refresh univers 1×/jour
    current_size = len(universe)

    while True:
        try:
            check_restart_request()
            
            with _lock: is_paused = _paused
            if is_paused:
                print("   -> (Pause)"); time.sleep(LOOP_DELAY); continue

            now_utc = datetime.now(timezone.utc)
            curr_hour = now_utc.hour
            curr_day = now_utc.day

            try:
                desired_size = int(database.get_setting('UNIVERSE_SIZE', UNIVERSE_SIZE))
            except Exception:
                desired_size = UNIVERSE_SIZE
            if desired_size != current_size or not universe:
                new_universe = build_universe(ex)
                if new_universe:
                    universe = new_universe[:desired_size]
                    current_size = desired_size
                    print(f"🔁 Univers mis à jour immédiatement ({len(universe)} paires).")
                    try:
                        notifier.send_main_menu(_paused)
                    except Exception:
                        pass

            if curr_day != last_day:
                try:
                    new_universe = utils.get_universe_by_market_cap(ex, current_size)
                    if new_universe:
                        universe = new_universe[:current_size]
                        print(f"🔁 Univers rafraîchi ({len(universe)} paires).")
                except Exception as e:
                    print(f"Refresh univers échoué — on conserve l'existant: {e}")
                last_day = curr_day

            if curr_hour != last_hour:
                select_and_execute_best_pending_signal(ex)
                last_hour = curr_hour

            check_scheduled_reports()
            cleanup_recent_signals()
            trader.manage_open_positions(ex)

            from state import set_pending_signal, get_pending_signals
            print(f"--- Scan de {len(universe)} paires ---")
            for symbol in universe:
                df = utils.fetch_and_prepare_df(ex, symbol, TIMEFRAME)
                if df is None: continue

                signal = trader.detect_signal(symbol, df)
                if signal:
                    # Timestamp ms de la bougie courante
                    try:
                        ts_ms = int(pd.Timestamp(df.index[-1]).value // 10**6)
                    except Exception:
                        ts_ms = int(time.time() * 1000)

                    # Mémoire en RAM
                    if symbol not in get_pending_signals():
                        print(f"✅ Signal détecté pour {symbol}! En attente de clôture.")
                        set_pending_signal(symbol, {
                            'signal': signal,
                            'symbol': symbol,
                            'candle_timestamp': df.index[-1],
                            'df': df
                        })
                        # Persistance DB immédiate
                        try:
                            database.upsert_signal_pending(
                                symbol=symbol, timeframe=TIMEFRAME, ts=ts_ms,
                                side=signal.get('side',''), regime=signal.get('regime',''),
                                rr=float(signal.get('rr', 0.0)),
                                entry=float(signal.get('entry', 0.0)),
                                sl=float(signal.get('sl', 0.0)),
                                tp=float(signal.get('tp', 0.0))
                            )
                        except Exception as e:
                            notifier.tg_send_error("Upsert PENDING signal", e)

                        if str(database.get_setting('PENDING_ALERTS', 'false')).lower() == 'true':
                            notifier.send_pending_signal_notification(symbol, signal)

                    # Historique court des signaux récents (affichage 6h bouton)
                    if not any(s['symbol'] == symbol and s['timestamp'] > time.time() - 3600 for s in _recent_signals):
                        _recent_signals.append({'timestamp': time.time(), 'symbol': symbol, 'signal': signal})

            time.sleep(LOOP_DELAY)

        except Exception:
            err = traceback.format_exc()
            print(err); notifier.tg_send_error("Erreur Trading", err); time.sleep(15)

def main():
    database.setup_database()
    startup_checks()
    ex = create_exchange()

    try:
        trader.sync_positions_with_exchange(ex)
    except Exception as e:
        notifier.tg_send_error("Sync positions au démarrage", e)
    
    start_live_sync(ex)
    
    if not database.get_setting('STRATEGY_MODE'):
        database.set_setting('STRATEGY_MODE', 'NORMAL')
        
    # Restaure l'état Pause/Reprise depuis la DB
    global _paused
    paused_raw = database.get_setting('PAUSED', 'false')
    _paused = str(paused_raw).lower() == 'true'
    
    # Initialisation du mode de trading (PAPIER/RÉEL) — source de vérité : DB
    paper_mode_setting = database.get_setting('PAPER_TRADING_MODE', None)
    if not paper_mode_setting:
        database.set_setting('PAPER_TRADING_MODE', 'false')  # défaut = RÉEL
        paper_mode_setting = 'false'

    current_paper_mode = str(paper_mode_setting).lower() == 'true'
    notifier.send_main_menu(_paused)
    
    # ✅ Univers robuste (ne casse pas le démarrage)
    universe = get_or_build_universe(ex)
    if not universe:
        # Ce cas ne devrait pas survenir grâce aux garde-fous,
        # mais on sécurise malgré tout avec 1 symbole.
        universe = ["BTC/USDT:USDT"]
        try:
            notifier.tg_send("⚠️ Démarrage sécurisé avec univers minimal ['BTC/USDT:USDT'].")
        except Exception:
            pass

    print(f"Univers de trading chargé avec {len(universe)} paires.")

    telegram_thread = threading.Thread(target=telegram_listener_loop, daemon=True)
    trading_thread = threading.Thread(target=trading_engine_loop, args=(ex, universe), daemon=True)

    telegram_thread.start()
    trading_thread.start()
    
    try:
        trading_thread.join()
    except KeyboardInterrupt:
        print("Arrêt demandé.")
        notifier.tg_send("⛔ Arrêt manuel.")

if __name__ == "__main__":
    main()
