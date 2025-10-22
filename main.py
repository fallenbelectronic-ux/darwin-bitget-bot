# Fichier: main.py
import os
import time
import ccxt
import pandas as pd
from typing import List, Dict, Any, Optional
import traceback

import database
import trader
import notifier
import utils
import reporting

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
FALLBACK_TESTNET = ["BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT"]

# --- VARIABLES GLOBALES ---
_last_update_id: Optional[int] = None
_paused = False

def create_exchange():
    """Initialise et retourne l'objet de l'exchange CCXT."""
    ex = ccxt.bitget({
        "apiKey": API_KEY, "secret": API_SECRET, "password": PASSPHRASSE,
        "enableRateLimit": True, "options": {"defaultType": "swap"}
    })
    if BITGET_TESTNET:
        ex.set_sandbox_mode(True)
    return ex

def build_universe(ex: ccxt.Exchange) -> List[str]:
    """Construit la liste des paires à trader, triées par volume."""
    print("Construction de l'univers de trading...")
    try:
        ex.load_markets()
        tickers = ex.fetch_tickers()
        # Filtre pour ne garder que les contrats perpétuels USDT avec un volume
        swap_tickers = {s: t for s, t in tickers.items() if ':USDT' in s and t.get('quoteVolume')}
        # Trie les symboles par volume de citation (quoteVolume) en ordre décroissant
        sorted_symbols = sorted(swap_tickers, key=lambda s: swap_tickers[s]['quoteVolume'], reverse=True)
        print(f"Top {UNIVERSE_SIZE} paires par volume sélectionnées.")
        return sorted_symbols[:UNIVERSE_SIZE]
    except Exception as e:
        print(f"Impossible de construire l'univers via l'API. Erreur: {e}. Utilisation de la liste de secours.")
        return FALLBACK_TESTNET

def detect_signal(df: pd.DataFrame, sym: str) -> Optional[Dict[str, Any]]:
    """Logique de détection de signal, réécrite pour la clarté et la correction."""
    if df is None or len(df) < 81: return None
    
    last, prev = df.iloc[-1], df.iloc[-2]
    
    # --- Filtre 1: Réintégration obligatoire ---
    if not utils.close_inside_bb20(last['close'], last['bb20_lo'], last['bb20_up']):
        return None
    
    # --- Filtre 2: Zone neutre autour de la MM80 ---
    dead_zone = last['bb80_mid'] * (MM_DEAD_ZONE_PERCENT / 100.0)
    if abs(last['close'] - last['bb80_mid']) < dead_zone:
        return None
    
    signal = None
    tick = last['close'] * TICK_RATIO
    
    # --- Pattern 1: Tendance (Extrême Correction) ---
    is_above_mm80 = last['close'] > last['bb80_mid']
    touched_bb20_low = utils.touched_or_crossed(prev['low'], prev['high'], prev['bb20_lo'], "buy")
    touched_bb20_high = utils.touched_or_crossed(prev['low'], prev['high'], prev['bb20_up'], "sell")

    if is_above_mm80 and touched_bb20_low:
        entry, sl, tp = last['close'], prev['low'] - (2 * tick), last['bb80_up']
        if (entry - sl) > 0 and (tp - entry) / (entry - sl) >= MIN_RR:
            signal = {"side": "buy", "regime": "Tendance", "entry": entry, "sl": sl, "tp": tp, "rr": (tp-entry)/(entry-sl)}
    elif not is_above_mm80 and touched_bb20_high:
        entry, sl, tp = last['close'], prev['high'] + (2 * tick), last['bb80_lo']
        if (sl - entry) > 0 and (entry - tp) / (sl - entry) >= MIN_RR:
            signal = {"side": "sell", "regime": "Tendance", "entry": entry, "sl": sl, "tp": tp, "rr": (entry-tp)/(sl-entry)}

    # --- Pattern 2: Contre-Tendance (Double Extrême) ---
    if not signal:
        touched_double_low = prev['low'] <= min(prev['bb20_lo'], prev['bb80_lo'])
        touched_double_high = prev['high'] >= max(prev['bb20_up'], prev['bb80_up'])

        if touched_double_low:
            entry, sl, tp = last['close'], prev['low'] - (2 * tick), last['bb20_mid']
            if (entry - sl) > 0 and (tp - entry) / (entry - sl) >= MIN_RR:
                signal = {"side": "buy", "regime": "Contre-tendance", "entry": entry, "sl": sl, "tp": tp, "rr": (tp-entry)/(entry-sl)}
        elif touched_double_high:
            entry, sl, tp = last['close'], prev['high'] + (2 * tick), last['bb20_lo']
            if (sl - entry) > 0 and (entry - tp) / (sl - entry) >= MIN_RR:
                signal = {"side": "sell", "regime": "Contre-tendance", "entry": entry, "sl": sl, "tp": tp, "rr": (entry-tp)/(sl-entry)}
    
    if signal:
        signal['bb20_mid'] = last['bb20_mid']
        return signal
    return None

def process_callback_query(callback_query: Dict):
    """Gère les clics sur les boutons interactifs."""
    global _paused
    data = callback_query.get('data', '')
    if data == 'pause':
        _paused = True; notifier.tg_send("⏸️ Bot mis en pause.")
    elif data == 'resume':
        _paused = False; notifier.tg_send("▶️ Bot relancé.")
    elif data == 'list_positions':
        notifier.format_open_positions(database.get_open_positions())
    elif data == 'get_stats':
        seven_days_ago = int(time.time()) - 7 * 24 * 60 * 60
        trades = database.get_closed_trades_since(seven_days_ago)
        notifier.send_report("📊 Bilan Hebdomadaire", trades)
    elif data.startswith('close_trade_'):
        try:
            trade_id = int(data.split('_')[-1])
            trader.close_position_manually(create_exchange(), trade_id)
        except (ValueError, IndexError):
            notifier.tg_send("Commande de fermeture invalide.")

def process_message(message: Dict):
    """Gère les commandes textuelles."""
    text = message.get("text", "").strip().lower()
    if text.startswith(("/start", "/menu")):
        notifier.send_main_menu(_paused)
    elif text.startswith("/pos"):
        notifier.format_open_positions(database.get_open_positions())

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

def main():
    """Fonction principale du bot."""
    ex = create_exchange()
    database.setup_database()
    notifier.send_start_banner("TESTNET" if BITGET_TESTNET else "LIVE", "PAPIER" if trader.PAPER_TRADING_MODE else "RÉEL", trader.RISK_PER_TRADE_PERCENT)
    universe = build_universe(ex)
    last_ts_seen = {}

    while True:
        try:
            poll_telegram_updates()
            if _paused:
                time.sleep(LOOP_DELAY)
                continue
            
            trader.manage_open_positions(ex)
            
            for symbol in universe:
                df = utils.fetch_and_prepare_df(ex, symbol, TIMEFRAME)
                if df is None or (symbol in last_ts_seen and last_ts_seen[symbol] == df.index[-1]):
                    continue
                last_ts_seen[symbol] = df.index[-1]
                
                signal = detect_signal(df, symbol)
                if signal:
                    print(f"✅ Signal '{signal['regime']}' détecté pour {symbol}!")
                    trader.execute_trade(ex, symbol, signal, df)
            
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
