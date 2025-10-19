# Fichier: main.py
import os
import time
import ccxt
import pandas as pd
from ta.volatility import BollingerBands
from typing import List, Dict, Any, Optional
import database
import trader
import notifier

# --- PARAMÈTRES CHARGÉS DEPUIS L'ENVIRONNEMENT ---
BITGET_TESTNET   = os.getenv("BITGET_TESTNET", "true").lower() in ("1", "true", "yes")
API_KEY          = os.getenv("BITGET_API_KEY", "")
API_SECRET       = os.getenv("BITGET_API_SECRET", "")
PASSPHRASSE      = os.getenv("BITGET_API_PASSWORD", "") or os.getenv("BITGET_PASSPHRASSE", "")

# Paramètres de la stratégie
TIMEFRAME        = os.getenv("TIMEFRAME", "1h") # Unité de temps unique
UNIVERSE_SIZE    = int(os.getenv("UNIVERSE_SIZE", "30"))
MIN_RR           = float(os.getenv("MIN_RR", "3.0"))
MM_DEAD_ZONE_PERCENT = float(os.getenv("MM_DEAD_ZONE_PERCENT", "0.1")) # Zone neutre autour de la MM20
TICK_RATIO       = 0.0005 # Pour calculer le "tick" comme un % du prix

# Paramètres du bot
LOOP_DELAY       = int(os.getenv("LOOP_DELAY", "5"))
FALLBACK_TESTNET = ["BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT", "LINK/USDT:USDT"]

def create_exchange():
    """Initialise et retourne l'objet d'échange CCXT."""
    ex = ccxt.bitget({
        "apiKey": API_KEY, "secret": API_SECRET, "password": PASSPHRASSE,
        "enableRateLimit": True, "options": {"defaultType": "swap", "testnet": BITGET_TESTNET}
    })
    if BITGET_TESTNET: ex.set_sandbox_mode(True)
    return ex

def fetch_ohlcv_df(ex: ccxt.Exchange, symbol: str, timeframe: str, limit: int = 120) -> Optional[pd.DataFrame]:
    """Récupère les données OHLCV. Augmentation de la limite pour la BB 80."""
    try:
        ohlcv = ex.fetch_ohlcv(symbol, timeframe, limit=limit)
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df.set_index('timestamp', inplace=True)
        return df
    except Exception as e:
        print(f"Erreur de récupération OHLCV pour {symbol} sur {timeframe}: {e}")
        return None

def build_universe(ex: ccxt.Exchange) -> List[str]:
    # ... (Cette fonction reste inchangée)
    print("Construction de l'univers de trading...")
    try:
        markets = ex.load_markets()
        symbols = [m['symbol'] for m in markets.values() if m.get('swap') and m.get('quote') == 'USDT' and m.get('linear')]
        if not symbols: return FALLBACK_TESTNET
        return symbols[:UNIVERSE_SIZE]
    except Exception as e:
        print(f"Impossible de construire l'univers via l'API. Utilisation de la liste de secours. Erreur: {e}")
        return FALLBACK_TESTNET

# ==============================================================================
# LOGIQUE DE DÉTECTION "DARWIN TRADING" MISE À JOUR (1 UT, 2 BB)
# ==============================================================================
def detect_signal(df: pd.DataFrame) -> Optional[Dict[str, Any]]:
    """
    Détecte les signaux de trading en utilisant deux Bandes de Bollinger sur une seule unité de temps.
    """
    # Nous avons besoin d'au moins 81 bougies pour calculer la BB(80)
    if df is None or len(df) < 81:
        return None

    # --- 1. Calcul des indicateurs ---
    # BB Standard (BB Blanche)
    bb_20 = BollingerBands(close=df['close'], window=20, window_dev=2)
    df['bb20_up'] = bb_20.bollinger_hband()
    df['bb20_mid'] = bb_20.bollinger_mavg()
    df['bb20_lo'] = bb_20.bollinger_lband()

    # BB Large (BB Jaune, approximant l'UT supérieure)
    bb_80 = BollingerBands(close=df['close'], window=80, window_dev=2)
    df['bb80_up'] = bb_80.bollinger_hband()
    df['bb80_mid'] = bb_80.bollinger_mavg()
    df['bb80_lo'] = bb_80.bollinger_lband()

    last = df.iloc[-1] # Bougie actuelle (de déclenchement potentielle)
    
    # --- 2. Définition de la tendance et de la zone neutre ---
    is_uptrend = last['close'] > last['bb20_mid']
    is_downtrend = last['close'] < last['bb20_mid']
    
    dead_zone = last['bb20_mid'] * (MM_DEAD_ZONE_PERCENT / 100)
    is_in_dead_zone = abs(last['close'] - last['bb20_mid']) < dead_zone

    signal = None
    tick_size = last['close'] * TICK_RATIO

    # --- 3. STRATÉGIE 1: PATTERN EN TENDANCE ("Extrême Correction") ---
    if not is_in_dead_zone:
        # Achat en tendance haussière : Contact avec la borne basse de la BB20
        if is_uptrend and last['low'] <= last['bb20_lo']:
            entry_price = last['close']
            sl_price = last['low'] - (2 * tick_size)
            tp_price = last['bb20_mid']
            
            if entry_price > sl_price and (tp_price - entry_price) / (entry_price - sl_price) >= MIN_RR:
                signal = {"side": "buy", "regime": "Tendance", "entry": entry_price, "sl": sl_price, "tp": tp_price, "rr": (tp_price - entry_price) / (entry_price - sl_price)}

        # Vente en tendance baissière : Contact avec la borne haute de la BB20
        elif is_downtrend and last['high'] >= last['bb20_up']:
            entry_price = last['close']
            sl_price = last['high'] + (2 * tick_size)
            tp_price = last['bb20_mid']
            
            if entry_price < sl_price and (entry_price - tp_price) / (sl_price - entry_price) >= MIN_RR:
                signal = {"side": "sell", "regime": "Tendance", "entry": entry_price, "sl": sl_price, "tp": tp_price, "rr": (entry_price - tp_price) / (sl_price - entry_price)}

    # --- 4. STRATÉGIE 2: PATTERN EN CONTRE-TENDANCE ("Double Extrême") ---
    if not signal:
        # Achat : Contact avec BB20 basse ET BB80 basse + Réintégration au-dessus de BB20 basse
        if last['low'] <= last['bb20_lo'] and last['low'] <= last['bb80_lo'] and last['close'] > last['bb20_lo']:
            entry_price = last['close']
            sl_price = last['low'] - (2 * tick_size)
            tp_price = last['bb20_mid']

            if entry_price > sl_price and (tp_price - entry_price) / (entry_price - sl_price) >= MIN_RR:
                signal = {"side": "buy", "regime": "Contre-tendance", "entry": entry_price, "sl": sl_price, "tp": tp_price, "rr": (tp_price - entry_price) / (entry_price - sl_price)}

        # Vente : Contact avec BB20 haute ET BB80 haute + Réintégration en dessous de BB20 haute
        elif last['high'] >= last['bb20_up'] and last['high'] >= last['bb80_up'] and last['close'] < last['bb20_up']:
            entry_price = last['close']
            sl_price = last['high'] + (2 * tick_size)
            tp_price = last['bb20_mid']

            if entry_price < sl_price and (entry_price - tp_price) / (sl_price - entry_price) >= MIN_RR:
                signal = {"side": "sell", "regime": "Contre-tendance", "entry": entry_price, "sl": sl_price, "tp": tp_price, "rr": (entry_price - tp_price) / (sl_price - entry_price)}

    if signal:
        signal['bb20_mid'] = last['bb20_mid']
        return signal
        
    return None

# ==============================================================================

_last_update_id: Optional[int] = None
_paused = False

# ... (Les fonctions de gestion Telegram process_callback_query, process_message, poll_telegram_updates restent INCHANGÉES) ...
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
        except (ValueError, IndexError):
            notifier.tg_send("Commande de fermeture invalide.")
def process_message(message: Dict):
    text = message.get("text", "").strip().lower()
    if text.startswith("/start"):
        notifier.send_main_menu(_paused)
    elif text.startswith("/pos"):
        positions = database.get_open_positions()
        notifier.format_open_positions(positions)
def poll_telegram_updates():
    global _last_update_id
    updates = notifier.tg_get_updates(_last_update_id + 1 if _last_update_id else None)
    for upd in updates:
        _last_update_id = upd.get("update_id", _last_update_id)
        if 'callback_query' in upd:
            process_callback_query(upd['callback_query'])
        elif 'message' in upd:
            process_message(upd['message'])
# ==============================================================================

def main():
    ex = create_exchange()
    database.setup_database()

    notifier.send_start_banner(
        "TESTNET" if BITGET_TESTNET else "LIVE",
        "PAPIER" if trader.PAPER_TRADING_MODE else "RÉEL",
        trader.RISK_PER_TRADE_PERCENT
    )
    
    universe = build_universe(ex)
    if not universe:
        notifier.tg_send("❌ Impossible de construire l'univers de trading. Arrêt du bot.")
        return
    print(f"Univers de trading chargé avec {len(universe)} paires.")
    
    last_ts_seen = {}

    while True:
        try:
            poll_telegram_updates()
            
            if _paused:
                print("Bot en pause...")
                time.sleep(LOOP_DELAY)
                continue
            
            trader.manage_open_positions(ex)
            
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Début du scan de l'univers...")
            for symbol in universe:
                # Récupérer les données pour UNE SEULE unité de temps
                df = fetch_ohlcv_df(ex, symbol, TIMEFRAME)
                
                if df is None or df.empty:
                    continue
                
                last_candle_ts = df.index[-1]
                if symbol in last_ts_seen and last_ts_seen[symbol] == last_candle_ts:
                    continue 
                last_ts_seen[symbol] = last_candle_ts

                # Détecter le signal en utilisant le dataframe unique
                signal = detect_signal(df)
                
                if signal:
                    print(f"✅ Signal '{signal['regime']}' détecté pour {symbol}!")
                    # Le dataframe `df` contient maintenant toutes les BB calculées,
                    # il peut être passé directement à la fonction de charting.
                    trader.execute_trade(ex, symbol, signal, df)

            print(f"Fin du scan. Prochain scan dans {LOOP_DELAY} secondes.")
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
