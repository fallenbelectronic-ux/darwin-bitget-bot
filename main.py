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

# --- (PARAMÈTRES STABLES) ---
BITGET_TESTNET   = os.getenv("BITGET_TESTNET", "true").lower() in ("1", "true", "yes")
API_KEY          = os.getenv("BITGET_API_KEY", "")
API_SECRET       = os.getenv("BITGET_API_SECRET", "")
PASSPHRASSE      = os.getenv("BITGET_API_PASSWORD", "") or os.getenv("BITGET_PASSPHRASSE", "")
TIMEFRAME        = os.getenv("TIMEFRAME", "1h")
UNIVERSE_SIZE    = int(os.getenv("UNIVERSE_SIZE", "30"))
MIN_RR           = float(os.getenv("MIN_RR", "3.0"))
MM80_DEAD_ZONE_PERCENT = float(os.getenv("MM80_DEAD_ZONE_PERCENT", "0.1"))
LOOP_DELAY       = int(os.getenv("LOOP_DELAY", "5"))
TICK_RATIO       = 0.0005
FALLBACK_TESTNET = ["BTC/USDT:USDT", "ETH/USDT:USDT", "XRP/USDT:USDT", "SOL/USDT:USDT"]

def create_exchange():
    """Initialise et retourne l'objet d'échange CCXT."""
    ex = ccxt.bitget({
        "apiKey": API_KEY, "secret": API_SECRET, "password": PASSPHRASSE,
        "enableRateLimit": True, "options": {"defaultType": "swap", "testnet": BITGET_TESTNET}
    })
    if BITGET_TESTNET: ex.set_sandbox_mode(True)
    return ex

# ==============================================================================
# FONCTIONS DE BASE AJOUTÉES
# ==============================================================================

def fetch_ohlcv_df(ex: ccxt.Exchange, symbol: str, timeframe: str = '1h', limit: int = 100) -> Optional[pd.DataFrame]:
    """Récupère les données OHLCV et les retourne sous forme de DataFrame Pandas."""
    try:
        ohlcv = ex.fetch_ohlcv(symbol, timeframe, limit=limit)
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df.set_index('timestamp', inplace=True)
        return df
    except Exception as e:
        print(f"Erreur lors de la récupération des données OHLCV pour {symbol}: {e}")
        return None

def build_universe(ex: ccxt.Exchange) -> List[str]:
    """Construit la liste des paires à trader en se basant sur le volume."""
    print("Construction de l'univers de trading...")
    try:
        markets = ex.load_markets()
        # Filtre pour les perpetuals linéaires USDT
        symbols = [
            m['symbol'] for m in markets.values()
            if m.get('swap', False) and m.get('quote', '') == 'USDT' and m.get('linear', False)
        ]
        
        # Pour trier par volume, une approche plus complexe serait nécessaire.
        # Pour l'instant, nous utilisons une liste de secours ou les premiers résultats.
        # Idéalement, il faudrait fetch les tickers et trier par quoteVolume.
        
        if not symbols:
            print("Aucun symbole trouvé via l'API, utilisation de la liste de secours.")
            return FALLBACK_TESTNET
        
        # Limiter à la taille de l'univers (si la liste est très grande)
        return symbols[:UNIVERSE_SIZE]
        
    except Exception as e:
        print(f"Impossible de construire l'univers via l'API, utilisation de la liste de secours. Erreur: {e}")
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

# ==============================================================================

_last_update_id: Optional[int] = None
_paused = False

def process_callback_query(callback_query: Dict):
    """Gère les clics sur les boutons interactifs."""
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

def main():
    ex = create_exchange()
    database.setup_database()

    notifier.send_start_banner(
        "TESTNET" if BITGET_TESTNET else "LIVE",
        "PAPIER" if trader.PAPER_TRADING_MODE else "RÉEL",
        trader.RISK_PER_TRADE_PERCENT
    )
    
    universe = build_universe(ex) # CET APPEL EST MAINTENANT VALIDE
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
            
            # APPEL À LA FONCTION MANQUANTE DANS TRADER.PY (VOIR CORRECTION CI-DESSOUS)
            # trader.manage_open_positions(ex) 
            
            print("Scanning de l'univers...")
            for symbol in universe:
                df = fetch_ohlcv_df(ex, symbol, TIMEFRAME)
                if df is None or df.empty:
                    continue
                
                last_candle_ts = df.index[-1]
                if symbol in last_ts_seen and last_ts_seen[symbol] == last_candle_ts:
                    continue # On a déjà vu cette bougie
                
                last_ts_seen[symbol] = last_candle_ts

                signal = detect_signal(df)
                if signal:
                    print(f"Signal détecté pour {symbol}!")
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
