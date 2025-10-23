# Fichier: utils.py
import ccxt
import pandas as pd
from typing import Optional
from ta.volatility import BollingerBands

def fetch_ohlcv_df(ex: ccxt.Exchange, symbol: str, timeframe: str, limit: int = 120) -> Optional[pd.DataFrame]:
    """Récupère les données OHLCV et les retourne sous forme de DataFrame Pandas."""
def fetch_and_prepare_df(ex: ccxt.Exchange, symbol: str, timeframe: str, limit: int = 120) -> Optional[pd.DataFrame]:
    """Récupère les données OHLCV et y ajoute les indicateurs Bollinger Bands."""
    try:
        # S'assurer que l'exchange a bien chargé les marchés
        if not ex.markets:
            ex.load_markets()
            
        ohlcv = ex.fetch_ohlcv(symbol, timeframe, limit=limit)
        if not ohlcv:
            return None # Retourner None si aucune donnée n'est reçue
            
        if not ohlcv or len(ohlcv) < 81:
            return None
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df.set_index('timestamp', inplace=True)
        return df
        df.set_index(pd.to_datetime(df['timestamp'], unit='ms'), inplace=True)
        # Calcul des indicateurs BB(20,2)
        bb20 = BollingerBands(close=df["close"], window=20, window_dev=2)
        df["bb20_up"], df["bb20_mid"], df["bb20_lo"] = bb20.bollinger_hband(), bb20.bollinger_mavg(), bb20.bollinger_lband()
        # Calcul des indicateurs BB(80,2)
        bb80 = BollingerBands(close=df["close"], window=80, window_dev=2)
        df["bb80_up"], df["bb80_mid"], df["bb80_lo"] = bb80.bollinger_hband(), bb80.bollinger_mavg(), bb80.bollinger_lband()
        return df.dropna()
    except Exception as e:
        print(f"Erreur de récupération OHLCV pour {symbol} sur {timeframe}: {e}")
        # print(f"Erreur de récupération OHLCV pour {symbol}: {e}") # Optionnel: peut être bruyant
        return None

def touched_or_crossed(low: float, high: float, band: float, side: str) -> bool:
    """Vérifie si une bougie a touché ou traversé une bande."""
    return (low <= band) if side == "buy" else (high >= band)

def close_inside_bb20(close: float, lo: float, up: float) -> bool:
    """Vérifie si un prix est à l'intérieur des bandes BB20."""
    return lo <= close <= up
