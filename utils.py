# Fichier: utils.py
import ccxt
import pandas as pd
from typing import Optional

def fetch_ohlcv_df(ex: ccxt.Exchange, symbol: str, timeframe: str, limit: int = 120) -> Optional[pd.DataFrame]:
    """Récupère les données OHLCV et les retourne sous forme de DataFrame Pandas."""
    try:
        # S'assurer que l'exchange a bien chargé les marchés
        if not ex.markets:
            ex.load_markets()
            
        ohlcv = ex.fetch_ohlcv(symbol, timeframe, limit=limit)
        if not ohlcv:
            return None # Retourner None si aucune donnée n'est reçue
            
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df.set_index('timestamp', inplace=True)
        return df
    except Exception as e:
        print(f"Erreur de récupération OHLCV pour {symbol} sur {timeframe}: {e}")
        return None
