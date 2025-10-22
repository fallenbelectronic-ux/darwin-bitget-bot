# Fichier: utils.py
import ccxt
import pandas as pd
from typing import Optional

def fetch_ohlcv_df(ex: ccxt.Exchange, symbol: str, timeframe: str, limit: int = 120) -> Optional[pd.DataFrame]:
    """
    Récupère les données OHLCV pour un symbole et les retourne sous forme de DataFrame Pandas.
    Retourne None en cas d'erreur.
    """
    try:
        if not ex.markets:
            ex.load_markets()
            
        ohlcv = ex.fetch_ohlcv(symbol, timeframe, limit=limit)
        if not ohlcv:
            print(f"Aucune donnée OHLCV reçue pour {symbol} sur {timeframe}.")
            return None
            
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df.set_index('timestamp', inplace=True)
        return df
    except Exception as e:
        print(f"Erreur lors de la récupération des données OHLCV pour {symbol}: {e}")
        return None
