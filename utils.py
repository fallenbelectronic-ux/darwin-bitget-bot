# Fichier: utils.py
import ccxt
import pandas as pd
from typing import Optional
from ta.volatility import BollingerBands, AverageTrueRange

def fetch_and_prepare_df(ex: ccxt.Exchange, symbol: str, timeframe: str, limit: int = 120) -> Optional[pd.DataFrame]:
    """Récupère les données OHLCV et y ajoute tous les indicateurs nécessaires."""
    try:
        if not ex.markets: ex.load_markets()
        ohlcv = ex.fetch_ohlcv(symbol, timeframe, limit=limit)
        if not ohlcv or len(ohlcv) < 81: return None
        
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df.set_index(pd.to_datetime(df['timestamp'], unit='ms'), inplace=True)
        
        bb20 = BollingerBands(close=df["close"], window=20, window_dev=2)
        df["bb20_up"], df["bb20_mid"], df["bb20_lo"] = bb20.bollinger_hband(), bb20.bollinger_mavg(), bb20.bollinger_lband()
        
        bb80 = BollingerBands(close=df["close"], window=80, window_dev=2)
        df["bb80_up"], df["bb80_mid"], df["bb80_lo"] = bb80.bollinger_hband(), bb80.bollinger_mavg(), bb80.bollinger_lband()
        
        df['atr'] = AverageTrueRange(high=df['high'], low=df['low'], close=df['close'], window=14).average_true_range()
        
        return df.dropna()
    except Exception:
        return None
