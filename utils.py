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

def close_inside_bb20(close_price: float, bb_lo: float, bb_up: float) -> bool:
    """Vrai si la clôture est à l'intérieur (ou sur) la BB20."""
    return bb_lo <= close_price <= bb_up

def touched_or_crossed(low: float, high: float, level: float, side: str) -> bool:
    """
    Vrai si la bougie a touché/croisé un niveau.
    side='buy'  -> on veut un contact vers le bas (low <= level)
    side='sell' -> on veut un contact vers le haut (high >= level)
    """
    if side == "buy":
        return low <= level
    if side == "sell":
        return high >= level
    return False
