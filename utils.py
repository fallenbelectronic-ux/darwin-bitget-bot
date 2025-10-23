# Fichier: utils.py
import ccxt
import pandas as pd
from ta.volatility import BollingerBands, AverageTrueRange

def fetch_and_prepare_df(ex, symbol, timeframe, limit=100):
    try:
        ohlcv = ex.fetch_ohlcv(symbol, timeframe, limit=limit)
        if not ohlcv: return None
        df = pd.DataFrame(ohlcv, columns=['ts','open','high','low','close','vol'])
        df['ts'] = pd.to_datetime(df['ts'], unit='ms')
        df.set_index('ts', inplace=True)
        
        bb20 = BollingerBands(df['close'], window=20, window_dev=2)
        df['bb20_up'], df['bb20_mid'], df['bb20_lo'] = bb20.bollinger_hband(), bb20.bollinger_mavg(), bb20.bollinger_lband()
        
        bb80 = BollingerBands(df['close'], window=80, window_dev=2)
        df['bb80_up'], df['bb80_mid'], df['bb80_lo'] = bb80.bollinger_hband(), bb80.bollinger_mavg(), bb80.bollinger_lband()
        
        df['atr'] = AverageTrueRange(df['high'], df['low'], df['close'], window=14).average_true_range()
        return df
    except: return None
