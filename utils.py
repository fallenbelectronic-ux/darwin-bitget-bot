# Fichier: utils.py
import ccxt
import pandas as pd
import numpy as np
from typing import Optional
from ta.volatility import BollingerBands, AverageTrueRange

_MIN_ROWS = 100          # pour BB80 + ATR confortablement
_EPS = 1e-9              # tolérance numérique

def fetch_and_prepare_df(ex: ccxt.Exchange, symbol: str, timeframe: str, limit: int = 200) -> Optional[pd.DataFrame]:
    """
    Récupère l'OHLCV et calcule:
      - BB(20,2): bb20_up, bb20_mid, bb20_lo
      - BB(80,2): bb80_up, bb80_mid, bb80_lo
      - ATR(14):  atr
    Retourne None si données insuffisantes.
    """
    try:
        if not getattr(ex, "markets", None):
            ex.load_markets()

        # Récupération OHLCV avec retries robustes (5xx/timeouts) — ajouté sans rien supprimer
        ohlcv = _safe_fetch_ohlcv_with_retries(ex, symbol, timeframe, limit=limit, params={})
        if not ohlcv or len(ohlcv) < _MIN_ROWS:
            return None

        df = pd.DataFrame(
            ohlcv,
            columns=["timestamp", "open", "high", "low", "close", "volume"]
        )

        # Index temporel propre (UTC, trié)
        ts = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
        df = df.drop(columns=["timestamp"])
        df.index = ts
        df.index.name = "timestamp"
        df.sort_index(inplace=True)

        # Cast robustes
        for c in ["open", "high", "low", "close", "volume"]:
            df[c] = pd.to_numeric(df[c], errors="coerce")

        # Calculs indicateurs
        bb20 = BollingerBands(close=df["close"], window=20, window_dev=2)
        df["bb20_up"]  = bb20.bollinger_hband()
        df["bb20_mid"] = bb20.bollinger_mavg()
        df["bb20_lo"]  = bb20.bollinger_lband()

        bb80 = BollingerBands(close=df["close"], window=80, window_dev=2)
        df["bb80_up"]  = bb80.bollinger_hband()
        df["bb80_mid"] = bb80.bollinger_mavg()
        df["bb80_lo"]  = bb80.bollinger_lband()

        atr = AverageTrueRange(
            high=df["high"], low=df["low"], close=df["close"], window=14
        ).average_true_range()
        df["atr"] = atr

        # Nettoyage final
        df = df.dropna().copy()
        if len(df) < _MIN_ROWS:
            return None

        return df

    except Exception as e:
        print(f"fetch_and_prepare_df error on {symbol} {timeframe}: {e}")
        return None

        return df

    except Exception as e:
        print(f"fetch_and_prepare_df error on {symbol} {timeframe}: {e}")
        return None

def close_inside_bb20(close_price: float, bb_lo: float, bb_up: float) -> bool:
    """Vrai si la clôture est à l'intérieur (ou sur) la BB20. Garde-fous NaN."""
    try:
        c = float(close_price); lo = float(bb_lo); up = float(bb_up)
    except (TypeError, ValueError):
        return False
    if np.isnan(c) or np.isnan(lo) or np.isnan(up):
        return False
    return (lo - _EPS) <= c <= (up + _EPS)

def touched_or_crossed(low: float, high: float, level: float, side: str) -> bool:
    """
    Vrai si la bougie a touché/croisé un niveau.
    - side='buy'  : on attend un contact vers le bas (support), donc low <= level <= high OU low <= level.
    - side='sell' : contact vers le haut (résistance), donc low <= level <= high OU high >= level.
    Petite tolérance numérique (_EPS).
    """
    try:
        lo = float(low); hi = float(high); lvl = float(level)
    except (TypeError, ValueError):
        return False
    if any(np.isnan(x) for x in (lo, hi, lvl)):
        return False

    # contact strict “dans la bougie”
    in_range = (lo - _EPS) <= lvl <= (hi + _EPS)

    if side == "buy":
        return in_range or (lo - _EPS) <= lvl
    if side == "sell":
        return in_range or (hi + _EPS) >= lvl
    return False
