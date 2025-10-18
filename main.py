import os
import time
import ccxt
import pandas as pd
import numpy as np
from ta.volatility import BollingerBands, AverageTrueRange
from dotenv import load_dotenv
from notifier import tg_send

load_dotenv()

# =======================
# ENV / PARAMÈTRES
# =======================
BITGET_TESTNET = os.getenv("BITGET_TESTNET", "true").lower() in ("1", "true", "yes")
API_KEY       = os.getenv("BITGET_API_KEY")
API_SECRET    = os.getenv("BITGET_API_SECRET")
PASSPHRASE    = os.getenv("BITGET_API_PASSWORD") or os.getenv("BITGET_PASSPHRASE")

TF                 = os.getenv("TIMEFRAME", "1h")          # H1 unique
RISK_PER_TRADE     = float(os.getenv("RISK_PER_TRADE", "0.01"))  # 1%
MIN_RR             = float(os.getenv("MIN_RR", "3"))       # RR mini 1:3
MAX_LEVERAGE       = int(os.getenv("MAX_LEVERAGE", "2"))
LOOP_DELAY         = int(os.getenv("LOOP_DELAY", "5"))
POSITION_MODE      = os.getenv("POSITION_MODE", "cross")

UNIVERSE_SIZE      = int(os.getenv("UNIVERSE_SIZE", "100"))
PICKS              = int(os.getenv("PICKS", "4"))
MAX_OPEN_TRADES    = int(os.getenv("MAX_OPEN_TRADES", "4"))
MIN_VOLUME_USDT    = float(os.getenv("MIN_VOLUME_USDT", "0"))

# SL “pro”
ATR_WINDOW         = 14
SL_ATR_CUSHION     = 0.25     # 0.25 * ATR au-delà de la mèche

# “Réaction rapide” en tendance
QUICK_BARS         = 3        # doit avancer vite en <= 3 barres
QUICK_PROGRESS     = 0.30     # >= 30% du chemin vers TP

# Pyramide
PYRAMID_MAX        = 1

# Fallback testnet : marchés USDT-perp disponibles le plus souvent
FALLBACK_TESTNET = [
    "BTC/USDT:USDT", "ETH/USDT:USDT", "LTC/USDT:USDT",
    "BCH/USDT:USDT", "XRP/USDT:USDT"
]

# =======================
# EXCHANGE
# =======================
def create_exchange():
    ex = ccxt.bitget({
        "apiKey": API_KEY,
        "secret": API_SECRET,
        "password": PASSPHRASE,
        "enableRateLimit": True,
        "options": {"defaultType": "swap", "testnet": BITGET_TESTNET}
    })
    if BITGET_TESTNET:
        try:
            ex.set_sandbox_mode(True)
            print("[INFO] Bitget sandbox mode ON (testnet)")
        except Exception as e:
            print("[WARN] set_sandbox_mode not available:", e)
    else:
        print("[INFO] Bitget LIVE mode")
    return ex

# =======================
# DATA / INDICATEURS
# =======================
def fetch_ohlcv_df(ex, symbol, timeframe, limit=500):
    raw = ex.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    df = pd.DataFrame(raw, columns=["ts","open","high","low","close","vol"])
    df["ts"] = pd.to_datetime(df["ts"], unit="ms")
    df.set_index("ts", inplace=True)

    # BB 20x2 (blanche)
    bb_fast = BollingerBands(close=df["close"], window=20, window_dev=2)
    df["bb_fast_mid"]   = bb_fast.bollinger_mavg()
    df["bb_fast_upper"] = bb_fast.bollinger_hband()
    df["bb_fast_lower"] = bb_fast.bollinger_lband()

    # BB 80x2 (jaune)
    bb_slow = BollingerBands(close=df["close"], window=80, window_dev=2)
    df["bb_slow_mid"]   = bb_slow.bollinger_mavg()
    df["bb_slow_upper"] = bb_slow.bollinger_hband()
    df["bb_slow_lower"] = bb_slow.bollinger_lband()

    # ATR
    atr = AverageTrueRange(high=df["high"], low=df["low"], close=df["close"], window=ATR_WINDOW)
    df["atr"] = atr.average_true_range()
    return df

def touches_band(c, band_price, side="lower", tol_pct=0.0006):
    if band_price is None or np.isnan(band_price):
        return False
    tol = band_price * tol_pct
    if side == "lower":
        return c["low"] <= (band_price + tol)
    return c["high"] >= (band_price - tol)

# =======================
# UNIVERS (Top 100)
# =======================
def build_universe(ex):
    """
    Construit le Top-N swaps USDT (linéaires) :
    - Live : vrai Top N par volume 24h
    - Testnet : si pas de volumes, fallback vers une petite liste
    Respecte UNIVERSE_SIZE & MIN_VOLUME_USDT.
    """
    print("[UNIVERSE] building top by 24h volume...")

    # 1) Candidats : markets USDT-perp
    try:
        ex.load_markets()
        candidates = []
        for m in ex.markets.values():
            if (
                (m.get("type") == "swap" or m.get("swap") is True) and
                (m.get("linear") is True) and
                (m.get("settle") == "USDT") and
                (m.get("quote") == "USDT") and
                (m.get("symbol") is not None)
            ):
                candidates.append(m["symbol"])
    except Exception as e:
        print("[UNIVERSE] load_markets failed:", e)
        candidates = []

    # 2) Tickers => volumes
    rows = []
    try:
        tickers = ex.fetch_tickers(candidates if candidates else None)
        for s, t in tickers.items():
            if "/USDT" not in s and ":USDT" not in s:
                continue
            vol = t.get("quoteVolume") or t.get("baseVolume") or 0
            try:
                vol = float(vol or 0.0)
            except Exception:
                vol = 0.0
            if MIN_VOLUME_USDT <= 0 or vol >= MIN_VOLUME_USDT:
                rows.append((s, vol))
    except Exception as e:
        print("[UNIVERSE] fetch_tickers failed:", e)

    # 3) Si volumes, trier & prendre Top N
    if rows:
        df = pd.DataFrame(rows, columns=["symbol", "volume"]).sort_values("volume", ascending=False)
        universe = df.head(UNIVERSE_SIZE)["]()_
